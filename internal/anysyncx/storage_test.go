package anysyncx

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	anystorev1 "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/go-sqlite"
	"github.com/anyproto/go-sqlite/sqlitex"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Opening a space store must point sqlite's temp directory at a dir
// under the provider root. Android has no usable entry in sqlite's
// built-in temp-dir candidates, and without one any write tx that
// spills a savepoint sub-journal dies with SQLITE_IOERR — remote
// new-tree pulls never persist (SYN-82).
func TestStorageProviderSqliteTempDir(t *testing.T) {
	root := filepath.Join(t.TempDir(), "anysync")
	p := newStorageProvider(root)
	require.NoError(t, p.Init(nil))
	require.DirExists(t, p.tmpDir())

	db, err := anystorev1.Open(context.Background(), filepath.Join(root, "test.db"), p.anyStoreConfig())
	require.NoError(t, err)
	defer db.Close()

	// temp_store_directory sets the process-global sqlite3_temp_directory,
	// so any fresh connection reflects what the open above configured.
	conn, err := sqlite.OpenConn(":memory:")
	require.NoError(t, err)
	defer conn.Close()
	var got string
	require.NoError(t, sqlitex.ExecuteTransient(conn, "PRAGMA temp_store_directory", &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			got = stmt.ColumnText(0)
			return nil
		},
	}))
	require.Equal(t, p.tmpDir(), got)
}

// stubSpaceStorage stands in for any-sync's space storage: the provider
// only ever hands it back, so nothing past Id is called.
type stubSpaceStorage struct {
	spacestorage.SpaceStorage
	id string
}

func (s stubSpaceStorage) Id() string { return s.id }

// countingOpen opens a real any-store v1 DB at the space's path and
// counts how often the provider asked for it.
func countingOpen(t *testing.T, p *storageProvider, id string, opens *atomic.Int32) func() (anystorev1.DB, spacestorage.SpaceStorage, error) {
	t.Helper()
	return func() (anystorev1.DB, spacestorage.SpaceStorage, error) {
		opens.Add(1)
		db, err := anystorev1.Open(context.Background(), p.dbPath(id), p.anyStoreConfig())
		if err != nil {
			return nil, nil, err
		}
		return db, stubSpaceStorage{id: id}, nil
	}
}

func newTestProvider(t *testing.T) *storageProvider {
	t.Helper()
	p := newStorageProvider(filepath.Join(t.TempDir(), "anysync"))
	require.NoError(t, p.Init(nil))
	return p
}

func refDB(st spacestorage.SpaceStorage) anystorev1.DB { return st.(*storeRef).e.db }

func requireDBClosed(t *testing.T, db anystorev1.DB) {
	t.Helper()
	_, err := db.ReadTx(context.Background())
	require.ErrorIs(t, err, anystorev1.ErrDBIsClosed)
}

func requireDBOpen(t *testing.T, db anystorev1.DB) {
	t.Helper()
	tx, err := db.ReadTx(context.Background())
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

// Holders racing on one space share a single DB; it closes with the last
// reference, and the next holder opens a fresh one.
func TestStorageProviderSharesOneStore(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()
	const id = "space.shared"
	var opens atomic.Int32
	open := countingOpen(t, p, id, &opens)

	const holders = 8
	refs := make([]spacestorage.SpaceStorage, holders)
	var wg sync.WaitGroup
	for i := range refs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st, err := p.acquire(ctx, id, false, open)
			assert.NoError(t, err)
			refs[i] = st
		}()
	}
	wg.Wait()
	require.EqualValues(t, 1, opens.Load())
	db := refDB(refs[0])
	for _, st := range refs[1:] {
		require.Same(t, db, refDB(st))
	}

	// A second Close on the same reference releases nothing.
	for range 2 {
		require.NoError(t, refs[0].Close(ctx))
	}
	for _, st := range refs[1 : holders-1] {
		require.NoError(t, st.Close(ctx))
	}
	requireDBOpen(t, db)

	require.NoError(t, refs[holders-1].Close(ctx))
	requireDBClosed(t, db)
	p.mu.Lock()
	require.Empty(t, p.stores)
	p.mu.Unlock()

	st, err := p.acquire(ctx, id, false, open)
	require.NoError(t, err)
	require.EqualValues(t, 2, opens.Load())
	requireDBOpen(t, refDB(st))
	require.NoError(t, st.Close(ctx))
}

// A create over an open store is refused without touching it.
func TestStorageProviderCreateOverOpenStore(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()
	const id = "space.exists"
	var opens atomic.Int32
	st, err := p.acquire(ctx, id, false, countingOpen(t, p, id, &opens))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close(ctx) })

	_, err = p.acquire(ctx, id, true, countingOpen(t, p, id, &opens))
	require.ErrorIs(t, err, spacestorage.ErrSpaceStorageExists)
	require.EqualValues(t, 1, opens.Load())
	requireDBOpen(t, refDB(st))
}

// A delete waits for the remaining holder, refuses new ones meanwhile,
// then closes the DB and removes its files.
func TestStorageProviderDeleteWaitsForHolders(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()
	const id = "space.delete"
	var opens atomic.Int32
	open := countingOpen(t, p, id, &opens)
	st, err := p.acquire(ctx, id, false, open)
	require.NoError(t, err)
	db := refDB(st)

	deleted := make(chan error, 1)
	go func() { deleted <- p.DeleteSpaceStorageFile(ctx, id) }()
	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		e := p.stores[id]
		return e != nil && e.deleting
	}, 5*time.Second, time.Millisecond)
	_, err = p.acquire(ctx, id, false, open)
	require.ErrorIs(t, err, errStorageDeleting)
	select {
	case err := <-deleted:
		t.Fatalf("delete finished under a holder: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, st.Close(ctx))
	select {
	case err := <-deleted:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("delete did not finish after the holder released")
	}
	requireDBClosed(t, db)
	require.NoFileExists(t, p.dbPath(id))
	require.False(t, p.SpaceExists(id))

	_, err = p.acquire(ctx, id, false, func() (anystorev1.DB, spacestorage.SpaceStorage, error) {
		return nil, nil, spacestorage.ErrSpaceStorageMissing
	})
	require.ErrorIs(t, err, spacestorage.ErrSpaceStorageMissing)
}

// A holder that never releases cannot block a delete past the drain
// bound; its late Close is harmless.
func TestStorageProviderDeleteClosesHeldStoreAfterTimeout(t *testing.T) {
	prev := storeDrainTimeout
	storeDrainTimeout = 50 * time.Millisecond
	t.Cleanup(func() { storeDrainTimeout = prev })

	p := newTestProvider(t)
	ctx := context.Background()
	const id = "space.leaked"
	var opens atomic.Int32
	st, err := p.acquire(ctx, id, false, countingOpen(t, p, id, &opens))
	require.NoError(t, err)
	db := refDB(st)

	require.NoError(t, p.DeleteSpaceStorageFile(ctx, id))
	requireDBClosed(t, db)
	require.NoFileExists(t, p.dbPath(id))
	require.NoError(t, st.Close(ctx))
}

// Deleting a space nobody holds removes its files; deleting a missing
// one is not an error.
func TestStorageProviderDeleteUnopened(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()
	const id = "space.cold"
	var opens atomic.Int32
	st, err := p.acquire(ctx, id, false, countingOpen(t, p, id, &opens))
	require.NoError(t, err)
	require.NoError(t, st.Close(ctx))
	require.FileExists(t, p.dbPath(id))

	require.NoError(t, p.DeleteSpaceStorageFile(ctx, id))
	require.NoFileExists(t, p.dbPath(id))
	require.NoError(t, p.DeleteSpaceStorageFile(ctx, id))
	p.mu.Lock()
	require.Empty(t, p.stores)
	p.mu.Unlock()
}

// closeAll closes stores still held, and nothing opens after it.
func TestStorageProviderCloseAll(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()
	const id = "space.shutdown"
	var opens atomic.Int32
	open := countingOpen(t, p, id, &opens)
	st, err := p.acquire(ctx, id, false, open)
	require.NoError(t, err)
	db := refDB(st)

	p.closeAll()
	requireDBClosed(t, db)
	_, err = p.acquire(ctx, id, false, open)
	require.ErrorIs(t, err, errStorageClosed)
	require.NoError(t, st.Close(ctx))
	require.EqualValues(t, 1, opens.Load())
}

// A failed space load releases the references it took; a loaded space
// keeps only its own.
func TestStorageProviderLoadScope(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()
	const id = "space.load"
	var opens atomic.Int32
	open := countingOpen(t, p, id, &opens)

	loadCtx, scope := withLoadScope(ctx)
	dropped, err := p.acquire(loadCtx, id, false, open)
	require.NoError(t, err)
	_, err = p.acquire(loadCtx, id, false, open)
	require.NoError(t, err)
	db := refDB(dropped)
	scope.releaseExcept(ctx, nil)
	requireDBClosed(t, db)

	loadCtx, scope = withLoadScope(ctx)
	_, err = p.acquire(loadCtx, id, false, open)
	require.NoError(t, err)
	kept, err := p.acquire(loadCtx, id, false, open)
	require.NoError(t, err)
	scope.releaseExcept(ctx, kept)
	requireDBOpen(t, refDB(kept))
	require.NoError(t, kept.Close(ctx))
	requireDBClosed(t, refDB(kept))
}
