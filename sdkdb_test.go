package anysyncsdk

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/config"
)

// oldFormatSDKDB puts a database in on-disk format 5 at <dir>/sdk.db and
// returns its path. testdata/format5.db.gz is a file any-store v2.4.3
// wrote: the collection "things" with five documents, each over the
// format's 997-byte leaf limit, and an index on "n".
func oldFormatSDKDB(t *testing.T) string {
	t.Helper()
	src, err := os.Open("testdata/format5.db.gz")
	require.NoError(t, err)
	defer src.Close()
	zr, err := gzip.NewReader(src)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "sdk.db")
	dst, err := os.Create(path)
	require.NoError(t, err)
	_, err = io.Copy(dst, zr)
	require.NoError(t, err)
	require.NoError(t, dst.Close())
	return path
}

// crashedSDKDB is oldFormatSDKDB as a process killed after a write leaves
// it: the document "inlog" is only in the file's log.
func crashedSDKDB(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	src := oldFormatSDKDB(t)
	db, err := anystore.Open(ctx, src, nil)
	require.NoError(t, err)
	coll, err := db.OpenCollection(ctx, "things")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"inlog","n":50,"text":"x"}`)))

	// The database file and its log as they are with the handle open.
	path := filepath.Join(t.TempDir(), "sdk.db")
	for _, suffix := range []string{"", "-wal"} {
		data, err := os.ReadFile(src + suffix)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path+suffix, data, 0o600))
	}
	require.NoError(t, db.Close())
	info, err := os.Stat(path + "-wal")
	require.NoError(t, err)
	require.NotZero(t, info.Size(), "the write is not in the log")
	return path
}

// requireCrashedThings checks crashedSDKDB's content in db.
func requireCrashedThings(t *testing.T, db anystore.DB) {
	t.Helper()
	requireThingsCount(t, db, 6)
	coll, err := db.OpenCollection(context.Background(), "things")
	require.NoError(t, err)
	doc, err := coll.FindId(context.Background(), "inlog")
	require.NoError(t, err)
	require.Equal(t, 50, doc.Value().GetInt("n"))
}

// requireThings checks the fixture's content in db.
func requireThings(t *testing.T, db anystore.DB) {
	t.Helper()
	requireThingsCount(t, db, 5)
}

// requireThingsCount checks the fixture's content in db, with count
// documents in the collection.
func requireThingsCount(t *testing.T, db anystore.DB, count int) {
	t.Helper()
	ctx := context.Background()
	coll, err := db.OpenCollection(ctx, "things")
	require.NoError(t, err)
	n, err := coll.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, count, n)
	doc, err := coll.FindId(ctx, "doc3")
	require.NoError(t, err)
	require.Equal(t, 3, doc.Value().GetInt("n"))
	require.Greater(t, len(doc.Value().GetString("text")), 997)
	indexes := coll.GetIndexes()
	require.Len(t, indexes, 1)
	require.Equal(t, []string{"n"}, indexes[0].Info().Fields)
}

func requireFormat(t *testing.T, db anystore.DB, want int) {
	t.Helper()
	format, err := anystore.SchemaFormat(db)
	require.NoError(t, err)
	require.Equal(t, want, format)
}

// dirNames lists the files beside sdk.db.
func dirNames(t *testing.T, path string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// An sdk.db in an older on-disk format is replaced by a copy in the latest
// one at open; the copy leaves none of its own files behind.
func TestOpenSDKDB_ReplacesOldFormat(t *testing.T) {
	ctx := context.Background()
	path := oldFormatSDKDB(t)

	db, err := openSDKDB(ctx, path)
	require.NoError(t, err)
	requireFormat(t, db, anystore.LatestSchemaFormat)
	requireThings(t, db)
	require.NoError(t, db.Close())
	require.ElementsMatch(t, []string{"sdk.db", "sdk.db-wal"}, dirNames(t, path))

	db, err = openSDKDB(ctx, path)
	require.NoError(t, err)
	requireFormat(t, db, anystore.LatestSchemaFormat)
	requireThings(t, db)
	require.NoError(t, db.Close())
}

// A new sdk.db is created in the latest format.
func TestOpenSDKDB_NewFile(t *testing.T) {
	db, err := openSDKDB(context.Background(), filepath.Join(t.TempDir(), "sdk.db"))
	require.NoError(t, err)
	requireFormat(t, db, anystore.LatestSchemaFormat)
	require.NoError(t, db.Close())
}

// A copy an interrupted open left can be older than the database: it is
// removed, never put in place.
func TestOpenSDKDB_RemovesStaleCopy(t *testing.T) {
	path := oldFormatSDKDB(t)
	require.NoError(t, os.WriteFile(path+sdkDBCopySuffix, []byte("stale"), 0o600))

	db, err := openSDKDB(context.Background(), path)
	require.NoError(t, err)
	requireFormat(t, db, anystore.LatestSchemaFormat)
	requireThings(t, db)
	require.NoError(t, db.Close())
	require.NoFileExists(t, path+sdkDBCopySuffix)
}

// A copy that cannot be made leaves the file as it is, and the open goes
// on in the old format.
func TestOpenSDKDB_KeepsFileWhenCopyFails(t *testing.T) {
	path := oldFormatSDKDB(t)
	// A file beside the copy's name that is not a copy's refuses it.
	blocker := path + sdkDBCopySuffix + "-wal"
	require.NoError(t, os.WriteFile(blocker, []byte("not a log"), 0o600))

	db, err := openSDKDB(context.Background(), path)
	require.NoError(t, err)
	requireFormat(t, db, 5)
	requireThings(t, db)
	require.NoError(t, db.Close())
	require.FileExists(t, blocker)
	require.NoFileExists(t, path+sdkDBCopySuffix)
}

// Documents written to the old-format file by this release are in the copy.
func TestOpenSDKDB_CopiesLaterWrites(t *testing.T) {
	ctx := context.Background()
	path := oldFormatSDKDB(t)

	db, err := anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	requireFormat(t, db, 5)
	coll, err := db.OpenCollection(ctx, "things")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"later","n":99,"text":"x"}`)))
	require.NoError(t, db.Close())

	db, err = openSDKDB(ctx, path)
	require.NoError(t, err)
	requireFormat(t, db, anystore.LatestSchemaFormat)
	requireThingsCount(t, db, 6)
	coll, err = db.OpenCollection(ctx, "things")
	require.NoError(t, err)
	doc, err := coll.FindId(ctx, "later")
	require.NoError(t, err)
	require.Equal(t, 99, doc.Value().GetInt("n"))
	require.NoError(t, db.Close())
}

// cancelAtCheck is a context that cancels itself at its nth Err or Done
// call, so a test can cancel at each point an open consults its context.
type cancelAtCheck struct {
	context.Context
	cancel context.CancelFunc
	left   atomic.Int64
}

func newCancelAtCheck(n int64) *cancelAtCheck {
	ctx, cancel := context.WithCancel(context.Background())
	c := &cancelAtCheck{Context: ctx, cancel: cancel}
	c.left.Store(n)
	return c
}

func (c *cancelAtCheck) check() {
	if c.left.Add(-1) == 0 {
		c.cancel()
	}
}

func (c *cancelAtCheck) Err() error {
	c.check()
	return c.Context.Err()
}

func (c *cancelAtCheck) Done() <-chan struct{} {
	c.check()
	return c.Context.Done()
}

// A replace that fails after the source is closed and before the rename
// leaves the old file: the open goes on with it, and the next open moves it.
func TestOpenSDKDB_KeepsFileWhenRenameFails(t *testing.T) {
	ctx := context.Background()
	path := crashedSDKDB(t)
	renameFile = func(string, string) error { return errors.New("rename refused") }
	t.Cleanup(func() { renameFile = os.Rename })

	db, err := openSDKDB(ctx, path)
	require.NoError(t, err)
	requireFormat(t, db, 5)
	requireCrashedThings(t, db)
	require.NoError(t, db.Close())
	require.NoFileExists(t, path+sdkDBCopySuffix)

	renameFile = os.Rename
	db, err = openSDKDB(ctx, path)
	require.NoError(t, err)
	requireFormat(t, db, anystore.LatestSchemaFormat)
	requireCrashedThings(t, db)
	require.NoError(t, db.Close())
}

// A replace that fails after the rename has put the copy in place: the open
// goes on with the copy.
func TestOpenSDKDB_OpensCopyWhenSyncAfterRenameFails(t *testing.T) {
	path := crashedSDKDB(t)
	calls := 0
	syncDir = func(p string) error {
		if calls++; calls == 2 {
			return errors.New("sync refused")
		}
		return syncDirOf(p)
	}
	t.Cleanup(func() { syncDir = syncDirOf })

	db, err := openSDKDB(context.Background(), path)
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	requireFormat(t, db, anystore.LatestSchemaFormat)
	requireCrashedThings(t, db)
	require.NoError(t, db.Close())
	require.NoFileExists(t, path+sdkDBCopySuffix)
}

// An open cancelled at any point fails with the old file and its log, or
// the complete copy, in place, and none of the copy's own files left; the
// next open finishes the move.
func TestOpenSDKDB_Cancelled(t *testing.T) {
	const maxChecks = 100000
	var failed, replaced int
	n := int64(1)
	for ; n <= maxChecks; n++ {
		path := crashedSDKDB(t)
		ctx := newCancelAtCheck(n)
		db, err := openSDKDB(ctx, path)
		if err == nil {
			// Either n is past the last check, or the cancel came when
			// nothing was left to stop.
			requireFormat(t, db, anystore.LatestSchemaFormat)
			requireCrashedThings(t, db)
			require.NoError(t, db.Close())
			if ctx.left.Load() > 0 {
				break
			}
			continue
		}
		failed++
		require.ErrorIs(t, err, context.Canceled, "cancel at check %d", n)
		for _, name := range dirNames(t, path) {
			require.Contains(t, []string{"sdk.db", "sdk.db-wal", "sdk.db-wal-shm", "sdk.db.lock"}, name, "cancel at check %d", n)
		}

		plain, err := anystore.Open(context.Background(), path, nil)
		require.NoError(t, err, "cancel at check %d", n)
		format, err := anystore.SchemaFormat(plain)
		require.NoError(t, err)
		if format == anystore.LatestSchemaFormat {
			replaced++
		} else {
			require.Equal(t, 5, format)
		}
		requireCrashedThings(t, plain)
		require.NoError(t, plain.Close())

		db, err = openSDKDB(context.Background(), path)
		require.NoError(t, err, "cancel at check %d", n)
		requireFormat(t, db, anystore.LatestSchemaFormat)
		requireCrashedThings(t, db)
		require.NoError(t, db.Close())
	}
	require.Less(t, n, int64(maxChecks), "the open never ran out of context checks")
	// The loop has to reach into the copy, not stop at the first open.
	require.Greater(t, failed, 10)
	t.Logf("%d context checks, %d cancelled opens, %d of them after the copy was in place", n-1, failed, replaced)
}

// failingProvider fails the key load, which stops Open before the any-sync
// app starts.
type failingProvider struct{ err error }

func (p failingProvider) AccountKey(context.Context) ([]byte, error) { return nil, p.err }
func (p failingProvider) DeviceKey(context.Context) ([]byte, error)  { return nil, p.err }

// Open moves an existing sdk.db onto the latest format before it creates
// the any-sync app, and closes the file when the app then fails.
func TestOpen_ExistingSDKDBOpensBeforeApp(t *testing.T) {
	path := oldFormatSDKDB(t)
	dataDir := filepath.Dir(path)
	errKeys := errors.New("no keys")

	_, err := Open(context.Background(), config.Config{Storage: config.Storage{DataDir: dataDir}}, failingProvider{err: errKeys})
	require.ErrorIs(t, err, errKeys)
	require.Empty(t, openFilesUnder(t, dataDir))

	db, err := anystore.Open(context.Background(), path, nil)
	require.NoError(t, err)
	requireFormat(t, db, anystore.LatestSchemaFormat)
	requireThings(t, db)
	require.NoError(t, db.Close())
}
