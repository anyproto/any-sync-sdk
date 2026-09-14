package anysyncx

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	anystorev1 "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"go.uber.org/zap"
)

var storageLog = logger.NewNamed("anysyncx.storage")

var (
	// errStorageDeleting refuses to open a space store while its files
	// are being deleted.
	errStorageDeleting = errors.New("anysyncx: space storage is being deleted")
	// errStorageClosed refuses to open a space store after shutdown.
	errStorageClosed = errors.New("anysyncx: space storage closed")
)

// storeDrainTimeout bounds how long a delete waits for the other holders
// of a store to release it before closing it under them. Var: test seam.
var storeDrainTimeout = 10 * time.Second

// storageProvider opens any-sync's per-space SpaceStorage. any-sync uses
// any-store v1 internally; the SDK's own CRDT data lives in a separate
// any-store v2 DB (wired by other packages). Keeping the two cleanly
// separated dodges the v1↔v2 import-path conflict.
//
// The provider owns each store's lifetime. One DB is open per space,
// shared by every holder (the loaded space, a discovery-key derivation);
// each holder gets its own reference, and the DB closes when the last
// one is released. any-sync's own SpaceStorage.Close is a no-op, so
// without this no space DB would ever close: its files stay locked on
// Windows, and a clean shutdown leaves the durability sentinel dirty.
type storageProvider struct {
	root string

	mu     sync.Mutex
	stores map[string]*storeEntry
	closed bool

	// onSetChange fires (async-safe, may be nil) after the set of
	// stored spaces changes — create or delete. The p2p exchange uses
	// it to re-advertise this device's space list to LAN peers.
	onSetChange func()
}

// storeEntry is one space's DB, shared by its holders. Guarded by
// storageProvider.mu.
type storeEntry struct {
	id string
	db anystorev1.DB
	st spacestorage.SpaceStorage
	// refs counts the holders that have not released their reference.
	refs int
	// pending is non-nil while the entry opens, closes or is deleted,
	// and closes when that finishes; the entry is unusable until then.
	pending chan struct{}
	// deleting refuses new references; drained closes when the last
	// holder releases during a delete.
	deleting bool
	drained  chan struct{}
	dbClosed bool
}

// storeRef is one holder's SpaceStorage. Close releases the reference
// once; the DB closes with the last one.
type storeRef struct {
	spacestorage.SpaceStorage
	p    *storageProvider
	e    *storeEntry
	once sync.Once
}

func (r *storeRef) Close(context.Context) (err error) {
	r.once.Do(func() { err = r.p.release(r.e) })
	return err
}

// SetOnSetChange installs the space-set change callback. Call during
// app assembly, before any create/delete can happen.
func (s *storageProvider) SetOnSetChange(fn func()) { s.onSetChange = fn }

func (s *storageProvider) notifySetChange() {
	if s.onSetChange != nil {
		s.onSetChange()
	}
}

func newStorageProvider(root string) *storageProvider {
	return &storageProvider{
		root:   root,
		stores: make(map[string]*storeEntry),
	}
}

func (s *storageProvider) Init(_ *app.App) error {
	// Creates root too; sqlite rejects a temp_store_directory that
	// doesn't exist.
	return os.MkdirAll(s.tmpDir(), 0o755)
}

// tmpDir is where sqlite is told to put its temp files (statement
// sub-journals, transient indices). sqlite's built-in candidates
// ($TMPDIR, /var/tmp, /tmp, cwd) are all absent or unwritable inside
// an Android app sandbox, and without a writable temp dir any write
// tx that spills a savepoint sub-journal fails with SQLITE_IOERR
// ("disk I/O error") — which permanently broke remote new-tree pulls
// (SYN-82). Pointing temp_store_directory under our own root works on
// every platform.
func (s *storageProvider) tmpDir() string {
	return filepath.Join(s.root, "tmp")
}

// anyStoreConfig builds a fresh per-open config: any-store mutates the
// options map during Open, so it must not be shared across concurrent
// opens.
//
// Tuning mirrors anytype-heart's production space-store profile. A
// device runs many space DBs at once, so per-DB read connections are
// capped instead of scaling with NumCPU. Commit-time fsync is relaxed
// to checkpoint-time (synchronous=normal): losing the last local
// commits on power loss is acceptable because space state re-syncs,
// and the sentinel triggers a quick-check on unclean shutdown. The
// commit-path autocheckpoint threshold is raised and the real
// checkpoint work happens on idle — without the idle flush, busy
// stores never checkpoint and all data accumulates in the WAL.
func (s *storageProvider) anyStoreConfig() *anystorev1.Config {
	return &anystorev1.Config{
		ReadConnections: 8,
		// Process-global pool shared by every sqlite connection,
		// initialized once on the first open (later values are ignored).
		SQLiteGlobalPageCachePreallocateSizeBytes: 1 << 26,
		SQLiteConnectionOptions: map[string]string{
			// The value is interpolated into "PRAGMA %s = %s" verbatim;
			// paths must be single-quoted (embedded quotes doubled).
			"temp_store_directory": "'" + strings.ReplaceAll(s.tmpDir(), "'", "''") + "'",
			"synchronous":          "normal",
			"wal_autocheckpoint":   "10000",
		},
		Durability: anystorev1.DurabilityConfig{
			AutoFlush: true,
			IdleAfter: 20 * time.Second,
			FlushMode: anystorev1.FlushModeCheckpointPassive,
			Sentinel:  true,
		},
	}
}

func (s *storageProvider) Name() string { return spacestorage.CName }

// dbPath returns the path to a space's any-sync v1 DB. Distinct from
// the SDK's own v2 DB(s) regardless of StorageTopology.
func (s *storageProvider) dbPath(spaceId string) string {
	return filepath.Join(s.root, spaceId+".db")
}

func (s *storageProvider) WaitSpaceStorage(ctx context.Context, id string) (spacestorage.SpaceStorage, error) {
	return s.acquire(ctx, id, false, func() (anystorev1.DB, spacestorage.SpaceStorage, error) {
		path := s.dbPath(id)
		fi, statErr := os.Stat(path)
		if errors.Is(statErr, os.ErrNotExist) {
			return nil, nil, spacestorage.ErrSpaceStorageMissing
		}
		// A zero-byte db is an uninitialized husk, not a space: sqlite's
		// open-with-create mints one when a reader races an offload that
		// removes the file between its stat and its open. Report missing
		// so the pull bootstrap recreates the space; CreateSpaceStorage
		// sweeps the husk. No removal here — a legit creator's file is
		// also briefly zero-byte and must not be unlinked under it.
		if statErr == nil && fi.Size() == 0 {
			return nil, nil, spacestorage.ErrSpaceStorageMissing
		}
		db, err := anystorev1.Open(ctx, path, s.anyStoreConfig())
		if err != nil {
			return nil, nil, err
		}
		st, err := spacestorage.New(ctx, id, db)
		if err != nil {
			_ = db.Close()
			return nil, nil, err
		}
		return db, st, nil
	})
}

func (s *storageProvider) CreateSpaceStorage(ctx context.Context, payload spacestorage.SpaceStorageCreatePayload) (spacestorage.SpaceStorage, error) {
	id := payload.SpaceHeaderWithId.Id
	st, err := s.acquire(ctx, id, true, func() (anystorev1.DB, spacestorage.SpaceStorage, error) {
		path := s.dbPath(id)
		if fi, err := os.Stat(path); err == nil {
			if fi.Size() != 0 {
				return nil, nil, spacestorage.ErrSpaceStorageExists
			}
			// Zero-byte husk (see WaitSpaceStorage) — sweep it, with its
			// companions, and create fresh. Safe here: acquire runs one
			// open per space, so no creator can be behind the empty file.
			_ = os.Remove(path)
			for _, suffix := range []string{"-wal", "-shm", ".lock"} {
				_ = os.Remove(path + suffix)
			}
		}
		db, err := anystorev1.Open(ctx, path, s.anyStoreConfig())
		if err != nil {
			return nil, nil, err
		}
		st, err := spacestorage.Create(ctx, db, payload)
		if err != nil {
			_ = db.Close()
			_ = os.Remove(path)
			return nil, nil, err
		}
		return db, st, nil
	})
	if err != nil {
		return nil, err
	}
	s.notifySetChange()
	return st, nil
}

// acquire returns a new reference to the space's store, opening it with
// open when no holder has it. Callers racing on one space share a single
// open. With create set, an already-open store is ErrSpaceStorageExists.
func (s *storageProvider) acquire(ctx context.Context, id string, create bool, open func() (anystorev1.DB, spacestorage.SpaceStorage, error)) (spacestorage.SpaceStorage, error) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil, errStorageClosed
		}
		e := s.stores[id]
		switch {
		case e == nil:
			e = &storeEntry{id: id, pending: make(chan struct{})}
			s.stores[id] = e
			s.mu.Unlock()
			return s.openEntry(e, open)
		case e.deleting:
			s.mu.Unlock()
			return nil, errStorageDeleting
		case e.pending != nil:
			pending := e.pending
			s.mu.Unlock()
			select {
			case <-pending:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		case create:
			s.mu.Unlock()
			return nil, spacestorage.ErrSpaceStorageExists
		default:
			e.refs++
			s.mu.Unlock()
			return &storeRef{SpaceStorage: e.st, p: s, e: e}, nil
		}
	}
}

func (s *storageProvider) openEntry(e *storeEntry, open func() (anystorev1.DB, spacestorage.SpaceStorage, error)) (spacestorage.SpaceStorage, error) {
	db, st, err := open()
	s.mu.Lock()
	pending := e.pending
	e.pending = nil
	if err != nil {
		delete(s.stores, e.id)
	} else {
		e.db, e.st, e.refs = db, st, 1
	}
	s.mu.Unlock()
	close(pending)
	if err != nil {
		return nil, err
	}
	return &storeRef{SpaceStorage: st, p: s, e: e}, nil
}

// release drops one reference and closes the DB with the last one. A
// store being deleted is closed by the delete instead.
func (s *storageProvider) release(e *storeEntry) error {
	s.mu.Lock()
	e.refs--
	if e.refs > 0 || e.dbClosed {
		s.mu.Unlock()
		return nil
	}
	if e.deleting {
		if e.drained != nil {
			close(e.drained)
			e.drained = nil
		}
		s.mu.Unlock()
		return nil
	}
	e.pending = make(chan struct{})
	e.dbClosed = true
	s.mu.Unlock()

	err := e.db.Close()

	s.mu.Lock()
	if s.stores[e.id] == e {
		delete(s.stores, e.id)
	}
	pending := e.pending
	e.pending = nil
	s.mu.Unlock()
	close(pending)
	return err
}

// AllSpaceIds lists every space with local any-sync storage, derived
// from the `<root>/<spaceId>.db` layout. Used by the p2p SpaceExchangeV2
// handshake to tell local peers what this device can sync.
func (s *storageProvider) AllSpaceIds() []string {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return nil
	}
	var ids []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".db") {
			continue
		}
		ids = append(ids, strings.TrimSuffix(name, ".db"))
	}
	return ids
}

func (s *storageProvider) SpaceExists(id string) bool {
	_, err := os.Stat(s.dbPath(id))
	return err == nil
}

// DeleteSpaceStorageFile closes a space's any-sync storage and removes
// its on-disk DB file (`<root>/<spaceId>.db`). The bulk of a space's
// local footprint — every object tree — lives here, so this is the
// main disk-reclaim step of an offload. A missing file is not an error.
//
// Callers evict the space first (App.EvictSpace), which releases the
// space's own reference. Any other holder gets storeDrainTimeout to
// release before the DB closes under it: Windows cannot delete an open
// file, and an unreclaimed file would keep advertising the space.
func (s *storageProvider) DeleteSpaceStorageFile(ctx context.Context, id string) error {
	e, err := s.claimForDelete(ctx, id)
	if err != nil {
		return err
	}
	s.drainForDelete(e)

	var closeErr error
	s.mu.Lock()
	db := e.db
	closeDB := db != nil && !e.dbClosed
	e.dbClosed = true
	s.mu.Unlock()
	if closeDB {
		closeErr = db.Close()
	}

	removeErr := os.Remove(s.dbPath(id))
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	if removeErr == nil {
		// Companion files survive an unclean close: sqlite's -wal/-shm
		// and the durability sentinel. A stale WAL next to a later
		// re-created space DB would corrupt it, so sweep them with the
		// main file.
		for _, suffix := range []string{"-wal", "-shm", ".lock"} {
			_ = os.Remove(s.dbPath(id) + suffix)
		}
	}

	s.mu.Lock()
	if s.stores[id] == e {
		delete(s.stores, id)
	}
	pending := e.pending
	e.pending = nil
	s.mu.Unlock()
	close(pending)

	if removeErr != nil {
		return errors.Join(removeErr, closeErr)
	}
	s.notifySetChange()
	return nil
}

// claimForDelete marks the space's entry as deleting, creating a
// placeholder when nothing is open so no open races the removal.
func (s *storageProvider) claimForDelete(ctx context.Context, id string) (*storeEntry, error) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil, errStorageClosed
		}
		e := s.stores[id]
		switch {
		case e == nil:
			e = &storeEntry{id: id, pending: make(chan struct{}), deleting: true, dbClosed: true}
			s.stores[id] = e
			s.mu.Unlock()
			return e, nil
		case e.deleting:
			s.mu.Unlock()
			return nil, errStorageDeleting
		case e.pending != nil:
			pending := e.pending
			s.mu.Unlock()
			select {
			case <-pending:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		default:
			e.deleting = true
			e.pending = make(chan struct{})
			if e.refs > 0 {
				e.drained = make(chan struct{})
			}
			s.mu.Unlock()
			return e, nil
		}
	}
}

// drainForDelete waits up to storeDrainTimeout for the entry's holders
// to release it. The wait ignores the caller's ctx: closing a DB under
// a running statement is unsafe, and the bound is short.
func (s *storageProvider) drainForDelete(e *storeEntry) {
	s.mu.Lock()
	drained := e.drained
	s.mu.Unlock()
	if drained == nil {
		return
	}
	timer := time.NewTimer(storeDrainTimeout)
	defer timer.Stop()
	select {
	case <-drained:
	case <-timer.C:
		s.mu.Lock()
		refs := e.refs
		s.mu.Unlock()
		storageLog.Warn("delete: closing a space store that is still held",
			zap.String("spaceId", e.id), zap.Int("refs", refs))
	}
}

// closeAll closes every open store and refuses later opens. It runs
// after the any-sync app has closed: the p2p server and the pool close
// last and can open a store until then.
func (s *storageProvider) closeAll() {
	s.mu.Lock()
	s.closed = true
	entries := make([]*storeEntry, 0, len(s.stores))
	for _, e := range s.stores {
		entries = append(entries, e)
	}
	s.mu.Unlock()

	for _, e := range entries {
		s.mu.Lock()
		pending := e.pending
		s.mu.Unlock()
		if pending != nil {
			<-pending
		}
		s.mu.Lock()
		if e.dbClosed || e.db == nil {
			s.mu.Unlock()
			continue
		}
		e.dbClosed = true
		refs := e.refs
		if s.stores[e.id] == e {
			delete(s.stores, e.id)
		}
		s.mu.Unlock()
		if refs > 0 {
			storageLog.Warn("close: space store still held",
				zap.String("spaceId", e.id), zap.Int("refs", refs))
		}
		if err := e.db.Close(); err != nil {
			storageLog.Warn("close: space store", zap.String("spaceId", e.id), zap.Error(err))
		}
	}
}
