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
	"github.com/anyproto/any-sync/commonspace/spacestorage"
)

// storageProvider opens any-sync's per-space SpaceStorage. any-sync uses
// any-store v1 internally; the SDK's own CRDT data lives in a separate
// any-store v2 DB (wired by other packages). Keeping the two cleanly
// separated dodges the v1↔v2 import-path conflict.
type storageProvider struct {
	root string

	mu   sync.Mutex
	open map[string]spacestorage.SpaceStorage

	// onSetChange fires (async-safe, may be nil) after the set of
	// stored spaces changes — create or delete. The p2p exchange uses
	// it to re-advertise this device's space list to LAN peers.
	onSetChange func()
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
		root: root,
		open: make(map[string]spacestorage.SpaceStorage),
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
		ReadConnections: 4,
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
	s.mu.Lock()
	if st, ok := s.open[id]; ok {
		s.mu.Unlock()
		return st, nil
	}
	s.mu.Unlock()

	path := s.dbPath(id)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil, spacestorage.ErrSpaceStorageMissing
	}
	db, err := anystorev1.Open(ctx, path, s.anyStoreConfig())
	if err != nil {
		return nil, err
	}
	st, err := spacestorage.New(ctx, id, db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	s.mu.Lock()
	s.open[id] = st
	s.mu.Unlock()
	return st, nil
}

func (s *storageProvider) CreateSpaceStorage(ctx context.Context, payload spacestorage.SpaceStorageCreatePayload) (spacestorage.SpaceStorage, error) {
	id := payload.SpaceHeaderWithId.Id
	path := s.dbPath(id)
	if _, err := os.Stat(path); err == nil {
		return nil, spacestorage.ErrSpaceStorageExists
	}
	db, err := anystorev1.Open(ctx, path, s.anyStoreConfig())
	if err != nil {
		return nil, err
	}
	st, err := spacestorage.Create(ctx, db, payload)
	if err != nil {
		_ = db.Close()
		_ = os.Remove(path)
		return nil, err
	}
	s.mu.Lock()
	s.open[id] = st
	s.mu.Unlock()
	s.notifySetChange()
	return st, nil
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

// CloseSpaceStorage closes the provider's cached SpaceStorage for id
// and drops it from the open map. Best-effort and idempotent: a space
// that was never opened, or whose storage any-sync already closed on
// space eviction, is a no-op. Callers evict the space from the cache
// (App.EvictSpace) first so any-sync releases its own handle.
func (s *storageProvider) CloseSpaceStorage(ctx context.Context, id string) error {
	s.mu.Lock()
	st, ok := s.open[id]
	delete(s.open, id)
	s.mu.Unlock()
	if !ok {
		return nil
	}
	// any-sync may have already closed the underlying store on space
	// eviction; a second Close is tolerated and its error ignored.
	_ = st.Close(ctx)
	return nil
}

// DeleteSpaceStorageFile closes a space's any-sync storage and removes
// its on-disk DB file (`<root>/<spaceId>.db`). The bulk of a space's
// local footprint — every object tree — lives here, so this is the
// main disk-reclaim step of an offload. A missing file is not an error.
func (s *storageProvider) DeleteSpaceStorageFile(ctx context.Context, id string) error {
	_ = s.CloseSpaceStorage(ctx, id)
	if err := os.Remove(s.dbPath(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// Companion files survive an unclean close: sqlite's -wal/-shm and
	// the durability sentinel. A stale WAL next to a later re-created
	// space DB would corrupt it, so sweep them with the main file.
	for _, suffix := range []string{"-wal", "-shm", ".lock"} {
		_ = os.Remove(s.dbPath(id) + suffix)
	}
	s.notifySetChange()
	return nil
}
