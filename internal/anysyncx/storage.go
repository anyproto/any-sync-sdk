package anysyncx

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"

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
	return os.MkdirAll(s.root, 0o755)
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
	db, err := anystorev1.Open(ctx, path, nil)
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
	db, err := anystorev1.Open(ctx, path, nil)
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
// from the `<root>/<spaceId>.db` layout. Used by the p2p SpaceExchange
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
	s.notifySetChange()
	return nil
}
