package anysyncx

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
	return st, nil
}

func (s *storageProvider) SpaceExists(id string) bool {
	_, err := os.Stat(s.dbPath(id))
	return err == nil
}
