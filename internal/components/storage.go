package components

import (
	"context"
	"os"
	"path/filepath"
	"sync"

	anystore "github.com/anyproto/any-store"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/osfuncs"
)

type StorageProvider struct {
	storagePath string
	storeConfig *anystore.Config
	mu          sync.Mutex
	open        map[string]spacestorage.SpaceStorage
}

func NewStorageProvider(storagePath string, storeConfig *anystore.Config) *StorageProvider {
	return &StorageProvider{
		storagePath: storagePath,
		storeConfig: storeConfig,
		open:        make(map[string]spacestorage.SpaceStorage),
	}
}

func (s *StorageProvider) Init(_ *app.App) error {
	return osfuncs.MkdirAll(s.storagePath, 0o755)
}

func (s *StorageProvider) Name() string {
	return spacestorage.CName
}

func (s *StorageProvider) dbPath(spaceId string) string {
	return filepath.Join(s.storagePath, spaceId+".db")
}

func (s *StorageProvider) WaitSpaceStorage(ctx context.Context, id string) (spacestorage.SpaceStorage, error) {
	s.mu.Lock()
	if st, ok := s.open[id]; ok {
		s.mu.Unlock()
		return st, nil
	}
	s.mu.Unlock()
	dbPath := s.dbPath(id)
	if _, err := osfuncs.Stat(dbPath); os.IsNotExist(err) {
		return nil, spacestorage.ErrSpaceStorageMissing
	}
	db, err := anystore.Open(ctx, dbPath, s.storeConfig)
	if err != nil {
		return nil, err
	}
	st, err := spacestorage.New(ctx, id, db)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.open[id] = st
	s.mu.Unlock()
	return st, nil
}

func (s *StorageProvider) CreateSpaceStorage(ctx context.Context, payload spacestorage.SpaceStorageCreatePayload) (spacestorage.SpaceStorage, error) {
	spaceId := payload.SpaceHeaderWithId.Id
	dbPath := s.dbPath(spaceId)
	if _, err := osfuncs.Stat(dbPath); err == nil {
		return nil, spacestorage.ErrSpaceStorageExists
	}
	db, err := anystore.Open(ctx, dbPath, s.storeConfig)
	if err != nil {
		return nil, err
	}
	st, err := spacestorage.Create(ctx, db, payload)
	if err != nil {
		_ = db.Close()
		_ = osfuncs.Remove(dbPath)
		return nil, err
	}
	s.mu.Lock()
	s.open[spaceId] = st
	s.mu.Unlock()
	return st, nil
}

func (s *StorageProvider) SpaceExists(id string) bool {
	_, err := osfuncs.Stat(s.dbPath(id))
	return err == nil
}
