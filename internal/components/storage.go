package components

import (
	"context"
	"os"
	"path/filepath"

	anystore "github.com/anyproto/any-store"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
)

type StorageProvider struct {
	storagePath string
}

func NewStorageProvider(storagePath string) *StorageProvider {
	return &StorageProvider{storagePath: storagePath}
}

func (s *StorageProvider) Init(_ *app.App) error {
	return os.MkdirAll(s.storagePath, 0o755)
}

func (s *StorageProvider) Name() string {
	return spacestorage.CName
}

func (s *StorageProvider) dbPath(spaceId string) string {
	return filepath.Join(s.storagePath, spaceId+".db")
}

func (s *StorageProvider) WaitSpaceStorage(ctx context.Context, id string) (spacestorage.SpaceStorage, error) {
	dbPath := s.dbPath(id)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, spacestorage.ErrSpaceStorageMissing
	}
	db, err := anystore.Open(ctx, dbPath, nil)
	if err != nil {
		return nil, err
	}
	return spacestorage.New(ctx, id, db)
}

func (s *StorageProvider) CreateSpaceStorage(ctx context.Context, payload spacestorage.SpaceStorageCreatePayload) (spacestorage.SpaceStorage, error) {
	spaceId := payload.SpaceHeaderWithId.Id
	dbPath := s.dbPath(spaceId)
	if _, err := os.Stat(dbPath); err == nil {
		return nil, spacestorage.ErrSpaceStorageExists
	}
	db, err := anystore.Open(ctx, dbPath, nil)
	if err != nil {
		return nil, err
	}
	st, err := spacestorage.Create(ctx, db, payload)
	if err != nil {
		_ = db.Close()
		_ = os.Remove(dbPath)
		return nil, err
	}
	return st, nil
}

func (s *StorageProvider) SpaceExists(id string) bool {
	_, err := os.Stat(s.dbPath(id))
	return err == nil
}
