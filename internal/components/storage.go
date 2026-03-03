package components

import (
	"context"
	"os"
	"path/filepath"

	anystore "github.com/anyproto/any-store"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/osfuncs"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
)

type StorageProvider struct {
	storagePath string
	storeConfig *anystore.Config
}

func NewStorageProvider(storagePath string, storeConfig *anystore.Config) *StorageProvider {
	return &StorageProvider{storagePath: storagePath, storeConfig: storeConfig}
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
	dbPath := s.dbPath(id)
	if _, err := osfuncs.Stat(dbPath); os.IsNotExist(err) {
		return nil, spacestorage.ErrSpaceStorageMissing
	}
	db, err := anystore.Open(ctx, dbPath, s.storeConfig)
	if err != nil {
		return nil, err
	}
	return spacestorage.New(ctx, id, db)
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
	return st, nil
}

func (s *StorageProvider) SpaceExists(id string) bool {
	_, err := osfuncs.Stat(s.dbPath(id))
	return err == nil
}
