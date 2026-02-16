package spaceimpl

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"
	"time"

	syncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/internal/components"
	"github.com/anyproto/any-sync-sdk/internal/kvimpl"
	"github.com/anyproto/any-sync-sdk/internal/objectimpl"

	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/commonspace/headsync/headstorage"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/objecttreebuilder"
	"github.com/anyproto/any-sync/commonspace/syncstatus"
)

// SpaceImpl implements syncsdk.Space with lazy initialization.
type SpaceImpl struct {
	id           string
	spaceService commonspace.SpaceService
	cfg          syncsdk.Config

	once    sync.Once
	initErr error
	cs      commonspace.Space

	kvOnce sync.Once
	kv     syncsdk.KeyValue

	mu       sync.Mutex
	objects  map[string]syncsdk.Object
	handlers []syncsdk.Handler
	closed   bool
}

func New(id string, spaceService commonspace.SpaceService, cfg syncsdk.Config) *SpaceImpl {
	return &SpaceImpl{
		id:           id,
		spaceService: spaceService,
		cfg:          cfg,
		objects:      make(map[string]syncsdk.Object),
	}
}

func (s *SpaceImpl) ensure(ctx context.Context) error {
	s.once.Do(func() {
		deps := commonspace.Deps{
			SyncStatus: syncstatus.NewNoOpSyncStatus(),
			TreeSyncer: components.NewTreeSyncer(),
		}
		cs, err := s.spaceService.NewSpace(ctx, s.id, deps)
		if err != nil {
			s.initErr = err
			return
		}
		if err = cs.Init(ctx); err != nil {
			s.initErr = err
			return
		}
		s.cs = cs
	})
	return s.initErr
}

func (s *SpaceImpl) ID() string {
	return s.id
}

func (s *SpaceImpl) GetObject(ctx context.Context, objectID string) (syncsdk.Object, error) {
	if err := s.ensure(ctx); err != nil {
		return nil, err
	}

	s.mu.Lock()
	if obj, ok := s.objects[objectID]; ok {
		s.mu.Unlock()
		return obj, nil
	}
	s.mu.Unlock()

	obj := objectimpl.NewObject(nil, s.id, s.cfg.SigningKey)
	tree, err := s.cs.TreeBuilder().BuildTree(ctx, objectID, objecttreebuilder.BuildTreeOpts{
		Listener: obj,
	})
	if err != nil {
		return nil, err
	}
	obj.SetTree(tree)

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.objects[objectID]; ok {
		tree.Close()
		return existing, nil
	}
	s.objects[objectID] = obj
	return obj, nil
}

func (s *SpaceImpl) CreateObject(ctx context.Context, opts ...syncsdk.ObjectCreateOption) (syncsdk.Object, error) {
	if err := s.ensure(ctx); err != nil {
		return nil, err
	}

	options := syncsdk.ResolveObjectCreateOptions(opts)

	seed := make([]byte, 32)
	if _, err := rand.Read(seed); err != nil {
		return nil, err
	}
	payload, err := s.cs.TreeBuilder().CreateTree(ctx, objecttree.ObjectTreeCreatePayload{
		PrivKey:    s.cfg.SigningKey,
		ChangeType: options.ChangeType,
		SpaceId:    s.id,
		Timestamp:  time.Now().Unix(),
		Seed:       seed,
	})
	if err != nil {
		return nil, err
	}

	obj := objectimpl.NewObject(nil, s.id, s.cfg.SigningKey)
	tree, err := s.cs.TreeBuilder().PutTree(ctx, payload, obj)
	if err != nil {
		return nil, err
	}
	obj.SetTree(tree)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[tree.Id()] = obj
	return obj, nil
}

func (s *SpaceImpl) DeriveObject(ctx context.Context, opts ...syncsdk.ObjectDeriveOption) (syncsdk.Object, error) {
	if err := s.ensure(ctx); err != nil {
		return nil, err
	}

	payload, err := s.cs.TreeBuilder().DeriveTree(ctx, objecttree.ObjectTreeDerivePayload{
		SpaceId: s.id,
	})
	if err != nil {
		return nil, err
	}

	obj := objectimpl.NewObject(nil, s.id, s.cfg.SigningKey)
	tree, err := s.cs.TreeBuilder().PutTree(ctx, payload, obj)
	if err != nil {
		if errors.Is(err, treestorage.ErrTreeExists) {
			// Tree already exists — open it instead.
			return s.GetObject(ctx, payload.RootRawChange.Id)
		}
		return nil, err
	}
	obj.SetTree(tree)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[tree.Id()] = obj
	return obj, nil
}

func (s *SpaceImpl) DeleteObject(ctx context.Context, objectID string) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	return s.cs.DeleteTree(ctx, objectID)
}

func (s *SpaceImpl) ListObjectIDs(ctx context.Context) ([]string, error) {
	if err := s.ensure(ctx); err != nil {
		return nil, err
	}
	// Build a set of internal IDs to exclude (ACL, settings, keyvalue).
	exclude := map[string]struct{}{
		s.cs.Acl().Id():                {},
		s.cs.KeyValue().DefaultStore().Id(): {},
	}
	stateStorage := s.cs.Storage().StateStorage()
	exclude[stateStorage.SettingsId()] = struct{}{}

	var ids []string
	err := s.cs.Storage().HeadStorage().IterateEntries(ctx, headstorage.IterOpts{}, func(entry headstorage.HeadsEntry) (bool, error) {
		if _, ok := exclude[entry.Id]; !ok {
			ids = append(ids, entry.Id)
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (s *SpaceImpl) KeyValue() syncsdk.KeyValue {
	s.kvOnce.Do(func() {
		if s.cs == nil {
			return
		}
		store := s.cs.KeyValue().DefaultStore()
		_ = store.Prepare()
		s.kv = kvimpl.NewKeyValue(store)
	})
	return s.kv
}

func (s *SpaceImpl) Subscribe(handler syncsdk.Handler) (unsubscribe func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers = append(s.handlers, handler)
	idx := len(s.handlers) - 1
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if idx < len(s.handlers) {
			s.handlers[idx] = nil
		}
	}
}

func (s *SpaceImpl) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	objects := make(map[string]syncsdk.Object, len(s.objects))
	for k, v := range s.objects {
		objects[k] = v
	}
	s.objects = nil
	s.mu.Unlock()

	for _, obj := range objects {
		_ = obj.Close()
	}

	if s.cs != nil {
		return s.cs.Close()
	}
	return nil
}

// CommonSpace returns the underlying commonspace.Space, initializing it lazily.
// This is used by other internal packages.
func (s *SpaceImpl) CommonSpace(ctx context.Context) (commonspace.Space, error) {
	if err := s.ensure(ctx); err != nil {
		return nil, err
	}
	return s.cs, nil
}
