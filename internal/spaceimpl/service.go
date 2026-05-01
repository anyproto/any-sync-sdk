package spaceimpl

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/util/crypto"

	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/internal/types"
	"github.com/anyproto/any-sync-sdk/space"
)

// replicationKeyFromSpaceId extracts the base36-encoded replication
// key suffix from a spaceId. Format: `<cid>.<repKey-base36>`.
// Returns 0 when the suffix is missing or unparseable — a defensive
// fallback that produces the legacy ".0" tail.
func replicationKeyFromSpaceId(spaceId string) uint64 {
	dot := strings.LastIndexByte(spaceId, '.')
	if dot <= 0 || dot == len(spaceId)-1 {
		return 0
	}
	v, err := strconv.ParseUint(spaceId[dot+1:], 36, 64)
	if err != nil {
		return 0
	}
	return v
}

// Service implements space.Service backed by the any-sync app and the
// tech-space's space-index. Held by the SDK for its lifetime.
//
// Per-space state — the spaceobjects.Store with its CRDT controllers
// and shared VersionAllocator — is created lazily on first access
// and held for the life of the SDK. The any-sync side of each space
// goes through the App's space cache and may TTL-evict; our store
// stays loaded so live writes don't pay the controller-build cost
// twice.
type Service struct {
	app *anysyncx.App
	tsp *techspace.Service
	db  anystore.DB

	// extTypes carry through to every per-space Store created by
	// storeFor — each type's handlers are applied alongside the
	// built-in catalog.
	extTypes []handler.Type

	mu     sync.Mutex
	stores map[string]*spaceobjects.Store
	allocs map[string]*object.VersionAllocator
}

// New returns a Service ready to be returned via SDK.Spaces(). The
// db argument is the shared SDK DB where CRDT collections live.
// extTypes must already have been validated by
// spaceobjects.ValidateExternalTypes at the SDK boundary.
func New(app *anysyncx.App, tsp *techspace.Service, db anystore.DB, extTypes []handler.Type) *Service {
	return &Service{
		app:      app,
		tsp:      tsp,
		db:       db,
		extTypes: extTypes,
		stores:   make(map[string]*spaceobjects.Store),
		allocs:   make(map[string]*object.VersionAllocator),
	}
}

// storeFor returns the per-space spaceobjects.Store, building it on
// first access. The allocator is also lazily created and shared
// across all objects in this space. First-touch also triggers a
// drain of the per-space _detached collection — picks up any
// changes whose missing shortIds landed during a previous session.
func (s *Service) storeFor(spaceId string) *spaceobjects.Store {
	s.mu.Lock()
	if st, ok := s.stores[spaceId]; ok {
		s.mu.Unlock()
		return st
	}
	alloc := object.NewVersionAllocator("")
	s.allocs[spaceId] = alloc
	st := spaceobjects.NewStore(s.app, s.db, s.app.AccountKeys().SignKey, spaceId, alloc, s.extTypes)
	s.stores[spaceId] = st
	s.mu.Unlock()
	// Kick the drainer once so prior-session parked rows whose
	// dependencies have since landed get picked up on first touch.
	st.NotifyDrainer(types.DataVersionPair{})
	return st
}

// Create creates a new regular space owned by the authenticated
// account, and writes its space-index entry into the tech space.
func (s *Service) Create(ctx context.Context, req space.CreateRequest) (space.Space, error) {
	keys := s.app.AccountKeys()
	if keys == nil {
		return nil, errors.New("spaceimpl: anysyncx app has no account keys")
	}

	readKey, err := crypto.NewRandomAES()
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: random read key: %w", err)
	}
	metadataKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: metadata key: %w", err)
	}

	spaceType := req.SpaceType
	if spaceType == "" {
		spaceType = space.SpaceTypeRegular
	}

	// Reuse the tech-space's replication key so all spaces for this
	// account land on the same coordinator shard. The tech space is
	// derived deterministically from the account key, so its repKey
	// is the canonical per-account value (per docs/03-space.md
	// § "Replication key").
	payload := spacepayloads.SpaceCreatePayload{
		SigningKey:     keys.SignKey,
		MasterKey:      keys.SignKey,
		ReadKey:        readKey,
		MetadataKey:    metadataKey,
		SpaceType:      spaceType,
		ReplicationKey: replicationKeyFromSpaceId(s.tsp.SpaceId()),
	}
	spaceId, err := s.app.SpaceService().CreateSpace(ctx, payload)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: create space: %w", err)
	}

	if _, err := s.tsp.Add(ctx, techspace.SpaceIndexRecord{
		Id:           spaceId,
		Type:         spaceType,
		Name:         req.Name,
		Description:  req.Description,
		IconCID:      req.IconCID,
		LocalStatus:  techspace.StatusActive,
		RemoteStatus: techspace.StatusActive,
	}); err != nil {
		return nil, fmt.Errorf("spaceimpl: write index entry: %w", err)
	}

	// Eagerly load once so the caller receives a live handle.
	if _, err := s.app.GetSpace(ctx, spaceId); err != nil {
		return nil, err
	}
	return newSpace(spaceId, s.app, s.tsp, s.storeFor(spaceId)), nil
}

// Get returns a handle to a known space.
func (s *Service) Get(ctx context.Context, spaceId string) (space.Space, error) {
	if _, ok := s.tsp.Get(ctx, spaceId); !ok {
		return nil, fmt.Errorf("spaceimpl: unknown space %q", spaceId)
	}
	return newSpace(spaceId, s.app, s.tsp, s.storeFor(spaceId)), nil
}

// List returns the space-index snapshot.
func (s *Service) List(ctx context.Context) ([]space.SpaceInfo, error) {
	rows := s.tsp.List(ctx)
	out := make([]space.SpaceInfo, 0, len(rows))
	for _, r := range rows {
		out = append(out, space.SpaceInfo{
			Id:          r.Id,
			Type:        r.Type,
			Name:        r.Name,
			Description: r.Description,
			IconCID:     r.IconCID,
			Status:      mapStatus(r.LocalStatus, r.RemoteStatus),
		})
	}
	return out, nil
}

// Delete is a soft-delete on the index.
func (s *Service) Delete(ctx context.Context, spaceId string) error {
	if _, err := s.tsp.SetLocalStatus(ctx, spaceId, techspace.StatusDeleted); err != nil {
		return fmt.Errorf("spaceimpl: mark deleted: %w", err)
	}
	return nil
}

// Subscribe is not yet wired — returns a no-op cancel.
func (s *Service) Subscribe(_ func(space.SpaceListEvent)) (cancel func()) {
	return func() {}
}

func (s *Service) Join(_ context.Context, _ space.JoinRequest) (space.Space, error) {
	return nil, errors.New("spaceimpl: Join not implemented")
}

func (s *Service) Derive(_ context.Context, _ space.DeriveRequest) (space.Space, error) {
	return nil, errors.New("spaceimpl: Derive not implemented")
}

func (s *Service) OneToOne(_ context.Context, _ string) (space.Space, error) {
	return nil, errors.New("spaceimpl: OneToOne not implemented")
}

// Close is a no-op — the space cache lives on the App and is closed
// during App.Close.
func (s *Service) Close(_ context.Context) error { return nil }

// SpaceRegistry implementation. Routes between the tech-space (which
// owns its own one-tree index) and regular spaces (which route
// through their per-space Store).

// GetTree resolves a tree by (spaceId, treeId). Tech-space lookups
// route to techspace.Service; everything else loads via the
// per-space Store, which builds the listener-bound *object.Object
// so inbound changes flow into the controller.
func (s *Service) GetTree(ctx context.Context, spaceId, treeId string) (objecttree.ObjectTree, error) {
	if spaceId == s.tsp.SpaceId() {
		return s.tsp.GetTree(ctx, spaceId, treeId)
	}
	store := s.storeFor(spaceId)
	obj, err := store.Get(ctx, treeId)
	if err != nil {
		return nil, err
	}
	tree := obj.Tree()
	if tree == nil {
		return nil, fmt.Errorf("spaceimpl: tree %s/%s has no bound any-sync tree", spaceId, treeId)
	}
	return tree, nil
}

// PutTree binds a remote-delivered tree payload. Tech-space's
// index tree is locally created, never put from the network — the
// tech-space's adapter rejects this, which is the correct behavior.
func (s *Service) PutTree(ctx context.Context, spaceId string, payload treestorage.TreeStorageCreatePayload) error {
	if spaceId == s.tsp.SpaceId() {
		return s.tsp.PutTree(ctx, spaceId, payload)
	}
	store := s.storeFor(spaceId)
	if _, err := store.PutTreeFromPayload(ctx, payload); err != nil {
		return err
	}
	return nil
}

// MarkTreeDeleted is the soft-delete hook fired when the settings
// tree announces a deletion. Drops the cached object so a future
// load reflects the deleted state. Tech-space defers to its own
// no-op adapter (the index tree itself is never marked deleted).
func (s *Service) MarkTreeDeleted(ctx context.Context, spaceId, treeId string) error {
	if spaceId == s.tsp.SpaceId() {
		return s.tsp.MarkTreeDeleted(ctx, spaceId, treeId)
	}
	s.storeFor(spaceId).Drop(treeId)
	return nil
}

// DeleteTree performs the per-tree cleanup the deletion-manager
// drives: marks the any-sync tree storage deleted via tree.Delete()
// and drops our cached *object.Object. Tech-space defers to its
// own adapter (the index tree is never explicitly deleted).
func (s *Service) DeleteTree(ctx context.Context, spaceId, treeId string) error {
	if spaceId == s.tsp.SpaceId() {
		return s.tsp.DeleteTree(ctx, spaceId, treeId)
	}
	return s.storeFor(spaceId).DeleteTree(ctx, treeId)
}

// Compile-time check that we satisfy the registry contract.
var _ anysyncx.SpaceRegistry = (*Service)(nil)

// mapStatus collapses (localStatus, remoteStatus) into the public
// space.Status enum.
func mapStatus(local, remote string) space.Status {
	switch {
	case local == techspace.StatusDeleted:
		return space.StatusDeleted
	case remote == techspace.StatusDeleted:
		return space.StatusRemoteDead
	case local == "" && remote == "":
		return space.StatusUnknown
	default:
		return space.StatusActive
	}
}
