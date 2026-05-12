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
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
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
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
	"github.com/anyproto/any-sync-sdk/space"
)

// normalizeSpaceType applies the SpaceType allow-list before
// stamping the value into an immutable space header. The
// any-sync-coordinator rejects anything outside this set with
// "unknown space type: <value>", causing periodic headsync to fail
// forever — and since the type is content-addressable into the
// header, you can't fix the space after the fact. Catch it here.
//
// Empty defaults to SpaceTypeRegular, mirroring anytype-heart and
// matching the coordinator's "" → SpaceTypeRegular treatment.
func normalizeSpaceType(t string) (string, error) {
	switch t {
	case "":
		return space.SpaceTypeRegular, nil
	case space.SpaceTypeRegular, space.SpaceTypeChat, space.SpaceTypeOneToOne:
		return t, nil
	default:
		return "", fmt.Errorf("spaceimpl: unsupported SpaceType %q (allowed: %s, %s, %s)",
			t, space.SpaceTypeRegular, space.SpaceTypeChat, space.SpaceTypeOneToOne)
	}
}

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
	app     *anysyncx.App
	tsp     *techspace.Service
	indexer space.Indexer
	db      anystore.DB

	// extTypes carry through to every per-space Store created by
	// storeFor — each type's handlers are applied alongside the
	// built-in catalog.
	extTypes []handler.Type

	mu     sync.Mutex
	stores map[string]*spaceobjects.Store
	allocs map[string]*object.VersionAllocator

	// spaceIndexIds caches the deterministic spaceIndex object id per
	// spaceId so SetMetadata / Info-side reads don't re-derive on every
	// call. Populated by ensureSpaceIndexWiring.
	spaceIndexIds map[string]string

	// spaceIndexWatchers holds one watcher per loaded spaceId; the
	// watcher subscribes to the spaceIndex object's properties dataset
	// and forwards converged state into the Indexer. Lifetime: until
	// SDK.Close (mirrors the members-watcher sticky lifecycle).
	spaceIndexWatchers map[string]*spaceIndexWatcher

	// watchers tracks every active members poller across all loaded
	// spaceImpls so SDK shutdown can drain them deterministically.
	watchers watcherRegistry
}

// New returns a Service ready to be returned via SDK.Spaces(). The
// db argument is the shared SDK DB where CRDT collections live.
// extTypes must already have been validated by
// spaceobjects.ValidateExternalTypes at the SDK boundary. indexer is
// the seam through which the tech-space mirrors converged in-space
// spaceIndex state into its rows; usually tsp itself (which
// satisfies space.Indexer).
func New(app *anysyncx.App, tsp *techspace.Service, indexer space.Indexer, db anystore.DB, extTypes []handler.Type) *Service {
	return &Service{
		app:                app,
		tsp:                tsp,
		indexer:            indexer,
		db:                 db,
		extTypes:           extTypes,
		stores:             make(map[string]*spaceobjects.Store),
		allocs:             make(map[string]*object.VersionAllocator),
		spaceIndexIds:      make(map[string]string),
		spaceIndexWatchers: make(map[string]*spaceIndexWatcher),
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

// StoreFor returns the per-space spaceobjects.Store, building it on
// first access. Exposed for callers outside the package (e.g. the
// spacesync catch-up driver invoked from SDK.Open) that need the
// store handle without going through Get / Create / Derive.
func (s *Service) StoreFor(spaceId string) *spaceobjects.Store { return s.storeFor(spaceId) }

// ensureSpaceIndexWiring is idempotent per spaceId: on first call it
// derives the deterministic spaceIndex object id, caches it, and
// spawns a spaceIndexWatcher that mirrors the converged in-space
// state into the tech-space row through the Indexer. Subsequent
// calls for the same spaceId return the cached id without rewiring.
//
// Called from every load path (Create / Get / Derive / OneToOne).
// Failures to derive the id are returned as errors so the caller
// can surface them — without an id, neither SetMetadata nor the
// mirror can target the right tree.
func (s *Service) ensureSpaceIndexWiring(ctx context.Context, spaceId string) (string, error) {
	s.mu.Lock()
	if id, ok := s.spaceIndexIds[spaceId]; ok {
		s.mu.Unlock()
		return id, nil
	}
	s.mu.Unlock()

	handle, err := s.app.GetSpace(ctx, spaceId)
	if err != nil {
		return "", fmt.Errorf("spaceimpl: ensureSpaceIndexWiring: %w", err)
	}
	derivePayload := objecttree.ObjectTreeDerivePayload{
		ChangePayload: []byte(spaceindex.WellKnownDeriveSeed),
		SpaceId:       spaceId,
		IsEncrypted:   true,
	}
	storagePayload, err := handle.Inner().TreeBuilder().DeriveTree(ctx, derivePayload)
	if err != nil {
		return "", fmt.Errorf("spaceimpl: derive spaceIndex tree id: %w", err)
	}
	objectId := storagePayload.RootRawChange.Id

	s.mu.Lock()
	if existing, ok := s.spaceIndexIds[spaceId]; ok {
		s.mu.Unlock()
		return existing, nil
	}
	s.spaceIndexIds[spaceId] = objectId
	store := s.stores[spaceId]
	s.mu.Unlock()

	// Spawn the watcher outside the lock — newSpaceIndexWatcher does
	// the initial reconcile read which may take any-store latency.
	if store != nil && s.indexer != nil {
		w := newSpaceIndexWatcher(ctx, store, s.indexer, spaceId, objectId)
		s.mu.Lock()
		if _, dup := s.spaceIndexWatchers[spaceId]; dup {
			// Race: another goroutine wired concurrently. Stop the
			// extra one so we don't leak. Cheap — the duplicate's
			// reconcile already ran and is harmless.
			s.mu.Unlock()
			w.stop()
		} else {
			s.spaceIndexWatchers[spaceId] = w
			s.watchers.register(w)
			s.mu.Unlock()
		}
	}
	return objectId, nil
}

// spaceIndexObjectIdFor returns the cached spaceIndex object id for
// spaceId, deriving on demand if ensureSpaceIndexWiring hasn't run
// yet. Used by SetMetadata to address the spaceIndex tree without
// requiring the caller to have hit a load path that wires first.
func (s *Service) spaceIndexObjectIdFor(ctx context.Context, spaceId string) (string, error) {
	s.mu.Lock()
	id, ok := s.spaceIndexIds[spaceId]
	s.mu.Unlock()
	if ok {
		return id, nil
	}
	return s.ensureSpaceIndexWiring(ctx, spaceId)
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

	spaceType, err := normalizeSpaceType(req.SpaceType)
	if err != nil {
		return nil, err
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
	store := s.storeFor(spaceId)
	if _, err := s.ensureSpaceIndexWiring(ctx, spaceId); err != nil {
		return nil, err
	}
	if err := s.seedSpaceIndexOnCreate(ctx, store, spaceId, req, spaceType); err != nil {
		return nil, fmt.Errorf("spaceimpl: seed spaceIndex: %w", err)
	}
	return newSpace(spaceId, s.app, s.tsp, store, s), nil
}

// Get returns a handle to a known space. Eagerly loads the any-sync
// side via the cache so periodic headsync / syncacl start running
// from this point — without this, Get only consults the tech-space
// index and the per-space components stay dormant until the first
// Modify or per-object Query (the cold paths that go through
// store.Get(objectId) → app.GetSpace). QueryObjects and members reads
// satisfied from local storage would otherwise leave a peer unable
// to receive pushed changes for an arbitrarily long stretch.
//
// Mirrors the eager-load that Create / Derive / OneToOne already do.
func (s *Service) Get(ctx context.Context, spaceId string) (space.Space, error) {
	if _, ok := s.tsp.Get(ctx, spaceId); !ok {
		return nil, fmt.Errorf("spaceimpl: unknown space %q", spaceId)
	}
	if _, err := s.app.GetSpace(ctx, spaceId); err != nil {
		return nil, fmt.Errorf("spaceimpl: load space %q: %w", spaceId, err)
	}
	store := s.storeFor(spaceId)
	if _, err := s.ensureSpaceIndexWiring(ctx, spaceId); err != nil {
		return nil, err
	}
	sp := newSpace(spaceId, s.app, s.tsp, store, s)
	// Background lazy-seed for legacy / never-seeded spaces: only the
	// owner can write the initial spaceIndex properties, and non-
	// owners skip silently (the owner's eventual write propagates).
	go sp.maybeLazySeedSpaceIndex()
	return sp, nil
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

// Derive creates (or rehydrates) a deterministic space from req.Seed.
// The same (account, seed) pair always produces the same spaceId, so
// repeat calls are idempotent and just return the existing space if
// it already exists locally.
func (s *Service) Derive(ctx context.Context, req space.DeriveRequest) (space.Space, error) {
	keys := s.app.AccountKeys()
	if keys == nil {
		return nil, errors.New("spaceimpl: anysyncx app has no account keys")
	}
	payload := spacepayloads.SpaceDerivePayload{
		SigningKey:   keys.SignKey,
		MasterKey:    keys.SignKey,
		SpaceType:    space.SpaceTypeRegular,
		SpacePayload: req.Seed,
	}
	spaceId, err := s.app.SpaceService().DeriveId(ctx, payload)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: derive id: %w", err)
	}
	if !s.app.SpaceExists(spaceId) {
		if _, err := s.app.SpaceService().DeriveSpace(ctx, payload); err != nil {
			return nil, fmt.Errorf("spaceimpl: derive: %w", err)
		}
	}
	if _, ok := s.tsp.Get(ctx, spaceId); !ok {
		if _, err := s.tsp.Add(ctx, techspace.SpaceIndexRecord{
			Id:           spaceId,
			Type:         space.SpaceTypeRegular,
			LocalStatus:  techspace.StatusActive,
			RemoteStatus: techspace.StatusActive,
		}); err != nil {
			return nil, fmt.Errorf("spaceimpl: write index entry: %w", err)
		}
	}
	if _, err := s.app.GetSpace(ctx, spaceId); err != nil {
		return nil, err
	}
	store := s.storeFor(spaceId)
	if _, err := s.ensureSpaceIndexWiring(ctx, spaceId); err != nil {
		return nil, err
	}
	sp := newSpace(spaceId, s.app, s.tsp, store, s)
	go sp.maybeLazySeedSpaceIndex()
	return sp, nil
}

// OneToOne returns the derived 1-1 space with otherIdentity, creating
// it locally if it does not yet exist. Same id regardless of which
// side called first — both peers land on the same space.
func (s *Service) OneToOne(ctx context.Context, otherIdentity string) (space.Space, error) {
	keys := s.app.AccountKeys()
	if keys == nil {
		return nil, errors.New("spaceimpl: anysyncx app has no account keys")
	}
	otherPk, err := decodeIdentity(otherIdentity)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: OneToOne: %w", err)
	}
	spaceId, err := s.app.SpaceService().DeriveOneToOneSpace(ctx, keys.SignKey, otherPk)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: derive 1-1: %w", err)
	}
	if _, ok := s.tsp.Get(ctx, spaceId); !ok {
		if _, err := s.tsp.Add(ctx, techspace.SpaceIndexRecord{
			Id:           spaceId,
			Type:         space.SpaceTypeOneToOne,
			LocalStatus:  techspace.StatusActive,
			RemoteStatus: techspace.StatusActive,
		}); err != nil {
			return nil, fmt.Errorf("spaceimpl: write index entry: %w", err)
		}
	}
	if _, err := s.app.GetSpace(ctx, spaceId); err != nil {
		return nil, err
	}
	store := s.storeFor(spaceId)
	if _, err := s.ensureSpaceIndexWiring(ctx, spaceId); err != nil {
		return nil, err
	}
	sp := newSpace(spaceId, s.app, s.tsp, store, s)
	go sp.maybeLazySeedSpaceIndex()
	return sp, nil
}

// ErrJoinPending is returned by Join after a RequestToJoin invite was
// successfully posted but the owner has not yet accepted. The space
// is recorded in the tech-space index with LocalStatus=Joining;
// callers can poll List for the status flip and then call Get.
var ErrJoinPending = errors.New("spaceimpl: join pending owner approval")

// Join sends a join request via the invite. v1 only supports
// RequestToJoin invites — AnyoneCanJoin is deferred until any-sync
// ships v2 of that invite type.
//
// Behavior: decode invite, send RequestJoin RPC to the network, write
// a tech-space record with LocalStatus=Joining. Returns
// (nil, ErrJoinPending) on success — the joiner doesn't yet have
// local space storage; that lands after the owner accepts and the
// space syncs down. Callers poll Service.List for the status flip
// to StatusActive.
func (s *Service) Join(ctx context.Context, req space.JoinRequest) (space.Space, error) {
	if req.Invite == "" {
		return nil, errors.New("spaceimpl: Join: Invite required")
	}
	inv, err := space.DecodeInvite(req.Invite)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: Join: %w", err)
	}
	jc := s.app.JoiningClient()
	if jc == nil {
		return nil, errors.New("spaceimpl: Join: joining client unavailable")
	}
	_, err = jc.RequestJoin(ctx, inv.SpaceId, list.RequestJoinPayload{
		InviteKey: inv.InviteKey,
		Metadata:  encodeMetadata(req.Metadata),
	})
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: RequestJoin: %w", err)
	}
	// Record the pending-join state in the tech space so it shows up
	// in List with StatusJoining. The actual space object lands after
	// owner approval + sync.
	if _, ok := s.tsp.Get(ctx, inv.SpaceId); !ok {
		if _, err := s.tsp.Add(ctx, techspace.SpaceIndexRecord{
			Id:           inv.SpaceId,
			Type:         space.SpaceTypeRegular,
			LocalStatus:  joiningLocalStatus,
			RemoteStatus: techspace.StatusActive,
		}); err != nil {
			return nil, fmt.Errorf("spaceimpl: write index entry: %w", err)
		}
	}
	return nil, ErrJoinPending
}

// joiningLocalStatus is the localStatus value the SDK stamps on a
// space-index entry while a RequestToJoin is still pending owner
// approval. Maps to space.StatusJoining via mapStatus.
const joiningLocalStatus = "joining"

// Close stops per-space subsystems the SDK owns directly — currently
// just members watchers (one polling goroutine each). The any-sync
// side of each space is owned by the App's space cache and torn down
// separately by App.Close.
func (s *Service) Close(_ context.Context) error {
	s.watchers.stopAll()
	return nil
}

// KickProfiles asks every running members watcher to refetch
// identityRepo profiles immediately. Called by the SDK after the
// caller publishes their own profile via Account.UpdateMetadata so
// the self-view propagates without waiting for the slow tick.
func (s *Service) KickProfiles(ctx context.Context) {
	s.watchers.kickProfiles(ctx)
}

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
	case local == joiningLocalStatus:
		return space.StatusJoining
	case local == "" && remote == "":
		return space.StatusUnknown
	default:
		return space.StatusActive
	}
}
