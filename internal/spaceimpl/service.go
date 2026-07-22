package spaceimpl

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/commonspace/acl/aclwaiter"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/util/crypto"

	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/files/fetch"
	"github.com/anyproto/any-sync-sdk/internal/files/status"
	filestore "github.com/anyproto/any-sync-sdk/internal/files/store"
	"github.com/anyproto/any-sync-sdk/internal/files/upload"
	"github.com/anyproto/any-sync-sdk/internal/inbox"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/readsync"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/subscribe"
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

	// readSyncSvc is the SDK-level read-state sync service, injected
	// by sdk.Open after construction (it needs the tech space open).
	// Guarded by readSyncMu; nil until SetReadSync.
	readSyncMu  sync.RWMutex
	readSyncSvc *readsync.Service

	// extTypes carry through to every per-space Store created by
	// storeFor — each type's handlers are applied alongside the
	// built-in catalog.
	extTypes []handler.Type

	// files/fetch/fstore/fqueue are the SDK-level byte-layer services
	// behind every space's Files() surface. Set once by SetFiles right
	// after construction (sdk.Open); nil only in tests that never
	// touch files.
	files  *upload.Service
	fetch  *fetch.Service
	fstore *filestore.Store
	fqueue *status.Queue

	// fileStatusSubs fans queue transitions + local attach events out
	// to per-space Files().SubscribeStatus callbacks.
	fileStatusSubs fileSubs

	mu     sync.Mutex
	stores map[string]*spaceobjects.Store
	allocs map[string]*object.VersionAllocator

	// oneToOneLocks serializes 1-1 materialization per space id.
	// DeriveOneToOneSpace reaches CreateSpaceStorage outside the space
	// cache's per-id single-flight, and that path deletes the db file
	// on a duplicate-create error — two concurrent materializations
	// (e.g. parallel Gets adopting a 1-1 accepted on another device)
	// would race destructively. Entries are never freed: one per 1-1
	// contact, bounded and tiny.
	oneToOneLocks map[string]*sync.Mutex

	// spaceIndexIds caches the deterministic spaceIndex object id per
	// spaceId so SetMetadata / Info-side reads don't re-derive on every
	// call. Populated by ensureSpaceIndexWiring.
	spaceIndexIds map[string]string

	// spaceIndexWatchers holds one watcher per loaded spaceId; the
	// watcher subscribes to the spaceIndex object's properties dataset
	// and forwards converged state into the Indexer. Lifetime: until
	// SDK.Close (mirrors the members-watcher sticky lifecycle).
	spaceIndexWatchers map[string]*spaceIndexWatcher
	// accountMirrors holds one account-values mirror per loaded
	// spaceId — the tech-space → target-space apply side of
	// account-scoped values (see accountmirror.go).
	accountMirrors map[string]*accountMirror

	// memberWatchers holds one members poller per loaded spaceId. Like
	// the spaceIndex/account watchers above it's a Service-level
	// singleton: the SDK mints a fresh spaceImpl (and membersAPI) on
	// every Get/Create/Derive, so without this the first members
	// Subscribe/Query on each handle would spawn its own never-reaped
	// watcher (two goroutines) — they only stop at SDK.Close. Keyed by
	// spaceId so all handles for one space share a single watcher.
	memberWatchers map[string]*memberWatcher

	// aclMirrorWatchers holds one ACL mirror per loaded spaceId
	// (see aclmirror_watcher.go) — ACL state → the tech-space row's
	// `push` + `ownRole` fields. Same sticky Service-level-singleton
	// lifecycle as the watchers above.
	aclMirrorWatchers map[string]*aclMirrorWatcher

	// aclMuxes fans each space's single syncacl AclUpdater slot out to
	// its subscribers (member + ACL mirror watchers) — see aclkick.go.
	aclMuxes map[string]*aclKickMux

	// watchers tracks every active members poller across all loaded
	// spaceImpls so SDK shutdown can drain them deterministically.
	watchers watcherRegistry

	// seedWG / seedCtx track the fire-and-forget spaceIndex lazy-seed
	// goroutines (goSeed). Close cancels seedCtx and waits on seedWG so
	// no background seed is still touching the store / tech space when
	// the SDK tears them down. closing (under mu) stops new seeds from
	// being spawned during shutdown.
	seedWG     sync.WaitGroup
	seedCtx    context.Context
	seedCancel context.CancelFunc
	closing    bool

	// Deletion reconciler: a background loop that drives locally-deleted
	// spaces to the coordinator (signed SpaceDelete) and offloads spaces
	// the coordinator reports gone. delKick wakes it immediately after a
	// local Delete; otherwise it ticks on deletionReconcileInterval.
	// delCancel/delWG are drained in Close before watchers.stopAll.
	delCancel context.CancelFunc
	delWG     sync.WaitGroup
	delKick   chan struct{}

	// Join controller: a background loop that drives joiner-side
	// post-acceptance loading. For each tech-space row with
	// localStatus="joining" it runs an any-sync ACL waiter; on
	// acceptance it loads the space and flips the row to active, on
	// decline it marks it deleted. joinKick wakes it immediately after
	// a local Join; otherwise it ticks on joinReconcileInterval.
	// joinWaiters holds the live waiter per spaceId (guarded by mu),
	// like memberWatchers. joinCancel/joinWG are drained in Close.
	joinCancel  context.CancelFunc
	joinWG      sync.WaitGroup
	joinKick    chan struct{}
	joinWaiters map[string]aclwaiter.AclWaiter

	// pendingLoads dedups the join controller's accepted-invite load
	// goroutines per spaceId (guarded by mu) — rows with
	// localStatus="inviteLoading" need a pull-until-available load but no
	// ACL waiter (the account is already a member).
	pendingLoads map[string]struct{}

	// pushKeys caches the per-space derived push-notification keys —
	// see push.go (PushKeys).
	pushKeys pushKeyCache

	// One-to-one inbox (Layer-2 discovery, docs/13). inboxNotifier is the
	// receive worker (coordinator push + poll → RegisterIncoming); the
	// invite* fields drive the send-retry loop that (re)delivers
	// initiated-1-1 notifications. Wired and started by StartOneToOneInbox
	// (from sdk.Open, after the tech space opens); drained in Close. Nil /
	// inert when the inbox transport is unavailable.
	inboxNotifier *inbox.Notifier
	inviteCtx     context.Context
	inviteCancel  context.CancelFunc
	inviteWG      sync.WaitGroup
	inviteKick    chan struct{}
}

// New returns a Service ready to be returned via SDK.Spaces(). The
// db argument is the shared SDK DB where CRDT collections live.
// extTypes must already have been validated by
// spaceobjects.ValidateExternalTypes at the SDK boundary. indexer is
// the seam through which the tech-space mirrors converged in-space
// spaceIndex state into its rows; usually tsp itself (which
// satisfies space.Indexer).
func New(app *anysyncx.App, tsp *techspace.Service, indexer space.Indexer, db anystore.DB, extTypes []handler.Type) *Service {
	s := &Service{
		app:                app,
		tsp:                tsp,
		indexer:            indexer,
		db:                 db,
		extTypes:           extTypes,
		stores:             make(map[string]*spaceobjects.Store),
		allocs:             make(map[string]*object.VersionAllocator),
		spaceIndexIds:      make(map[string]string),
		spaceIndexWatchers: make(map[string]*spaceIndexWatcher),
		accountMirrors:     make(map[string]*accountMirror),
		memberWatchers:     make(map[string]*memberWatcher),
		aclMirrorWatchers:  make(map[string]*aclMirrorWatcher),
		aclMuxes:           make(map[string]*aclKickMux),
		delKick:            make(chan struct{}, 1),
		joinKick:           make(chan struct{}, 1),
		joinWaiters:        make(map[string]aclwaiter.AclWaiter),
		pendingLoads:       make(map[string]struct{}),
		inviteKick:         make(chan struct{}, 1),
		oneToOneLocks:      make(map[string]*sync.Mutex),
	}
	s.seedCtx, s.seedCancel = context.WithCancel(context.Background())
	s.startDeletionReconciler()
	s.startJoinController()
	// Wire the Total source for the sync-status rollup. The rollup
	// loop reads the per-space `objects` row count via the live Store
	// to compute Synced/Total. nil-store cases (querying status for
	// a space before storeFor has run) report 0, which is correct.
	if ss := app.SyncStatus(); ss != nil {
		ss.SetTotalFn(func(spaceId string) int {
			s.mu.Lock()
			store := s.stores[spaceId]
			s.mu.Unlock()
			if store == nil {
				return 0
			}
			return store.RegularObjectCount(context.Background())
		})
	}
	return s
}

// SetFiles wires the SDK-level files byte-layer services. Called once
// from sdk.Open before any Space handle is handed out.
func (s *Service) SetFiles(up *upload.Service, fe *fetch.Service, st *filestore.Store, q *status.Queue) {
	s.files, s.fetch, s.fstore, s.fqueue = up, fe, st, q
}

// filesStore returns the local payload store (nil-safe accessor for
// the Files surface).
func (s *Service) filesStore() *filestore.Store { return s.fstore }

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
	// Read-only gate on every user-authored synced write
	// (Object.LocalWrite, Store.Create). Guest-mode is fixed for the
	// store's lifetime — a key refresh tears the runtime down — so it
	// seeds once here; ACL role changes (reader) are maintained
	// event-driven by the per-space ACL mirror (mirrorWriteGate).
	if rec, ok := s.tsp.Get(context.Background(), spaceId); ok && rec.GuestKey != "" {
		st.SetWriteGateErr(space.ErrReadOnlySpace)
	}
	// Resolve the read-sync service lazily: stores can be created
	// before sdk.Open injects it, and untracked spaces never call it.
	st.SetSeedHeadsProvider(func(ctx context.Context, objectId string) ([][]string, error) {
		rs := s.readSync()
		if rs == nil {
			return nil, nil
		}
		return rs.PublishedFrontiers(ctx, spaceId, objectId)
	})
	// Drain the one-off applySeq backfill BEFORE publishing the store, so
	// no apply or consumer read ever triggers the backfill's WriteTx from
	// inside the applySeqMeta sync.Once. That lazy path otherwise
	// lock-order-inverts against any-store's write mutex (an apply holds
	// writeMu then wants the Once; a concurrent ChangedObjects holds the
	// Once then wants writeMu) and deadlocks. storeFor is single-threaded
	// per space and its caller never holds the write mutex, so forcing the
	// backfill here is the safe, race-free place. Best-effort: a transient
	// error is cached by the Once and resurfaces on the next feed read.
	_ = st.EnsureApplySeq(context.Background())
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

// peekStore returns spaceId's Store only if already built — never
// constructs. Used by watchers that must not create runtime state.
func (s *Service) peekStore(spaceId string) *spaceobjects.Store {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stores[spaceId]
}

// SetReadSync injects the SDK-level read-state sync service. Called
// once from sdk.Open after the tech space is up.
func (s *Service) SetReadSync(rs *readsync.Service) {
	s.readSyncMu.Lock()
	s.readSyncSvc = rs
	s.readSyncMu.Unlock()
}

func (s *Service) readSync() *readsync.Service {
	s.readSyncMu.RLock()
	defer s.readSyncMu.RUnlock()
	return s.readSyncSvc
}

// accountMirror returns the live account-values mirror for spaceId,
// or nil before the space's wiring ran. Used by Properties.Set's
// account route for the inline read-your-writes replay.
func (s *Service) accountMirror(spaceId string) *accountMirror {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accountMirrors[spaceId]
}

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

	// Tell the sync-status tracker to ignore this tree — the
	// spaceIndex is a derived per-space metadata object, not a
	// user-visible regular object, so it shouldn't contribute to
	// the Pending count.
	if ss := s.app.SyncStatus(); ss != nil {
		ss.For(spaceId).AddExcluded(objectId)
	}

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

	// Account-values mirror: tech-space carrier → this space's objects
	// rows. Same dedup dance; the initial re-mirror runs inside
	// newAccountMirror.
	if store != nil && s.tsp != nil {
		s.mu.Lock()
		_, dup := s.accountMirrors[spaceId]
		s.mu.Unlock()
		if !dup {
			if m := newAccountMirror(ctx, store, s.tsp, spaceId); m != nil {
				s.mu.Lock()
				if _, raced := s.accountMirrors[spaceId]; raced {
					s.mu.Unlock()
					m.stop()
				} else {
					s.accountMirrors[spaceId] = m
					s.watchers.register(m)
					s.mu.Unlock()
				}
			}
		}
	}

	// ACL mirror: ACL state → this space's tech-space row `push` +
	// `ownRole` fields (aclmirror_watcher.go). Same dedup dance; the
	// initial mirror pass is primed inside newAclMirrorWatcher.
	// Registered on the space's ACL kick fan-out so read-key rotations
	// and permission changes land promptly.
	if s.tsp != nil {
		s.mu.Lock()
		_, dup := s.aclMirrorWatchers[spaceId]
		s.mu.Unlock()
		if !dup {
			w := newAclMirrorWatcher(s, spaceId)
			s.mu.Lock()
			if _, raced := s.aclMirrorWatchers[spaceId]; raced {
				s.mu.Unlock()
				w.stop()
			} else {
				s.aclMirrorWatchers[spaceId] = w
				s.watchers.register(w)
				s.mu.Unlock()
				var mux *aclKickMux
				if acl := handle.Inner().Acl(); acl != nil {
					mux = s.aclKickFanout(spaceId, acl)
					mux.add(w)
					// Close the wiring gap: the primed mirror pass may
					// already have read ACL state BEFORE the mux
					// subscription above — an ACL record applied in that
					// window produced no kick, and on a quiet space
					// nothing else ever would. One post-subscribe kick
					// re-mirrors from current state; coalescing makes it
					// free when the primed pass hasn't run yet.
					w.kick()
				}
				// Re-check against a concurrent offload: closeSpaceRuntime
				// stops watchers and clears the maps under s.mu, but this
				// wiring ran outside it — if the space was torn down
				// meanwhile (spaceIndexIds entry gone), unwind so no live
				// watcher survives against a closed space.
				s.mu.Lock()
				if _, live := s.spaceIndexIds[spaceId]; !live {
					delete(s.aclMirrorWatchers, spaceId)
					s.mu.Unlock()
					if mux != nil {
						mux.remove(w)
					}
					s.watchers.unregister(w)
					w.stop()
				} else {
					s.mu.Unlock()
				}
			}
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
	// Owner metadata in the ACL root is the owner's metadata symkey only,
	// so joiners learn the key and resolve the owner's profile from
	// identityRepo (no inline name/icon on the ACL).
	ownerMeta, err := encodeSelfSymKeyMetadata(keys.SignKey)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: derive metadata key: %w", err)
	}
	payload := spacepayloads.SpaceCreatePayload{
		SigningKey:     keys.SignKey,
		MasterKey:      keys.SignKey,
		ReadKey:        readKey,
		MetadataKey:    metadataKey,
		Metadata:       ownerMeta,
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
		SpaceType:    spaceType,
		Name:         req.Name,
		Description:  req.Description,
		IconCID:      req.IconCID,
		RemoteStatus: techspace.StatusActive,
	}); err != nil {
		return nil, fmt.Errorf("spaceimpl: write index entry: %w", err)
	}

	// Add() can't write localStatus — it's a device-local field set only
	// via the local-set path (see techspace/record.go). Stamp it active
	// now so an owner-created space reports StatusActive consistently
	// instead of sitting at localStatus="" until a join flow heals it.
	// Without this, consumers that filter on the raw localStatus row
	// never match their own freshly-created spaces.
	if _, err := s.tsp.SetLocalStatus(ctx, spaceId, techspace.StatusActive); err != nil {
		return nil, fmt.Errorf("spaceimpl: set local status: %w", err)
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
	rec, ok := s.tsp.Get(ctx, spaceId)
	if !ok {
		return nil, fmt.Errorf("spaceimpl: unknown space %q", spaceId)
	}
	// Deleted rows are refused outright, mirroring the boot eager-load:
	// the storage is offloaded (or was never created), a load would
	// SpacePull a space the account removed, and the node rejects
	// deleted spaces anyway. mapStatus ranks deleted above pending, so
	// without this a deleted-elsewhere pending row would fall through
	// the guard into a pull.
	if rec.IsDeleted() {
		return nil, fmt.Errorf("spaceimpl: space %q is deleted", spaceId)
	}
	if err := MaterializeBlock(rec); err != nil {
		return nil, err
	}
	// A 1-1 accepted (or initiated) on another of this account's devices:
	// the synced remote=active landed while this device never processed
	// the 1-1 itself — its localStatus is a stale pending (own inbox
	// discovery) or empty (row synced in first). Adopt via the accept
	// path instead of a bare load — AcceptOneToOne derives the 1-1
	// storage locally (load would SpacePull) and stamps local=active, so
	// acceptance stays an account-level decision made once. A local
	// delete (localStatus=deleted) is not adopted — this device removed
	// the space on purpose.
	if rec.Type == space.SpaceTypeOneToOne &&
		rec.RemoteStatus == techspace.StatusActive &&
		(rec.LocalStatus == "" || rec.LocalStatus == oneToOnePendingLocalStatus) {
		return s.AcceptOneToOne(ctx, spaceId)
	}
	return s.load(ctx, spaceId)
}

// MaterializeBlock reports why rec must not be materialized — loaded,
// and, when local storage is absent, pulled from the responsible nodes
// (any-sync's NewSpace falls back to a SpacePull bootstrap on missing
// storage) — or nil when loading is allowed. "Nothing is downloaded
// until accepted" is the whole materialization gate: it guards every
// pending flavor, not just direct-add invites. Every accept path flips
// its row (or calls the unguarded load) before materializing, so
// legitimate loads pass:
//   - join: the ACL waiter's loadJoinedSpace uses load directly (the
//     row stays "joining" until the load succeeds);
//   - incoming 1-1: activateOneToOne flips both statuses to active
//     first;
//   - direct-add invite: AcceptInvite flips the row to active before
//     it loads.
//
// Shared with sdk.Open's boot eager-load, which must skip pending rows
// even when storage exists (a pre-guard build may have materialized
// one; eager-loading it would run headsync/treesyncer on a space this
// account can't read yet).
//
// The predicate is the mapStatus classification, not the raw fields, so
// the guard and the surfaced Status can never disagree. In particular
// acceptance is account-scoped: a 1-1 accepted or initiated on ANY of
// the account's devices carries synced remote=active, which mapStatus
// resolves as Active over a stale device-local pending — such a row
// must load, not demand a per-device re-accept. Conversely a bare 1-1
// row synced in before this device set any status classifies as
// pending and blocks.
func MaterializeBlock(rec techspace.SpaceIndexRecord) error {
	switch mapStatus(rec.Type, rec.LocalStatus, rec.RemoteStatus) {
	case space.StatusJoining:
		return fmt.Errorf("spaceimpl: space %q join is pending owner approval: %w", rec.Id, space.ErrSpaceNotAccepted)
	case space.StatusOneToOnePending:
		return fmt.Errorf("spaceimpl: space %q is an incoming 1-1; AcceptOneToOne it first: %w", rec.Id, space.ErrSpaceNotAccepted)
	case space.StatusOneToOneDeclined:
		return fmt.Errorf("spaceimpl: space %q is a declined 1-1; OneToOne the peer to un-decline: %w", rec.Id, space.ErrSpaceNotAccepted)
	case space.StatusInvitePending, space.StatusInviteDeclined:
		return fmt.Errorf("spaceimpl: space %q is a pending direct-add invite; AcceptInvite it first: %w", rec.Id, space.ErrSpaceNotAccepted)
	}
	return nil
}

// load materializes spaceId unconditionally — no pending guard. Used by
// Get after MaterializeBlock, and by the join controller's
// post-acceptance loaders (loadJoinedSpace / loadAcceptedInvite), which
// run while the row still carries its pending localStatus: the flip to
// active happens only after a successful load.
func (s *Service) load(ctx context.Context, spaceId string) (space.Space, error) {
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
	s.goSeed(sp)
	return sp, nil
}

// SyncSpaceList forces an immediate head-sync round on the tech space
// so spaces added or removed on other devices land in the local index
// without waiting for the periodic headsync timer. Call before List to
// converge on demand.
func (s *Service) SyncSpaceList(ctx context.Context) error {
	return s.tsp.SyncHeads(ctx)
}

// List returns the space-index snapshot.
func (s *Service) List(ctx context.Context) ([]space.SpaceInfo, error) {
	rows := s.tsp.List(ctx)
	out := make([]space.SpaceInfo, 0, len(rows))
	for _, r := range rows {
		out = append(out, s.recordToInfo(ctx, r))
	}
	return out, nil
}

// recordToInfo maps a tech-space index record to the public SpaceInfo,
// resolving the spaceType / author and folding local+remote status.
// Shared by List and the Subscribe translator so both surface the same
// shape.
func (s *Service) recordToInfo(ctx context.Context, r techspace.SpaceIndexRecord) space.SpaceInfo {
	// A 1-1 space's ACL "owner" is a synthetic shared key nobody holds
	// (see docs/13), so resolveAuthor returns nothing meaningful. The
	// useful value is the other participant's account identity, recorded
	// on the row as OneToOnePeer; surface it as Author so clients can tell
	// who a 1-1 is with — and resolve their profile — straight from
	// List/Subscribe, without loading or even materializing the space.
	author := r.OneToOnePeer
	if r.Type != space.SpaceTypeOneToOne {
		author = s.resolveAuthor(ctx, r.Id)
	}
	info := space.SpaceInfo{
		Id:          r.Id,
		Type:        r.Type,
		SpaceType:   s.resolveSpaceType(ctx, r.Id, r.SpaceType),
		Author:      author,
		Name:        r.Name,
		Description: r.Description,
		IconCID:     r.IconCID,
		Status:      mapStatus(r.Type, r.LocalStatus, r.RemoteStatus),
		OwnRole:     r.OwnRole,
		Settings:    r.Settings,
		PushKeys:    r.PushKeys,
	}
	// A 1-1 has no space-set name; show the friend's resolved profile from
	// the identities directory (the row's name/icon stays as an out-of-band
	// displayHint fallback when nothing is resolved yet).
	if r.Type == space.SpaceTypeOneToOne && r.OneToOnePeer != "" {
		if id, ok := s.tsp.GetIdentity(ctx, r.OneToOnePeer); ok && id.Name != "" {
			info.Name = id.Name
			info.Description = id.Description
			info.IconCID = id.IconCID
		}
	}
	if r.CreatedAt > 0 {
		info.CreatedAt = time.Unix(r.CreatedAt, 0)
	}
	return info
}

// SetSettings patches the per-space client settings on spaceId's
// tech-space row (see space.Service.SetSettings for the contract).
// The row must exist: the techspace write is a strict (non-upsert)
// modify — on an absent id it would silently no-op — so the unknown-id
// case is turned into an error here. Deleted rows pass: the handler's
// terminal-delete rule guards only status fields, and a tombstone's
// settings staying editable is deliberate (the row remains the
// account's record of the space).
func (s *Service) SetSettings(ctx context.Context, spaceId string, set map[string]any, unset []string) error {
	if _, ok := s.tsp.Get(ctx, spaceId); !ok {
		return fmt.Errorf("spaceimpl: unknown space %q", spaceId)
	}
	_, err := s.tsp.SetSettings(ctx, spaceId, set, unset)
	return err
}

// Delete removes a space. It is offline-first and returns as soon as the
// local work is done — no network round trip on the call path:
//  1. write the SYNCED remoteStatus=deleted tombstone (propagates the
//     delete to the account's other devices, and is the durable intent
//     the reconciler scans; also drives the Subscribe `Removed` event);
//  2. offload all local state immediately (reclaim disk even offline);
//  3. kick the deletion reconciler, which sends the signed SpaceDelete
//     to the coordinator now (if online) or on a later tick (when it
//     reconnects). Only owners' deletes reach the coordinator; for a
//     non-owned space this offloads locally and the reconciler no-ops.
//
// The tech-space row is never physically removed — it stays in List with
// Status = StatusDeleted as a sticky tombstone.
//
// 1-1 spaces take a separate path: they are derived (re-creatable) and not
// owned on the network, so deleting one must NOT remove it from the nodes.
// Instead the SYNCED, non-terminal oneToOneDeleted marker propagates the
// delete to the account's other devices — each offloads its local copy —
// while the row stays re-creatable (a later OneToOne(peer) flips it back to
// active). No coordinator SpaceDelete is ever sent.
func (s *Service) Delete(ctx context.Context, spaceId string) error {
	if rec, ok := s.tsp.Get(ctx, spaceId); ok && rec.Type == space.SpaceTypeOneToOne {
		if _, err := s.tsp.SetRemoteStatus(ctx, spaceId, techspace.OneToOneDeletedStatus); err != nil {
			return fmt.Errorf("spaceimpl: mark 1-1 deleted: %w", err)
		}
		s.OffloadSpace(ctx, spaceId)
		// No coordinator kick: a 1-1 is never node-deleted. Other devices
		// offload via their own reconciler when the synced marker arrives.
		return nil
	}
	if rec, ok := s.tsp.Get(ctx, spaceId); ok && rec.GuestKey != "" {
		// Guest-mode space: same shape as the 1-1 delete — synced,
		// NON-terminal marker (a later JoinGuest re-adds), local offload,
		// no coordinator SpaceDelete (the space isn't ours on the
		// network). Other devices offload via their reconciler when the
		// marker arrives.
		if _, err := s.tsp.SetRemoteStatus(ctx, spaceId, techspace.GuestDeletedRemoteStatus); err != nil {
			return fmt.Errorf("spaceimpl: mark guest space deleted: %w", err)
		}
		s.OffloadSpace(ctx, spaceId)
		return nil
	}
	if _, err := s.tsp.SetRemoteStatus(ctx, spaceId, techspace.StatusDeleted); err != nil {
		return fmt.Errorf("spaceimpl: mark deleted: %w", err)
	}
	s.OffloadSpace(ctx, spaceId)
	s.kickDeletionReconciler()
	return nil
}

// Subscribe delivers live space-list deltas. It registers a sub on the
// tech-space index object's `spaces` dataset (which now projects inbound
// head-sync changes live, since the index object is Store-backed with a
// deferred-updater listener) and translates each engine event into a
// SpaceListEvent. Delta-only: callers seed current state via List.
//
// Mapping: a row whose localStatus is "deleted" (sticky soft-delete)
// surfaces under Removed regardless of whether the engine classified it
// Added/Updated; everything else maps to Added/Updated as the engine
// saw it. The cancel func stops the drain goroutine and closes the sub.
func (s *Service) Subscribe(cb func(space.SpaceListEvent)) (cancel func()) {
	sub, err := s.tsp.SubEngine().Subscribe(subscribe.SubConfig{
		Scope: subscribe.Scope{ObjectId: s.tsp.IndexObjectId(), Dataset: techspace.SpaceIndexDataset},
	}, func(func(id string, doc *anyenc.Value)) error { return nil })
	if err != nil {
		return func() {}
	}
	ctx, cancelCtx := context.WithCancel(context.Background())
	go func() {
		events := sub.Events()
		for {
			evs, werr := events.Wait(ctx)
			if werr != nil {
				return
			}
			for _, ev := range evs {
				if out, ok := s.toSpaceListEvent(ctx, ev); ok {
					cb(out)
				}
			}
		}
	}()
	return func() {
		cancelCtx()
		_ = sub.Close()
	}
}

// toSpaceListEvent translates a record-level SubscriptionEvent on the
// `spaces` dataset into a SpaceListEvent. Returns ok=false when the
// event carries nothing actionable.
func (s *Service) toSpaceListEvent(ctx context.Context, ev space.SubscriptionEvent) (space.SpaceListEvent, bool) {
	var out space.SpaceListEvent
	classify := func(rec space.SubRecord, updated bool) {
		if rec.Doc == nil {
			return
		}
		r := techspace.DecodeSpaceIndexRecord(rec.Doc)
		if r.RemoteStatus == techspace.StatusDeleted || r.RemoteStatus == techspace.OneToOneDeletedStatus {
			// Account-wide delete (synced) → leaves the live list. The 1-1
			// offload marker counts too (synced, surfaced as deleted).
			out.Removed = append(out.Removed, r.Id)
			return
		}
		info := s.recordToInfo(ctx, r)
		if updated {
			out.Updated = append(out.Updated, info)
		} else {
			out.Added = append(out.Added, info)
		}
	}
	for _, rec := range ev.Added {
		classify(rec, false)
	}
	for _, rec := range ev.Updated {
		classify(rec, true)
	}
	for _, rec := range ev.Removed {
		out.Removed = append(out.Removed, rec.Id)
	}
	if len(out.Added) == 0 && len(out.Updated) == 0 && len(out.Removed) == 0 {
		return space.SpaceListEvent{}, false
	}
	return out, true
}

// SpaceIndexObjectId returns the well-known id of the tech-space index
// object. Pass it to Query/Subscribe to read the system datasets
// (spaces, profile) generically. Future system objects expose their
// own ids the same way.
func (s *Service) SpaceIndexObjectId() string { return s.tsp.IndexObjectId() }

// Query builds a generic read query over a tech-space system object's
// dataset — same chainable Filter/Sort/Limit/Subscribe surface regular
// spaces use, but bound to the tech Store. Use SpaceIndexObjectId() for
// the spaces / profile datasets.
func (s *Service) Query(objectId, dataset string) space.Query {
	return newQuery(s.tsp.Store(), objectId, dataset)
}

// Datasets returns the JSON-Schema description of the tech-space system
// datasets (spaces, profile) for discovery.
func (s *Service) Datasets() []space.DatasetSchema {
	return toDatasetSchemas(s.tsp.Store().Schemas())
}

// toDatasetSchemas marshals each dataset's declared schema into a public
// JSON-Schema document for discovery. A schema that fails to marshal is
// skipped (should never happen — the doc is a plain map).
func toDatasetSchemas(named []spaceobjects.NamedSchema) []space.DatasetSchema {
	out := make([]space.DatasetSchema, 0, len(named))
	for _, ns := range named {
		raw, err := json.Marshal(ns.Schema)
		if err != nil {
			continue
		}
		out = append(out, space.DatasetSchema{Name: ns.Name, JSONSchema: raw})
	}
	return out
}

// Status returns a snapshot of spaceId's rolled-up sync state. Routes
// through the per-account syncstatus.Service on anysyncx.App.
func (s *Service) Status(spaceId string) space.SpaceSyncStatus {
	return s.app.SyncStatus().Status(spaceId)
}

// SubscribeStatus registers cb for SpaceSyncStatus events across
// every known space. Account-wide firehose backed by the syncstatus
// Service's subscriber registry.
func (s *Service) SubscribeStatus(cb func(space.SpaceSyncStatus)) (cancel func()) {
	return s.app.SyncStatus().SubscribeStatus(cb)
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
	payload := s.derivePayload(keys, req)
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
			SpaceType:    deriveSpaceTypeTag(req.SpaceType),
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
	s.goSeed(sp)
	return sp, nil
}

// DeriveId returns the deterministic spaceId for req without creating
// or loading the space — pure computation over the account keys and
// seed. Same id as Derive(req).Id() for the same request.
func (s *Service) DeriveId(ctx context.Context, req space.DeriveRequest) (string, error) {
	keys := s.app.AccountKeys()
	if keys == nil {
		return "", errors.New("spaceimpl: anysyncx app has no account keys")
	}
	id, err := s.app.SpaceService().DeriveId(ctx, s.derivePayload(keys, req))
	if err != nil {
		return "", fmt.Errorf("spaceimpl: derive id: %w", err)
	}
	return id, nil
}

// derivePayload builds the any-sync derive payload shared by Derive and
// DeriveId. The on-wire header SpaceType stays SpaceTypeRegular
// (coordinator-gated). The app-level SpaceType tag and the seed are
// encoded together into SpaceHeaderPayload, which is part of the header
// (and thus the derived id) and is recoverable from the header alone on
// cold restore. Both Derive and DeriveId build the identical payload, so
// the derived id is stable for a given (seed, SpaceType) pair.
func (s *Service) derivePayload(keys *accountdata.AccountKeys, req space.DeriveRequest) spacepayloads.SpaceDerivePayload {
	return spacepayloads.SpaceDerivePayload{
		SigningKey:   keys.SignKey,
		MasterKey:    keys.SignKey,
		SpaceType:    space.SpaceTypeRegular,
		SpacePayload: encodeDerivePayload(req.Seed, deriveSpaceTypeTag(req.SpaceType)),
	}
}

// deriveSpaceTypeTag resolves the app-level SpaceType tag, defaulting an
// empty request value to SpaceTypeRegular so existing Derive callers
// keep today's behavior.
func deriveSpaceTypeTag(t string) string {
	if t == "" {
		return space.SpaceTypeRegular
	}
	return t
}

// oneToOnePendingLocalStatus is the DEVICE-LOCAL localStatus stamped on
// an incoming 1-1 row awaiting approval. Like joiningLocalStatus,
// discovery is per-device so the prompt is per-device — only the decline
// is synced. Maps to space.StatusOneToOnePending.
const oneToOnePendingLocalStatus = "oneToOnePending"

// oneToOneDeclinedRemoteStatus is the SYNCED remoteStatus value written
// when a 1-1 is declined. Sticky account-wide but — unlike
// techspace.StatusDeleted — NOT terminal, so an explicit OneToOne(peer)
// flips it back to active (un-decline). Maps to
// space.StatusOneToOneDeclined.
const oneToOneDeclinedRemoteStatus = "oneToOneDeclined"

// lockOneToOne takes the per-space 1-1 materialization lock, returning
// the unlock. See the oneToOneLocks field comment.
func (s *Service) lockOneToOne(spaceId string) func() {
	s.mu.Lock()
	m, ok := s.oneToOneLocks[spaceId]
	if !ok {
		m = &sync.Mutex{}
		s.oneToOneLocks[spaceId] = m
	}
	s.mu.Unlock()
	m.Lock()
	return m.Unlock
}

// OneToOne reaches out to — or explicitly accepts / un-declines — the 1-1
// space shared with otherIdentity. Derives the shared space, materializes
// its storage, and activates it locally (implicit self-approval). Same id
// regardless of which side called first; idempotent. Overrides a prior
// local decline.
func (s *Service) OneToOne(ctx context.Context, otherIdentity string) (space.Space, error) {
	keys := s.app.AccountKeys()
	if keys == nil {
		return nil, errors.New("spaceimpl: anysyncx app has no account keys")
	}
	if otherIdentity == keys.SignKey.GetPublic().Account() {
		return nil, errors.New("spaceimpl: OneToOne: cannot pair with self")
	}
	otherPk, err := decodeIdentity(otherIdentity)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: OneToOne: %w", err)
	}
	spaceId, err := deriveOneToOneId(keys.SignKey, otherPk)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: derive 1-1 id: %w", err)
	}
	unlock := s.lockOneToOne(spaceId)
	defer unlock()
	// DeriveOneToOneSpace creates the storage as a side effect (idempotent —
	// ErrSpaceStorageExists is swallowed).
	if _, err := s.app.SpaceService().DeriveOneToOneSpace(ctx, keys.SignKey, otherPk); err != nil {
		return nil, fmt.Errorf("spaceimpl: derive 1-1: %w", err)
	}
	sp, err := s.activateOneToOne(ctx, spaceId, otherIdentity)
	if err != nil {
		return nil, err
	}
	// Initiate path only: notify the peer via the inbox so their device
	// surfaces the incoming 1-1 without an out-of-band exchange. Durable +
	// retried (best-effort); Accept deliberately does NOT notify back.
	s.markOneToOneInviteToSend(ctx, spaceId)
	return sp, nil
}

// AcceptOneToOne approves an incoming pending 1-1 by space id: materializes
// its storage and activates it. The peer identity is read off the row, so
// the caller only needs the id surfaced in the space list. Also un-declines
// a previously declined 1-1 (an explicit accept overrides the sticky
// marker).
func (s *Service) AcceptOneToOne(ctx context.Context, spaceId string) (space.Space, error) {
	keys := s.app.AccountKeys()
	if keys == nil {
		return nil, errors.New("spaceimpl: anysyncx app has no account keys")
	}
	rec, ok := s.tsp.Get(ctx, spaceId)
	if !ok {
		return nil, fmt.Errorf("spaceimpl: AcceptOneToOne: unknown space %q", spaceId)
	}
	if rec.Type != space.SpaceTypeOneToOne {
		return nil, fmt.Errorf("spaceimpl: AcceptOneToOne: %q is not a 1-1 space", spaceId)
	}
	if rec.OneToOnePeer == "" {
		return nil, fmt.Errorf("spaceimpl: AcceptOneToOne: row %q carries no peer identity", spaceId)
	}
	otherPk, err := decodeIdentity(rec.OneToOnePeer)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: AcceptOneToOne: %w", err)
	}
	unlock := s.lockOneToOne(spaceId)
	defer unlock()
	// Pending rows carry no storage — materialize it now.
	if _, err := s.app.SpaceService().DeriveOneToOneSpace(ctx, keys.SignKey, otherPk); err != nil {
		return nil, fmt.Errorf("spaceimpl: materialize 1-1: %w", err)
	}
	return s.activateOneToOne(ctx, spaceId, rec.OneToOnePeer)
}

// DeclineOneToOne rejects an incoming 1-1. Writes the synced, sticky
// oneToOneDeclined marker so the request is suppressed on every device; an
// explicit OneToOne(peer) later overrides it. Pending rows carry no
// storage, so there is nothing to offload.
func (s *Service) DeclineOneToOne(ctx context.Context, spaceId string) error {
	rec, ok := s.tsp.Get(ctx, spaceId)
	if !ok {
		return fmt.Errorf("spaceimpl: DeclineOneToOne: unknown space %q", spaceId)
	}
	if rec.Type != space.SpaceTypeOneToOne {
		return fmt.Errorf("spaceimpl: DeclineOneToOne: %q is not a 1-1 space", spaceId)
	}
	if _, err := s.tsp.SetRemoteStatus(ctx, spaceId, oneToOneDeclinedRemoteStatus); err != nil {
		return fmt.Errorf("spaceimpl: DeclineOneToOne: %w", err)
	}
	return nil
}

// RegisterIncoming records an incoming 1-1 request learned out-of-band (no
// coordinator) as a device-local pending row for the user to approve. No
// storage is materialized until AcceptOneToOne. displayHint is an optional
// name/icon snapshot for the UI. No-op if a row for the derived space
// already exists — respecting an active space or a sticky decline.
func (s *Service) RegisterIncoming(ctx context.Context, peerIdentity string, displayHint space.AccountMetadata) error {
	keys := s.app.AccountKeys()
	if keys == nil {
		return errors.New("spaceimpl: anysyncx app has no account keys")
	}
	if peerIdentity == keys.SignKey.GetPublic().Account() {
		return errors.New("spaceimpl: RegisterIncoming: cannot pair with self")
	}
	otherPk, err := decodeIdentity(peerIdentity)
	if err != nil {
		return fmt.Errorf("spaceimpl: RegisterIncoming: %w", err)
	}
	// Pure id derivation — must NOT create storage (pending is not
	// materialized until accept).
	spaceId, err := deriveOneToOneId(keys.SignKey, otherPk)
	if err != nil {
		return fmt.Errorf("spaceimpl: RegisterIncoming: %w", err)
	}
	if rec, ok := s.tsp.Get(ctx, spaceId); ok {
		// Existing row: respect an active space or sticky decline (no-op).
		// For a still-pending row, (re)try resolving the peer's name — an
		// earlier attempt may have run before the peer's symkey arrived.
		if rec.LocalStatus == oneToOnePendingLocalStatus {
			go s.resolveOneToOnePeerName(context.Background(), peerIdentity)
		}
		return nil
	}
	if _, err := s.tsp.Add(ctx, techspace.SpaceIndexRecord{
		Id:           spaceId,
		Type:         space.SpaceTypeOneToOne,
		SpaceType:    space.SpaceTypeOneToOne,
		Name:         displayHint.Name,
		Description:  displayHint.Description,
		IconCID:      displayHint.IconCID,
		OneToOnePeer: peerIdentity,
	}); err != nil {
		return fmt.Errorf("spaceimpl: RegisterIncoming: write index entry: %w", err)
	}
	if _, err := s.tsp.SetLocalStatus(ctx, spaceId, oneToOnePendingLocalStatus); err != nil {
		return fmt.Errorf("spaceimpl: RegisterIncoming: mark pending: %w", err)
	}
	// Resolve the peer's display name from identityRepo in the background:
	// with symkey-only invites the pending row starts identity-only, so we
	// fetch + decrypt the peer's profile and write name/icon onto the row.
	go s.resolveOneToOnePeerName(context.Background(), peerIdentity)
	return nil
}

// resolveOneToOnePeerName best-effort resolves a 1-1 peer's identityRepo
// profile (decrypted with the cached metadata symkey) into the identities
// directory and records the 1-1 space as a sighting, so the space list
// (via recordToInfo) shows the friend by name rather than identity-only.
// No-op when the symkey isn't cached yet, the coordinator is offline, the
// profile is empty, or the row is no longer a non-declined 1-1.
// Best-effort: meant to run in a goroutine; all failures are dropped (a
// later RegisterIncoming retries once the key/coordinator are available).
func (s *Service) resolveOneToOnePeerName(ctx context.Context, peerIdentity string) {
	keys := s.app.AccountKeys()
	if keys == nil {
		return
	}
	otherPk, err := decodeIdentity(peerIdentity)
	if err != nil {
		return
	}
	spaceId, err := deriveOneToOneId(keys.SignKey, otherPk)
	if err != nil {
		return
	}
	rec, ok := s.tsp.Get(ctx, spaceId)
	if !ok || rec.Type != space.SpaceTypeOneToOne || rec.RemoteStatus == oneToOneDeclinedRemoteStatus {
		return
	}
	// Record the sighting regardless of whether the profile resolves.
	_ = s.tsp.AddIdentitySpace(ctx, peerIdentity, spaceId)

	key := s.metadataSymKeyFor(ctx, peerIdentity)
	if key == nil {
		return
	}
	prof, ok := s.fetchIdentityProfile(ctx, peerIdentity, key)
	if !ok || (prof.Name == "" && prof.Description == "" && prof.IconCID == "") {
		return
	}
	_ = s.tsp.SetIdentityProfile(ctx, peerIdentity, prof.Name, prof.Description, prof.IconCID)
}

// activateOneToOne wires a 1-1 whose storage already exists and flips its
// index row to active, clearing any pending (device-local) or declined
// (synced) state. Shared by OneToOne (initiate) and AcceptOneToOne.
// peerIdentity is recorded on a freshly-created row so any of the account's
// devices can re-derive the space.
func (s *Service) activateOneToOne(ctx context.Context, spaceId, peerIdentity string) (space.Space, error) {
	if rec, ok := s.tsp.Get(ctx, spaceId); ok {
		// SetRemoteStatus(active) overrides a synced decline
		// (oneToOneDeclined is non-terminal); SetLocalStatus clears a
		// device-local pending. Both no-op if already active.
		if rec.RemoteStatus != techspace.StatusActive {
			if _, err := s.tsp.SetRemoteStatus(ctx, spaceId, techspace.StatusActive); err != nil {
				return nil, fmt.Errorf("spaceimpl: activate 1-1 remote: %w", err)
			}
		}
		if rec.LocalStatus != techspace.StatusActive {
			if _, err := s.tsp.SetLocalStatus(ctx, spaceId, techspace.StatusActive); err != nil {
				return nil, fmt.Errorf("spaceimpl: activate 1-1 local: %w", err)
			}
		}
	} else if _, err := s.tsp.Add(ctx, techspace.SpaceIndexRecord{
		Id:           spaceId,
		Type:         space.SpaceTypeOneToOne,
		SpaceType:    space.SpaceTypeOneToOne,
		RemoteStatus: techspace.StatusActive,
		OneToOnePeer: peerIdentity,
	}); err != nil {
		return nil, fmt.Errorf("spaceimpl: write index entry: %w", err)
	}
	if _, err := s.app.GetSpace(ctx, spaceId); err != nil {
		return nil, err
	}
	store := s.storeFor(spaceId)
	if _, err := s.ensureSpaceIndexWiring(ctx, spaceId); err != nil {
		return nil, err
	}
	sp := newSpace(spaceId, s.app, s.tsp, store, s)
	s.goSeed(sp)
	// Resolve the friend's name onto the active 1-1 row so the space list
	// shows it (no-op until we hold their symkey; best-effort).
	go s.resolveOneToOnePeerName(context.Background(), peerIdentity)
	return sp, nil
}

// deriveOneToOneId computes the derived 1-1 space id from the account sign
// key and the peer pubkey WITHOUT creating storage — unlike
// SpaceService.DeriveOneToOneSpace, which materializes it. Used by
// RegisterIncoming to record a pending row before the user accepts.
func deriveOneToOneId(mySignKey crypto.PrivKey, peerPub crypto.PubKey) (string, error) {
	payload, err := spacepayloads.StoragePayloadForOneToOneSpace(mySignKey, peerPub)
	if err != nil {
		return "", err
	}
	return payload.SpaceHeaderWithId.Id, nil
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
	keys := s.app.AccountKeys()
	if keys == nil {
		return nil, errors.New("spaceimpl: Join: anysyncx app has no account keys")
	}
	// The join record carries our metadata symkey only, not inline
	// name/icon: co-members cache it and read our profile from
	// identityRepo (which our account republishes on boot / UpdateMetadata).
	// req.Metadata no longer flows into the ACL.
	joinMeta, err := encodeSelfSymKeyMetadata(keys.SignKey)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: Join: derive metadata key: %w", err)
	}
	aclHeadId, err := jc.RequestJoin(ctx, inv.SpaceId, list.RequestJoinPayload{
		InviteKey: inv.InviteKey,
		Metadata:  joinMeta,
	})
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: RequestJoin: %w", err)
	}
	// Record the pending-join state in the tech space so it shows up
	// in List with StatusJoining. The synced row carries the metadata;
	// the joining lifecycle is per-device (localStatus is a local field),
	// so it's set separately via SetLocalStatus after the row exists.
	if _, ok := s.tsp.Get(ctx, inv.SpaceId); !ok {
		if _, err := s.tsp.Add(ctx, techspace.SpaceIndexRecord{
			Id:           inv.SpaceId,
			Type:         space.SpaceTypeRegular,
			RemoteStatus: techspace.StatusActive,
		}); err != nil {
			return nil, fmt.Errorf("spaceimpl: write index entry: %w", err)
		}
		if _, err := s.tsp.SetLocalStatus(ctx, inv.SpaceId, joiningLocalStatus); err != nil {
			return nil, fmt.Errorf("spaceimpl: mark joining: %w", err)
		}
	}
	// Persist the ACL head so the post-acceptance waiter can detect a
	// decline, then kick the join controller to start waiting now (it
	// would otherwise pick the row up on its next boot/tick pass).
	if _, err := s.tsp.SetAclHeadId(ctx, inv.SpaceId, aclHeadId); err != nil {
		return nil, fmt.Errorf("spaceimpl: record acl head: %w", err)
	}
	s.kickJoinController()
	return nil, ErrJoinPending
}

// joiningLocalStatus is the localStatus value the SDK stamps on a
// space-index entry while a RequestToJoin is still pending owner
// approval. Maps to space.StatusJoining via mapStatus.
const joiningLocalStatus = "joining"

// guestLoadingLocalStatus is the DEVICE-LOCAL localStatus stamped by
// JoinGuest while this device is still pulling the guest space's
// content. Crash-recoverable exactly like inviteLoading: the join
// controller's boot/tick pass resumes the load (no ACL waiter — the
// shared guest identity is already an ACL member); the load-complete
// flip clears it to active.
const guestLoadingLocalStatus = "guestLoading"

// guestRevokedLocalStatus is the DEVICE-LOCAL localStatus the ACL
// mirror stamps on a guest-mode row when the shared guest identity has
// been removed from the ACL (owner revoked public access). Maps to
// space.StatusGuestRevoked. Non-terminal: the mirror flips it back to
// active if a fresh ACL shows the identity active again.
const guestRevokedLocalStatus = "guestRevoked"

// JoinGuest adds a space via a guest invite — see space.Service.JoinGuest.
// No ACL write and no approval: the invite key IS the shared read-only
// guest identity, already an active ACL member. The row is recorded
// durably (synced guest key + device-local loading marker) before the
// single bounded load attempt, so a crash or offline start resumes in
// the join controller.
func (s *Service) JoinGuest(ctx context.Context, invite string) (space.Space, error) {
	if invite == "" {
		return nil, errors.New("spaceimpl: JoinGuest: invite required")
	}
	inv, err := space.DecodeInvite(invite)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: JoinGuest: %w", err)
	}
	if inv.Kind != space.InviteKindGuest {
		return nil, errors.New("spaceimpl: JoinGuest: not a guest invite — use Join")
	}
	encoded, err := crypto.EncodeKeyToString(inv.InviteKey)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: JoinGuest: encode guest key: %w", err)
	}
	if rec, ok := s.tsp.Get(ctx, inv.SpaceId); ok {
		rejoin := rec.GuestKey != "" && rec.RemoteStatus == techspace.GuestDeletedRemoteStatus &&
			rec.LocalStatus != techspace.StatusDeleted
		if rec.IsDeleted() && !rejoin {
			return nil, fmt.Errorf("spaceimpl: JoinGuest: space %q was deleted on this account (tombstones are sticky)", inv.SpaceId)
		}
		if rec.GuestKey == "" {
			return nil, fmt.Errorf("spaceimpl: JoinGuest: space %q is already tracked by this account — guest access would demote it", inv.SpaceId)
		}
		// Guest row already exists (another device joined, a prior
		// attempt, or a guestDeleted row being re-added) — refresh the
		// key when the invite carries a rotated one (owner revoked +
		// re-created), then (re)drive the load.
		if rec.GuestKey != encoded {
			if _, err := s.tsp.SetGuestKey(ctx, inv.SpaceId, encoded); err != nil {
				return nil, fmt.Errorf("spaceimpl: JoinGuest: refresh guest key: %w", err)
			}
			// The loaded runtime (commonspace AND the Store's cached
			// objects/trees) still signs and decrypts as the old
			// identity; tear it all down so the next load rebuilds with
			// the fresh key. Disk state stays.
			s.closeSpaceRuntime(ctx, inv.SpaceId)
		}
		if rejoin {
			// Un-delete AFTER the loading marker is durable, mirroring
			// AcceptInvite: a crash between the two writes must leave a
			// resumable marker, never an account-wide un-delete nobody
			// finishes. loadAcceptedInvite completes the flip when it
			// finds the marker with the row still guestDeleted.
			if _, err := s.tsp.SetLocalStatus(ctx, inv.SpaceId, guestLoadingLocalStatus); err != nil {
				return nil, fmt.Errorf("spaceimpl: JoinGuest: mark loading: %w", err)
			}
			if _, err := s.tsp.SetRemoteStatus(ctx, inv.SpaceId, techspace.StatusActive); err != nil {
				return nil, fmt.Errorf("spaceimpl: JoinGuest: un-delete: %w", err)
			}
		}
	} else {
		if _, err := s.tsp.Add(ctx, techspace.SpaceIndexRecord{
			Id:           inv.SpaceId,
			Type:         space.SpaceTypeRegular,
			RemoteStatus: techspace.StatusActive,
			GuestKey:     encoded,
		}); err != nil {
			return nil, fmt.Errorf("spaceimpl: JoinGuest: write index entry: %w", err)
		}
	}
	if _, err := s.tsp.SetLocalStatus(ctx, inv.SpaceId, guestLoadingLocalStatus); err != nil {
		return nil, fmt.Errorf("spaceimpl: JoinGuest: mark loading: %w", err)
	}
	sp, err := s.load(ctx, inv.SpaceId)
	if err != nil {
		// Content not pullable yet (or offline). The join controller owns
		// the retry; the guest row itself is already durable.
		s.kickJoinController()
		return nil, space.ErrGuestJoinPending
	}
	if _, err := s.tsp.SetLocalStatus(ctx, inv.SpaceId, techspace.StatusActive); err != nil {
		return nil, fmt.Errorf("spaceimpl: JoinGuest: flip active: %w", err)
	}
	s.kickJoinController() // reap any pending-load bookkeeping
	return sp, nil
}

// inviteLoadingLocalStatus is the DEVICE-LOCAL localStatus stamped by
// AcceptInvite while this device is still pulling the accepted space's
// content. Crash-recoverable: the join controller's boot/tick pass
// resumes the load (no ACL waiter — the account is already a member);
// loadJoinedSpace clears it to active. Maps to StatusActive via
// mapStatus' default — the synced accept already happened, loading is a
// device detail, and the account's other devices show Active too.
const inviteLoadingLocalStatus = "inviteLoading"

// AcceptInvite approves a direct-add invite by space id: flips the
// SYNCED row status to active (every device converges — declined is
// non-terminal, so this also un-declines) and loads the space. The
// account is already an ACL member, so unlike Join there is nothing to
// wait for on the ACL — just a pull-until-available load. One bounded
// synchronous attempt is made; if the content isn't pullable yet
// (add still propagating, offline) it returns (nil,
// ErrInviteAcceptPending) and the join controller finishes the load
// durably in the background.
func (s *Service) AcceptInvite(ctx context.Context, spaceId string) (space.Space, error) {
	rec, ok := s.tsp.Get(ctx, spaceId)
	if !ok {
		return nil, fmt.Errorf("spaceimpl: AcceptInvite: unknown space %q", spaceId)
	}
	if rec.Type == space.SpaceTypeOneToOne {
		return nil, fmt.Errorf("spaceimpl: AcceptInvite: %q is a 1-1 space — use AcceptOneToOne", spaceId)
	}
	if rec.IsDeleted() {
		return nil, fmt.Errorf("spaceimpl: AcceptInvite: space %q is deleted", spaceId)
	}
	if rec.LocalStatus == joiningLocalStatus {
		// A token-join awaiting owner approval rides the same
		// remote=active row shape (Join stamps it at request time);
		// accepting it here would clobber the joining marker and tear
		// down its ACL waiter, silencing an eventual owner decline.
		return nil, fmt.Errorf("spaceimpl: AcceptInvite: space %q is awaiting join approval, not a direct-add invite", spaceId)
	}
	switch rec.RemoteStatus {
	case techspace.InvitePendingRemoteStatus, techspace.InviteDeclinedRemoteStatus:
	case techspace.StatusActive:
		// Idempotent re-accept / resume of an interrupted load — but only
		// from a plain active/loading row; any other device-local
		// lifecycle (offloaded, …) is not this API's business.
		switch rec.LocalStatus {
		case "", techspace.StatusActive, inviteLoadingLocalStatus:
		default:
			return nil, fmt.Errorf("spaceimpl: AcceptInvite: space %q is not invite-pending", spaceId)
		}
	default:
		return nil, fmt.Errorf("spaceimpl: AcceptInvite: space %q is not invite-pending", spaceId)
	}
	// Persist the loading obligation BEFORE the synced accept flip: a
	// crash between the two writes must leave a resumable marker, never a
	// durable account-wide accept nobody finishes. loadAcceptedInvite
	// completes the remote flip when it finds the marker with the row
	// still invite-pending.
	if rec.LocalStatus != techspace.StatusActive {
		if _, err := s.tsp.SetLocalStatus(ctx, spaceId, inviteLoadingLocalStatus); err != nil {
			return nil, fmt.Errorf("spaceimpl: AcceptInvite: mark loading: %w", err)
		}
	}
	if rec.RemoteStatus != techspace.StatusActive {
		if _, err := s.tsp.SetRemoteStatus(ctx, spaceId, techspace.StatusActive); err != nil {
			return nil, fmt.Errorf("spaceimpl: AcceptInvite: %w", err)
		}
	}
	sp, err := s.Get(ctx, spaceId)
	if err != nil {
		// Content not pullable yet (or offline). The join controller owns
		// the retry; the accept itself is already durable.
		s.kickJoinController()
		return nil, ErrInviteAcceptPending
	}
	if _, err := s.tsp.SetLocalStatus(ctx, spaceId, techspace.StatusActive); err != nil {
		return nil, fmt.Errorf("spaceimpl: AcceptInvite: flip active: %w", err)
	}
	s.kickJoinController() // reap any pending-load bookkeeping
	return sp, nil
}

// ErrInviteAcceptPending is returned by AcceptInvite when the accept was
// recorded (synced account-wide) but the space content is not pullable
// yet. Loading continues durably in the background and across restarts;
// callers poll List/Get or Subscribe for the flip to StatusActive.
var ErrInviteAcceptPending = errors.New("spaceimpl: invite accepted; space load pending")

// DeclineInvite rejects a direct-add invite. Writes the synced, sticky
// InviteDeclined marker so the request is suppressed on every device; a
// later AcceptInvite overrides it (non-terminal). No ACL write happens —
// the account stays a member on the space's ACL — and nothing was
// materialized, so there is nothing to offload.
func (s *Service) DeclineInvite(ctx context.Context, spaceId string) error {
	rec, ok := s.tsp.Get(ctx, spaceId)
	if !ok {
		return fmt.Errorf("spaceimpl: DeclineInvite: unknown space %q", spaceId)
	}
	if rec.Type == space.SpaceTypeOneToOne {
		return fmt.Errorf("spaceimpl: DeclineInvite: %q is a 1-1 space — use DeclineOneToOne", spaceId)
	}
	switch rec.RemoteStatus {
	case techspace.InviteDeclinedRemoteStatus:
		return nil // idempotent
	case techspace.InvitePendingRemoteStatus:
	default:
		return fmt.Errorf("spaceimpl: DeclineInvite: space %q is not invite-pending", spaceId)
	}
	if _, err := s.tsp.SetRemoteStatus(ctx, spaceId, techspace.InviteDeclinedRemoteStatus); err != nil {
		return fmt.Errorf("spaceimpl: DeclineInvite: %w", err)
	}
	return nil
}

// Close stops per-space subsystems the SDK owns directly — currently
// just members watchers (one polling goroutine each). The any-sync
// side of each space is owned by the App's space cache and torn down
// separately by App.Close.
func (s *Service) Close(_ context.Context) error {
	// Stop spawning new lazy-seed goroutines, cancel any in flight, and
	// wait for them to return before the caller tears down the store /
	// tech space — otherwise a background seed can race teardown
	// (techspace.Service.Get vs Close, store use-after-close).
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	s.seedCancel()
	s.seedWG.Wait()
	// Stop the 1-1 inbox notifier + send-retry loop before draining the
	// rest — both write the tech space, which Close tears down after this.
	s.stopOneToOneInbox()
	// Stop the deletion reconciler before draining watchers — it may
	// otherwise kick off an OffloadSpace (which stops watchers) during
	// shutdown.
	if s.delCancel != nil {
		s.delCancel()
	}
	s.delWG.Wait()
	// Stop the join controller and close any live ACL waiters before
	// draining members watchers — onFinish loads a space (which starts
	// watchers), so it must not run during shutdown.
	if s.joinCancel != nil {
		s.joinCancel()
	}
	s.joinWG.Wait()
	s.stopJoinWaiters()
	s.watchers.stopAll()
	return nil
}

// goSeed launches the spaceIndex lazy-seed in the background, tracked by
// seedWG and bound to seedCtx so Close can drain it. No-op once closing —
// keeps the WaitGroup from gaining work during shutdown.
func (s *Service) goSeed(sp *spaceImpl) {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return
	}
	s.seedWG.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.seedWG.Done()
		sp.maybeLazySeedSpaceIndex(s.seedCtx)
	}()
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
// tree announces a deletion for a tree not present in local storage.
// Reflects the deletion in local materialized state and drops the
// cached object so a future load reflects the deleted state.
// Tech-space defers to its own adapter (the index tree itself is
// never marked deleted).
func (s *Service) MarkTreeDeleted(ctx context.Context, spaceId, treeId string) error {
	if spaceId == s.tsp.SpaceId() {
		return s.tsp.MarkTreeDeleted(ctx, spaceId, treeId)
	}
	return s.storeFor(spaceId).MarkTreeDeleted(ctx, treeId)
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

// ShouldPullTree is the selective-sync pull decision for a
// locally-missing tree announced by a head update. The tech space is
// always fully synced; regular spaces delegate to their Store, which
// classifies by the update's root changeType and, when declining,
// refreshes the tree's heads-only stub so the sync diff converges.
func (s *Service) ShouldPullTree(ctx context.Context, spaceId, treeId string, root *treechangeproto.RawTreeChangeWithId, heads []string) bool {
	if spaceId == s.tsp.SpaceId() {
		return true
	}
	return s.storeFor(spaceId).ShouldPullTree(ctx, treeId, root, heads)
}

// Compile-time check that we satisfy the registry contract.
var _ anysyncx.SpaceRegistry = (*Service)(nil)

// mapStatus collapses (type, localStatus, remoteStatus) into the public
// space.Status enum.
func mapStatus(typ, local, remote string) space.Status {
	switch {
	case local == techspace.StatusDeleted:
		// Device-local delete — the user removed this space on THIS device.
		// Must report Deleted even when remote is still active, else a
		// locally-deleted space surfaces as Active and clients trusting the
		// mapped status re-adopt a space the user removed. Checked before
		// the remote case so a local delete always wins locally. (Regressed
		// when the index was Store-backed; this restores the prior case.)
		return space.StatusDeleted
	case remote == techspace.StatusDeleted:
		// Account-wide delete (synced) — propagated to every device.
		return space.StatusDeleted
	case remote == techspace.OneToOneDeletedStatus:
		// 1-1 delete (synced, non-terminal): offloaded everywhere but
		// re-creatable. Surfaced as Deleted; checked before the
		// declined/pending 1-1 cases.
		return space.StatusDeleted
	case remote == techspace.GuestDeletedRemoteStatus:
		// Guest-space delete (synced, non-terminal): offloaded
		// everywhere, re-addable via JoinGuest. Surfaced as Deleted.
		return space.StatusDeleted
	case remote == oneToOneDeclinedRemoteStatus:
		// Synced, sticky 1-1 decline — account-wide (could be declined on
		// another device). Checked before the pending/active cases.
		return space.StatusOneToOneDeclined
	case remote == techspace.InviteDeclinedRemoteStatus:
		// Synced, sticky direct-add decline — account-wide, non-terminal
		// (AcceptInvite overrides).
		return space.StatusInviteDeclined
	case typ == space.SpaceTypeOneToOne && remote == techspace.StatusActive:
		// Account-scoped resolution wins over a device-local pending. A 1-1
		// processed (accepted/initiated) on ANY device carries synced
		// remote=active; a stale pending — e.g. an old inbox invite replayed
		// on another/new device before the active row synced in — must not
		// shadow it. "Processed is account-scoped": the synced row is the
		// truth, the local pending is just this device's unresolved view.
		return space.StatusActive
	case local == oneToOnePendingLocalStatus:
		return space.StatusOneToOnePending
	case remote == techspace.InvitePendingRemoteStatus:
		// Synced direct-add pending: the account was added to the space's
		// ACL and no device has accepted or declined yet. Before the
		// joining/local cases — until the accept commits the synced active
		// flip, the account-wide truth is still "pending".
		return space.StatusInvitePending
	case local == joiningLocalStatus:
		return space.StatusJoining
	case local == guestRevokedLocalStatus:
		// Guest-mode space whose shared identity was removed from the
		// ACL. Device-local (each device's mirror detects it) and
		// non-terminal — the mirror self-heals back to active.
		return space.StatusGuestRevoked
	case typ == space.SpaceTypeOneToOne && local != techspace.StatusActive && remote != techspace.StatusActive:
		// A 1-1 row that exists but carries no active/declined signal and no
		// device-local pending: the row synced from the device that
		// registered the request before this device set its own status.
		// Surface it as an incoming request rather than Unknown.
		return space.StatusOneToOnePending
	case local == "" && remote == "":
		// localStatus is device-local and absent means active; remote
		// absent too means we have no info yet.
		return space.StatusUnknown
	default:
		return space.StatusActive
	}
}
