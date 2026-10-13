package anysyncsdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/object/keyvalue/keyvaluestorage"
	"github.com/anyproto/any-sync/identityrepo/identityrepoproto"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/util/crypto"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/auth"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/files/broker"
	"github.com/anyproto/any-sync-sdk/internal/files/fetch"
	"github.com/anyproto/any-sync-sdk/internal/files/filep2p"
	"github.com/anyproto/any-sync-sdk/internal/files/gc"
	"github.com/anyproto/any-sync-sdk/internal/files/status"
	filestore "github.com/anyproto/any-sync-sdk/internal/files/store"
	"github.com/anyproto/any-sync-sdk/internal/files/upload"
	"github.com/anyproto/any-sync-sdk/internal/pushclient"
	"github.com/anyproto/any-sync-sdk/internal/readstate"
	"github.com/anyproto/any-sync-sdk/internal/readsync"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/spacesync"
	"github.com/anyproto/any-sync-sdk/internal/subscribe"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/p2p"
	"github.com/anyproto/any-sync-sdk/space"
)

var log = logger.NewNamed("sdk")

// SDK is the top-level handle held by middleware for the lifetime of
// use. Constructed by Open; torn down by Close.
type SDK struct {
	app        *anysyncx.App
	db         anystore.DB
	tsp        *techspace.Service
	spaces     *spaceimpl.Service
	account    *accountImpl
	push       *pushclient.Service
	filesQueue *status.Queue
	filesGC    *gc.Service
	readSync   *readsync.Service
	// stopIndexWatch stops the tech-space index watcher that kicks the
	// join controller on every index change and, with p2p on, LAN
	// re-handshakes and the index follower when the known-space set
	// grows; nil in headless mode.
	stopIndexWatch func()
	// indexGrew wakes the index follower (one pending signal).
	indexGrew chan struct{}
	// followerDone is closed when the index follower exits; nil in
	// headless mode.
	followerDone chan struct{}

	// bootstrapCancel / bootstrapDone track the SDK-owned background
	// boot pass (see bootstrap): profile republish, the serial eager
	// space-loading + offline catch-up loop, and the read-state
	// reconcile. bootstrapDone is closed by the goroutine on exit;
	// Close cancels and joins it before any teardown. Both nil in
	// headless mode, which skips the pass entirely.
	bootstrapCancel context.CancelFunc
	bootstrapDone   chan struct{}
	// caughtUp is the watermark-snapshot ALLOWLIST: spaces whose sdk.db
	// projection is known current this session — caught up by a
	// successful boot Run, or born here (Create/Derive: the author
	// device materializes its own writes by definition). Absent means
	// dirty, the safe default — Close persists nothing and the next
	// boot replays. Joins/accepts are deliberately never marked: they
	// stay dirty until their first boot Run, the conservative choice
	// for the dangerous case (content pulled, not authored). Guarded by
	// caughtUpMu — the bootstrap goroutine and user Create/Derive calls
	// write concurrently.
	caughtUpMu sync.Mutex
	caughtUp   map[string]struct{}
}

// markCaughtUp adds spaceId to the watermark-snapshot allowlist.
func (s *SDK) markCaughtUp(spaceId string) {
	s.caughtUpMu.Lock()
	if s.caughtUp == nil {
		s.caughtUp = map[string]struct{}{}
	}
	s.caughtUp[spaceId] = struct{}{}
	s.caughtUpMu.Unlock()
}

// caughtUpIds snapshots the allowlist for iteration.
func (s *SDK) caughtUpIds() []string {
	s.caughtUpMu.Lock()
	defer s.caughtUpMu.Unlock()
	ids := make([]string, 0, len(s.caughtUp))
	for id := range s.caughtUp {
		ids = append(ids, id)
	}
	return ids
}

// bootstrapClosed is the pre-closed BootstrapDone result for SDK
// handles without a background boot pass (headless mode, zero handle).
var bootstrapClosed = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// BootstrapDone returns a channel closed once the background boot pass
// has finished (or immediately for a headless Open). "Done" means no
// longer running — it also closes when Close cancels an in-flight
// pass. Local reads (Spaces().List, queries against loaded spaces)
// never need to wait on it; select on it when you need full offline
// catch-up — every space loaded and replayed up to what the sync nodes
// hold. Until a space's turn comes, queries against it serve the
// pre-offline state (the per-object lazy ColdRestore still covers
// direct Gets).
func (s *SDK) BootstrapDone() <-chan struct{} {
	if s.bootstrapDone == nil {
		return bootstrapClosed
	}
	return s.bootstrapDone
}

// FileCacheSize returns the local bytes currently held by file content
// across all spaces (complete + partial copies; inline files hold no
// cache bytes).
func (s *SDK) FileCacheSize(ctx context.Context) (int64, error) {
	return s.filesGC.CacheSize(ctx)
}

// FreeUpFileCache reclaims local file bytes until at least `bytes` are
// freed, least-recently-used first, dropping only content that is safe
// to drop (backed up on the network — a later Open refetches — or no
// longer referenced by any file). Returns the bytes actually freed,
// which is less than requested when nothing else is safely evictable.
func (s *SDK) FreeUpFileCache(ctx context.Context, bytes int64) (freed int64, err error) {
	return s.filesGC.FreeUp(ctx, bytes)
}

// SweepFileCache runs one file-cache safety pass: prunes references of
// deleted files, deletes content no file references anymore (past a
// grace period), and drops long-untouched partial downloads of
// backed-up files. Never touches content that is not safely
// refetchable. This is the manual trigger; the same pass runs
// periodically only when cfg.Files.GCInterval is set.
func (s *SDK) SweepFileCache(ctx context.Context) error {
	return s.filesGC.Sweep(ctx)
}

// Open brings up the SDK: initializes auth, opens storage, boots any-sync,
// derives the tech space, and returns a ready handle. Open is serial
// local I/O with no synchronous network dependency.
//
// Contract: when Open returns, local reads are safe — Spaces().List,
// queries against loaded spaces, Create/Get/Modify all work. The
// account-facing boot work (eager space loading, offline catch-up
// replay, profile republish, read-state reconcile) runs on ONE
// SDK-owned background goroutine, strictly serial, started here and
// cancelled+joined by Close; BootstrapDone exposes its completion.
// Until a space's turn comes, queries against it serve the pre-offline
// state (the per-object lazy ColdRestore still covers direct Gets).
//
// Storage layout:
//
//	<DataDir>/anysync/<spaceId>.db   — any-sync per-space state (any-store v1)
//	<DataDir>/sdk.db                 — SDK CRDT collections (any-store v2, shared)
//
// any-sync uses v1 internally for its tree storage; the SDK uses v2
// for everything it owns (CRDT controller collections, _meta watermark,
// type registry, _detached parked changes, per-space objects values).
// The two coexist via the /v2 module path.
func Open(ctx context.Context, cfg config.Config, provider auth.Provider) (*SDK, error) {
	if cfg.Storage.DataDir == "" {
		return nil, errors.New("anysyncsdk: Storage.DataDir is required")
	}
	if err := spaceobjects.ValidateExternalTypes(cfg.Types); err != nil {
		return nil, fmt.Errorf("anysyncsdk: %w", err)
	}
	if err := spaceobjects.ValidateExternalCollections(cfg.Types, cfg.Collections, cfg.Modules); err != nil {
		return nil, fmt.Errorf("anysyncsdk: %w", err)
	}
	if err := spaceobjects.ValidateExternalModules(cfg.Types, cfg.Modules); err != nil {
		return nil, fmt.Errorf("anysyncsdk: %w", err)
	}

	// any-sync stores its per-space state under <DataDir>/anysync (v1
	// DB, owned by any-sync). The SDK's CRDT state lives at
	// <DataDir>/sdk.db (v2 DB, owned by us) — separate file so the
	// two any-store versions don't share schema/state.
	cfg.Storage.DataDir = filepath.Join(cfg.Storage.DataDir, "anysync")

	app, err := anysyncx.New(ctx, cfg, provider)
	if err != nil {
		return nil, err
	}

	sdkDBPath := filepath.Join(filepath.Dir(cfg.Storage.DataDir), "sdk.db")
	InitPageBuffer()
	db, err := anystore.Open(ctx, sdkDBPath, &anystore.Config{
		UseGlobalPageBuffer: true,
		// One shared DB serves every space's reads; keep the engine's
		// CPU scaling but never drop below 8 concurrent readers.
		ReadConcurrency: max(runtime.NumCPU(), 8),
		// Without the idle flush, checkpoints only happen when the WAL
		// hits the commit-path threshold (10000 frames ≈ 40 MB) — a busy
		// DB sits under a huge WAL that must be replayed on every open.
		// Idle checkpointing keeps it bounded; the sentinel adds a
		// quick-check after unclean shutdowns.
		Durability: anystore.DurabilityConfig{
			AutoFlush: true,
			IdleAfter: 20 * time.Second,
			FlushMode: anystore.FlushModeCheckpointPassive,
			Sentinel:  true,
		},
	})
	if err != nil {
		_ = app.Close(ctx)
		return nil, fmt.Errorf("anysyncsdk: open sdk db: %w", err)
	}

	tsp := techspace.New(app, db)
	spaces := spaceimpl.New(app, tsp, tsp, db, cfg.Types, cfg.Collections, cfg.Modules)
	// Per-space p2p advertising: the tech-space row's switch, on unless
	// set off; the tech space itself never carries a device row (own
	// devices come from the account record). Wired before tsp.Open loads
	// the tech space, so its load publishes nothing.
	app.SetAdvertiseFn(func(spaceId string) bool {
		if spaceId == tsp.SpaceId() {
			return false
		}
		rec, ok := tsp.Get(context.Background(), spaceId)
		return !ok || rec.P2PAdvertise
	})

	// Push notifications (SYN-47): the push node is a direct out-of-band
	// peer — register its dial addresses so the pool can reach it, then
	// bind the client to spaceimpl's ACL-derived key provider. The
	// service is ALWAYS constructed; unconfigured, its transport is nil
	// and every method returns space.ErrPushNotConfigured at call time.
	var pushTransport *pushclient.Client
	if cfg.Push.PeerId != "" && len(cfg.Push.Addrs) > 0 {
		app.SetPeerAddrs(cfg.Push.PeerId, cfg.Push.Addrs)
		pushTransport = pushclient.NewClient(app.Pool(), cfg.Push.PeerId)
	}
	push := pushclient.NewService(pushTransport, spaces, app.AccountKeys())

	// Files byte layer (SYN-25/27/28): CARv2s under <DataDir>/files (a
	// sibling of anysync/ and sdk.db — cfg.Storage.DataDir was
	// re-pointed to anysync/ above), metadata in the shared SDK DB.
	// Uploads go through the fileV2 broker; downloads are public-read
	// first ({base}/blob/…), with the base resolved once via the broker
	// Info RPC and persisted (or pinned by cfg.Files.PublicReadBaseUrl).
	filesRoot := filepath.Join(filepath.Dir(cfg.Storage.DataDir), "files")
	filesStore, err := filestore.New(ctx, filesRoot, db)
	if err != nil {
		_ = db.Close()
		_ = app.Close(ctx)
		return nil, fmt.Errorf("anysyncsdk: open files store: %w", err)
	}
	filesBroker := broker.New(app.Pool(), app.FileV2Peers, app.NetworkId(), app.FileNetworkId)
	baseURL := fetch.NewBaseURL(filesStore, app.NetworkId(), cfg.Files.PublicReadBaseUrl,
		func(ctx context.Context) (string, error) {
			info, err := filesBroker.Info(ctx)
			if err != nil {
				return "", err
			}
			return info.PublicReadBaseUrl, nil
		})
	filesUpload := upload.New(filesStore, filesBroker)
	// The persistent files work queue (SYN-29): drive-toward-durable
	// retries + Pin background fetches, surviving restarts. Runs after
	// spaces exist (jobs resolve rows through them); closed first.
	filesQueue, err := status.NewQueue(ctx, db, spaces.RunFileJob, spaces.OnFileJobChange)
	if err != nil {
		_ = db.Close()
		_ = app.Close(ctx)
		return nil, fmt.Errorf("anysyncsdk: open files queue: %w", err)
	}
	filesUpload.SetQueue(filesQueue)
	filesFetch := fetch.New(filesStore, baseURL)
	// P2P files (SYN-48): serve our stored CAR objects to LAN peers and
	// prefer a LAN peer over the public GET when fetching. Rides the
	// existing p2p toggle; the peer store + DRPC server come from the app.
	if app.P2PEnabled() || app.GlobalP2PEnabled() {
		filesFetch.SetPeer(filep2p.NewSource(app.Pool(), app.PeerStore()))
		// The server was registered on the DRPC mux during app start
		// (as a component, to avoid a serving race); hand it the store now.
		app.SetFileStore(filesStore)
	}
	spaces.SetFiles(filesUpload, filesFetch, filesStore, filesQueue)
	// Cache reclamation (SYN-26): fully embedder-driven —
	// FileCacheSize/FreeUpFileCache/SweepFileCache and per-file
	// Offload. The periodic safety sweep runs ONLY when configured
	// (cfg.Files.GCInterval > 0); the default is no background GC.
	filesGC := gc.New(filesStore, spaces, func(ctx context.Context, spaceId, fileId string) error {
		return filesQueue.Enqueue(ctx, status.KindDurable, spaceId, fileId)
	})
	// Wire spaceimpl.Service as the space registry so any-sync's
	// treemanager-driven callbacks (deletion-manager DeleteTree,
	// space-sync PutTree, head-sync GetTree for arbitrary trees)
	// route to the per-space Store. spaceimpl internally delegates
	// the tech-space's own indexId back to techspace.
	app.SetSpaceRegistry(spaces)

	if err := tsp.Open(ctx); err != nil {
		_ = db.Close()
		_ = app.Close(ctx)
		return nil, fmt.Errorf("anysyncsdk: open techspace: %w", err)
	}
	// The CRDT version mark: an account a newer SDK has written refuses
	// to open (space.ErrCRDTVersionNewer); an older or unmarked one is
	// stamped with this SDK's version. Before any other boot work so a
	// refused account is touched by nothing.
	if err := tsp.EnsureCRDTVersion(ctx); err != nil {
		_ = tsp.Close(ctx)
		_ = db.Close()
		_ = app.Close(ctx)
		return nil, fmt.Errorf("anysyncsdk: %w", err)
	}
	// Orphan-collection GC: drop CRDT collections whose owner space /
	// object no longer exists — heals interrupted offloads and
	// historical purge leaks. Deliberately synchronous, unlike the
	// eager loop (see bootstrap): its safety argument is "only the tech
	// space is open, no applies are running, no caller holds the SDK
	// handle yet", which only this spot provides — and at 1.6-40ms
	// measured it costs Open nothing. Best-effort, never fails Open.
	spaces.SweepOrphanCollections(ctx)
	// Guest-identity resolver for guest-mode (public-access) spaces:
	// loadSpaceForCache opens a space whose row carries a guest key with
	// an account-service override, signing as that shared identity. Must
	// be wired before any space load — guest rows are only ever loaded
	// after the tech space is open, so this spot qualifies for both the
	// headless and regular paths.
	app.SetGuestKeyFn(func(spaceId string) crypto.PrivKey {
		rec, ok := tsp.Get(context.Background(), spaceId)
		if !ok || rec.GuestKey == "" {
			return nil
		}
		key, err := crypto.DecodeKeyFromString(rec.GuestKey, crypto.UnmarshalEd25519PrivateKey, nil)
		if err != nil {
			return nil
		}
		return key
	})
	// Start the background workers only after the last fallible Open
	// step — a failed Open must not leak goroutines polling a closed DB.
	filesQueue.Run()
	filesGC.Run(cfg.Files.GCInterval)
	account := newAccountImpl(app, tsp, spaces)

	// Headless: skip the account-facing boot work below — profile
	// republish, the 1-1 inbox, identity resolution, pending-join
	// resume, and the eager space-loading loop. A broker tracks foreign
	// spaces and opens them on demand via Get; nothing account-shaped
	// exists to resume, and eager-loading every tracked space defeats
	// open/close-on-demand. (The tech space itself was already pinned
	// local-only inside tsp.Open.)
	if cfg.Headless {
		return &SDK{
			app:        app,
			db:         db,
			tsp:        tsp,
			spaces:     spaces,
			account:    account,
			push:       push,
			filesQueue: filesQueue,
			filesGC:    filesGC,
		}, nil
	}

	// Read-state sync: merge other devices' published read frontiers
	// (tech-space KV) into the per-space readstate engines, and publish
	// local marks. Wired before any account-facing boot step —
	// ResumePendingJoins can complete a join and load a space, and that
	// space's first-load seed must already see the provider. EngineFor
	// is gated on tech-space liveness (readSyncEngineFor) so merges for
	// deleted / pending / storage-less spaces are dropped instead of
	// building a Store for a dead space. Live hook here + idempotent
	// per-space Reconcile in the bootstrap pass.
	readSync := readsync.New(
		readSyncEngineFor(
			func(spaceId string) (techspace.SpaceIndexRecord, bool) {
				return tsp.Get(context.Background(), spaceId)
			},
			app.SpaceExists,
			func(spaceId string) *readstate.Engine {
				st := spaces.StoreFor(spaceId)
				if st == nil {
					return nil
				}
				return st.ReadState()
			},
		),
		func(ctx context.Context) (keyvaluestorage.Storage, error) {
			return app.KeyValueStore(ctx, tsp.SpaceId())
		},
		app.AccountKeys().PeerKey.GetPublic().PeerId(),
	)
	app.OnKeyValues(tsp.SpaceId(), readSync.OnKeyValues)
	spaces.SetReadSync(readSync)

	// Resume any join left pending from a previous session now that the
	// tech space is open (the join controller started in spaceimpl.New,
	// before this point, so its initial scan saw an empty index).
	spaces.ResumePendingJoins()

	// Start the Layer-2 1-1 inbox subsystem now that the tech space is
	// open: the receive notifier (coordinator push + poll → pending rows)
	// and the send-retry loop. No-op when the inbox transport is
	// unavailable; the out-of-band 1-1 path works without it.
	spaces.StartOneToOneInbox(ctx)

	// Cold-sync resolve: a fresh device syncs the identities directory's
	// symkeys but no profiles (those are device-local). Batch-fetch the
	// missing profiles from identityRepo in the background. The
	// goroutine is owned by spaceimpl (seedCtx/seedWG), so spaces.Close
	// cancels and drains it instead of racing teardown.
	spaces.ResolveIdentityProfilesAsync()

	sdk := &SDK{
		app:        app,
		db:         db,
		tsp:        tsp,
		spaces:     spaces,
		account:    account,
		push:       push,
		filesQueue: filesQueue,
		filesGC:    filesGC,
		readSync:   readSync,
		indexGrew:  make(chan struct{}, 1),
	}
	sdk.registerIndexWatch()
	// Born-clean: a space created or derived this session enters the
	// watermark allowlist directly — see SDK.caughtUp.
	spaces.SetOnSpaceBorn(sdk.markCaughtUp)

	// Open is done — local reads are safe from here. The account-facing
	// boot work (profile republish, eager space loading + offline
	// catch-up, read-state reconcile) runs in ONE background goroutine
	// owned by the SDK. Its ctx derives from Background, not from
	// Open's ctx: the latter is the caller's startup window and is
	// routinely cancelled right after Open returns, while the bootstrap
	// pass must keep going until it finishes or Close cancels+joins it.
	bootstrapCtx, cancel := context.WithCancel(context.Background())
	sdk.bootstrapCancel = cancel
	sdk.bootstrapDone = make(chan struct{})
	go func() {
		defer close(sdk.bootstrapDone)
		// Every bootstrap step is best-effort by contract; a panic in
		// one must not kill the process (pre-rework it would have
		// surfaced inside Open where the embedder could recover). Log
		// and finish — bootstrapDone still closes via the outer defer,
		// and untouched spaces simply stay off the caughtUp allowlist.
		defer func() {
			if r := recover(); r != nil {
				log.Error("bootstrap: panic recovered",
					zap.Any("panic", r), zap.ByteString("stack", debug.Stack()))
			}
		}()
		sdk.bootstrap(bootstrapCtx)
	}()
	// The index follower pulls spaces the index learns of after the
	// boot pass — a recovering device gets its space list from a
	// sibling long after Open. Serial with the pass: it starts once the
	// pass is done.
	sdk.followerDone = make(chan struct{})
	go sdk.followIndex(bootstrapCtx)
	return sdk, nil
}

// followIndex waits for the boot pass, then pulls every space the
// tech-space index names but this device does not hold, each time the
// index grows (coalesced) — one space at a time, best-effort, until
// Close cancels it.
func (s *SDK) followIndex(ctx context.Context) {
	defer close(s.followerDone)
	select {
	case <-ctx.Done():
		return
	case <-s.bootstrapDone:
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.indexGrew:
		}
		s.pullMissingSpaces(ctx)
	}
}

// kickIndexFollower asks the follower for a pass; pending kicks coalesce.
func (s *SDK) kickIndexFollower() {
	if s.indexGrew == nil {
		return
	}
	select {
	case s.indexGrew <- struct{}{}:
	default:
	}
}

// pullMissingSpaces pulls the spaces the index names, is not blocked on
// and this device holds no storage for. A pull needs a source — a node
// or a direct peer that has the space; a failure is retried on the next
// index change.
func (s *SDK) pullMissingSpaces(ctx context.Context) {
	for _, boot := range s.tsp.List(ctx) {
		if ctx.Err() != nil {
			return
		}
		rec, ok := s.tsp.Get(ctx, boot.Id)
		if !ok || rec.IsDeleted() || spaceimpl.MaterializeBlock(rec) != nil || s.app.SpaceExists(rec.Id) {
			continue
		}
		if _, err := s.spaces.Get(ctx, rec.Id); err != nil && ctx.Err() == nil {
			log.Debug("index follower: pull space", zap.String("spaceId", rec.Id), zap.Error(err))
		}
	}
}

// bootstrapTestHook, when non-nil, runs first inside bootstrap — a
// test-only seam to hold the pass open and observe its cancellation.
// Always nil in production.
var bootstrapTestHook func(ctx context.Context)

// bootstrap is the SDK-owned background boot pass, started by Open and
// cancelled+joined by Close before any teardown. STRICTLY SERIAL on
// purpose: concurrent commonspace builds spike RAM/CPU exactly on the
// constrained devices this pass exists for (mobile foregrounding —
// iOS jetsam kills over memory spikes); one space at a time keeps the
// boot footprint flat. Every step is best-effort — errors are logged,
// never fatal (and not worth warning about when the ctx was cancelled
// by Close).
func (s *SDK) bootstrap(ctx context.Context) {
	if h := bootstrapTestHook; h != nil {
		h(ctx)
	}
	warn := func(msg string, err error, fields ...zap.Field) {
		if err != nil && ctx.Err() == nil {
			log.Warn("bootstrap: "+msg, append(fields, zap.Error(err))...)
		}
	}

	// Republish the locally-stored profile to identityRepo on every
	// boot. Heart's ownProfileSubscription does the equivalent (reads
	// the local profile object, calls IdentityRepoPut). Without this,
	// other peers only see our profile after we explicitly call
	// UpdateMetadata in this process — which never happens for a
	// freshly-started client that's only re-loading existing state.
	//
	// Best-effort: a failed push doesn't prevent SDK use. The next
	// successful push (next UpdateMetadata or next boot) heals it.
	warn("republish profile", s.account.republishStoredProfile(ctx))
	s.account.startProfileSync()

	// Eager-load every non-deleted space from the tech-space index so
	// per-space headsync / syncacl start running at boot rather than
	// waiting for the first caller-driven access. Combined with the
	// disabled space-cache TTL (anysyncx/spacecache.go), this means
	// every joined space stays subscribed for the SDK's lifetime —
	// idle peers receive ACL updates and pushed changes without anyone
	// touching them first.
	//
	// Best-effort per space: a single failure (e.g. corrupted local
	// storage for one space) is logged and skipped so the rest of the
	// spaces still load.
	//
	// Each row is re-read fresh before acting on it: the pass runs
	// concurrently with user calls, and GetSpace below bypasses
	// Service.Get's tombstone guard — acting on a stale active row
	// after a mid-session Delete would re-pull the space from the nodes
	// and resurrect it until the next boot. The fresh read shrinks that
	// race to one iteration's processing time; the airtight fix is a
	// tombstone guard on the load path itself — follow-up material.
	for _, boot := range s.tsp.List(ctx) {
		if ctx.Err() != nil {
			// Close cancelled the pass; the remaining spaces never
			// enter the caughtUp allowlist, so the next boot replays.
			return
		}
		rec, ok := s.tsp.Get(ctx, boot.Id)
		if !ok {
			// Row unreadable — act on nothing rather than stale data.
			continue
		}
		// Skip deletion tombstones. Both delete paths — local
		// Service.Delete and inbound reconcile — record the delete via
		// the SYNCED RemoteStatus field (LocalStatus is never set to
		// "deleted"). Eager-loading a deleted space rebuilds its
		// commonspace and storage and starts periodic headsync, which
		// the node rejects with "space is deleted". If local storage
		// still lingers (e.g. an offload that was interrupted, or one
		// re-created by a previous build that eager-loaded tombstones),
		// reclaim it now — OffloadSpace is idempotent and best-effort.
		// Includes the 1-1 synced offload marker (oneToOneDeleted) — a
		// deleted 1-1 must be offloaded, not eager-loaded, on every device.
		// An ended join is skipped but its storage is kept: storage under
		// that marker is the accept-vs-cancel race (one device loaded the
		// accepted space while another's withdrawal won the row), and the
		// join controller reloads it when the ACL grants membership —
		// offloading here would discard a member's local copy.
		if rec.IsDeleted() {
			if s.app.SpaceExists(rec.Id) && !rec.JoinEnded() {
				s.spaces.OffloadSpace(ctx, rec.Id)
			}
			continue
		}
		// Skip not-yet-accepted rows (pending join / incoming 1-1 /
		// direct-add invite) by STATUS, not by storage existence: those
		// normally have no storage, but a build that predates the
		// Service.Get pending guard may have materialized one. Eager-
		// loading it would run headsync + treesyncer against a space
		// this account holds no read key for — every tree parks on "no
		// read key" and retries forever. The storage is left in place;
		// acceptance loads it (and its already-pulled changes) as usual.
		if spaceimpl.MaterializeBlock(rec) != nil {
			continue
		}
		if !s.app.SpaceExists(rec.Id) {
			continue
		}
		if _, err := s.app.GetSpace(ctx, rec.Id); err != nil {
			warn("eager-load space", err, zap.String("spaceId", rec.Id))
			continue
		}
		// Catch up any trees that advanced while we were offline (or
		// that we have never opened). Best-effort: a failure here
		// shouldn't block boot for the rest of the spaces — the
		// per-object lazy ColdRestore on first user touch still works.
		if err := spacesync.Run(ctx, s.app, s.db, s.spaces.StoreFor(rec.Id), rec.Id); err != nil {
			warn("space catch-up", err, zap.String("spaceId", rec.Id))
		} else {
			// Caught up: from here every head-store advance for this
			// space is applied live, so Close may snapshot its
			// watermark.
			s.markCaughtUp(rec.Id)
		}
		// Backstop the sdk.db-rebuild deletion gap: purge any local row for
		// an object any-sync has flipped to Deleted (and stamp the consumer
		// deletion feed). Best-effort; runs after Run so it also cleans
		// anything the forward catch-up or a lazy Get re-materialized.
		warn("reconcile deletions", spacesync.ReconcileDeletions(ctx, s.app, s.db, s.spaces.StoreFor(rec.Id), rec.Id),
			zap.String("spaceId", rec.Id))
	}

	// Replay published read frontiers through the idempotent merge —
	// covers marks made by other devices while this one was offline and
	// live-hook drops. Must run after the eager loop: merges route
	// through engineFor, which only resolves spaces the loop's
	// Run/StoreFor established tracking for. One pass over the
	// tech-space store for ALL spaces; cheap when nothing changed.
	warn("reconcile read state", s.readSync.ReconcileAll(ctx))
}

// registerIndexWatch wires the tech-space index change hooks. Always:
// the join controller is kicked so a join requested on another device
// (its row synced in as joining) gets its ACL waiter here promptly, and
// a verdict synced in stops one. With p2p on, the LAN cold-restore
// hooks too: the p2p exchange probes for spaces this account knows of
// but hasn't pulled yet, and known LAN peers are re-handshaken whenever
// the index grows (a fresh device learns a space id and wants a pull
// source right away). Cheap and local-only — one sub on the tech-space
// engine, no network; the controller throttles its own chain probes —
// so it runs on Open's fast path.
func (s *SDK) registerIndexWatch() {
	lan, global := s.app.P2PEnabled(), s.app.GlobalP2PEnabled()
	if lan {
		s.app.SetKnownSpaceIdsFn(func() []string {
			recs := s.tsp.List(context.Background())
			ids := make([]string, 0, len(recs))
			for _, rec := range recs {
				if !rec.IsDeleted() {
					ids = append(ids, rec.Id)
				}
			}
			return ids
		})
	}
	s.stopIndexWatch = watchSpaceIndex(s.tsp, func() {
		s.spaces.ResumePendingJoins()
		if !lan && !global {
			return
		}
		if lan {
			s.app.BroadcastP2P()
		}
		s.kickIndexFollower()
	})
}

// readSyncEngineFor wraps the per-space engine lookup with the
// tech-space liveness gate. Read-frontier keys (`read/<spaceId>/…`)
// outlive their space in the tech-space KV store, and the underlying
// StoreFor constructs a Store for ANY spaceId — so an ungated merge for
// a dead space rebuilds runtime state the boot orphan GC just swept
// (the `<spaceId>__detached` collection) and dials for storage that no
// longer exists. Skip — return nil, readsync drops the value — when:
//
//   - the tech-space row is absent (space unknown to this account);
//   - the row is deleted (row STATUS decides, not lingering storage:
//     an interrupted offload's leftovers must not resurrect merges);
//   - the row is blocked from materializing (pending join / incoming
//     1-1 — the same statuses the boot eager-loader skips);
//   - local space storage is missing (a merge must never trigger a
//     remote space fetch).
//
// A live space that merely hasn't loaded yet passes all four checks —
// engineOf (StoreFor) builds its Store lazily as before. Both merge
// paths — boot ReconcileAll and the live OnKeyValues hook — route
// through this one gate.
func readSyncEngineFor(
	rowFor func(spaceId string) (techspace.SpaceIndexRecord, bool),
	storageExists func(spaceId string) bool,
	engineOf readsync.EngineFor,
) readsync.EngineFor {
	return func(spaceId string) *readstate.Engine {
		rec, ok := rowFor(spaceId)
		if !ok || rec.IsDeleted() || spaceimpl.MaterializeBlock(rec) != nil || !storageExists(spaceId) {
			return nil
		}
		return engineOf(spaceId)
	}
}

// watchSpaceIndex subscribes to the tech-space `spaces` dataset and
// calls onChange on every change, coalesced — the mirror of spaceimpl's
// spaceIndexWatcher pattern. Best-effort: on mailbox overflow the sub
// closes and the watcher exits; the periodic discovery resweep and the
// next boot cover what was missed.
func watchSpaceIndex(tsp *techspace.Service, onChange func()) (stop func()) {
	sub, err := tsp.SubEngine().Subscribe(subscribe.SubConfig{
		Scope: subscribe.Scope{
			Shared:   false,
			ObjectId: tsp.IndexObjectId(),
			Dataset:  techspace.SpaceIndexDataset,
		},
	}, func(yield func(id string, doc *anyenc.Value)) error { return nil })
	if err != nil || sub == nil {
		return func() {}
	}
	stopCh := make(chan struct{})
	go func() {
		mb := sub.Events()
		for {
			select {
			case <-stopCh:
				return
			default:
			}
			if _, err := mb.Wait(context.Background()); err != nil {
				return // ErrClosed on stop / overflow
			}
			onChange()
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			_ = sub.Close()
			close(stopCh)
		})
	}
}

// Close tears down the SDK: joins the bootstrap pass, snapshots
// per-space catch-up watermarks, stops the files queue, closes loaded
// spaces, the tech space, the SDK DB, and finally the any-sync app.
func (s *SDK) Close() error {
	ctx := context.Background()
	// Cancel and JOIN the bootstrap pass before any teardown: it
	// touches readSync, spaces, tsp, db and app, all of which are
	// closed below — including the case of a cold GetSpace hanging on
	// an unreachable sync-node, which the cancel unblocks.
	if s.app != nil {
		// The sync jobs stop before anything they write to closes; what
		// they still hold stays counted for the watermark gate below.
		s.app.CloseSyncJobs()
	}
	if s.bootstrapDone != nil {
		s.bootstrapCancel()
		<-s.bootstrapDone
		if s.followerDone != nil {
			<-s.followerDone
		}
		// Everything is still up — snapshot the per-space watermarks
		// now, while head stores and sdk.db are readable.
		s.snapshotWatermarks(ctx)
	}
	if s.stopIndexWatch != nil {
		s.stopIndexWatch()
	}
	if s.account != nil {
		s.account.close()
	}
	if s.readSync != nil {
		s.readSync.Close()
	}
	if s.filesGC != nil {
		s.filesGC.Close()
	}
	if s.filesQueue != nil {
		s.filesQueue.Close()
	}
	if s.spaces != nil {
		_ = s.spaces.Close(ctx)
	}
	if s.tsp != nil {
		_ = s.tsp.Close(ctx)
	}
	if s.db != nil {
		_ = s.db.Close()
	}
	if s.app != nil {
		return s.app.Close(ctx)
	}
	return nil
}

// snapshotWatermarks persists the catch-up watermark for every space
// on the caughtUp allowlist at clean Close, so the next boot's replay
// no-ops instead of force-loading every tree the session touched
// (measured: ~0.8s for a session that wrote 2×3000 objects; mobile
// hosts restart the SDK on every foregrounding). A crash — or any
// space absent from the allowlist — keeps the boot replay. Two guards
// on top: a space with trees parked in its treesyncer, or still held
// by its sync job, is skipped (such trees can be storage-committed
// but not materialized; the in-memory retry set doesn't survive a
// restart, so the boot replay is their only recovery), and the write
// itself never regresses. PickSpace
// never loads, so offloaded/deleted spaces are naturally skipped.
// Anything the head store accepts after the per-space read lands above
// the snapshot and replays next boot; there is deliberately no
// periodic persist (no natural tick to ride) and no quiescence barrier
// (any-sync has no stop-intake hook): a tree a peer pushes while Close
// runs is stored before its replay ends and is covered by neither
// guard.
func (s *SDK) snapshotWatermarks(ctx context.Context) {
	for _, id := range s.caughtUpIds() {
		if n := s.parkedTreeCount(id); n > 0 {
			log.Info("close: watermark snapshot skipped — parked trees keep the boot replay",
				zap.String("spaceId", id), zap.Int("parked", n))
			continue
		}
		handle, ok := s.app.PickSpace(ctx, id)
		if !ok {
			continue
		}
		if err := spacesync.SnapshotWatermark(ctx, s.db, handle, id); err != nil {
			log.Warn("close: snapshot watermark", zap.String("spaceId", id), zap.Error(err))
		}
	}
}

// parkedCountHook overrides parkedTreeCount in tests — a seam for
// simulating parked trees without a failing network. Always nil in
// production.
var parkedCountHook func(spaceId string) int

// parkedTreeCount is the number of trees of spaceId that may be in
// storage without being in the projection: parked in its treesyncer,
// or still held by its sync job.
func (s *SDK) parkedTreeCount(spaceId string) int {
	if h := parkedCountHook; h != nil {
		return h(spaceId)
	}
	return s.app.ParkedTreeCount(spaceId) + s.app.SyncingTreeCount(spaceId)
}

// Spaces returns the space-level entrypoint.
func (s *SDK) Spaces() space.Service { return s.spaces }

// Identities returns the account-global directory of identities this
// account has encountered (profiles + the spaces where each was seen).
func (s *SDK) Identities() space.IdentitiesAPI { return spaceimpl.NewIdentitiesAPI(s.tsp, s.spaces) }

// Account returns the account-level API.
func (s *SDK) Account() AccountAPI { return s.account }

// Push returns the push-notification API — device-token registration,
// space registration, topic subscriptions and encrypted publishes
// against the configured push node (config.Push). Always non-nil; when
// no push node is configured every method returns
// space.ErrPushNotConfigured.
func (s *SDK) Push() space.PushAPI { return s.push }

// PubSub returns the account-wide ephemeral pub/sub surface: the same
// API as Space.PubSub(), bound to the tech space. The tech space's ACL
// is owner-only, so its peers are exactly this account's own devices —
// publishes here fan out account-wide with no separate transport. In
// headless mode the tech space is local-only, so delivery degrades to
// in-process loopback (no error).
func (s *SDK) PubSub() space.PubSubAPI { return spaceimpl.NewPubSubAPI(s.app, s.tsp.SpaceId()) }

// PoolInternal exposes the any-sync peer pool (dial by peerId with this
// account's identity in the handshake). Same-module internal surface —
// mirrors the PayloadsInternal pattern — used by the e2e suite to speak
// node-side protocols (e.g. fileprotov2 against a fileV2 broker).
func (s *SDK) PoolInternal() pool.Pool { return s.app.Pool() }

// PeerId returns this device's libp2p peer id — stable per device
// installation, distinct from the account identity (Account().Id()).
// It is the row id of this device's entry in the devices registry
// (Spaces().SetDevice / ListDevices) and the value election consumers
// compare against space.ActiveDevice's winner.
func (s *SDK) PeerId() string { return s.tsp.PeerId() }

// CRDTVersion reports the account's CRDT version state: the version
// this SDK supports, the highest one recorded on the tech space, and
// whether the account is read-only because the recorded one is newer
// (space.CRDTVersion). A newer mark arriving through sync flips Newer
// at runtime; every synced write then fails with
// space.ErrCRDTVersionNewer until the SDK is upgraded.
func (s *SDK) CRDTVersion() space.CRDTVersionState { return s.tsp.CRDTVersion() }

// TechSpaceId returns the account's tech space id. Spaces().Get with
// it yields the restricted tech-space handle — the home of
// account-level bundles (see space.Service.Get, space.ErrUnsupported).
func (s *SDK) TechSpaceId() string { return s.tsp.SpaceId() }

// Store returns the SDK's any-store DB (sdk.db) for consumer-owned,
// non-CRDT collections. The handle is the SDK's: it is open for the
// SDK's lifetime and closed by Close — consumers never close it.
//
// Contract for consumers:
//   - Create and address collections ONLY under a consumer tag that is
//     not a content id — a name whose segment before the first "_" is
//     neither a space id nor a cid. The boot-time orphan sweep
//     classifies such names ownerNone and never touches them (see
//     docs/space.md § Space Lifecycle); every other prefix belongs
//     to the CRDT layer, is swept by owner, and is rewritten by
//     re-index. The "l_" tag is reserved for the any server's local
//     store.
//   - Never write an SDK collection ("_meta", "<spaceId>_*",
//     "<objectId>_*", "files_*", "_history_*", "_read_*"): a direct
//     write bypasses the DAG and is reverted by the next re-index.
//   - Never open a write tx that spans a consumer collection and an SDK
//     one. Reads across both in one tx are fine (that is the point of
//     sharing the file: one snapshot for $lookup).
//   - Consumer collections are not rebuildable: a wiped sdk.db loses
//     them, and the SDK's re-index paths leave them alone.
func (s *SDK) Store() anystore.DB { return s.db }

// P2PStatus reports the local-network layer: listener state, discovery
// possibility, and every known LAN peer with its shared spaces and
// live-connection flag. Per-space p2p state lives in SpaceSyncStatus
// (P2P / LocalPeers); this is the account-wide debug view.
func (s *SDK) P2PStatus() p2p.Status { return s.app.P2PStatus() }

// SetLocalDiscoveryEnabled switches mDNS announce and browse on or off
// without a restart: off ends the running session at once, on starts
// one immediately (subject to the possibility probe), a restatement is
// a no-op. Config p2p.localDiscovery sets the state at Open.
//
// Discovery traffic only: the QUIC listener, the global (iroh) layer and
// LAN peers already known are unaffected, so a live connection is kept
// and sync status keeps reporting it. Stopping every local-network
// exchange is p2p.enabled's job.
//
// For hosts that own a local-network permission flow: the macOS Local
// Network prompt fires on the first multicast send, so such a host
// starts off and turns discovery on once the user has answered. A
// user-facing "LAN discovery" setting uses the same switch.
func (s *SDK) SetLocalDiscoveryEnabled(enabled bool) { s.app.SetLocalDiscoveryEnabled(enabled) }

// LocalDiscoveryEnabled is the local-discovery switch state, false as
// well while p2p is disabled in config. Cheap: hosts polling it need
// not build the P2PStatus snapshot.
func (s *SDK) LocalDiscoveryEnabled() bool { return s.app.LocalDiscoveryEnabled() }

// AccountAPI exposes account-level operations outside any space.
type AccountAPI interface {
	// Id returns the account's identity string (StrKey-encoded).
	Id() string

	// Metadata returns the locally-stored profile (the source-of-truth
	// copy that's also pushed to identityRepo on UpdateMetadata).
	// Reading from the tech-space is deterministic — no coordinator
	// round-trip, no 60-second watcher tick — so callers that just want
	// to read back what they wrote (e.g. a settings UI rendering after a
	// page reload) don't depend on identityRepo being reachable.
	//
	// `present` is false when no profile has been written yet on this
	// device (fresh wallet). The same `present` and zero-value distinction
	// the SDK exposes via tsp.GetProfile. IconCID is set only while an
	// avatar file id backs it.
	Metadata(ctx context.Context) (meta space.AccountMetadata, present bool, err error)

	// UpdateMetadata publishes the account's display metadata — name
	// and description, both replaced — to identityRepo. Applies across
	// all spaces. The icon is a file: an IconCID in meta fails with
	// space.ErrProfileIconReadOnly, and the stored avatar is kept. A
	// NUL byte in either field fails with space.ErrProfileFieldInvalid.
	UpdateMetadata(ctx context.Context, meta space.AccountMetadata) error

	// SetIcon stores r as the account's avatar: a files-v2 object in
	// the tech space, replacing the previous one. The returned metadata
	// carries the new IconCID (the file's content address). The avatar
	// reaches contacts once its network backup is confirmed: the
	// profile is republished with the icon reference on that flip, so a
	// reader never holds a reference it cannot fetch; until then
	// contacts see no avatar. r is capped at space.MaxIconSize
	// (space.ErrIconTooLarge beyond it).
	SetIcon(ctx context.Context, r io.Reader, opts space.IconOpts) (space.AccountMetadata, error)

	// DeleteIcon removes the avatar and republishes the profile without
	// it. No-op when none is set.
	DeleteIcon(ctx context.Context) error

	// OpenIcon returns a reader over the own avatar's bytes. ErrNotFound
	// when none is set; ErrFileNotAvailable when this device of the
	// account holds the profile row but not yet the avatar's file row
	// or its bytes.
	OpenIcon(ctx context.Context) (space.FileReader, error)
}

// accountImpl exposes the account-level surface. Id() returns the
// account's StrKey-encoded identity (PubKey.Account()), the same form
// used for change authorship and identityRepo. The profile row in the
// tech space is the source of truth; every writer pushes it to
// identityRepo, and so does any change to the row that arrives by
// sync, so each device of the account mirrors the row it holds.
type accountImpl struct {
	app    *anysyncx.App
	tsp    *techspace.Service
	spaces *spaceimpl.Service
	// pushMu serializes each local-profile-read/write → IdentityRepoPut
	// sequence. The boot republish runs on the bootstrap goroutine,
	// concurrent with user calls; without the lock its in-flight put of
	// the old profile can land AFTER a fresh UpdateMetadata and leave
	// identityRepo stale until the next boot (the local copy stays
	// fresh, so the divergence is invisible on this device). The avatar
	// watch is armed and stopped under it too, so the watched file is
	// always the one the profile row names.
	pushMu sync.Mutex
	// lastPushed is the plaintext of the last record this device put
	// successfully; a sync-driven push that would repeat it is skipped.
	lastPushed []byte

	// The avatar watch: one goroutine at a time waiting for the
	// pending avatar file's custody receipt, to republish the profile
	// with the reference. The receipt reaches the uploading device as
	// a status event and every other device of the account through
	// sync of the file row, so the watch listens for the event and
	// polls the row.
	watchMu   sync.Mutex
	watchFile string
	watchStop chan struct{}

	// closeCtx bounds every push the background goroutines make; it is
	// cancelled by close(), which then joins them through wg.
	closeCtx    context.Context
	closeCancel context.CancelFunc
	wg          sync.WaitGroup
	closeOnce   sync.Once
}

const (
	// iconWatchPoll is how often the avatar watch re-reads the file row.
	iconWatchPoll = 30 * time.Second
	// iconPushTimeout bounds one republish attempt from a background
	// goroutine.
	iconPushTimeout = 30 * time.Second
	// profileSyncDebounce coalesces a burst of profile-row events into
	// one sync-driven push.
	profileSyncDebounce = 2 * time.Second
)

func newAccountImpl(app *anysyncx.App, tsp *techspace.Service, spaces *spaceimpl.Service) *accountImpl {
	ctx, cancel := context.WithCancel(context.Background())
	return &accountImpl{app: app, tsp: tsp, spaces: spaces, closeCtx: ctx, closeCancel: cancel}
}

// close stops the background pushes and joins them. Called by
// SDK.Close before the services they read are torn down. A push in
// flight is cancelled; one queued behind a caller that holds pushMu
// waits for that caller's own RPC.
func (a *accountImpl) close() {
	a.closeOnce.Do(a.closeCancel)
	a.stopIconWatch()
	a.wg.Wait()
}

func (a *accountImpl) closed() bool { return a.closeCtx.Err() != nil }

func (a *accountImpl) Id() string {
	keys := a.app.AccountKeys()
	if keys == nil {
		return ""
	}
	return keys.SignKey.GetPublic().Account()
}

// Metadata reads the locally-persisted profile from the tech-space.
// Symmetric with the writers' local-first writes — never hits the
// network. See AccountAPI.Metadata for the (present, zero-value)
// contract.
func (a *accountImpl) Metadata(ctx context.Context) (space.AccountMetadata, bool, error) {
	if a.tsp == nil {
		return space.AccountMetadata{}, false, nil
	}
	rec, ok := a.tsp.GetProfile(ctx)
	if !ok {
		return space.AccountMetadata{}, false, nil
	}
	return recordMetadata(rec), true, nil
}

// recordMetadata is the public view of the profile row. A cid with no
// file id behind it (written by a client before avatars were files)
// names nothing fetchable and is not reported.
func recordMetadata(rec techspace.ProfileRecord) space.AccountMetadata {
	meta := space.AccountMetadata{Name: rec.Name, Description: rec.Description}
	if rec.IconFileId != "" {
		meta.IconCID = rec.IconCID
	}
	return meta
}

// UpdateMetadata publishes the account's name and description to
// identityRepo. The bytes are signed with the account signing key so
// other peers can verify authenticity at fetch time.
//
// Kind is "anysync-sdk-profile" — namespaced separately from
// anytype-heart's "profile" record because the on-the-wire format
// differs (this SDK uses a flat NUL-separated layout; heart uses an
// encrypted protobuf). Switching consumers between the two would
// require explicit format negotiation, which is out of scope.
func (a *accountImpl) UpdateMetadata(ctx context.Context, meta space.AccountMetadata) error {
	if a.app.AccountKeys() == nil {
		return errors.New("anysyncsdk: UpdateMetadata: no account keys")
	}
	if meta.IconCID != "" {
		return fmt.Errorf("anysyncsdk: UpdateMetadata: %w", space.ErrProfileIconReadOnly)
	}
	if strings.IndexByte(meta.Name, 0) >= 0 || strings.IndexByte(meta.Description, 0) >= 0 {
		return fmt.Errorf("anysyncsdk: UpdateMetadata: %w", space.ErrProfileFieldInvalid)
	}
	if meta.Name == "" && meta.Description == "" {
		return errors.New("anysyncsdk: UpdateMetadata: empty metadata")
	}
	// Persist locally first so a future boot can republish without
	// the user re-supplying the metadata. Tech-space writes are
	// owner-only and cheap. The write touches only the two fields this
	// call owns, so a concurrent avatar change on another device is
	// not overwritten. The local write and the repo push happen under
	// pushMu as one unit so the repo converges to the LAST local write
	// even when a boot republish is in flight.
	a.pushMu.Lock()
	rec := techspace.ProfileRecord{Name: meta.Name, Description: meta.Description}
	if a.tsp != nil {
		if err := a.tsp.SetProfileMetadata(ctx, meta.Name, meta.Description); err != nil {
			a.pushMu.Unlock()
			return fmt.Errorf("anysyncsdk: persist profile: %w", err)
		}
		rec, _ = a.tsp.GetProfile(ctx)
	}
	err := a.pushProfile(ctx, rec, false)
	a.pushMu.Unlock()
	if err != nil {
		return err
	}
	a.kickProfiles()
	return nil
}

// SetIcon stores the avatar as a tech-space file and records it on the
// profile row. See AccountAPI.SetIcon.
func (a *accountImpl) SetIcon(ctx context.Context, r io.Reader, opts space.IconOpts) (space.AccountMetadata, error) {
	if a.app.AccountKeys() == nil {
		return space.AccountMetadata{}, errors.New("anysyncsdk: SetIcon: no account keys")
	}
	if a.tsp == nil || a.spaces == nil {
		return space.AccountMetadata{}, errors.New("anysyncsdk: SetIcon: account not open")
	}
	info, err := a.spaces.AttachTechFile(ctx, space.LimitIcon(r), space.AttachOpts{Name: opts.Name, Mime: opts.Mime})
	if err != nil {
		return space.AccountMetadata{}, fmt.Errorf("anysyncsdk: SetIcon: %w", err)
	}
	a.pushMu.Lock()
	rec, _ := a.tsp.GetProfile(ctx)
	prev := rec.IconFileId
	if err := a.tsp.SetProfileIcon(ctx, info.RootCid, info.FileId); err != nil {
		a.pushMu.Unlock()
		_ = a.spaces.DeleteTechFile(ctx, info.FileId)
		return space.AccountMetadata{}, fmt.Errorf("anysyncsdk: persist profile: %w", err)
	}
	rec.IconCID, rec.IconFileId, rec.IconPublishedFileId = info.RootCid, info.FileId, ""
	// A dedup hit on already-durable content publishes at once;
	// otherwise the push arms the watch that republishes on the flip.
	err = a.pushProfile(ctx, rec, false)
	a.pushMu.Unlock()
	if prev != "" && prev != info.FileId {
		if derr := a.spaces.DeleteTechFile(ctx, prev); derr != nil && !errors.Is(derr, space.ErrNotFound) {
			log.Warn("delete replaced avatar", zap.String("fileId", prev), zap.Error(derr))
		}
	}
	if err != nil {
		return recordMetadata(rec), err
	}
	a.kickProfiles()
	return recordMetadata(rec), nil
}

// DeleteIcon removes the avatar file and republishes the profile
// without it. See AccountAPI.DeleteIcon. A cid left by a client that
// wrote the field before avatars were files is cleared the same way.
func (a *accountImpl) DeleteIcon(ctx context.Context) error {
	if a.app.AccountKeys() == nil {
		return errors.New("anysyncsdk: DeleteIcon: no account keys")
	}
	if a.tsp == nil || a.spaces == nil {
		return errors.New("anysyncsdk: DeleteIcon: account not open")
	}
	a.pushMu.Lock()
	rec, ok := a.tsp.GetProfile(ctx)
	if !ok || (rec.IconFileId == "" && rec.IconCID == "") {
		a.pushMu.Unlock()
		return nil
	}
	prev := rec.IconFileId
	if err := a.tsp.SetProfileIcon(ctx, "", ""); err != nil {
		a.pushMu.Unlock()
		return fmt.Errorf("anysyncsdk: persist profile: %w", err)
	}
	rec.IconCID, rec.IconFileId, rec.IconPublishedFileId = "", "", ""
	err := a.pushProfile(ctx, rec, false)
	a.pushMu.Unlock()
	if prev != "" {
		if derr := a.spaces.DeleteTechFile(ctx, prev); derr != nil && !errors.Is(derr, space.ErrNotFound) {
			log.Warn("delete avatar", zap.String("fileId", prev), zap.Error(derr))
		}
	}
	if err != nil {
		return err
	}
	a.kickProfiles()
	return nil
}

// OpenIcon opens the own avatar's bytes. See AccountAPI.OpenIcon.
func (a *accountImpl) OpenIcon(ctx context.Context) (space.FileReader, error) {
	if a.tsp == nil || a.spaces == nil {
		return nil, errors.New("anysyncsdk: OpenIcon: account not open")
	}
	rec, ok := a.tsp.GetProfile(ctx)
	if !ok || rec.IconFileId == "" {
		return nil, fmt.Errorf("anysyncsdk: OpenIcon: no avatar: %w", space.ErrNotFound)
	}
	return a.spaces.OpenOwnIcon(ctx, rec.IconFileId)
}

// kickProfiles wakes every running members watcher so a just-published
// profile is reflected in their snapshots without waiting for the
// periodic identityRepo tick (60s).
func (a *accountImpl) kickProfiles() {
	if a.spaces != nil {
		a.spaces.KickProfiles()
	}
}

// republishStoredProfile reads the locally-stored profile (if any) and
// pushes it to identityRepo. Called on SDK boot to mirror heart's
// ownProfileSubscription.Run path. No-op when no profile has been
// written yet (fresh device, never called UpdateMetadata). See
// pushProfile for how a pending avatar is treated.
func (a *accountImpl) republishStoredProfile(ctx context.Context) error {
	if a.tsp == nil {
		return nil
	}
	// Read + push under pushMu: a concurrent UpdateMetadata either
	// finishes first (we re-read and republish its fresh profile —
	// harmless duplicate) or waits and pushes after us (its value wins).
	a.pushMu.Lock()
	defer a.pushMu.Unlock()
	rec, ok := a.tsp.GetProfile(ctx)
	if !ok || rec.IsEmpty() {
		return nil
	}
	return a.pushProfile(ctx, rec, true)
}

// profileFor builds the record to push from the profile row. ready
// reports whether the avatar reference is in it: the file row is here
// with its key, and either carries the custody receipt or the row's
// published mark names this very file — a device of the account
// already pushed its reference, so it is durable and the receipt just
// has not synced here yet.
func (a *accountImpl) profileFor(ctx context.Context, rec techspace.ProfileRecord) (profile space.Profile, ready bool) {
	profile = space.Profile{AccountMetadata: space.AccountMetadata{Name: rec.Name, Description: rec.Description}}
	if rec.IconFileId == "" || a.spaces == nil {
		return profile, false
	}
	row, err := a.spaces.TechFileRow(ctx, rec.IconFileId)
	if err != nil || row.RootCid == "" || len(row.Enc.Key) == 0 ||
		(row.NetworkSign == "" && rec.IconPublishedFileId != rec.IconFileId) {
		return profile, false
	}
	profile.IconCID = row.RootCid
	profile.IconSpaceId = a.spaces.TechSpaceId()
	profile.IconKey = space.EncodeIconKey(row.Enc.Key)
	return profile, true
}

// pushProfile signs and uploads the profile bytes to the coordinator's
// identityRepo. The avatar reference rides along only when profileFor
// reports it ready — the public read path serves nothing else. While
// an avatar is pending (uploading, or its row or mark not synced here
// yet) the watch is armed so the flip republishes, and a push that
// mirrors the row (fromSync: boot or sync-driven) is skipped — this
// device cannot tell whether another device already published the
// reference, and pushing without it would drop it for every contact.
// A caller's own write (UpdateMetadata, SetIcon, DeleteIcon) goes out
// without the icon. The watch, if any, is stopped only after a
// successful put. Returns the raw error from the RPC so callers can
// decide whether to surface it. Runs under pushMu.
func (a *accountImpl) pushProfile(ctx context.Context, rec techspace.ProfileRecord, fromSync bool) error {
	keys := a.app.AccountKeys()
	symKey, err := space.DeriveAccountMetadataSymKey(keys.SignKey)
	if err != nil {
		return fmt.Errorf("anysyncsdk: derive metadata key: %w", err)
	}
	profile, ready := a.profileFor(ctx, rec)
	pending := rec.IconFileId != "" && !ready
	if pending {
		a.armIconWatch(rec.IconFileId)
		if fromSync {
			return nil
		}
	}
	// Encrypt the profile with our account metadata symkey before upload —
	// only contacts who have received the key (via a shared space's ACL or
	// a 1-1 invite) can read it. The signature is over the CIPHERTEXT so
	// the fetch path verifies before it decrypts (matches
	// decodeIdentityRepoProfile).
	plain := space.EncodeProfile(profile)
	payload, err := symKey.Encrypt(plain)
	if err != nil {
		return fmt.Errorf("anysyncsdk: encrypt profile: %w", err)
	}
	signature, err := keys.SignKey.Sign(payload)
	if err != nil {
		return fmt.Errorf("anysyncsdk: sign profile: %w", err)
	}
	// identityRepo on the coordinator expects the strkey-encoded
	// "account address" form for the identity, not the libp2p PeerId
	// our public Account.Id() returns. Translate at the boundary.
	identity := keys.SignKey.GetPublic().Account()
	if err := a.app.Coordinator().IdentityRepoPut(ctx, identity, []*identityrepoproto.Data{{
		Kind:      space.IdentityProfileKind,
		Data:      payload,
		Signature: signature,
	}}); err != nil {
		return err
	}
	a.lastPushed = plain
	if !pending {
		a.stopIconWatch()
	}
	if ready && rec.IconPublishedFileId != rec.IconFileId {
		// A local write: it must not share the push's cancellation.
		markCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), iconPushTimeout)
		defer cancel()
		if perr := a.tsp.SetProfileIconPublished(markCtx, rec.IconFileId); perr != nil {
			log.Warn("mark avatar published", zap.Error(perr))
		}
	}
	return nil
}

// armIconWatch starts the watch for fileId's receipt unless one is
// already running for it. Runs under pushMu.
func (a *accountImpl) armIconWatch(fileId string) {
	a.watchMu.Lock()
	defer a.watchMu.Unlock()
	if a.watchStop != nil {
		if a.watchFile == fileId {
			return
		}
		close(a.watchStop)
	}
	if a.closed() {
		a.watchStop, a.watchFile = nil, ""
		return
	}
	stop := make(chan struct{})
	a.watchStop, a.watchFile = stop, fileId
	a.wg.Add(1)
	go a.runIconWatch(fileId, stop)
}

// stopIconWatch ends the running watch, if any, without waiting for it.
func (a *accountImpl) stopIconWatch() {
	a.watchMu.Lock()
	if a.watchStop != nil {
		close(a.watchStop)
		a.watchStop, a.watchFile = nil, ""
	}
	a.watchMu.Unlock()
}

// runIconWatch republishes the profile once fileId's custody receipt is
// on its row, then returns. It re-checks the row on every status
// event for the file (the uploading device's own queue), on a timer
// (the receipt arrives through sync on every other device of the
// account), and once right away so a receipt that landed before the
// subscription existed is not missed. A failed republish is retried
// on the next tick. The watch ends when the profile row stops naming
// fileId, when it is replaced or stopped, or when the SDK closes; on
// the way out it clears the slot if the slot is still its own.
func (a *accountImpl) runIconWatch(fileId string, stop chan struct{}) {
	defer func() {
		a.watchMu.Lock()
		if a.watchStop == stop {
			a.watchStop, a.watchFile = nil, ""
		}
		a.watchMu.Unlock()
		a.wg.Done()
	}()
	kick := make(chan struct{}, 1)
	unsub := a.spaces.SubscribeTechFileStatus(func(st space.FileStatus) {
		if st.FileId != fileId || st.State != space.FileStateDurable {
			return
		}
		select {
		case kick <- struct{}{}:
		default:
		}
	})
	defer unsub()
	t := time.NewTicker(iconWatchPoll)
	defer t.Stop()
	for {
		if a.tryPublishIcon(fileId) {
			return
		}
		select {
		case <-stop:
			return
		case <-a.closeCtx.Done():
			return
		case <-kick:
		case <-t.C:
		}
	}
}

// tryPublishIcon republishes the profile with fileId's reference when
// the row is durable. It reports whether the watch is done: published,
// or no longer wanted because the profile row names another avatar or
// the SDK is closing.
func (a *accountImpl) tryPublishIcon(fileId string) bool {
	if a.closed() {
		return true
	}
	a.pushMu.Lock()
	defer a.pushMu.Unlock()
	if a.closed() {
		return true
	}
	ctx, cancel := context.WithTimeout(a.closeCtx, iconPushTimeout)
	defer cancel()
	rec, ok := a.tsp.GetProfile(ctx)
	if !ok || rec.IconFileId != fileId {
		return true
	}
	if _, ready := a.profileFor(ctx, rec); !ready {
		return false
	}
	if err := a.pushProfile(ctx, rec, false); err != nil {
		log.Warn("republish profile after avatar backup", zap.String("fileId", fileId), zap.Error(err))
		return false
	}
	a.kickProfiles()
	return true
}

// startProfileSync mirrors profile-row changes that arrive by sync to
// identityRepo: a name edited on another device, an avatar whose
// fields reach this device after it published without them, the
// published mark. It reconciles once at start (the row may have
// changed under the boot push), then pushes after each burst of
// events; a failed push is retried with backoff, and a subscription
// the engine closed (overflow) is reopened. Called once by the
// bootstrap after the boot republish; the goroutine ends with closeCtx
// and is joined by close().
func (a *accountImpl) startProfileSync() {
	if a.tsp == nil || a.closed() {
		return
	}
	a.wg.Add(1)
	go a.runProfileSync()
}

func (a *accountImpl) runProfileSync() {
	defer a.wg.Done()
	const (
		retryMin = 5 * time.Second
		retryMax = 5 * time.Minute
	)
	retry := retryMin
	for !a.closed() {
		sub, err := a.tsp.SubEngine().Subscribe(subscribe.SubConfig{
			Scope: subscribe.Scope{ObjectId: a.tsp.IndexObjectId(), Dataset: techspace.ProfileDataset},
		}, func(func(id string, doc *anyenc.Value)) error { return nil })
		if err != nil {
			log.Warn("profile sync: subscribe", zap.Error(err))
			if !a.sleep(retryMax) {
				return
			}
			continue
		}
		pushed := a.syncPush()
		events := sub.Events()
		// One waiter per subscription; it ends with the mailbox or the
		// close context. Events only wake the loop, so a wake lost while
		// a push ran is harmless: the push reads the row as it stands.
		woke := make(chan error, 1)
		go func() {
			for {
				if _, err := events.Wait(a.closeCtx); err != nil {
					woke <- err
					return
				}
				select {
				case woke <- nil:
				default:
				}
			}
		}()
		reopen := false
		for !reopen {
			var wait <-chan time.Time
			if !pushed {
				wait = time.After(retry)
			}
			select {
			case err := <-woke:
				if err != nil {
					reopen = true // the engine closed the mailbox (overflow)
					continue
				}
				if !a.sleep(profileSyncDebounce) {
					_ = sub.Close()
					return
				}
			case <-wait:
			case <-a.closeCtx.Done():
				_ = sub.Close()
				return
			}
			pushed = a.syncPush()
			if pushed {
				retry = retryMin
			} else if retry < retryMax {
				retry *= 2
			}
		}
		_ = sub.Close()
		if !a.sleep(retryMin) {
			return
		}
	}
}

// sleep waits d or until close; it reports whether the wait completed.
func (a *accountImpl) sleep(d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-a.closeCtx.Done():
		return false
	}
}

// syncPush mirrors the profile row as it now stands. It reports true
// when nothing is left to do — the record is out, identical to this
// device's last put, or waiting on a pending avatar (the watch is
// armed for that) — and false when the put failed and should be
// retried.
func (a *accountImpl) syncPush() bool {
	if a.closed() {
		return true
	}
	a.pushMu.Lock()
	defer a.pushMu.Unlock()
	if a.closed() {
		return true
	}
	ctx, cancel := context.WithTimeout(a.closeCtx, iconPushTimeout)
	defer cancel()
	rec, ok := a.tsp.GetProfile(ctx)
	if !ok || rec.IsEmpty() {
		return true
	}
	profile, ready := a.profileFor(ctx, rec)
	if rec.IconFileId != "" && !ready {
		a.armIconWatch(rec.IconFileId)
		return true
	}
	if bytes.Equal(space.EncodeProfile(profile), a.lastPushed) {
		return true
	}
	if err := a.pushProfile(ctx, rec, true); err != nil {
		log.Warn("profile sync: push", zap.Error(err))
		return false
	}
	a.kickProfiles()
	return true
}

// InitPageBuffer sizes the process-global any-store v2 page pool: 128 MiB
// of 4 KiB pages (the v2 default page size), twice the sqlite-side
// preallocation for the v1 space stores because the pool is shared by
// sdk.db and the host's index store, and a bulk re-index must not push
// sdk.db under slab pressure. Every v2 store opened with
// UseGlobalPageBuffer draws from it, so a host that opens such a store
// before the SDK boots calls this first to get the same pool.
// Idempotent: the first call wins.
func InitPageBuffer() {
	anystore.InitPageBuffer(4096, (1<<27)/4096)
}
