package anysyncx

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/anyproto/any-sync/accountservice"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/debugstat"
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/commonspace/acl/aclclient"
	"github.com/anyproto/any-sync/commonspace/acl/aclwaiter"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/coordinator/inboxclient"
	"github.com/anyproto/any-sync/coordinator/nodeconfsource"
	"github.com/anyproto/any-sync/coordinator/subscribeclient"
	"github.com/anyproto/any-sync/net/peerservice"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/rpc/server"
	"github.com/anyproto/any-sync/net/secureservice"
	"github.com/anyproto/any-sync/net/streampool"
	"github.com/anyproto/any-sync/node/nodeclient"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/nodeconf/nodeconfstore"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/anyproto/any-sync/util/syncqueues"

	"github.com/anyproto/any-sync-sdk/auth"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/syncstatus"
)

// App is the running any-sync app plus the components downstream
// packages need to reach. Constructed by New, torn down by Close.
type App struct {
	a *app.App

	spaceService commonspace.SpaceService
	coord        coordinatorclient.CoordinatorClient
	streamPool   streampool.StreamPool
	joining      aclclient.AclJoiningClient
	inbox        inboxclient.InboxClient

	// inboxReceiver holds the live push callback installed by the inbox
	// notifier. The forwarder registered before app.Start delegates to
	// it; nil until the notifier calls OnInboxMessage, so early pushes
	// (before the notifier starts) are dropped — the notifier's initial
	// poll catches anything missed.
	inboxReceiver atomic.Pointer[InboxMessageHandler]

	sync     *spaceSyncHandler
	tree     *treeManagerAdapter
	storage  *storageProvider
	nodeConf nodeconf.Service

	spaceCache ocache.OCache
	headCache  *HeadCache

	syncStatus *syncstatus.Service

	// kvHandlers fans applied key-value writes out per space — see
	// kvdispatcher.go. Guarded by kvMu.
	kvMu       sync.RWMutex
	kvHandlers map[string][]*kvHandlerReg

	// syncers indexes the per-space treeSyncerAdapter so the debug
	// surface can read per-peer SyncAll stats by spaceId. Populated
	// from newTreeSyncerForSpace; never removed (adapter lives as
	// long as the space stays in the spaceCache, and stats survive
	// brief ocache evictions when the same id loads again).
	syncersMu sync.Mutex
	syncers   map[string]*treeSyncerAdapter

	keys *accountdata.AccountKeys

	// selectiveTreeTypes is cfg.Sync.TreeTypes — the selective-sync
	// tree-type allowlist. Empty = sync and materialize everything.
	selectiveTreeTypes []string

	// headless is cfg.Headless — embedded-backend mode. The flag itself
	// only gates boot behavior in sdk.Open and the tech space's
	// local-only marking; the enforcement lives in localOnly.
	headless bool

	// localOnly pins spaces to this device — no node subscribe, no
	// push, no coordinator receipt. See localOnlySpaces.
	localOnly *localOnlySpaces
}

// New brings up the any-sync app. Order matters: keys first (provider
// may block on user input), then nodeconf parsing, then component
// registration mirroring the legacy bootstrap order.
//
// Logger setup is the caller's job — call (logger.Config{}).ApplyGlobal
// once before Open. We intentionally don't touch the global logger here
// because ApplyGlobal mutates named-logger structs in place, and a
// second Open in the same process would race goroutines from the first
// SDK that are already using those loggers.
func New(ctx context.Context, cfg config.Config, provider auth.Provider) (*App, error) {
	keys, err := loadAccountKeys(ctx, provider)
	if err != nil {
		return nil, err
	}
	nodeConf, err := parseNodeConf(cfg.Network.NodeConfYAML)
	if err != nil {
		return nil, err
	}

	cfgAdapter := newConfig(cfg, nodeConf)
	accountAdapter := newAccount(keys)
	storage := newStorageProvider(cfg.Storage.DataDir)
	sync := newSpaceSyncHandler()
	stream := newStreamHandler(sync)
	tree := newTreeManager()
	localOnly := newLocalOnlySpaces()

	a := new(app.App)
	a.Register(cfgAdapter).
		Register(accountAdapter).
		Register(debugstat.New()).
		Register(newCredentialProvider(localOnly)).
		Register(nodeconfstore.New()).
		Register(nodeconfsource.New()).
		Register(nodeconf.New()).
		Register(secureservice.New())
	registerTransports(a)
	inbox := inboxclient.New()
	a.Register(peerservice.New()).
		Register(server.New()).
		Register(stream).
		Register(streampool.New()).
		Register(sync).
		Register(pool.New()).
		Register(newPeerManagerProvider(localOnly)).
		Register(coordinatorclient.New()).
		Register(nodeclient.New()).
		Register(storage).
		Register(tree).
		Register(syncqueues.New()).
		Register(commonspace.New()).
		Register(aclclient.NewAclJoiningClient()).
		Register(subscribeclient.New()).
		Register(inbox)

	out := &App{
		sync:               sync,
		tree:               tree,
		storage:            storage,
		headCache:          newHeadCache(),
		syncStatus:         syncstatus.NewService(),
		syncers:            map[string]*treeSyncerAdapter{},
		keys:               keys,
		inbox:              inbox,
		selectiveTreeTypes: cfg.Sync.TreeTypes,
		headless:           cfg.Headless,
		localOnly:          localOnly,
	}

	// Install the push forwarder BEFORE Start: inboxClient.Run rejects a
	// nil receiver. The forwarder delegates to the notifier's handler
	// once it installs one via OnInboxMessage; until then pushes are
	// dropped (the notifier's poll catches up). Set on `out` so the
	// closure reads the same atomic the notifier writes.
	if err := inbox.SetMessageReceiver(func(ev *coordinatorproto.NotifySubscribeEvent) {
		if h := out.inboxReceiver.Load(); h != nil {
			(*h)(ev)
		}
	}); err != nil {
		return nil, fmt.Errorf("anysyncx: set inbox receiver: %w", err)
	}

	if err := a.Start(ctx); err != nil {
		return nil, fmt.Errorf("anysyncx: app start: %w", err)
	}

	out.a = a
	out.spaceService = a.MustComponent(commonspace.CName).(commonspace.SpaceService)
	out.coord = a.MustComponent(coordinatorclient.CName).(coordinatorclient.CoordinatorClient)
	out.streamPool = a.MustComponent(streampool.CName).(streampool.StreamPool)
	out.joining = a.MustComponent(aclclient.CName).(aclclient.AclJoiningClient)
	// Wire the responsible-node resolver so per-space trackers can
	// filter inbound HeadsApply senders. nodeconf is registered above;
	// fetch the component once here so the closure stays cheap.
	nc := a.MustComponent(nodeconf.CName).(nodeconf.Service)
	out.nodeConf = nc
	out.syncStatus.SetNodeIdsFn(nc.NodeIds)
	// Start the rollup loop. The loop ticks once per second, drains
	// the dirty set, and dispatches SpaceSyncStatus events to
	// account-wide subscribers. Close() cancels via syncStatus.Close.
	out.syncStatus.Run(context.Background())
	out.spaceCache = out.newSpaceCache()
	// Wire the head cache into the sync handler so HeadSync's fast
	// path sees the same map updated by space loads.
	sync.headCache = out.headCache
	return out, nil
}

// Close stops the any-sync app and releases resources. The space
// cache is closed first so per-space goroutines wind down before the
// app's components.
func (a *App) Close(ctx context.Context) error {
	if a.spaceCache != nil {
		_ = a.spaceCache.Close()
	}
	if a.syncStatus != nil {
		a.syncStatus.Close()
	}
	return a.a.Close(ctx)
}

// SyncStatus exposes the per-account sync-status registry. Wired
// into commonspace.Deps.SyncStatus in Phase 2; for now the space
// layer reads it for snapshot + subscribe.
func (a *App) SyncStatus() *syncstatus.Service { return a.syncStatus }

// SpaceService is any-sync's per-account space create/derive/join surface.
func (a *App) SpaceService() commonspace.SpaceService { return a.spaceService }

// Coordinator client — used for SpaceMakeShareable, NetworkConfiguration,
// space delete confirmations, account delete.
func (a *App) Coordinator() coordinatorclient.CoordinatorClient { return a.coord }

// StreamPool is the outbound DRPC stream pool. The space layer uses it
// to send SpaceSubscription messages when a new space loads.
func (a *App) StreamPool() streampool.StreamPool { return a.streamPool }

// JoiningClient is the account-level ACL client used to send
// RequestJoin / CancelJoin RPCs to the coordinator+nodes for a space
// the caller is not yet a member of.
func (a *App) JoiningClient() aclclient.AclJoiningClient { return a.joining }

// InboxMessageHandler is the push callback the inbox notifier installs.
// The body-less NotifySubscribeEvent only signals "new mail" — the
// notifier's handler reacts by fetching.
type InboxMessageHandler = func(*coordinatorproto.NotifySubscribeEvent)

// InboxClient is the coordinator inbox transport (fetch / add-message).
// Always non-nil in this build (the component is registered
// unconditionally), but callers should treat a nil return as "inbox
// unavailable" so a future coordinator-less deployment degrades to the
// out-of-band 1-1 path.
func (a *App) InboxClient() inboxclient.InboxClient { return a.inbox }

// OnInboxMessage installs the push callback invoked when the coordinator
// signals a new inbox message. Replaces any previous handler; pass nil
// to detach (the notifier does this on Close). The forwarder registered
// before app.Start reads this atomically.
func (a *App) OnInboxMessage(h InboxMessageHandler) {
	if h == nil {
		a.inboxReceiver.Store(nil)
		return
	}
	a.inboxReceiver.Store(&h)
}

// AccountKeys holds the decoded peer/sign keys.
func (a *App) AccountKeys() *accountdata.AccountKeys { return a.keys }

// NewAclWaiter builds an any-sync ACL waiter bound to the running app:
// a background poller over the network ACL (no local space storage
// needed) that fires onFinish once the account gains permissions on
// spaceId, or onReject once the join request is declined at/after
// aclHeadId. The caller drives its lifecycle via Run(ctx) / Close(ctx).
// Used by the joiner-side post-acceptance loader.
func (a *App) NewAclWaiter(spaceId, aclHeadId string, onFinish, onReject func(list.AclList) error) (aclwaiter.AclWaiter, error) {
	w := aclwaiter.New(spaceId, aclHeadId, onFinish, onReject)
	if err := w.Init(a.a); err != nil {
		return nil, fmt.Errorf("anysyncx: init acl waiter: %w", err)
	}
	return w, nil
}

// NetworkId is the id of the any-sync network this app is bound to.
// Needed to build the signed space-delete confirmation, which the
// coordinator verifies against its own network id.
func (a *App) NetworkId() string { return a.nodeConf.Configuration().NetworkId }

// FileV2Peers is the current fileV2 fleet (nodeconf.NodeTypeFileV2).
// The files broker routes its RPCs across these peers.
func (a *App) FileV2Peers() []string { return a.nodeConf.FileV2Peers() }

// FileNetworkId is the identity of the fileV2 fleet's shared
// receipt-signing key (nodeconf fileNetworkId). Custody receipts
// (networkSign) verify against this one stable key; empty on networks
// without a fileV2 fleet.
func (a *App) FileNetworkId() string { return a.nodeConf.Configuration().FileNetworkId }

// Pool exposes the any-sync peer pool (dial by peerId, addresses
// resolved from the nodeconf). Lets embedders/e2e speak node-side
// protocols (e.g. fileprotov2) over a connection that carries this
// account's identity in the handshake.
func (a *App) Pool() pool.Pool { return a.a.MustComponent(pool.CName).(pool.Pool) }

// SetSpaceRegistry wires the tree manager to a space-level registry.
// Called once by the space package after it builds its ocache.
func (a *App) SetSpaceRegistry(r SpaceRegistry) { a.tree.SetRegistry(r) }

// SelectiveTreeTypes is the selective-sync tree-type allowlist
// (cfg.Sync.TreeTypes). Empty = sync and materialize everything.
func (a *App) SelectiveTreeTypes() []string { return a.selectiveTreeTypes }

// Headless reports whether the SDK runs in embedded-backend mode
// (cfg.Headless). See config.Config.Headless for the contract.
func (a *App) Headless() bool { return a.headless }

// MarkSpaceLocalOnly pins spaceId to this device: its peer manager
// resolves no peers (no node subscribe, no diff-sync, no push) and the
// credential provider refuses to request a coordinator receipt for it.
// Must be called before the space is first loaded — the peer manager is
// chosen at NewSpace time. Used by the tech space in headless mode.
func (a *App) MarkSpaceLocalOnly(spaceId string) { a.localOnly.mark(spaceId) }

// SpaceExists reports whether any-sync has local storage for spaceId.
// Used by the space layer to decide between Open and Create paths.
func (a *App) SpaceExists(spaceId string) bool { return a.storage.SpaceExists(spaceId) }

// DeleteSpaceStorage closes and removes a space's any-sync on-disk
// storage (`<DataDir>/anysync/<spaceId>.db`). Call after EvictSpace so
// any-sync has released the space; part of the space-offload path.
func (a *App) DeleteSpaceStorage(ctx context.Context, spaceId string) error {
	return a.storage.DeleteSpaceStorageFile(ctx, spaceId)
}

// HeadCache exposes the per-space hash cache. Useful for tests and
// the eventual SyncStatus integration; the spaceSyncHandler already
// has a direct reference for its fast path.
func (a *App) HeadCache() *HeadCache { return a.headCache }

// newTreeSyncerForSpace returns a fresh TreeSyncer instance bound to
// spaceId, ready to be passed into commonspace.Deps. One per space;
// any-sync's commonspace wires it into the per-space app via
// spacestate during NewSpace.
//
// Captures the SpaceRegistry currently set on the tree manager so the
// per-space SyncAll can route through it (this is what hooks the
// CRDT-controller listener onto inbound trees). Invoked from
// loadSpaceForCache, which only runs after sdk.Open's SetSpaceRegistry
// call, so the registry is reliably wired by then.
//
// The adapter is also indexed in a.syncers so PeerSyncStats(spaceId)
// can read its per-peer counters for the debug surface.
func (a *App) newTreeSyncerForSpace(spaceId string) *treeSyncerAdapter {
	// onRound: a 0/0 success round against a responsible peer means
	// the entire space is converged — sweep every tree the tracker
	// has seen to Synced, and anchor lastAllSyncedAt so trees the
	// tracker has never seen (cold-restored, no recent hook) also
	// read as Synced. Non-zero counts or errors are no-ops here;
	// the per-tree HeadsApply path still handles them.
	onRound := func(peerId string, newCount, changedCount int, err error) {
		if err != nil || newCount != 0 || changedCount != 0 {
			return
		}
		a.syncStatus.For(spaceId).BulkSyncedFromPeer(peerId)
	}
	ts := newTreeSyncer(spaceId, a.tree.registry, onRound)
	a.syncersMu.Lock()
	a.syncers[spaceId] = ts
	a.syncersMu.Unlock()
	return ts
}

// PeerSyncStats returns the latest per-peer SyncAll snapshots for
// spaceId. Empty slice if the space has never had an outbound diff
// round (or was never loaded). In-memory only — cleared on SDK
// restart. Used by the debug API; not a stable surface.
func (a *App) PeerSyncStats(spaceId string) []PeerSyncSnapshot {
	a.syncersMu.Lock()
	ts := a.syncers[spaceId]
	a.syncersMu.Unlock()
	if ts == nil {
		return nil
	}
	return ts.Stats()
}

// loadAccountKeys decodes the raw seeds from the auth.Provider into
// any-sync crypto keys. Both keys are required — empty seeds are a
// caller bug.
func loadAccountKeys(ctx context.Context, p auth.Provider) (*accountdata.AccountKeys, error) {
	if p == nil {
		return nil, fmt.Errorf("anysyncx: nil auth.Provider")
	}
	accSeed, err := p.AccountKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("anysyncx: AccountKey: %w", err)
	}
	devSeed, err := p.DeviceKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("anysyncx: DeviceKey: %w", err)
	}
	signKey, err := crypto.NewSigningEd25519PrivKeyFromBytes(accSeed)
	if err != nil {
		return nil, fmt.Errorf("anysyncx: account key: %w", err)
	}
	peerKey, err := crypto.NewSigningEd25519PrivKeyFromBytes(devSeed)
	if err != nil {
		return nil, fmt.Errorf("anysyncx: device key: %w", err)
	}
	return accountdata.New(peerKey, signKey), nil
}

// quiet "imported and not used" if accountservice changes.
var _ = accountservice.CName
