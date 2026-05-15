package anysyncx

import (
	"context"
	"fmt"
	"sync"

	"github.com/anyproto/any-sync/accountservice"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/debugstat"
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/commonspace/acl/aclclient"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/coordinator/nodeconfsource"
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

	sync    *spaceSyncHandler
	tree    *treeManagerAdapter
	storage *storageProvider

	spaceCache ocache.OCache
	headCache  *HeadCache

	syncStatus *syncstatus.Service

	// syncers indexes the per-space treeSyncerAdapter so the debug
	// surface can read per-peer SyncAll stats by spaceId. Populated
	// from newTreeSyncerForSpace; never removed (adapter lives as
	// long as the space stays in the spaceCache, and stats survive
	// brief ocache evictions when the same id loads again).
	syncersMu sync.Mutex
	syncers   map[string]*treeSyncerAdapter

	keys *accountdata.AccountKeys
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

	a := new(app.App)
	a.Register(cfgAdapter).
		Register(accountAdapter).
		Register(debugstat.New()).
		Register(newCredentialProvider()).
		Register(nodeconfstore.New()).
		Register(nodeconfsource.New()).
		Register(nodeconf.New()).
		Register(secureservice.New())
	registerTransports(a)
	a.Register(peerservice.New()).
		Register(server.New()).
		Register(stream).
		Register(streampool.New()).
		Register(sync).
		Register(pool.New()).
		Register(newPeerManagerProvider()).
		Register(coordinatorclient.New()).
		Register(nodeclient.New()).
		Register(storage).
		Register(tree).
		Register(syncqueues.New()).
		Register(commonspace.New()).
		Register(aclclient.NewAclJoiningClient())

	if err := a.Start(ctx); err != nil {
		return nil, fmt.Errorf("anysyncx: app start: %w", err)
	}

	out := &App{
		a:            a,
		spaceService: a.MustComponent(commonspace.CName).(commonspace.SpaceService),
		coord:        a.MustComponent(coordinatorclient.CName).(coordinatorclient.CoordinatorClient),
		streamPool:   a.MustComponent(streampool.CName).(streampool.StreamPool),
		joining:      a.MustComponent(aclclient.CName).(aclclient.AclJoiningClient),
		sync:         sync,
		tree:         tree,
		storage:      storage,
		headCache:    newHeadCache(),
		syncStatus:   syncstatus.NewService(),
		syncers:      map[string]*treeSyncerAdapter{},
		keys:         keys,
	}
	// Wire the responsible-node resolver so per-space trackers can
	// filter inbound HeadsApply senders. nodeconf is registered above;
	// fetch the component once here so the closure stays cheap.
	nc := a.MustComponent(nodeconf.CName).(nodeconf.Service)
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

// AccountKeys holds the decoded peer/sign keys.
func (a *App) AccountKeys() *accountdata.AccountKeys { return a.keys }

// SetSpaceRegistry wires the tree manager to a space-level registry.
// Called once by the space package after it builds its ocache.
func (a *App) SetSpaceRegistry(r SpaceRegistry) { a.tree.SetRegistry(r) }

// SpaceExists reports whether any-sync has local storage for spaceId.
// Used by the space layer to decide between Open and Create paths.
func (a *App) SpaceExists(spaceId string) bool { return a.storage.SpaceExists(spaceId) }

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
	ts := newTreeSyncer(spaceId, a.tree.registry)
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
