package anysyncx

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

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
	"github.com/anyproto/any-sync-sdk/internal/files/filep2p"
	filestore "github.com/anyproto/any-sync-sdk/internal/files/store"
	"github.com/anyproto/any-sync-sdk/internal/p2p"
	"github.com/anyproto/any-sync-sdk/internal/syncstatus"
	sdkp2p "github.com/anyproto/any-sync-sdk/p2p"
	"github.com/anyproto/any-sync-sdk/space"
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

	sync          *spaceSyncHandler
	tree          *treeManagerAdapter
	storage       *storageProvider
	discoveryKeys *discoveryKeySource
	nodeConf      nodeconf.Service
	peerStore     *p2p.PeerStore
	p2pServer     *p2pServer
	discovery     *p2p.Discovery
	exchange      *p2p.Exchange
	fileP2PServer *filep2p.Server

	// p2pEnabled is cfg.P2P.IsEnabled(), captured for the status
	// surfaces.
	p2pEnabled bool

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

	// guestKeyFn resolves the shared guest identity for a guest-mode
	// space (nil for regular spaces). Set once by sdk.Open after the
	// tech space is up; loadSpaceForCache consults it to open guest
	// spaces signing as that identity. Atomic — space loads can race
	// the wiring during boot, in which case they resolve nil (guest
	// rows aren't eager-loaded before the fn is set).
	guestKeyFn atomic.Pointer[func(spaceId string) crypto.PrivKey]

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
	peerStore := p2p.NewPeerStore()
	// advertisedSpaceIds is the set the p2p exchange discloses and
	// serves: every locally-stored space EXCEPT local-only ones, which
	// are pinned to this device and must never reach the network (see
	// localOnlySpaces).
	advertisedSpaceIds := func() []string {
		all := storage.AllSpaceIds()
		out := all[:0]
		for _, id := range all {
			if !localOnly.has(id) {
				out = append(out, id)
			}
		}
		return out
	}
	// Discovery keys for the v2 token exchange: derived from each
	// space's first ACL read key, cached in memory. Spaces whose ACL
	// isn't readable yet are skipped until it syncs in.
	discoveryKeys := newDiscoveryKeySource(storage, keys)
	// A handshaked local peer's shared space set changed — head-sync
	// whatever we share with it right away rather than on the next
	// diff tick.
	exchange := p2p.NewExchange(keys.PeerId, peerStore, advertisedSpaceIds, discoveryKeys.DiscoveryKeys, func(_ string, spaceIds []string) {
		sync.SyncSpaces(spaceIds)
	})
	exchange.SetAccountKeysFn(discoveryKeys.AccountDiscoveryKeys)
	// Resolve the effective p2p config. Headless mode defaults p2p off
	// (see Config.ResolveP2P): an embedded backend has no reason to
	// announce itself over mDNS or accept LAN peers.
	p2pCfg := cfg.ResolveP2P()
	p2pSrv := newP2PServer(p2pCfg, cfg.Storage.DataDir)
	discovery := p2p.NewDiscovery(p2pCfg, keys.PeerId, func() (int, bool) {
		return p2pSrv.Port(), p2pSrv.Started()
	}, exchange)
	exchange.SetOwnAddressesFn(func() sdkp2p.OwnAddresses {
		return p2p.CurrentOwnAddresses(p2pSrv.Port())
	})
	// When this device's own space set changes (create, p2p pull,
	// delete) re-handshake known LAN peers so they fold us into the
	// affected spaces' peer sets promptly. Reset the discovery-key
	// negative cache first: a just-pulled space (fresh join) failed
	// derivation moments ago at request time, and its ACL — read key
	// included — arrives with the pull.
	storage.SetOnSetChange(func() {
		discoveryKeys.ResetNegative()
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			exchange.Broadcast(ctx)
		}()
	})

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
		Register(p2pSrv).
		Register(newPeerManagerProvider(localOnly, peerStore)).
		Register(coordinatorclient.New()).
		Register(nodeclient.New()).
		Register(storage).
		Register(tree).
		Register(syncqueues.New()).
		Register(commonspace.New()).
		Register(aclclient.NewAclJoiningClient()).
		Register(subscribeclient.New()).
		Register(inbox).
		Register(peerStore).
		Register(exchange).
		// Discovery is registered last: by the time it announces and
		// starts handshaking, every component it can trigger (server,
		// pool, exchange, spaces) is already running.
		Register(discovery)

	// The p2p file server registers its FileP2P handler on the DRPC mux
	// during Init (before the accept loop), so it must be a component —
	// registering after Start would race concurrent serving. Its file
	// store is injected later (SetFileStore), once sdk.Open builds it.
	var fileP2PServer *filep2p.Server
	if p2pCfg.IsEnabled() {
		fileP2PServer = filep2p.NewServer(func(peerId, spaceId string) bool {
			for _, id := range peerStore.SpaceIds(peerId) {
				if id == spaceId {
					return true
				}
			}
			return false
		})
		a.Register(fileP2PServer)
	}

	out := &App{
		fileP2PServer:      fileP2PServer,
		sync:               sync,
		tree:               tree,
		storage:            storage,
		discoveryKeys:      discoveryKeys,
		peerStore:          peerStore,
		p2pServer:          p2pSrv,
		discovery:          discovery,
		exchange:           exchange,
		p2pEnabled:         p2pCfg.IsEnabled(),
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
	// Connected LAN peers are responsible senders too, so a space
	// synced purely over the LAN still advances to Synced.
	out.syncStatus.SetLocalPeerIdsFn(peerStore.LocalPeerIds)
	// Peer presence for sync status: live-connection counts via the
	// non-dialing pool.Pick, refreshed when the p2p peer store or the
	// discovery possibility changes. (The "Phase 3 peer-presence
	// reader" slot from docs/09-sync-status-proposal.md.)
	poolComp := a.MustComponent(pool.CName).(pool.Pool)
	pickable := func(id string) bool {
		_, err := poolComp.Pick(context.Background(), id)
		return err == nil
	}
	out.syncStatus.SetPeerCountsFn(func(spaceId string) (networkPeers, localPeers int) {
		for _, id := range nc.NodeIds(spaceId) {
			if pickable(id) {
				networkPeers++
			}
		}
		for _, id := range peerStore.LocalPeerIds(spaceId) {
			if pickable(id) {
				localPeers++
			}
		}
		return
	})
	out.syncStatus.SetP2PStateFn(func(spaceId string) space.P2PState {
		return p2pStateFor(out.p2pEnabled, discovery.Possibility(), peerStore.LocalPeerIds(spaceId), pickable)
	})
	peerStore.AddObserver(func(_ string, before, after []string, _ bool) {
		seen := map[string]struct{}{}
		for _, id := range append(append([]string{}, before...), after...) {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out.syncStatus.Refresh(id)
		}
	})
	discovery.RegisterPossibilityHook(func(sdkp2p.Possibility) {
		out.syncStatus.RefreshAll()
	})
	// Start the rollup loop. The loop ticks once per second, drains
	// the dirty set, and dispatches SpaceSyncStatus events to
	// account-wide subscribers. Close() cancels via syncStatus.Close.
	out.syncStatus.Run(context.Background())
	out.spaceCache = out.newSpaceCache()
	// Wire the head cache into the sync handler so HeadSync's fast
	// path sees the same map updated by space loads, and the app
	// back-reference the peer-facing SpacePush handler loads through.
	sync.headCache = out.headCache
	sync.app.Store(out)
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

// SetPeerAddrs registers dial addresses for a peer that is NOT in the
// nodeconf — a direct out-of-band peer like the push-notification node
// (config.Push). After registration Pool().Get(peerId) dials it over
// the same secure transports as any node. Calling again replaces the
// address list; there is no removal (the entry is process-lifetime).
func (a *App) SetPeerAddrs(peerId string, addrs []string) {
	a.a.MustComponent(peerservice.CName).(peerservice.PeerService).SetPeerAddrs(peerId, addrs)
}

// PeerStore exposes the p2p local-peer registry (which LAN peers share
// which spaces). Used by the files p2p source for peer selection.
func (a *App) PeerStore() *p2p.PeerStore { return a.peerStore }

// DRPCServer is the inbound DRPC mux. The files p2p server registers its
// read-only FileP2P handler here after the file store is built.
func (a *App) DRPCServer() server.DRPCServer {
	return a.a.MustComponent(server.CName).(server.DRPCServer)
}

// P2PEnabled reports whether the local-network layer is on (cfg.P2P).
func (a *App) P2PEnabled() bool { return a.p2pEnabled }

// SetFileStore injects the file store into the p2p file server, enabling
// it to serve stored CAR objects to LAN peers. Called by sdk.Open once
// the store is built. No-op when p2p is disabled.
func (a *App) SetFileStore(st *filestore.Store) {
	if a.fileP2PServer != nil {
		a.fileP2PServer.SetStore(st)
	}
}

// SetKnownSpaceIdsFn wires the p2p exchange's probe source: the space
// ids this account knows of (tech-space index), regardless of whether
// they are stored locally. Local-only spaces are filtered out here —
// they must never reach the exchange in any form. Called once by
// sdk.Open after the tech space is up.
func (a *App) SetKnownSpaceIdsFn(fn func() []string) {
	a.exchange.SetKnownSpaceIdsFn(func() []string {
		ids := fn()
		out := ids[:0]
		for _, id := range ids {
			if !a.localOnly.has(id) {
				out = append(out, id)
			}
		}
		return out
	})
}

// SetGuestKeyFn wires the guest-identity resolver for guest-mode
// (public-access) spaces: fn returns the shared guest identity's
// private key for spaceId, or nil for regular spaces. Called once by
// sdk.Open after the tech space is up (the resolver reads the
// tech-space index). Guest spaces load with an account-service
// override so the commonspace signs as the guest identity — see
// loadSpaceForCache.
func (a *App) SetGuestKeyFn(fn func(spaceId string) crypto.PrivKey) {
	a.guestKeyFn.Store(&fn)
}

// guestKeyFor resolves spaceId's guest identity, nil when the resolver
// isn't wired yet or the space isn't guest-mode.
func (a *App) guestKeyFor(spaceId string) crypto.PrivKey {
	fn := a.guestKeyFn.Load()
	if fn == nil {
		return nil
	}
	return (*fn)(spaceId)
}

// BroadcastP2P re-runs the LAN handshake with every known local peer —
// called when the tech-space index changes (new row synced, a joining
// row flipped active) so a fresh device starts probing for the space
// right away instead of on the next discovery resweep. The index change
// is also a "key may be derivable now" signal, so the discovery-key
// negative cache is reset first — without that, a fresh join's failed
// request-time derivation would suppress the space from the handshake
// for up to negativeRetryAfter even though its ACL has synced in.
func (a *App) BroadcastP2P() {
	a.discoveryKeys.ResetNegative()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		a.exchange.Broadcast(ctx)
	}()
}

// SetSpaceRegistry wires the tree manager to a space-level registry.
// Called once by the space package after it builds its ocache.
func (a *App) SetSpaceRegistry(r SpaceRegistry) { a.tree.SetRegistry(r) }

// SelectiveTreeTypes is the selective-sync tree-type allowlist
// (cfg.Sync.TreeTypes). Empty = sync and materialize everything.
func (a *App) SelectiveTreeTypes() []string { return a.selectiveTreeTypes }

// Headless reports whether the SDK runs in embedded-backend mode
// (cfg.Headless). See config.Config.Headless for the contract.
func (a *App) Headless() bool { return a.headless }

// IsLocalOnly reports whether spaceId is pinned to this device. The
// p2p-facing sync handlers use it to refuse serving or advertising
// local-only spaces even if a peer names one directly.
func (a *App) IsLocalOnly(spaceId string) bool { return a.localOnly.has(spaceId) }

// LocalPeerHasSpace reports whether a LAN peer advertised sharing
// spaceId in the exchange. The SpacePush handler uses it to bound
// which spaces a peer may seed onto this device.
func (a *App) LocalPeerHasSpace(peerId, spaceId string) bool {
	for _, id := range a.peerStore.SpaceIds(peerId) {
		if id == spaceId {
			return true
		}
	}
	return false
}

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

// ParkedTreeCount returns the number of trees parked for retry in
// spaceId's treesyncer adapter — fetched to storage but never
// materialized into the CRDT projection (see treeSyncerAdapter.pending).
// 0 when the space was never loaded this session. Consumed by the
// SDK's close-time watermark gate: a space with parked trees must keep
// its boot replay, so its watermark is not snapshotted at Close.
func (a *App) ParkedTreeCount(spaceId string) int {
	a.syncersMu.Lock()
	ts := a.syncers[spaceId]
	a.syncersMu.Unlock()
	if ts == nil {
		return 0
	}
	return ts.pendingCount()
}

// p2pStateFor resolves a space's local-network state. Pure so it's
// unit-testable without the app graph.
func p2pStateFor(enabled bool, poss sdkp2p.Possibility, localPeerIds []string, pickable func(string) bool) space.P2PState {
	if !enabled || poss == sdkp2p.PossibilityNoInterfaces {
		return space.P2PStateNotPossible
	}
	if poss == sdkp2p.PossibilityRestricted {
		return space.P2PStateRestricted
	}
	for _, id := range localPeerIds {
		if pickable(id) {
			return space.P2PStateConnected
		}
	}
	return space.P2PStateNotConnected
}

// P2PStatus is the account-wide local-network snapshot for the debug
// surface: listener state, discovery possibility, and every peer in
// the local peer store with its live-connection flag.
func (a *App) P2PStatus() sdkp2p.Status {
	pickable := func(id string) bool {
		_, err := a.Pool().Pick(context.Background(), id)
		return err == nil
	}
	allPeers := a.peerStore.AllLocalPeers()
	st := sdkp2p.Status{
		PeerId:          a.keys.PeerId,
		Enabled:         a.p2pEnabled,
		ListenerStarted: a.p2pServer.Started(),
		Port:            a.p2pServer.Port(),
		Possibility:     a.discovery.Possibility(),
		State:           p2pStateFor(a.p2pEnabled, a.discovery.Possibility(), allPeers, pickable),
	}
	for _, peerId := range allPeers {
		st.Peers = append(st.Peers, sdkp2p.PeerStatus{
			PeerId:    peerId,
			SpaceIds:  a.peerStore.SpaceIds(peerId),
			Connected: pickable(peerId),
		})
	}
	return st
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
