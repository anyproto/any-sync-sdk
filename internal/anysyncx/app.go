package anysyncx

import (
	"context"
	"fmt"
	"path/filepath"
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
	"github.com/anyproto/any-sync/commonspace/object/acl/recordverifier"
	"github.com/anyproto/any-sync/commonspace/pubsub"
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
	"github.com/anyproto/any-sync-sdk/internal/p2p/account"
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
	addrBook      *p2p.AddrBook
	// global is the internet-wide p2p layer; nil when cfg.P2P.Global is
	// off.
	global *p2p.Global

	// p2pEnabled / globalEnabled are cfg.P2P.IsEnabled() and
	// cfg.P2P.Global.IsEnabled(), captured for the status surfaces.
	p2pEnabled    bool
	globalEnabled bool

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

	// pubsub is the commonspace/pubsub engine (ephemeral, ACL-gated
	// pub/sub). Registered as an app component; reached through the
	// PubSub* pass-throughs.
	pubsub pubsub.Service
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
	stream.peers = peerStore
	addrBook := p2p.NewAddrBook()
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
	// pubsub engine + its Deps adapters. The adapters are late-bound to
	// the App below (their methods only run post-Start); the engine is
	// registered as a component so the app drives its lifecycle.
	psPeers := &pubsubPeers{}
	psCrypto := &pubsubCrypto{}
	psMembership := &pubsubMembership{}
	psvc := pubsub.New(pubsub.Deps{
		Membership: psMembership,
		Crypto:     psCrypto,
		Peers:      psPeers,
		OnStatus:   logPubSubStatus,
	})
	// A handshaked local peer's shared space set changed — head-sync
	// whatever we share with it right away rather than on the next
	// diff tick, and re-push pubsub interest so the fresh LAN peer
	// starts relaying to us before the engine's next resync tick.
	exchange := p2p.NewExchange(keys.PeerId, peerStore, advertisedSpaceIds, discoveryKeys.DiscoveryKeys, func(_ string, spaceIds []string) {
		sync.SyncSpaces(spaceIds)
		for _, id := range spaceIds {
			pubsubSyncInterest(psvc, id)
		}
	})
	exchange.SetAccountKeysFn(discoveryKeys.AccountDiscoveryKeys)
	// Resolve the effective p2p config. Headless mode defaults p2p off
	// (see Config.ResolveP2P): an embedded backend has no reason to
	// announce itself over mDNS or accept LAN peers.
	p2pCfg := cfg.ResolveP2P()
	p2pSrv := newP2PServer(p2pCfg, cfg.Storage.DataDir)
	// Global p2p: liveness records persist across restarts; the layer
	// itself (records, connector, inbound gate) exists only when on.
	globalCfg := p2pCfg.Global
	globalEnabled := globalCfg.IsEnabled()
	if err := globalCfg.Validate(); err != nil {
		return nil, fmt.Errorf("anysyncx: %w", err)
	}
	statusBook := p2p.NewStatusBook(filepath.Join(cfg.Storage.DataDir, p2pPeersFileName), p2p.ThresholdsFrom(globalCfg))
	peerStore.SetStatus(statusBook)
	var (
		global *p2p.Global
		subs   *globalSubs
	)
	peerManagers := newPeerManagerProvider(localOnly, peerStore, globalPeersOrNil(peerStore, globalEnabled), globalCfg.MaxConnections, nil)
	if globalEnabled {
		global = p2p.NewGlobal(globalCfg, keys.PeerId, keys.SignKey.GetPublic().Account(), peerStore, statusBook, addrBook)
		// asks from global peers are recorded here and read by the
		// peer managers at send time; a newly connected global peer is
		// asked right away by every node-less space
		subs = newGlobalSubs(globalSubExpiry)
		stream.subs = subs
		peerManagers.subs = subs
		global.SetOnLive(func(string) { peerManagers.wakeGlobalAsks() })
		// The account record: every device of the account registers
		// itself under a key derived from the identity key and resolves
		// its siblings from it — the same path a fresh restore takes.
		if globalCfg.AccountEnabled() {
			signKey, encKey, err := crypto.DeriveDiscoveryKeys(keys.SignKey)
			if err != nil {
				return nil, fmt.Errorf("anysyncx: discovery keys: %w", err)
			}
			acctKeys, err := account.NewKeys(signKey, encKey)
			if err != nil {
				return nil, fmt.Errorf("anysyncx: discovery keys: %w", err)
			}
			acctClient, err := account.NewClient(globalCfg.PkarrRelayURLs)
			if err != nil {
				return nil, fmt.Errorf("anysyncx: %w", err)
			}
			global.SetAccount(acctKeys, acctClient, globalCfg.InsecureRelay, filepath.Join(cfg.Storage.DataDir, accountRecordFileName))
		}
	}
	// A LAN entry that goes away hands the peer's addresses over to its
	// iroh ticket, if any (addr book: LAN xor ticket).
	peerStore.AddSourceObserver(func(src p2p.Source, peerId string, present bool) {
		if src == p2p.SourceLAN && !present {
			addrBook.ClearLAN(peerId)
		}
	})
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
	registerTransports(a, globalEnabled)
	inbox := inboxclient.New()
	a.Register(peerservice.New()).
		Register(server.New()).
		Register(stream).
		Register(streampool.New()).
		Register(sync).
		Register(pool.New()).
		Register(p2pSrv).
		Register(peerManagers).
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
		Register(addrBook).
		// The pubsub engine owns a private streampool; pubsubRpc puts
		// its stream handler on the DRPC mux during Init (before the
		// accept loop), so both must be components. Registered after
		// server (mux) and accountAdapter (engine Init resolves it).
		Register(psvc).
		Register(pubsubRpc{}).
		Register(exchange).
		// Discovery is registered last: by the time it announces and
		// starts handshaking, every component it can trigger (server,
		// pool, exchange, spaces) is already running.
		Register(discovery)
	// The global layer runs after the transports, pool and peer store it
	// reads; its Run only starts workers, so ordering after discovery is
	// about Close order (workers stop before the pool closes).
	if global != nil {
		a.Register(global)
	}

	// The p2p file server registers its FileP2P handler on the DRPC mux
	// during Init (before the accept loop), so it must be a component —
	// registering after Start would race concurrent serving. Its file
	// store is injected later (SetFileStore), once sdk.Open builds it.
	// Serves LAN and global peers alike: the peer store's SpaceIds is
	// the union of both sources.
	var fileP2PServer *filep2p.Server
	if p2pCfg.IsEnabled() || globalEnabled {
		// a sibling holds every space of the account — except the ones
		// pinned to this device
		fileP2PServer = filep2p.NewServer(func(peerId, spaceId string) bool {
			return !localOnly.has(spaceId) && peerStore.HasSpace(peerId, spaceId)
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
		addrBook:           addrBook,
		global:             global,
		p2pEnabled:         p2pCfg.IsEnabled(),
		globalEnabled:      globalEnabled,
		headCache:          newHeadCache(),
		syncStatus:         syncstatus.NewService(),
		syncers:            map[string]*treeSyncerAdapter{},
		keys:               keys,
		inbox:              inbox,
		selectiveTreeTypes: cfg.Sync.TreeTypes,
		headless:           cfg.Headless,
		localOnly:          localOnly,
		pubsub:             psvc,
	}
	psPeers.app = out
	psCrypto.app = out
	psMembership.app = out
	if global != nil {
		global.SetKVSubscriber(func(spaceId string, h p2p.KVHandler) func() {
			return out.OnKeyValues(spaceId, KeyValueHandler(h))
		})
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

	// Fields the pubsub adapters and inbound handlers read (a, nodeConf,
	// spaceService, spaceCache) are assigned BEFORE Start: listeners
	// come up during Start, and an early inbound frame reading a field
	// assigned after Start returns would be a data race. MustComponent
	// only needs registration, which is complete here.
	out.a = a
	out.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	out.spaceService = a.MustComponent(commonspace.CName).(commonspace.SpaceService)
	out.spaceCache = out.newSpaceCache()

	if err := a.Start(ctx); err != nil {
		return nil, fmt.Errorf("anysyncx: app start: %w", err)
	}

	out.coord = a.MustComponent(coordinatorclient.CName).(coordinatorclient.CoordinatorClient)
	out.streamPool = a.MustComponent(streampool.CName).(streampool.StreamPool)
	out.joining = a.MustComponent(aclclient.CName).(aclclient.AclJoiningClient)
	// Wire the responsible-node resolver so per-space trackers can
	// filter inbound HeadsApply senders.
	nc := out.nodeConf
	out.syncStatus.SetNodeIdsFn(nc.NodeIds)
	// Connected direct peers (LAN or global) are responsible senders
	// too, so a space synced purely peer-to-peer still advances to
	// Synced.
	out.syncStatus.SetLocalPeerIdsFn(func(spaceId string) []string {
		return append(peerStore.LocalPeerIds(spaceId), peerStore.GlobalPeerIds(spaceId)...)
	})
	// A tree pulled whole because a peer's head update named it is in
	// sync with that peer, like one fetched during a diff round (see
	// treeSyncerAdapter.onFetched); without this a tree that only ever
	// arrives by push never reaches the tracker.
	out.tree.onFetched = func(spaceId, peerId, treeId string, heads []string) {
		out.syncStatus.For(spaceId).HeadsApply(peerId, treeId, heads, true)
	}
	// Peer presence for sync status: live-connection counts via the
	// non-dialing pool.Pick, refreshed when the p2p peer store or the
	// discovery possibility changes (docs/sync-status-proposal.md).
	poolComp := a.MustComponent(pool.CName).(pool.Pool)
	pickable := func(id string) bool { return pickLive(poolComp, id) }
	out.syncStatus.SetPeerCountsFn(func(spaceId string) (networkPeers, localPeers, globalPeers int) {
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
		for _, id := range peerStore.GlobalPeerIds(spaceId) {
			if pickable(id) {
				globalPeers++
			}
		}
		return
	})
	out.syncStatus.SetP2PStateFn(func(spaceId string) space.P2PState {
		return p2pStateFor(out.p2pEnabled, globalEnabled, discovery.Possibility(), peerStore.LocalPeerIds(spaceId), peerStore.GlobalPeerIds(spaceId), pickable)
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
	// a sibling carries no space set, so the space observer above never
	// fires for it; it counts for every space's GlobalPeers
	peerStore.AddSourceObserver(func(src p2p.Source, _ string, _ bool) {
		if src == p2p.SourceAccount {
			out.syncStatus.RefreshAll()
		}
	})
	// Start the rollup loop. The loop ticks once per second, drains
	// the dirty set, and dispatches SpaceSyncStatus events to
	// account-wide subscribers. Close() cancels via syncStatus.Close.
	out.syncStatus.Run(context.Background())
	// Wire the head cache into the sync handler so HeadSync's fast
	// path sees the same map updated by space loads, and the app
	// back-reference the peer-facing SpacePush handler loads through.
	sync.headCache = out.headCache
	sync.app.Store(out)
	return out, nil
}

// Close stops the any-sync app and releases resources. The space
// cache is closed first so per-space goroutines wind down before the
// app's components; the space stores close last.
func (a *App) Close(ctx context.Context) error {
	if a.spaceCache != nil {
		_ = a.spaceCache.Close()
	}
	if a.syncStatus != nil {
		a.syncStatus.Close()
	}
	err := a.a.Close(ctx)
	if a.storage != nil {
		a.storage.closeAll()
	}
	return err
}

// SyncStatus exposes the per-account sync-status registry. Its
// Trackers feed commonspace.Deps.SyncStatus; the space layer reads it
// for snapshot + subscribe.
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

// AclSnapshot builds an in-memory, verified ACL for spaceId from the
// records the nodes serve — the read the joining client and the ACL
// waiter each make on their own. It needs no local storage and
// materializes nothing, so it is safe on a space this account is not a
// member of: the join controller reads membership / the pending request
// off it for a row whose request was posted elsewhere.
func (a *App) AclSnapshot(ctx context.Context, spaceId string) (list.AclList, error) {
	if a.joining == nil {
		return nil, fmt.Errorf("anysyncx: acl snapshot %q: joining client unavailable", spaceId)
	}
	recs, err := a.joining.AclGetRecords(ctx, spaceId, "")
	if err != nil {
		return nil, fmt.Errorf("anysyncx: acl snapshot %q: %w", spaceId, err)
	}
	if len(recs) == 0 {
		return nil, fmt.Errorf("anysyncx: acl snapshot %q: no records", spaceId)
	}
	storage, err := list.NewInMemoryStorage(recs[0].Id, recs)
	if err != nil {
		return nil, fmt.Errorf("anysyncx: acl snapshot %q: %w", spaceId, err)
	}
	verifier := recordverifier.AcceptorVerifier(recordverifier.NewValidateFull())
	if networkId := a.nodeConf.Configuration().NetworkId; networkId != "" {
		netKey, err := crypto.DecodeNetworkId(networkId)
		if err != nil {
			return nil, fmt.Errorf("anysyncx: acl snapshot: invalid networkId: %w", err)
		}
		verifier = recordverifier.New(netKey)
	}
	acl, err := list.BuildAclListWithIdentity(a.keys, storage, verifier)
	if err != nil {
		return nil, fmt.Errorf("anysyncx: acl snapshot %q: %w", spaceId, err)
	}
	return acl, nil
}

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

// GlobalP2PEnabled reports whether the internet-wide layer is on
// (cfg.P2P.Global).
func (a *App) GlobalP2PEnabled() bool { return a.globalEnabled }

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

// SetAdvertiseFn wires the per-space p2p advertising decision (the
// tech-space row's switch; the tech space itself never gets a row —
// own devices come from the account record). Set before the tech
// space loads; loaded spaces are re-evaluated at once.
func (a *App) SetAdvertiseFn(fn func(spaceId string) bool) {
	if a.global != nil {
		a.global.SetAdvertiseFn(fn)
	}
}

// RepublishGlobalRecord re-sets this device's global p2p row in a space
// (advertising switched on).
func (a *App) RepublishGlobalRecord(spaceId string) {
	if a.global != nil {
		a.global.Republish(spaceId)
	}
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

// SpaceStorageClaim holds a space's any-sync store for deletion: nothing
// can open it until Delete removes it or Release gives it back.
type SpaceStorageClaim interface {
	// Delete closes the store and removes `<DataDir>/anysync/<spaceId>.db`.
	Delete(ctx context.Context) error
	// Release gives the store back without deleting it.
	Release()
}

// ClaimSpaceStorage claims a space's any-sync store for deletion. The
// offload path claims before EvictSpace, so no load can reopen the
// space while it is torn down.
func (a *App) ClaimSpaceStorage(ctx context.Context, spaceId string) (SpaceStorageClaim, error) {
	c, err := a.storage.ClaimSpaceStorage(ctx, spaceId)
	if err != nil {
		return nil, err
	}
	return c, nil
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
	// A tree fetched whole from a responsible peer (node, LAN or global)
	// is in sync with it: without this a space that converges through
	// peer pulls alone would sit in Syncing until a fully empty round.
	ts.onFetched = func(peerId, treeId string, heads []string) {
		a.syncStatus.For(spaceId).HeadsApply(peerId, treeId, heads, true)
	}
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

// pickLive reports a live pool connection to the peer within
// p2p.PickTimeout; it never dials and never waits out somebody else's
// dial.
func pickLive(pl pool.Pool, id string) bool {
	_, err := p2p.PickLive(context.Background(), pl, id)
	return err == nil
}

// p2pStateFor resolves a space's direct-peer state over both layers.
// A live LAN or global peer is Connected. With nobody live, the LAN
// verdicts (NotPossible on missing interfaces, Restricted on a denied
// local-network permission) still surface while the LAN layer is on —
// the global layer does not hide why the LAN path is down. Local
// discovery switched off stops finding peers but keeps the ones already
// known dialable, so a live LAN peer still counts and known ones make
// it NotConnected rather than NotPossible; NotPossible only when no
// layer can find anyone and nobody is known. Pure so it's unit-testable
// without the app graph.
func p2pStateFor(lanEnabled, globalEnabled bool, poss sdkp2p.Possibility, localPeerIds, globalPeerIds []string, pickable func(string) bool) space.P2PState {
	if !lanEnabled && !globalEnabled {
		return space.P2PStateNotPossible
	}
	lanUsable := lanEnabled && poss != sdkp2p.PossibilityNoInterfaces && poss != sdkp2p.PossibilityRestricted
	if lanUsable {
		for _, id := range localPeerIds {
			if pickable(id) {
				return space.P2PStateConnected
			}
		}
	}
	if globalEnabled {
		for _, id := range globalPeerIds {
			if pickable(id) {
				return space.P2PStateConnected
			}
		}
	}
	if lanEnabled {
		switch poss {
		case sdkp2p.PossibilityNoInterfaces:
			return space.P2PStateNotPossible
		case sdkp2p.PossibilityRestricted:
			return space.P2PStateRestricted
		case sdkp2p.PossibilityDisabled:
			if !globalEnabled && len(localPeerIds) == 0 {
				return space.P2PStateNotPossible
			}
		}
	}
	return space.P2PStateNotConnected
}

// globalPeersOrNil hands the peer manager its global peer source only
// when the layer is on, so an opted-out device never consults it.
func globalPeersOrNil(store *p2p.PeerStore, enabled bool) globalPeerSource {
	if !enabled {
		return nil
	}
	return store
}

// SetLocalDiscoveryEnabled switches mDNS announce and browse at runtime.
func (a *App) SetLocalDiscoveryEnabled(enabled bool) { a.discovery.SetEnabled(enabled) }

// LocalDiscoveryEnabled is the mDNS switch state.
func (a *App) LocalDiscoveryEnabled() bool { return a.discovery.Enabled() }

// P2PStatus is the account-wide p2p snapshot for the debug surface:
// LAN listener state, discovery possibility, every LAN peer with its
// live-connection flag, and the global layer.
func (a *App) P2PStatus() sdkp2p.Status {
	pl := a.Pool()
	pickable := func(id string) bool { return pickLive(pl, id) }
	allPeers := a.peerStore.AllLocalPeers()
	st := sdkp2p.Status{
		PeerId:          a.keys.PeerId,
		Enabled:         a.p2pEnabled,
		LocalDiscovery:  a.discovery.Enabled(),
		ListenerStarted: a.p2pServer.Started(),
		Port:            a.p2pServer.Port(),
		Possibility:     a.discovery.Possibility(),
		State:           p2pStateFor(a.p2pEnabled, a.globalEnabled, a.discovery.Possibility(), allPeers, a.peerStore.AllGlobalPeers(), pickable),
		Global:          sdkp2p.GlobalStatus{Enabled: a.globalEnabled},
	}
	for _, peerId := range allPeers {
		ps := sdkp2p.PeerStatus{
			PeerId:    peerId,
			SpaceIds:  a.peerStore.SpaceIds(peerId),
			Connected: pickable(peerId),
		}
		for _, src := range a.peerStore.Sources(peerId) {
			ps.Sources = append(ps.Sources, src.String())
		}
		// liveness fields exist only for peers known through records
		if a.global != nil && a.peerStore.HasGlobalPeer(peerId) {
			gs := a.global.PeerStatus(peerId)
			ps.LastSeen, ps.Tier, ps.Failures = gs.LastSeen, gs.Tier, gs.Failures
		}
		st.Peers = append(st.Peers, ps)
	}
	if a.global != nil {
		st.Global = a.global.Status()
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
