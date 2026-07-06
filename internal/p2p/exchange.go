package p2p

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/clientspaceproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/peerservice"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/rpc/server"
	"github.com/anyproto/any-sync/net/transport"
	"go.uber.org/zap"
	"storj.io/drpc"

	sdkp2p "github.com/anyproto/any-sync-sdk/p2p"
)

const exchangeCName = "sdk.p2p.exchange"

var log = logger.NewNamed("sdk.p2p")

// Exchange runs the SpaceExchangeV2 handshake with discovered local
// peers: both sides learn each other's dialable addresses and which
// spaces they SHARE, recorded in the PeerStore. Reuses any-sync's
// clientspaceproto wire shape.
//
// Peers exchange per-space HMAC tokens keyed by a member-only discovery
// key and learn only the INTERSECTION of their space sets. A matching
// token proves the sender's membership, so strangers on the LAN learn
// nothing, can't track a device across sessions, and can't poison the
// peer store with spaces they don't hold. Spaces whose discovery key
// isn't derivable yet (ACL not synced) are skipped until it is.
//
// The legacy plaintext SpaceExchange v1 is NOT supported: the SDK never
// calls it, and the inbound handler refuses it — full space-id lists
// must never leave this device, and there is no fallback an attacker
// could downgrade to. Pre-v2 peers simply don't pair over LAN.
type Exchange struct {
	peerService peerService
	pool        dialPool
	store       *PeerStore
	selfPeerId  string
	allSpaceIds func() []string
	// discoveryKeys resolves per-space discovery keys (spaceId → key)
	// for the v2 token exchange; spaces absent from the result are not
	// covered by the exchange. See anysyncx.discoveryKeySource.
	discoveryKeys func(ctx context.Context, spaceIds []string) map[string][]byte
	// onPeerUpdated fires after a handshake recorded a peer's shared
	// space set — the app layer uses it to kick an immediate head-sync
	// of the shared spaces instead of waiting for the periodic diff.
	onPeerUpdated func(peerId string, spaceIds []string)
	// ownAddrs supplies this device's current announce (LAN IPs +
	// port) for proactive re-handshakes; see Broadcast.
	ownAddrs func() sdkp2p.OwnAddresses
}

// peerService / dialPool are the slices of any-sync this component
// touches, held as narrow interfaces so tests can fake them.
type peerService interface {
	SetPeerAddrs(peerId string, addrs []string)
}

type dialPool interface {
	Get(ctx context.Context, id string) (peer.Peer, error)
}

func NewExchange(selfPeerId string, store *PeerStore, allSpaceIds func() []string, discoveryKeys func(ctx context.Context, spaceIds []string) map[string][]byte, onPeerUpdated func(peerId string, spaceIds []string)) *Exchange {
	return &Exchange{selfPeerId: selfPeerId, store: store, allSpaceIds: allSpaceIds, discoveryKeys: discoveryKeys, onPeerUpdated: onPeerUpdated}
}

func (e *Exchange) Init(a *app.App) error {
	if e.peerService == nil {
		e.peerService = a.MustComponent(peerservice.CName).(peerservice.PeerService)
	}
	if e.pool == nil {
		e.pool = a.MustComponent(pool.CName).(pool.Pool)
	}
	return clientspaceproto.DRPCRegisterClientSpace(a.MustComponent(server.CName).(server.DRPCServer), e)
}

func (e *Exchange) Name() string { return exchangeCName }

// SetOwnAddressesFn wires the announce source Broadcast embeds in
// proactive re-handshakes. Set once during app assembly.
func (e *Exchange) SetOwnAddressesFn(fn func() sdkp2p.OwnAddresses) { e.ownAddrs = fn }

// PeerDiscovered is the discovery notifier: register the peer's
// addresses, dial, and run the handshake. Errors are logged, not
// returned — discovery re-announces periodically, so a failed attempt
// retries on the next sighting.
func (e *Exchange) PeerDiscovered(ctx context.Context, discovered sdkp2p.DiscoveredPeer, own sdkp2p.OwnAddresses) {
	e.peerService.SetPeerAddrs(discovered.PeerId, addSchema(discovered.Addrs))
	e.handshake(ctx, discovered.PeerId, own)
}

// Broadcast re-runs the handshake with every known local peer. Called
// when this device's own space set changes (space created, pulled, or
// deleted) so peers learn the new set promptly instead of on the next
// discovery resweep.
func (e *Exchange) Broadcast(ctx context.Context) {
	if e.ownAddrs == nil {
		return
	}
	own := e.ownAddrs()
	for _, peerId := range e.store.AllLocalPeers() {
		e.handshake(ctx, peerId, own)
	}
}

// handshake dials a peer whose addresses are already registered and
// runs one SpaceExchangeV2 round, recording the result. A peer too old
// to serve v2 just fails here (logged) — there is no v1 fallback.
func (e *Exchange) handshake(ctx context.Context, peerId string, own sdkp2p.OwnAddresses) {
	p, err := e.pool.Get(ctx, peerId)
	if err != nil {
		log.Info("dial local peer", zap.String("peerId", peerId), zap.Error(err))
		return
	}
	spaceIds := e.allSpaceIds()
	keys := e.discoveryKeys(ctx, spaceIds)
	nonce, err := clientspaceproto.NewNonceV2()
	if err != nil {
		log.Error("space exchange v2: nonce", zap.Error(err))
		return
	}
	tokens := make([][]byte, 0, len(keys))
	for _, spaceId := range spaceIds {
		if key, ok := keys[spaceId]; ok {
			tokens = append(tokens, clientspaceproto.RequestTokenV2(key, nonce, e.selfPeerId, peerId))
		}
	}
	tokens, err = clientspaceproto.PadTokensV2(tokens)
	if err != nil {
		log.Error("space exchange v2: pad", zap.Error(err))
		return
	}
	var resp *clientspaceproto.SpaceExchangeV2Response
	err = p.DoDrpc(ctx, func(conn drpc.Conn) error {
		var dErr error
		resp, dErr = clientspaceproto.NewDRPCClientSpaceClient(conn).SpaceExchangeV2(ctx, &clientspaceproto.SpaceExchangeV2Request{
			Nonce:       nonce,
			SpaceTokens: tokens,
			LocalServer: &clientspaceproto.LocalServer{
				Ips:  own.Addrs,
				Port: int32(own.Port),
			},
		})
		return dErr
	})
	if err != nil {
		log.Info("space exchange v2", zap.String("peerId", peerId), zap.Error(err))
		return
	}
	received := tokenSet(resp.SpaceTokens)
	var shared []string
	for _, spaceId := range spaceIds {
		key, ok := keys[spaceId]
		if !ok {
			continue
		}
		if _, ok = received[string(clientspaceproto.ResponseTokenV2(key, nonce, e.selfPeerId, peerId))]; ok {
			shared = append(shared, spaceId)
		}
	}
	log.Debug("space exchange v2 done", zap.String("peerId", peerId), zap.Int("shared", len(shared)))
	e.store.UpdateLocalPeer(peerId, shared)
	if e.onPeerUpdated != nil {
		e.onPeerUpdated(peerId, shared)
	}
}

// SpaceExchange refuses the legacy plaintext v1 handshake: it would
// hand our full space-id list to any LAN peer, and serving it at all
// would give an active attacker a downgrade target. The method exists
// only because the DRPC service interface requires it.
func (e *Exchange) SpaceExchange(_ context.Context, _ *clientspaceproto.SpaceExchangeRequest) (*clientspaceproto.SpaceExchangeResponse, error) {
	return nil, fmt.Errorf("p2p: legacy SpaceExchange is not supported; use SpaceExchangeV2")
}

// SpaceExchangeV2 is the inbound side of the token handshake: compute
// this device's expected request token for every space it holds a
// discovery key for, intersect with what the caller sent, and answer
// with membership proofs for the intersection only — keyed by the
// caller's nonce, so they can't be precomputed or replayed.
func (e *Exchange) SpaceExchangeV2(ctx context.Context, req *clientspaceproto.SpaceExchangeV2Request) (*clientspaceproto.SpaceExchangeV2Response, error) {
	peerId, err := e.callerPeerId(ctx)
	if err != nil {
		return nil, err
	}
	if len(req.Nonce) != clientspaceproto.NonceSizeV2 {
		return nil, fmt.Errorf("p2p: space exchange v2: bad nonce size %d", len(req.Nonce))
	}
	if len(req.SpaceTokens) > clientspaceproto.MaxTokensV2 {
		return nil, fmt.Errorf("p2p: space exchange v2: too many tokens (%d)", len(req.SpaceTokens))
	}
	received := tokenSet(req.SpaceTokens)

	spaceIds := e.allSpaceIds()
	keys := e.discoveryKeys(ctx, spaceIds)
	var (
		shared     []string
		respTokens [][]byte
	)
	for _, spaceId := range spaceIds {
		key, ok := keys[spaceId]
		if !ok {
			continue
		}
		if _, ok = received[string(clientspaceproto.RequestTokenV2(key, req.Nonce, peerId, e.selfPeerId))]; ok {
			shared = append(shared, spaceId)
			respTokens = append(respTokens, clientspaceproto.ResponseTokenV2(key, req.Nonce, peerId, e.selfPeerId))
		}
	}
	// A request without LocalServer is a plain probe: answer the proofs
	// (they cost the caller a valid membership token per space) but
	// record nothing.
	if req.LocalServer != nil && e.recordPeerAddrs(ctx, peerId, req.LocalServer) {
		e.store.UpdateLocalPeer(peerId, shared)
		log.Debug("space exchange v2 received", zap.String("peerId", peerId), zap.Int("shared", len(shared)))
		if e.onPeerUpdated != nil {
			e.onPeerUpdated(peerId, shared)
		}
	}
	return &clientspaceproto.SpaceExchangeV2Response{SpaceTokens: respTokens}, nil
}

// callerPeerId extracts the authenticated peer id of the caller and
// refuses a remote presenting OUR peer id — two devices sharing a
// device key (e.g. a copied wallet file). They can never pair —
// discovery filters "self" by peerId — so make the misconfiguration
// loud instead of silently ignoring it.
func (e *Exchange) callerPeerId(ctx context.Context) (string, error) {
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return "", err
	}
	if peerId == e.selfPeerId {
		log.Error("space exchange from a peer with OUR OWN peer id — two devices share a device key; refusing",
			zap.String("peerId", peerId))
		return "", fmt.Errorf("p2p: remote peer uses this device's own peer id %s (shared device key?)", peerId)
	}
	return peerId, nil
}

// recordPeerAddrs validates the caller's announced LAN addresses and
// registers them with the peer service, preferring the address it
// actually dialed us from. Returns false when nothing usable was
// announced — the caller's space set is then not recorded either.
func (e *Exchange) recordPeerAddrs(ctx context.Context, peerId string, localServer *clientspaceproto.LocalServer) bool {
	port := int(localServer.Port)
	if port <= 0 || port > 65535 {
		log.Info("space exchange with invalid port; ignoring addresses",
			zap.String("peerId", peerId), zap.Int32("port", localServer.Port))
		return false
	}
	var addrs []string
	// The IP the peer connected FROM is the one proven to route, but
	// its port is the dialer's ephemeral source port — nobody listens
	// there. Pair that observed IP with the peer's ADVERTISED listen
	// port instead so dial-back after a drop reaches a live socket.
	if peerAddr := peer.CtxPeerAddr(ctx); peerAddr != "" {
		if u, uErr := url.Parse(peerAddr); uErr == nil {
			if host, _, sErr := net.SplitHostPort(u.Host); sErr == nil {
				if ip := net.ParseIP(host); ip != nil {
					addrs = appendAddr(addrs, ip, port)
				}
			}
		}
	}
	for _, raw := range localServer.Ips {
		ip := net.ParseIP(raw)
		if ip == nil {
			// Reject anything that isn't a literal IP — no hostnames,
			// no garbage that could displace good addresses or point
			// us at an arbitrary host.
			continue
		}
		addrs = appendAddr(addrs, ip, port)
	}
	if len(addrs) == 0 {
		log.Info("space exchange with no usable addresses", zap.String("peerId", peerId))
		return false
	}
	e.peerService.SetPeerAddrs(peerId, addSchema(addrs))
	return true
}

// tokenSet indexes received tokens for O(1) matching. Undersized
// entries are dropped (real tokens are exactly TokenSizeV2 bytes).
func tokenSet(tokens [][]byte) map[string]struct{} {
	set := make(map[string]struct{}, len(tokens))
	for _, t := range tokens {
		if len(t) == clientspaceproto.TokenSizeV2 {
			set[string(t)] = struct{}{}
		}
	}
	return set
}

// appendAddr adds "ip:port" to addrs unless already present. IP is a
// validated net.IP; JoinHostPort brackets IPv6 correctly.
func appendAddr(addrs []string, ip net.IP, port int) []string {
	a := net.JoinHostPort(ip.String(), strconv.Itoa(port))
	if slices.Contains(addrs, a) {
		return addrs
	}
	return append(addrs, a)
}

// addSchema pins the quic transport on each addr. Without an explicit
// scheme the dialer falls back to yamux/TCP, but SDK peers listen on
// UDP/QUIC only.
func addSchema(addrs []string) []string {
	out := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		out = append(out, transport.Quic+"://"+addr)
	}
	return out
}
