package p2p

import (
	"context"
	"fmt"
	"net/url"
	"slices"

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

// Exchange runs the symmetric SpaceExchange handshake with discovered
// local peers: both sides learn each other's dialable addresses and
// full space-id lists, recorded in the PeerStore. Reuses any-sync's
// clientspaceproto wire shape (same one anytype-heart speaks).
//
// SECURITY: the handshake is unauthenticated-by-space — any LAN peer
// that completes the secure-channel handshake learns ALL space ids on
// this device. Accepted for now (space ids grant no data access; sync
// still enforces ACLs); replacing this with a space-scoped exchange is
// a tracked follow-up.
type Exchange struct {
	peerService peerService
	pool        dialPool
	store       *PeerStore
	selfPeerId  string
	allSpaceIds func() []string
	// onPeerUpdated fires after a handshake recorded a peer's space
	// set — the app layer uses it to kick an immediate head-sync of
	// the shared spaces instead of waiting for the periodic diff.
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

func NewExchange(selfPeerId string, store *PeerStore, allSpaceIds func() []string, onPeerUpdated func(peerId string, spaceIds []string)) *Exchange {
	return &Exchange{selfPeerId: selfPeerId, store: store, allSpaceIds: allSpaceIds, onPeerUpdated: onPeerUpdated}
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
// runs one SpaceExchange round, recording the result.
func (e *Exchange) handshake(ctx context.Context, peerId string, own sdkp2p.OwnAddresses) {
	p, err := e.pool.Get(ctx, peerId)
	if err != nil {
		log.Info("dial local peer", zap.String("peerId", peerId), zap.Error(err))
		return
	}
	var resp *clientspaceproto.SpaceExchangeResponse
	err = p.DoDrpc(ctx, func(conn drpc.Conn) error {
		var dErr error
		resp, dErr = clientspaceproto.NewDRPCClientSpaceClient(conn).SpaceExchange(ctx, &clientspaceproto.SpaceExchangeRequest{
			SpaceIds: e.allSpaceIds(),
			LocalServer: &clientspaceproto.LocalServer{
				Ips:  own.Addrs,
				Port: int32(own.Port),
			},
		})
		return dErr
	})
	if err != nil {
		log.Info("space exchange", zap.String("peerId", peerId), zap.Error(err))
		return
	}
	log.Debug("space exchange done", zap.String("peerId", peerId), zap.Int("spaces", len(resp.SpaceIds)))
	e.store.UpdateLocalPeer(peerId, resp.SpaceIds)
	if e.onPeerUpdated != nil {
		e.onPeerUpdated(peerId, resp.SpaceIds)
	}
}

// SpaceExchange is the inbound side of the handshake. Mirrors the
// outbound: record the caller's addresses (preferring the one it
// actually dialed us from) and space set, return our own space ids.
func (e *Exchange) SpaceExchange(ctx context.Context, req *clientspaceproto.SpaceExchangeRequest) (*clientspaceproto.SpaceExchangeResponse, error) {
	if req.LocalServer != nil {
		peerId, err := peer.CtxPeerId(ctx)
		if err != nil {
			return nil, err
		}
		if peerId == e.selfPeerId {
			// Another device presenting OUR peer id means two devices
			// share a device key (e.g. a copied wallet file). They can
			// never pair — discovery filters "self" by peerId — so make
			// the misconfiguration loud instead of silently ignoring it.
			log.Error("space exchange from a peer with OUR OWN peer id — two devices share a device key; refusing",
				zap.String("peerId", peerId))
			return nil, fmt.Errorf("p2p: remote peer uses this device's own peer id %s (shared device key?)", peerId)
		}
		var addrs []string
		if peerAddr := peer.CtxPeerAddr(ctx); peerAddr != "" {
			// The address the peer connected from is the one proven to
			// route — put it first.
			if u, uErr := url.Parse(peerAddr); uErr == nil && u.Host != "" {
				addrs = append(addrs, u.Host)
			}
		}
		for _, ip := range req.LocalServer.Ips {
			addr := fmt.Sprintf("%s:%d", ip, req.LocalServer.Port)
			if !slices.Contains(addrs, addr) {
				addrs = append(addrs, addr)
			}
		}
		e.peerService.SetPeerAddrs(peerId, addSchema(addrs))
		e.store.UpdateLocalPeer(peerId, req.SpaceIds)
		log.Debug("space exchange received", zap.String("peerId", peerId), zap.Int("spaces", len(req.SpaceIds)))
		if e.onPeerUpdated != nil {
			e.onPeerUpdated(peerId, req.SpaceIds)
		}
	}
	return &clientspaceproto.SpaceExchangeResponse{SpaceIds: e.allSpaceIds()}, nil
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
