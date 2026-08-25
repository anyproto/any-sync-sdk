package p2p

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/url"
	"slices"
	"strconv"
	"sync/atomic"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/clientspaceproto"
	"github.com/anyproto/any-sync/net/peer"
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
// The ACL-derived key alone dead-locks the offline cold restore: a
// fresh device of the SAME account knows a space's id (from the synced
// tech-space index) but can't derive its discovery key before pulling
// the space — and can't pull over LAN without the key. PROBE tokens
// break the cycle: for known-but-keyless spaces the caller sends a
// token keyed by an account-derived key (HKDF of the account signing
// key — only the account's own devices hold it). The responder answers
// a probe with a membership proof but records NOTHING: a probe claims
// interest, not possession, so it must not put the caller into the
// responder's per-space peer set. The caller records the responder as
// holding the space and pulls from it; the post-pull re-handshake
// (storage set change → Broadcast) then advertises the space normally.
//
// The legacy plaintext SpaceExchange v1 is NOT supported: the SDK never
// calls it, and the inbound handler refuses it — full space-id lists
// must never leave this device, and there is no fallback an attacker
// could downgrade to. Pre-v2 peers simply don't pair over LAN.
type Exchange struct {
	addrs       lanAddrs
	pool        dialPool
	store       *PeerStore
	selfPeerId  string
	allSpaceIds func() []string
	// discoveryKeys resolves per-space discovery keys (spaceId → key)
	// for the v2 token exchange; spaces absent from the result are not
	// covered by the exchange. See anysyncx.discoveryKeySource.
	discoveryKeys func(ctx context.Context, spaceIds []string) map[string][]byte
	// accountKeys resolves per-space ACCOUNT-derived discovery keys —
	// derivable by every device of this account without the space's
	// ACL. Used for probe tokens (cold restore) and for answering
	// probes on spaces this device holds. Nil disables probing.
	accountKeys func(ctx context.Context, spaceIds []string) map[string][]byte
	// knownSpaceIds lists spaces this device knows OF (tech-space
	// index) but may not store yet — the probe candidates. Nil
	// disables probing. Atomic because it's wired after the discovery
	// loop is already handshaking (see SetKnownSpaceIdsFn).
	knownSpaceIds atomic.Pointer[func() []string]
	// onPeerUpdated fires after a handshake recorded a peer's shared
	// space set — the app layer uses it to kick an immediate head-sync
	// of the shared spaces instead of waiting for the periodic diff.
	onPeerUpdated func(peerId string, spaceIds []string)
	// ownAddrs supplies this device's current announce (LAN IPs +
	// port) for proactive re-handshakes; see Broadcast.
	ownAddrs func() sdkp2p.OwnAddresses
}

// lanAddrs / dialPool are the slices of the addr book and any-sync
// this component touches, held as narrow interfaces so tests can fake
// them. LAN addresses go through the AddrBook, which keeps them apart
// from a peer's iroh ticket.
type lanAddrs interface {
	SetLAN(peerId string, addrs []string)
	ClearLAN(peerId string)
}

type dialPool interface {
	Get(ctx context.Context, id string) (peer.Peer, error)
}

func NewExchange(selfPeerId string, store *PeerStore, allSpaceIds func() []string, discoveryKeys func(ctx context.Context, spaceIds []string) map[string][]byte, onPeerUpdated func(peerId string, spaceIds []string)) *Exchange {
	return &Exchange{selfPeerId: selfPeerId, store: store, allSpaceIds: allSpaceIds, discoveryKeys: discoveryKeys, onPeerUpdated: onPeerUpdated}
}

func (e *Exchange) Init(a *app.App) error {
	if e.addrs == nil {
		e.addrs = a.MustComponent(addrBookCName).(*AddrBook)
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

// SetAccountKeysFn wires the account-derived discovery key source for
// probe tokens. Set once during app assembly.
func (e *Exchange) SetAccountKeysFn(fn func(ctx context.Context, spaceIds []string) map[string][]byte) {
	e.accountKeys = fn
}

// SetKnownSpaceIdsFn wires the known-space-ids source (the tech-space
// index) for probe tokens. Set after the SDK layers are up — discovery
// handshakes may already be running concurrently, hence the atomic;
// handshakes that run before simply don't probe.
func (e *Exchange) SetKnownSpaceIdsFn(fn func() []string) { e.knownSpaceIds.Store(&fn) }

// PeerDiscovered is the discovery notifier: register the peer's
// addresses, dial, and run the handshake. Errors are logged, not
// returned — discovery re-announces periodically, so a failed attempt
// retries on the next sighting.
func (e *Exchange) PeerDiscovered(ctx context.Context, discovered sdkp2p.DiscoveredPeer, own sdkp2p.OwnAddresses) {
	e.addrs.SetLAN(discovered.PeerId, addSchema(discovered.Addrs))
	if !e.handshake(ctx, discovered.PeerId, own) && !e.store.HasLocalPeer(discovered.PeerId) {
		// never handshaked: the addresses would otherwise pin the peer to
		// the LAN and keep its iroh ticket, if any, from taking over
		e.addrs.ClearLAN(discovered.PeerId)
	}
}

// PeerLost is the discovery notifier for a peer that left the LAN: its
// LAN addresses and presence go, so its iroh ticket, if any, takes
// over. The next sighting re-adds it through PeerDiscovered.
func (e *Exchange) PeerLost(peerId string) {
	e.addrs.ClearLAN(peerId)
	e.store.RemoveLocalPeer(peerId)
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
// Reports whether the round completed.
func (e *Exchange) handshake(ctx context.Context, peerId string, own sdkp2p.OwnAddresses) bool {
	p, err := e.pool.Get(ctx, peerId)
	if err != nil {
		log.Info("dial local peer", zap.String("peerId", peerId), zap.Error(err))
		return false
	}
	spaceIds := e.allSpaceIds()
	keys := e.discoveryKeys(ctx, spaceIds)
	probeIds, probeKeys := e.probeSet(ctx, spaceIds, keys)
	nonce, err := clientspaceproto.NewNonceV2()
	if err != nil {
		log.Error("space exchange v2: nonce", zap.Error(err))
		return false
	}
	tokens := make([][]byte, 0, len(keys)+len(probeIds))
	for _, spaceId := range spaceIds {
		if key, ok := keys[spaceId]; ok {
			tokens = append(tokens, clientspaceproto.RequestTokenV2(key, nonce, e.selfPeerId, peerId))
		}
	}
	for _, spaceId := range probeIds {
		if key, ok := probeKeys[spaceId]; ok {
			tokens = append(tokens, probeRequestTokenV2(key, nonce, e.selfPeerId, peerId))
		}
	}
	tokens, err = clientspaceproto.PadTokensV2(tokens)
	if err != nil {
		log.Error("space exchange v2: pad", zap.Error(err))
		return false
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
		return false
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
	// A proof for a probed space means the peer HOLDS it (and is a
	// device of this account) — record it so the space pull has a LAN
	// peer to fetch from.
	for _, spaceId := range probeIds {
		key, ok := probeKeys[spaceId]
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
	return true
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
	akeys := e.accountKeysFor(ctx, spaceIds)
	var (
		shared     []string
		respTokens [][]byte
	)
	for _, spaceId := range spaceIds {
		if key, ok := keys[spaceId]; ok {
			if _, ok = received[string(clientspaceproto.RequestTokenV2(key, req.Nonce, peerId, e.selfPeerId))]; ok {
				shared = append(shared, spaceId)
				respTokens = append(respTokens, clientspaceproto.ResponseTokenV2(key, req.Nonce, peerId, e.selfPeerId))
				continue
			}
		}
		// Probe from a same-account device that can't derive the ACL
		// key yet: prove we hold the space, but do NOT add it to
		// shared — the caller doesn't hold it, so it must not enter
		// our per-space peer set (it can't serve pulls or absorb
		// pushes for a space it hasn't materialized).
		if akey, ok := akeys[spaceId]; ok {
			if _, ok = received[string(probeRequestTokenV2(akey, req.Nonce, peerId, e.selfPeerId))]; ok {
				respTokens = append(respTokens, clientspaceproto.ResponseTokenV2(akey, req.Nonce, peerId, e.selfPeerId))
			}
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
	e.addrs.SetLAN(peerId, addSchema(addrs))
	return true
}

// probeSet computes the probe candidates for one handshake: every
// space this device knows of (tech-space index) or stores but has no
// ACL-derived key for, paired with their account-derived keys. Empty
// when the probe sources aren't wired.
func (e *Exchange) probeSet(ctx context.Context, storedIds []string, keyed map[string][]byte) ([]string, map[string][]byte) {
	knownFn := e.knownSpaceIds.Load()
	if knownFn == nil || e.accountKeys == nil {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(storedIds))
	var probeIds []string
	for _, id := range append((*knownFn)(), storedIds...) {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if _, ok := keyed[id]; ok {
			continue
		}
		probeIds = append(probeIds, id)
	}
	if len(probeIds) == 0 {
		return nil, nil
	}
	return probeIds, e.accountKeys(ctx, probeIds)
}

// accountKeysFor is the nil-safe responder-side account key lookup.
func (e *Exchange) accountKeysFor(ctx context.Context, spaceIds []string) map[string][]byte {
	if e.accountKeys == nil {
		return nil
	}
	return e.accountKeys(ctx, spaceIds)
}

// probeLabelV2 domain-separates probe tokens from any-sync's
// request/response labels: a probe claims "same account + interested",
// never "holds the space", and must not collide with either.
var probeLabelV2 = []byte("sdk:probe-request:v1")

// probeRequestTokenV2 is the caller's token for a space it knows of
// but cannot derive the ACL discovery key for. Same HMAC construction
// as any-sync's tokenV2 (length-prefixed fields) with the probe label
// and the account-derived key.
func probeRequestTokenV2(accountKey, nonce []byte, callerPeerId, responderPeerId string) []byte {
	mac := hmac.New(sha256.New, accountKey)
	writeTokenField(mac, probeLabelV2)
	writeTokenField(mac, nonce)
	writeTokenField(mac, []byte(callerPeerId))
	writeTokenField(mac, []byte(responderPeerId))
	return mac.Sum(nil)
}

// writeTokenField length-prefixes a field so variable-length peer ids
// cannot produce colliding concatenations — mirrors any-sync's private
// writeField.
func writeTokenField(w io.Writer, field []byte) {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(field)))
	_, _ = w.Write(lenBuf[:])
	_, _ = w.Write(field)
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

// addSchema registers each addr under BOTH transports. SDK peers listen
// on TCP (yamux) and UDP (QUIC) on the same port; yamux is what we want
// dialed first on the LAN — a dead peer answers a TCP dial with an RST
// in one RTT, while a QUIC dial waits out the whole handshake timeout
// because quic-go gets no ICMP feedback on its unconnected dial
// sockets. peerservice's per-address ordering puts yamux first for
// local addrs on its own; the quic candidate stays as the fallback for
// peers that still listen quic-only.
func addSchema(addrs []string) []string {
	out := make([]string, 0, 2*len(addrs))
	for _, addr := range addrs {
		out = append(out, transport.Yamux+"://"+addr, transport.Quic+"://"+addr)
	}
	return out
}
