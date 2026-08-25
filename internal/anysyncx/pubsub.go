// Client wiring for any-sync's commonspace/pubsub: the Deps adapters
// (peers / crypto / membership), the RPC-registration component, and
// the App-level pass-throughs the space layer calls. The engine itself
// (pubsub.New) is registered as a regular app component in app.go; the
// adapters hold a late-bound *App because their methods only run after
// app.Start.
package anysyncx

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/pubsub"
	"github.com/anyproto/any-sync/commonspace/pubsub/pubsubproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/server"
	"github.com/anyproto/any-sync/util/crypto"
	"go.uber.org/zap"
)

var psLog = logger.NewNamed("anysyncx.pubsub")

// pubsubEncKeyPath is the SLIP-0021 path deriving the pubsub payload
// key from the space read key. Domain-separated from the raw ACL read
// key and from the SDK's other derived keys (payloads); the wire keyId
// stays the ACL key-record id of the read key. Wire-compat constant —
// never change once shipped.
const pubsubEncKeyPath = "m/SLIP-0021/anysync-sdk/pubsub/enc"

// ErrPubSubNoKey is returned by the pubsub crypto adapter when the
// local identity has no read access to the space (keyless reader) or
// the referenced key-record id is unknown. Publishing without a read
// key fails rather than silently sending plaintext into an encrypted
// space.
var ErrPubSubNoKey = errors.New("anysyncx: no pubsub key")

// ErrPubSubGuestSpace rejects pubsub on guest-mode (public access)
// spaces: the engine signs and handshakes as the real account, which a
// guest space's ACL does not contain, so peers would silently reject
// every publish and subscribe as non-member. Fail fast instead.
var ErrPubSubGuestSpace = errors.New("anysyncx: pubsub unavailable in guest-mode spaces")

// aclOpTimeout bounds the space-handle acquisition on the encrypt
// path, which the engine calls without a context: a wedged space load
// must not stall a Publish forever.
const aclOpTimeout = 10 * time.Second

// pubsubKeyCacheLimit bounds the derived-key cache. Kids accumulate
// across every space's key rotations for the process lifetime; on
// overflow the cache resets wholesale — derivation is a cheap re-do on
// miss.
const pubsubKeyCacheLimit = 1024

// pubsubPeers is the engine's PeerProvider: the responsible sync node,
// every connectable LAN peer sharing the space, and every global peer
// sharing it that is ALREADY connected (pool.Pick — never a dial; the
// global connector owns those). Local-only spaces resolve nobody
// (mirrors localPeerManager), so publishes on them deliver
// loopback-only. Dial errors drop the peer silently: the engine only
// logs peer-resolution failures and its resync loop retries.
type pubsubPeers struct {
	app *App
}

func (p *pubsubPeers) SpacePeers(ctx context.Context, spaceId string) ([]peer.Peer, error) {
	a := p.app
	if a == nil || a.localOnly.has(spaceId) {
		return nil, nil
	}
	var peers []peer.Peer
	if nodeIds := a.nodeConf.NodeIds(spaceId); len(nodeIds) > 0 {
		if np, err := a.Pool().GetOneOf(ctx, nodeIds); err == nil {
			peers = append(peers, np)
		}
	}
	for _, id := range a.peerStore.LocalPeerIds(spaceId) {
		if lp, err := a.Pool().Get(ctx, id); err == nil {
			peers = append(peers, lp)
		}
	}
	if a.globalEnabled {
		for _, id := range a.peerStore.GlobalPeerIds(spaceId) {
			if gp, err := a.Pool().Pick(ctx, id); err == nil {
				peers = append(peers, gp)
			}
		}
	}
	return peers, nil
}

// pubsubCrypto is the engine's Crypto: encrypt with the space's current
// ACL read key (keyId = the ACL key-record id), decrypt via the
// historical key set (any-sync retains rotated read keys in
// AclState().Keys()). Derived keys are cached per kid — key-record ids
// are content-addressed, so a flat map is safe across spaces.
type pubsubCrypto struct {
	app *App

	mu    sync.Mutex
	cache map[string]crypto.SymKey
}

func (c *pubsubCrypto) Encrypt(spaceId string, payload []byte) (keyId string, encrypted []byte, err error) {
	acl, err := c.aclFor(spaceId, true)
	if err != nil {
		return "", nil, err
	}
	acl.RLock()
	state := acl.AclState()
	kid := state.CurrentReadKeyId()
	readKey, keyErr := state.CurrentReadKey()
	acl.RUnlock()
	if keyErr != nil || readKey == nil {
		return "", nil, ErrPubSubNoKey
	}
	key, err := c.derived(kid, readKey)
	if err != nil {
		return "", nil, err
	}
	encrypted, err = key.Encrypt(payload)
	if err != nil {
		return "", nil, err
	}
	return kid, encrypted, nil
}

func (c *pubsubCrypto) Decrypt(spaceId, keyId string, encrypted []byte) ([]byte, error) {
	c.mu.Lock()
	key := c.cache[keyId]
	c.mu.Unlock()
	if key == nil {
		// Resident-only (no load): Decrypt runs inline on the engine's
		// stream read path, where a blocking space load would stall
		// every pubsub frame from that peer — and membership already
		// proved residency for anything that reaches here.
		acl, err := c.aclFor(spaceId, false)
		if err != nil {
			return nil, err
		}
		acl.RLock()
		keys, ok := acl.AclState().Keys()[keyId]
		acl.RUnlock()
		if !ok || keys.ReadKey == nil {
			return nil, ErrPubSubNoKey
		}
		if key, err = c.derived(keyId, keys.ReadKey); err != nil {
			return nil, err
		}
	}
	return key.Decrypt(encrypted)
}

// aclFor resolves the space's ACL. load=true (publish path) may load
// the space, bounded by aclOpTimeout; load=false (receive path) is
// PickSpace-only — non-resident spaces error and the engine drops the
// message.
func (c *pubsubCrypto) aclFor(spaceId string, load bool) (list.AclList, error) {
	a := c.app
	if a == nil {
		return nil, errors.New("anysyncx: pubsub crypto not wired")
	}
	ctx, cancel := context.WithTimeout(context.Background(), aclOpTimeout)
	defer cancel()
	var handle SpaceHandle
	if load {
		var err error
		if handle, err = a.GetSpace(ctx, spaceId); err != nil {
			return nil, fmt.Errorf("anysyncx: pubsub load space: %w", err)
		}
	} else {
		var ok bool
		if handle, ok = a.PickSpace(ctx, spaceId); !ok {
			return nil, fmt.Errorf("anysyncx: pubsub: space %s not resident", spaceId)
		}
	}
	acl := handle.Inner().Acl()
	if acl == nil {
		return nil, ErrPubSubNoKey
	}
	return acl, nil
}

func (c *pubsubCrypto) derived(kid string, readKey crypto.SymKey) (crypto.SymKey, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if key, ok := c.cache[kid]; ok {
		return key, nil
	}
	raw, err := readKey.Raw()
	if err != nil {
		return nil, fmt.Errorf("anysyncx: pubsub read key raw: %w", err)
	}
	key, err := crypto.DeriveSymmetricKey(raw, pubsubEncKeyPath)
	if err != nil {
		return nil, err
	}
	if c.cache == nil || len(c.cache) >= pubsubKeyCacheLimit {
		c.cache = make(map[string]crypto.SymKey)
	}
	c.cache[kid] = key
	return key, nil
}

// pubsubMembership is the engine's MembershipChecker: identity must be
// StatusActive in the space's ACL. Resident spaces only (PickSpace,
// never a load) — the check runs on peer-driven paths (inbound LAN
// subscribes, the receive filter), and an unauthenticated peer naming
// arbitrary space ids must not be able to force space loads.
type pubsubMembership struct {
	app *App
}

func (m *pubsubMembership) CheckMember(ctx context.Context, spaceId string, identity crypto.PubKey) error {
	a := m.app
	if a == nil {
		return errors.New("anysyncx: pubsub membership not wired")
	}
	handle, ok := a.PickSpace(ctx, spaceId)
	if !ok {
		return fmt.Errorf("anysyncx: pubsub: space %s not resident: %w", spaceId, pubsubproto.ErrNotAMember)
	}
	acl := handle.Inner().Acl()
	if acl == nil {
		return pubsubproto.ErrNotAMember
	}
	acl.RLock()
	defer acl.RUnlock()
	for _, acc := range acl.AclState().CurrentAccounts() {
		if acc.PubKey.Equals(identity) {
			if acc.Status == list.StatusActive {
				return nil
			}
			break
		}
	}
	return pubsubproto.ErrNotAMember
}

// pubsubRpc registers the pubsub stream handler on the app DRPC server
// so LAN peers can subscribe/publish through us (LAN pubsub is
// symmetric — both clients run the same engine). Registered
// unconditionally: registration during Init is free, inbound streams
// only arrive over transports that are up, and every serving path is
// membership-gated.
type pubsubRpc struct{}

func (pubsubRpc) Init(a *app.App) error {
	return pubsub.RegisterRpc(
		a.MustComponent(server.CName).(server.DRPCServer),
		a.MustComponent(pubsub.CName).(pubsub.Service),
	)
}

func (pubsubRpc) Name() string { return "sdk.pubsub.rpc" }

// logPubSubStatus surfaces serving-peer rejections (rate limit, not a
// member, pattern caps…). Publish is fire-and-forget by design, so v1
// logs instead of feeding an API surface.
func logPubSubStatus(peerId string, status *pubsubproto.Status) {
	psLog.Warn("pubsub rejection",
		zap.String("peerId", peerId),
		zap.String("spaceId", status.SpaceId),
		zap.String("code", status.Code.String()),
		zap.Strings("topics", status.Topics))
}

// pubsubSyncInterest pushes the space's local subscription patterns to
// its current peers — hooked into the LAN exchange's peer-updated
// event. Background context with NO per-call cancel: the pool only
// ENQUEUES a send closure capturing this ctx, so cancelling on return
// would kill the send before the dial worker runs it. No-op inside the
// engine when the space has no local patterns; the engine's own resync
// loop is the fallback.
func pubsubSyncInterest(svc pubsub.Service, spaceId string) {
	_ = svc.SyncInterest(context.Background(), spaceId)
}

// PubSubPublish signs, encrypts and fans payload out on spaceId/topic.
// Fire-and-forget past local validation; own publishes deliver to
// local subscribers synchronously. The network send is detached from
// the caller's cancelation (WithoutCancel): the pool runs the send
// closure after Publish returns, and the public contract promises
// delivery survives the caller's ctx.
func (a *App) PubSubPublish(ctx context.Context, spaceId, topic string, payload []byte) error {
	if a.guestKeyFor(spaceId) != nil {
		return ErrPubSubGuestSpace
	}
	return a.pubsub.Publish(context.WithoutCancel(ctx), spaceId, topic, payload)
}

// PubSubSubscribe registers h for topics matching pattern in spaceId
// and pushes the interest to the space's peers. The returned cancel is
// idempotent.
func (a *App) PubSubSubscribe(spaceId, pattern string, h pubsub.Handler) (cancel func(), err error) {
	if a.guestKeyFor(spaceId) != nil {
		return nil, ErrPubSubGuestSpace
	}
	return a.pubsub.Subscribe(spaceId, pattern, h)
}

// PubSubCloseSpace drops every local subscription and remote interest
// for spaceId. Called from the space layer's deliberate teardown
// funnel (evict/offload) — deliberately NOT from cache eviction, which
// must not kill live subscriptions.
func (a *App) PubSubCloseSpace(spaceId string) {
	a.pubsub.CloseSpace(spaceId)
}

// PubSubRevalidate re-checks the engine's per-space membership state
// against the current ACL, dropping serving-side interest of removed
// members. Called on ACL record apply; a no-op for non-resident
// spaces.
func (a *App) PubSubRevalidate(spaceId string) {
	handle, ok := a.PickSpace(context.Background(), spaceId)
	if !ok {
		return
	}
	acl := handle.Inner().Acl()
	if acl == nil {
		return
	}
	acl.RLock()
	active := make(map[string]struct{})
	for _, acc := range acl.AclState().CurrentAccounts() {
		if acc.Status == list.StatusActive {
			active[acc.PubKey.Account()] = struct{}{}
		}
	}
	acl.RUnlock()
	a.pubsub.RevalidateMembers(spaceId, func(account string) bool {
		_, ok := active[account]
		return ok
	})
}
