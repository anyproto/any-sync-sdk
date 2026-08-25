package filep2p

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/net/peer"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
	"storj.io/drpc"
)

type globalOnlyPeers struct{ global []string }

func (g globalOnlyPeers) LocalPeerIds(string) []string  { return nil }
func (g globalOnlyPeers) GlobalPeerIds(string) []string { return g.global }

// stubPeer answers every RPC with an error, so holdsFull reports false.
type stubPeer struct {
	peer.Peer
	id string
}

func (s stubPeer) Id() string { return s.id }
func (s stubPeer) DoDrpc(context.Context, func(conn drpc.Conn) error) error {
	return errors.New("no rpc in test")
}

// hangingDialPool: Get blocks until ctx is done (a relay dial that
// never answers), Pick serves the connected map at once.
type hangingDialPool struct {
	mu        sync.Mutex
	gets      int
	connected map[string]peer.Peer
}

func (h *hangingDialPool) Get(ctx context.Context, _ string) (peer.Peer, error) {
	h.mu.Lock()
	h.gets++
	h.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (h *hangingDialPool) Pick(_ context.Context, id string) (peer.Peer, error) {
	if p, ok := h.connected[id]; ok {
		return p, nil
	}
	return nil, errors.New("not connected")
}

func TestSourceGlobalPeersPickOnly(t *testing.T) {
	pool := &hangingDialPool{connected: map[string]peer.Peer{"g1": stubPeer{id: "g1"}}}
	src := NewSource(pool, globalOnlyPeers{global: []string{"g1", "g2"}})
	root, err := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	require.NoError(t, err)

	start := time.Now()
	_, _, ok := src.SourceFor(context.Background(), "s", root)
	require.False(t, ok)
	require.Less(t, time.Since(start), perPeerTimeout, "a disconnected global peer is skipped, never dialed")
	require.Equal(t, 0, pool.gets, "pool.Get never reaches a global peer")

	// A disconnected global peer is not banned: it is simply not
	// connected right now.
	src.banMu.Lock()
	_, banned := src.banned["g2"]
	src.banMu.Unlock()
	require.False(t, banned)
}
