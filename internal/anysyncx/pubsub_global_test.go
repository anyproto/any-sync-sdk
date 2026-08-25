package anysyncx

import (
	"context"
	"testing"
	"time"

	"github.com/anyproto/any-sync/net/peer"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/p2p"
)

// pubsub resolves global peers with a bounded Pick: a pool whose Pick
// waits on an in-flight load neither stalls the engine nor turns into a
// dial.
func TestPubsubPeersNeverWaitOnGlobalPeers(t *testing.T) {
	store := p2p.NewPeerStore()
	store.UpdateGlobalPeer("g1", []string{"s"})
	store.UpdateGlobalPeer("g2", []string{"s"})
	pool := &blockingPickPool{hangingPool{fakePool: fakePool{peers: map[string]peer.Peer{}}}}
	peers := &pubsubPeers{
		app:  &App{peerStore: store, localOnly: newLocalOnlySpaces(), globalEnabled: true, nodeConf: &fakeNodeConf{}},
		pool: pool,
	}

	start := time.Now()
	got, err := peers.SpacePeers(context.Background(), "s")
	require.NoError(t, err)
	require.Empty(t, got)
	require.Less(t, time.Since(start), 500*time.Millisecond)
	require.Equal(t, 0, pool.getCount(), "global peers are never dialed")
}
