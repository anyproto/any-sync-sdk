package anysyncx

import (
	"context"
	"errors"
	"testing"

	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/stretchr/testify/require"
)

// fakePeer implements just enough of peer.Peer for identity checks.
type fakePeer struct {
	peer.Peer
	id string
}

func (f fakePeer) Id() string { return f.id }

// fakePool serves peers from a fixed map; everything else errors.
type fakePool struct {
	peers map[string]peer.Peer
}

var errPeerUnreachable = errors.New("unreachable")

func (f *fakePool) Get(_ context.Context, id string) (peer.Peer, error) {
	if p, ok := f.peers[id]; ok {
		return p, nil
	}
	return nil, errPeerUnreachable
}

func (f *fakePool) GetOneOf(_ context.Context, ids []string) (peer.Peer, error) {
	for _, id := range ids {
		if p, ok := f.peers[id]; ok {
			return p, nil
		}
	}
	return nil, errPeerUnreachable
}

func (f *fakePool) AddPeer(context.Context, peer.Peer) error { return nil }
func (f *fakePool) Pick(_ context.Context, id string) (peer.Peer, error) {
	if p, ok := f.peers[id]; ok {
		return p, nil
	}
	return nil, errPeerUnreachable
}
func (f *fakePool) Flush(context.Context) error { return nil }

var _ pool.Pool = (*fakePool)(nil)

// fakeLocalPeers is a minimal localPeerSource with removal recording.
type fakeLocalPeers struct {
	ids     []string
	removed []string
}

func (f *fakeLocalPeers) LocalPeerIds(string) []string { return f.ids }
func (f *fakeLocalPeers) RemoveLocalPeer(id string)    { f.removed = append(f.removed, id) }

// fakeNodeConf provides the node ids for one space; embedding the
// interface keeps it small — only NodeIds may be called.
type fakeNodeConf struct {
	nodeconf.Service
	nodeIds []string
}

func (f *fakeNodeConf) NodeIds(string) []string { return f.nodeIds }

func newTestManager(nodes []string, local *fakeLocalPeers, reachable map[string]peer.Peer) *spacePeerManager {
	return &spacePeerManager{
		spaceId:    "space1",
		nodeConf:   &fakeNodeConf{nodeIds: nodes},
		pool:       &fakePool{peers: reachable},
		localPeers: local,
	}
}

func peerIds(peers []peer.Peer) []string {
	out := make([]string, 0, len(peers))
	for _, p := range peers {
		out = append(out, p.Id())
	}
	return out
}

func TestGetResponsiblePeersFoldsLocalPeers(t *testing.T) {
	local := &fakeLocalPeers{ids: []string{"lp1", "lp2"}}
	m := newTestManager([]string{"node1"}, local, map[string]peer.Peer{
		"node1": fakePeer{id: "node1"},
		"lp1":   fakePeer{id: "lp1"},
		"lp2":   fakePeer{id: "lp2"},
	})
	peers, err := m.GetResponsiblePeers(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"node1", "lp1", "lp2"}, peerIds(peers))
	require.Empty(t, local.removed)
}

func TestGetResponsiblePeersLocalOnlyWhenNodesDown(t *testing.T) {
	local := &fakeLocalPeers{ids: []string{"lp1"}}
	m := newTestManager([]string{"node1"}, local, map[string]peer.Peer{
		"lp1": fakePeer{id: "lp1"},
	})
	peers, err := m.GetResponsiblePeers(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"lp1"}, peerIds(peers))
}

func TestGetResponsiblePeersNodeErrorWhenNobodyReachable(t *testing.T) {
	local := &fakeLocalPeers{}
	m := newTestManager([]string{"node1"}, local, nil)
	_, err := m.GetResponsiblePeers(context.Background())
	require.ErrorIs(t, err, errPeerUnreachable)
}

func TestGetLocalPeersRemovesUnreachableAfterStrikes(t *testing.T) {
	local := &fakeLocalPeers{ids: []string{"dead", "lp1"}}
	m := newTestManager(nil, local, map[string]peer.Peer{
		"lp1": fakePeer{id: "lp1"},
	})
	// A single dial miss must NOT evict — a peer briefly restarting its
	// QUIC session would otherwise be stranded until its next mDNS
	// re-announce.
	for i := 0; i < localDialStrikes-1; i++ {
		peers, err := m.GetResponsiblePeers(context.Background())
		require.NoError(t, err)
		require.Equal(t, []string{"lp1"}, peerIds(peers))
		require.Empty(t, local.removed, "evicted too early on strike %d", i+1)
	}
	// The strike that reaches the threshold evicts.
	peers, err := m.GetResponsiblePeers(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"lp1"}, peerIds(peers))
	require.Equal(t, []string{"dead"}, local.removed)
}

func TestGetLocalPeersStrikeResetsOnSuccess(t *testing.T) {
	local := &fakeLocalPeers{ids: []string{"flaky"}}
	reachable := map[string]peer.Peer{}
	m := newTestManager(nil, local, reachable)
	// Two misses, then it comes back (resets), then two more misses —
	// must not evict because the streak never reaches the threshold.
	_, _ = m.GetResponsiblePeers(context.Background()) // miss 1
	_, _ = m.GetResponsiblePeers(context.Background()) // miss 2
	reachable["flaky"] = fakePeer{id: "flaky"}
	_, _ = m.GetResponsiblePeers(context.Background()) // success → reset
	delete(reachable, "flaky")
	_, _ = m.GetResponsiblePeers(context.Background()) // miss 1 again
	_, _ = m.GetResponsiblePeers(context.Background()) // miss 2 again
	require.Empty(t, local.removed)
}

func TestGetLocalPeersKeepsPeerOnCancelledContext(t *testing.T) {
	local := &fakeLocalPeers{ids: []string{"lp1"}}
	m := newTestManager(nil, local, nil) // nobody reachable
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = m.getLocalPeers(ctx)
	require.Empty(t, local.removed)
}

func TestGetBroadcastPeersIsNodesPlusLocal(t *testing.T) {
	local := &fakeLocalPeers{ids: []string{"lp1"}}
	m := newTestManager([]string{"node1", "node2"}, local, map[string]peer.Peer{
		"node1": fakePeer{id: "node1"},
		"lp1":   fakePeer{id: "lp1"},
	})
	peers, err := m.getBroadcastPeers(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"node1", "lp1"}, peerIds(peers))

	// GetNodePeers must stay nodes-only.
	nodePeers, err := m.GetNodePeers(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"node1"}, peerIds(nodePeers))
}
