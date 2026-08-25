package anysyncx

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/stretchr/testify/require"
)

// scriptedRegistry fails GetTree for the ids in fail; every call is
// recorded so tests can assert retry behavior.
type scriptedRegistry struct {
	fail  map[string]error
	trees map[string]objecttree.ObjectTree
	calls []string
}

func (r *scriptedRegistry) GetTree(_ context.Context, _, treeId string) (objecttree.ObjectTree, error) {
	r.calls = append(r.calls, treeId)
	if err, ok := r.fail[treeId]; ok {
		return nil, err
	}
	if tr, ok := r.trees[treeId]; ok {
		return tr, nil
	}
	// nil tree is fine for SyncAll: the synctree.SyncTree assertion
	// just comes back false and the ping is skipped.
	return nil, nil
}

func (r *scriptedRegistry) PutTree(context.Context, string, treestorage.TreeStorageCreatePayload) error {
	return nil
}
func (r *scriptedRegistry) MarkTreeDeleted(context.Context, string, string) error { return nil }
func (r *scriptedRegistry) DeleteTree(context.Context, string, string) error      { return nil }
func (r *scriptedRegistry) ShouldPullTree(context.Context, string, string, *treechangeproto.RawTreeChangeWithId, []string) bool {
	return true
}

// TestTreeSyncerRetriesFailedGetTree pins the parked-tree repair loop:
// a tree whose GetTree fails during a SyncAll round must be retried on
// a later round even when that round's diff is EMPTY. That empty-diff
// retry is the whole point — the failed fetch typically already wrote
// the tree's changes to storage, so the headsync diff converges and
// never offers the id again; without the parked set the CRDT
// projection silently diverges from storage until process restart.
func TestTreeSyncerRetriesFailedGetTree(t *testing.T) {
	reg := &scriptedRegistry{fail: map[string]error{"t1": errors.New("boom")}}
	ts := newTreeSyncer("space1", reg, nil)
	p := fakePeer{id: "peer1"}

	require.NoError(t, ts.SyncAll(context.Background(), p, nil, []string{"t1", "t2"}))
	require.Equal(t, []string{"t1", "t2"}, reg.calls)
	stats := ts.Stats()
	require.Len(t, stats, 1)
	require.Equal(t, 1, stats[0].Pending, "failed id must be parked")

	// Next round: diff converged (no missing, no existing). The parked
	// id must still be retried — and recover once GetTree succeeds.
	delete(reg.fail, "t1")
	reg.calls = nil
	require.NoError(t, ts.SyncAll(context.Background(), p, nil, nil))
	require.Equal(t, []string{"t1"}, reg.calls)
	require.Equal(t, 0, ts.Stats()[0].Pending, "recovered id must clear")

	// And once clear, quiet rounds stay no-op.
	reg.calls = nil
	require.NoError(t, ts.SyncAll(context.Background(), p, nil, nil))
	require.Empty(t, reg.calls)
}

// TestTreeSyncerParksOnDeadCtx: when the round context is already dead,
// remaining ids are parked without pointless GetTree attempts and
// retried on the next round.
func TestTreeSyncerParksOnDeadCtx(t *testing.T) {
	reg := &scriptedRegistry{}
	ts := newTreeSyncer("space1", reg, nil)
	p := fakePeer{id: "peer1"}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, ts.SyncAll(ctx, p, []string{"t2"}, []string{"t1"}))
	require.Empty(t, reg.calls, "dead ctx must not hit the registry")
	require.Equal(t, 2, ts.Stats()[0].Pending)

	require.NoError(t, ts.SyncAll(context.Background(), p, nil, nil))
	require.ElementsMatch(t, []string{"t1", "t2"}, reg.calls)
	require.Equal(t, 0, ts.Stats()[0].Pending)
}

// TestTreeSyncerDedupsPendingAgainstDiff: an id present in both the
// round's diff and the parked set is resolved once per round.
func TestTreeSyncerDedupsPendingAgainstDiff(t *testing.T) {
	reg := &scriptedRegistry{fail: map[string]error{"t1": errors.New("boom")}}
	ts := newTreeSyncer("space1", reg, nil)
	p := fakePeer{id: "peer1"}

	require.NoError(t, ts.SyncAll(context.Background(), p, nil, []string{"t1"}))
	delete(reg.fail, "t1")
	reg.calls = nil
	// t1 arrives in the diff again while also parked.
	require.NoError(t, ts.SyncAll(context.Background(), p, nil, []string{"t1"}))
	require.Equal(t, []string{"t1"}, reg.calls)
	require.Equal(t, 0, ts.Stats()[0].Pending)
}

// headsTree is the slice of an object tree SyncAll reads after a fetch.
type headsTree struct {
	objecttree.ObjectTree
	heads []string
}

func (h headsTree) Lock()           {}
func (h headsTree) Unlock()         {}
func (h headsTree) Heads() []string { return h.heads }

// A tree fetched whole from the peer is reported with its heads; a
// tree that already existed is not.
func TestTreeSyncerReportsFetchedTrees(t *testing.T) {
	reg := &scriptedRegistry{trees: map[string]objecttree.ObjectTree{
		"missing":  headsTree{heads: []string{"h1", "h2"}},
		"existing": headsTree{heads: []string{"h3"}},
	}}
	ts := newTreeSyncer("space1", reg, nil)
	var got []string
	ts.onFetched = func(peerId, treeId string, heads []string) {
		got = append(got, peerId+"/"+treeId+"/"+strings.Join(heads, ","))
	}
	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "n1"}, []string{"existing"}, []string{"missing"}))
	require.Equal(t, []string{"n1/missing/h1,h2"}, got)
}
