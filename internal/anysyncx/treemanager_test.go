package anysyncx

import (
	"context"
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/stretchr/testify/require"
)

// pushRegistry answers HasTree from a fixed set and serves every tree
// from GetTree, so the adapter's "new from this peer" detection can be
// pinned without any-sync internals.
type pushRegistry struct {
	stored map[string]bool
	trees  map[string]objecttree.ObjectTree
}

func (r *pushRegistry) GetTree(_ context.Context, _, treeId string) (objecttree.ObjectTree, error) {
	if tr, ok := r.trees[treeId]; ok {
		return tr, nil
	}
	return nil, ErrSpaceRegistryUnknown
}
func (r *pushRegistry) HasTree(_ context.Context, _, treeId string) (bool, error) {
	return r.stored[treeId], nil
}
func (r *pushRegistry) PutTree(context.Context, string, treestorage.TreeStorageCreatePayload) error {
	return nil
}
func (r *pushRegistry) MarkTreeDeleted(context.Context, string, string) error { return nil }
func (r *pushRegistry) DeleteTree(context.Context, string, string) error      { return nil }
func (r *pushRegistry) ShouldPullTree(context.Context, string, string, *treechangeproto.RawTreeChangeWithId, []string) bool {
	return true
}

var ErrSpaceRegistryUnknown = context.Canceled

// A tree built for the first time under a peer's request context is
// reported as fetched from that peer; a tree that already existed, or
// a load without a peer in the context, is not.
func TestTreeManagerReportsPushedTrees(t *testing.T) {
	tm := newTreeManager()
	tm.SetRegistry(&pushRegistry{
		stored: map[string]bool{"existing": true},
		trees: map[string]objecttree.ObjectTree{
			"pushed":   headsTree{heads: []string{"h1", "h2"}},
			"existing": headsTree{heads: []string{"h3"}},
		},
	})
	var got []string
	tm.onFetched = func(spaceId, peerId, treeId string, heads []string) {
		got = append(got, spaceId+"/"+peerId+"/"+treeId+"/"+heads[0])
	}
	peerCtx := peer.CtxWithPeerId(context.Background(), "p1")

	_, err := tm.GetTree(peerCtx, "s1", "pushed")
	require.NoError(t, err)
	_, err = tm.GetTree(peerCtx, "s1", "existing")
	require.NoError(t, err)
	_, err = tm.GetTree(context.Background(), "s1", "pushed")
	require.NoError(t, err)
	_, err = tm.GetTree(peerCtx, "s1", "missing")
	require.Error(t, err)

	require.Equal(t, []string{"s1/p1/pushed/h1"}, got)
}
