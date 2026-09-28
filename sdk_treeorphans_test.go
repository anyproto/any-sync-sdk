package anysyncsdk

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/space"
)

// Tree-orphan rebuild tests. The changes a tree load sets aside are
// produced by deleting one stored change out of the space's tree
// storage, which leaves its descendants stored under an absent parent.
// Fully offline: a reachable node would hand the deleted change back.

// orphanConfig is wmConfig with every node address unreachable and LAN
// sync off.
func orphanConfig(t *testing.T, dataDir string) config.Config {
	t.Helper()
	cfg := wmConfig(t, dataDir)
	cfg.Network.NodeConfYAML = regexp.MustCompile(`127\.0\.0\.1:\d+`).
		ReplaceAll(cfg.Network.NodeConfYAML, []byte("127.0.0.1:1"))
	off := false
	cfg.P2P.Enabled = &off
	return cfg
}

type orphanFixture struct {
	ctx   context.Context
	sdk   *SDK
	sp    space.Space
	store *spaceobjects.Store
	objId string
	// writes[i] and seqs[i] belong to write #i+1: its result and the
	// AddSeq tree storage gave its change.
	writes []space.ModifyResult
	seqs   []uint64
}

// newOrphanFixture creates an object and makes five writes to it, each
// one tree change: write #i sets r1.n = i and creates record c<i>.
func newOrphanFixture(t *testing.T) *orphanFixture {
	t.Helper()
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	sdk, err := Open(ctx, orphanConfig(t, dataDir), wmProvider(t, dataDir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "orphans"})
	require.NoError(t, err)
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: "wm-blocks-type"})
	require.NoError(t, err)

	f := &orphanFixture{ctx: ctx, sdk: sdk, sp: sp, store: sdk.spaces.StoreFor(sp.Id()), objId: objId}
	for i := 1; i <= 5; i++ {
		res := f.write(t, i, fmt.Sprintf("c%d", i))
		ch, err := f.get(t).Tree().Storage().Get(ctx, res.ChangeId)
		require.NoError(t, err)
		require.Equal(t, ch.OrderId, string(res.VersionId), "a local write's version is its change's order id")
		f.writes = append(f.writes, res)
		f.seqs = append(f.seqs, ch.AddSeq)
	}
	return f
}

// write sets r1.n = n and creates record rec in one change.
func (f *orphanFixture) write(t *testing.T, n int, rec string) space.ModifyResult {
	t.Helper()
	res, err := f.sp.Modify(f.ctx, space.ModifyBatch{
		ObjectId: f.objId, Dataset: wmDataset,
		Records: []space.RecordModify{
			{Id: "r1", Upsert: true, Ops: []space.Op{{Type: space.OpSet, Path: "n", Value: n}}},
			{Id: rec, Upsert: true, Ops: []space.Op{{Type: space.OpSet, Path: "n", Value: n}}},
		},
	})
	require.NoError(t, err)
	return res
}

func (f *orphanFixture) get(t *testing.T) *object.Object {
	t.Helper()
	obj, err := f.store.Get(f.ctx, f.objId)
	require.NoError(t, err)
	return obj
}

// damage deletes write #3's stored change, then evicts the object. The
// deletion lands while the object is resident, so whichever load comes
// next builds the tree over it.
func (f *orphanFixture) damage(t *testing.T) {
	t.Helper()
	handle, err := f.sdk.app.GetSpace(f.ctx, f.sp.Id())
	require.NoError(t, err)
	changes, err := handle.Inner().Storage().AnyStore().OpenCollection(f.ctx, objecttree.CollName)
	require.NoError(t, err)
	require.NoError(t, changes.DeleteId(f.ctx, f.writes[2].ChangeId))
	f.store.Drop(f.objId)
}

// rows reads r1 and c1..c6 as JSON, "" for an absent record.
func (f *orphanFixture) rows(obj *object.Object) map[string]string {
	out := map[string]string{}
	for _, id := range []string{"r1", "c1", "c2", "c3", "c4", "c5", "c6"} {
		out[id] = ""
		if v := obj.Controller().Get(f.ctx, wmDataset, id); v != nil {
			out[id] = v.String()
		}
	}
	return out
}

func (f *orphanFixture) meta(t *testing.T) anystore.Collection {
	t.Helper()
	coll, err := f.sdk.db.OpenCollection(f.ctx, crdt.MetaCollectionName)
	require.NoError(t, err)
	return coll
}

// applySeq reads the object's _meta applySeq.
func (f *orphanFixture) applySeq(t *testing.T) uint64 {
	t.Helper()
	_, seq, _, err := crdt.LoadMeta(f.ctx, f.meta(t), f.objId)
	require.NoError(t, err)
	return seq
}

// assertReplayedTwo checks the rows hold exactly writes #1 and #2.
func (f *orphanFixture) assertReplayedTwo(t *testing.T, obj *object.Object) {
	t.Helper()
	ctrl := obj.Controller()
	r1 := ctrl.Get(f.ctx, wmDataset, "r1")
	require.NotNil(t, r1)
	assert.Equal(t, 2, r1.GetInt("n"), "r1 holds write #2's value")
	for _, id := range []string{"c1", "c2"} {
		assert.NotNil(t, ctrl.Get(f.ctx, wmDataset, id), "%s comes from an attached change", id)
	}
	for _, id := range []string{"c3", "c4", "c5"} {
		assert.Nil(t, ctrl.Get(f.ctx, wmDataset, id), "%s comes from a change no longer in the tree", id)
	}
}

// Changes an object applied and the tree storage later sets aside are
// dropped from the rows by a rebuild on load. A write that then reuses
// their order ids lands, and the stamped mark keeps the next load from
// rebuilding again.
func TestLoad_RebuildsAfterTreeOrphans(t *testing.T) {
	f := newOrphanFixture(t)
	ctx, w := f.ctx, f.writes

	obj := f.get(t)
	r1 := obj.Controller().Get(ctx, wmDataset, "r1")
	require.NotNil(t, r1)
	require.Equal(t, 5, r1.GetInt("n"))
	for i := 1; i <= 5; i++ {
		require.NotNil(t, obj.Controller().Get(ctx, wmDataset, fmt.Sprintf("c%d", i)))
	}

	f.damage(t)
	reloaded := f.get(t)
	require.NotSame(t, obj, reloaded, "the load builds the tree from storage")
	obj = reloaded

	// any-sync set #4 and #5 aside and rewound the heads to #2.
	var orphans []string
	require.NoError(t, obj.Tree().Storage().GetOrphans(ctx, func(_ context.Context, ch objecttree.StorageChange) (bool, error) {
		orphans = append(orphans, ch.Id)
		return true, nil
	}))
	assert.Equal(t, []string{w[3].ChangeId, w[4].ChangeId}, orphans)
	assert.Equal(t, []string{w[1].ChangeId}, obj.Tree().Heads())
	for _, id := range []string{w[3].ChangeId, w[4].ChangeId} {
		has, err := obj.Tree().Storage().Has(ctx, id)
		require.NoError(t, err)
		assert.False(t, has, "an orphan leaves the changes collection")
	}

	f.assertReplayedTwo(t, obj)
	assert.Equal(t, f.seqs[4], obj.Controller().TreeOrphanSeq(), "the mark covers the highest orphan")

	// The next change reuses #3's order id: gated out by #4/#5's versions
	// if they were still in the rows.
	res := f.write(t, 10, "c6")
	assert.Empty(t, res.Rejections)
	assert.Equal(t, w[2].VersionId, res.VersionId, "the order id after #2 is reused")
	r1 = obj.Controller().Get(ctx, wmDataset, "r1")
	require.NotNil(t, r1)
	assert.Equal(t, 10, r1.GetInt("n"), "the write lands on r1")
	assert.NotNil(t, obj.Controller().Get(ctx, wmDataset, "c6"), "the write creates c6")

	// Orphans at or under the mark rebuild nothing.
	rowsBefore, seqBefore, markBefore := f.rows(obj), f.applySeq(t), obj.Controller().TreeOrphanSeq()
	f.store.Drop(f.objId)
	again := f.get(t)
	require.NotSame(t, obj, again)
	assert.Equal(t, seqBefore, f.applySeq(t), "no replay re-applied anything")
	assert.Equal(t, rowsBefore, f.rows(again))
	assert.Equal(t, markBefore, again.Controller().TreeOrphanSeq())
}

// An object whose rows never held the set-aside changes replays without
// a rebuild and still gets the mark.
func TestLoad_TreeOrphansNeverAppliedOnlyMark(t *testing.T) {
	f := newOrphanFixture(t)
	ctx := f.ctx
	f.damage(t)

	// Forget the materialization: no _meta row, no rows.
	require.NoError(t, f.meta(t).DeleteId(ctx, f.objId))
	shared, err := f.store.SharedObjects(ctx)
	require.NoError(t, err)
	require.NoError(t, shared.DeleteId(ctx, f.objId))
	coll, err := f.store.OpenObjectCollection(ctx, f.objId, wmDataset)
	require.NoError(t, err)
	for _, id := range []string{"r1", "c1", "c2", "c3", "c4", "c5"} {
		require.NoError(t, coll.DeleteId(ctx, id))
	}
	// A row no change produces: a rebuild's wipe drops it, a plain
	// replay keeps it.
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":"probe"}`)))

	obj := f.get(t)
	f.assertReplayedTwo(t, obj)
	assert.NotNil(t, obj.Controller().Get(ctx, wmDataset, "probe"), "no rebuild wiped the rows")
	assert.Equal(t, f.seqs[4], obj.Controller().TreeOrphanSeq())
}
