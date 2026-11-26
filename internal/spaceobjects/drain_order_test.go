package spaceobjects

import (
	"context"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree/updatelistener"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// The parked changes of an object replay in the order of its tree: a
// strict update parked next to its record's create lands after the
// create, whatever order their change ids sort in. Two objects in one
// pass each keep their own order.
func TestDrain_ReplaysParkedChangesInTreeOrder(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, t.TempDir()+"/test.db", nil)
	require.NoError(t, err)
	defer db.Close()

	const dataset = "blocks"
	store := NewStore(nil, db, nil, "spaceA", nil, nil, nil, nil)
	ctrls := map[string]*crdt.Controller{}
	for _, objectId := range []string{"obj-1", "obj-2"} {
		ctrls[objectId], err = crdt.NewController(ctx, objectId, db,
			crdt.HandlerReg{Name: dataset, Handler: &crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}})
		require.NoError(t, err)
	}
	_ = store.cache.Close()
	store.cache = ocache.New(func(_ context.Context, objectId string) (ocache.Object, error) {
		return object.New(object.Config{SpaceId: "spaceA", Controller: ctrls[objectId]},
			func(updatelistener.UpdateListener) (objecttree.ObjectTree, error) {
				return &closableStubTree{stubTree{id: objectId}}, nil
			})
	})
	defer store.cache.Close()

	a := &anyenc.Arena{}
	park := func(objectId, changeId, orderId string, upsert bool, removed *anyenc.Value) {
		t.Helper()
		payload, err := object.NewCodec().Encode(&crdt.Change{Dataset: dataset, DataVersion: "test-v1", Records: []crdt.RecordChange{{
			Id: "r1", Upsert: upsert, Ops: []crdt.Op{{Type: crdt.OpSet, Path: []string{"removed"}, Payload: removed}},
		}}})
		require.NoError(t, err)
		require.NoError(t, store.Park(ctx, DetachedRow{
			ChangeId: changeId, SpaceId: "spaceA", ObjectId: objectId, AddSeq: 10, OrderId: orderId, Payload: payload,
		}))
	}
	// Each update's change id sorts before its create's; its order id
	// after. The rows of the two objects interleave in change-id order.
	park("obj-1", "bafy-a-update", "!!b", false, a.NewTrue())
	park("obj-2", "bafy-b-update", "!!d", false, a.NewTrue())
	park("obj-2", "bafy-y-create", "!!c", true, a.NewFalse())
	park("obj-1", "bafy-z-create", "!!a", true, a.NewFalse())

	require.NoError(t, store.Drain(ctx))

	for objectId, ctrl := range ctrls {
		rec, err := ctrl.Lookup(ctx, dataset, "r1")
		require.NoError(t, err)
		require.NotNil(t, rec, objectId)
		require.True(t, rec.GetBool("removed"), "%s: the update lands on the record its create made: %s", objectId, rec.String())
	}
	var left int
	require.NoError(t, store.IterDetached(ctx, func(DetachedRow) bool { left++; return true }))
	require.Zero(t, left)
}
