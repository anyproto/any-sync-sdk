package crdt

import (
	"path/filepath"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// PeekCollection answers without waiting on any-store's writer: with a
// write transaction held open it returns at once and ensures no index,
// while Collection blocks on the writer to reconcile the index set. It
// caches nothing, so the ordinary open still reconciles afterwards.
func TestPeekCollection_NeverWaitsOnTheWriter(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "peek.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctrl, err := NewController(ctx, "obj1", db, HandlerReg{
		Name: testDS, Handler: DefaultHandler{}, Schema: dynSchema,
		Indexes: []anystore.IndexInfo{{Name: "idx_n", Fields: []string{"n"}}},
	})
	require.NoError(t, err)

	coll, err := ctrl.PeekCollection(ctx, "unregistered")
	require.NoError(t, err)
	assert.Nil(t, coll)
	coll, err = ctrl.PeekCollection(ctx, testDS)
	require.NoError(t, err)
	assert.Nil(t, coll, "no collection on disk yet")

	// The collection appears behind the controller's back; its handle
	// is closed so the peek has to open it by name.
	raw, err := db.Collection(ctx, "obj1_"+testDS)
	require.NoError(t, err)
	require.NoError(t, raw.Close())

	wtx, err := db.WriteTx(ctx)
	require.NoError(t, err)
	peeked := make(chan anystore.Collection, 1)
	go func() {
		c, err := ctrl.PeekCollection(ctx, testDS)
		assert.NoError(t, err)
		peeked <- c
	}()
	select {
	case c := <-peeked:
		require.NotNil(t, c)
		assert.Empty(t, indexNames(c.GetIndexes()), "a peek ensures no index")
	case <-time.After(5 * time.Second):
		t.Fatal("PeekCollection waited on the writer")
	}
	opened := make(chan anystore.Collection, 1)
	go func() { opened <- ctrl.Collection(ctx, testDS) }()
	select {
	case <-opened:
		t.Fatal("Collection returned with the writer held")
	case <-time.After(300 * time.Millisecond):
	}
	require.NoError(t, wtx.Rollback())

	select {
	case coll = <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("Collection did not return once the writer was released")
	}
	require.NotNil(t, coll)
	assert.ElementsMatch(t, []string{"idx__addSeq", "idx_n"}, indexNames(coll.GetIndexes()), "the ordinary open reconciles")
	again, err := ctrl.PeekCollection(ctx, testDS)
	require.NoError(t, err)
	assert.Same(t, coll, again, "resident handle afterwards")
}
