package crdt

import (
	"context"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// HandlerReg.Indexes are ensured the first time the dataset's per-object
// collection materialises through an apply.
func TestHandlerReg_Indexes_LazyEnsureOnFirstWrite(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	c, err := NewController(ctx, "obj1", db, HandlerReg{
		Name:    testDS,
		Handler: DefaultHandler{},
		Indexes: []anystore.IndexInfo{{Name: "idx_status", Fields: []string{"status"}, Sparse: true}},
		Schema:  dynSchema,
	})
	require.NoError(t, err)

	arena := &anyenc.Arena{}
	require.NoError(t, c.ApplyChange(context.Background(),
		makeUpsert("v1", "rec1", Op{Type: OpSet, Path: []string{"status"}, Payload: arena.NewString("open")})))

	coll, err := db.OpenCollection(ctx, "obj1_"+testDS)
	require.NoError(t, err)
	names := indexNames(coll.GetIndexes())
	assert.Contains(t, names, "idx_status")
}

// HandlerReg.Indexes run at registerHandler time for datasets pre-wired
// via SharedCollections, since the lazy open path is skipped.
func TestHandlerReg_Indexes_SharedCollectionEagerEnsure(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	shared, err := db.Collection(ctx, "space1_shared")
	require.NoError(t, err)

	_, err = NewControllerWithShared(ctx, "obj1", db,
		SharedCollections{testDS: shared}, HandlerReg{
			Name:    testDS,
			Handler: DefaultHandler{},
			Indexes: []anystore.IndexInfo{{Name: "idx_kind", Fields: []string{"kind"}}},
			Schema:  dynSchema,
		})
	require.NoError(t, err)

	names := indexNames(shared.GetIndexes())
	assert.Contains(t, names, "idx_kind")
}

func indexNames(idx []anystore.Index) []string {
	out := make([]string, 0, len(idx))
	for _, i := range idx {
		out = append(out, i.Info().Name)
	}
	return out
}
