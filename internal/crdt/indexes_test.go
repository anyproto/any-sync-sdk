package crdt

import (
	"path/filepath"
	"sort"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func collIndexNames(coll anystore.Collection) []string {
	var names []string
	for _, idx := range coll.GetIndexes() {
		names = append(names, idx.Info().Name)
	}
	sort.Strings(names)
	return names
}

// The set under a prefix follows what is wanted; indexes named
// otherwise are left alone.
func TestDropStaleIndexes(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	coll, err := db.Collection(ctx, "c")
	require.NoError(t, err)

	keep := anystore.IndexInfo{Name: "dx_a", Fields: []string{"a"}}
	stale := anystore.IndexInfo{Name: "dx_b", Fields: []string{"b"}}
	other := anystore.IndexInfo{Name: "idx_c", Fields: []string{"c"}}
	require.NoError(t, coll.EnsureIndex(ctx, keep, stale, other))

	want := []anystore.IndexInfo{keep, {Name: "dx_d", Fields: []string{"d"}}}
	assert.Equal(t, []string{"dx_b"}, StaleIndexes(coll, want, "dx_"))
	assert.Nil(t, StaleIndexes(coll, want, ""), "no prefix, nothing is stale")
	missing := MissingIndexes(coll, want)
	require.Len(t, missing, 1)
	assert.Equal(t, "dx_d", missing[0].Name)

	require.NoError(t, DropStaleIndexes(ctx, coll, want, "dx_"))
	assert.Equal(t, []string{"dx_a", "idx_c"}, collIndexNames(coll))
	require.NoError(t, DropStaleIndexes(ctx, coll, want, "dx_"), "a second pass is a no-op")
	require.NoError(t, DropStaleIndexes(ctx, coll, nil, "dx_"))
	assert.Equal(t, []string{"idx_c"}, collIndexNames(coll))
}

// A per-object collection takes its registration's indexes when it is
// opened, and loses the ones under the prune prefix the registration no
// longer names.
func TestController_RegistrationIndexesFollowTheOpen(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	a := &anyenc.Arena{}

	open := func(indexes ...anystore.IndexInfo) *Controller {
		c, err := NewController(ctx, "obj1", db, HandlerReg{
			Name: stampNotes, Handler: DefaultHandler{}, Schema: dynSchema,
			Indexes: indexes, PruneIndexPrefix: "dx_",
		})
		require.NoError(t, err)
		return c
	}
	write := func(c *Controller, ver VersionId) {
		require.NoError(t, c.ApplyChange(ctx, Change{
			ObjectId: "obj1", Dataset: stampNotes, ChangeId: "ch-" + string(ver), VersionId: ver,
			Timestamp: 1, DataVersion: "dv",
			Records: []RecordChange{{Id: "r-" + string(ver), Upsert: true, Ops: []Op{setOp(a, "v", "x")}}},
		}))
	}
	collNames := func() []string {
		coll, err := db.OpenCollection(ctx, "obj1_"+stampNotes)
		require.NoError(t, err)
		return collIndexNames(coll)
	}

	c := open(anystore.IndexInfo{Name: "dx_v", Fields: []string{"v"}}, anystore.IndexInfo{Name: "dx_w", Fields: []string{"w"}})
	write(c, "v1")
	assert.Equal(t, []string{"dx_v", "dx_w", "idx__addSeq"}, collNames())
	require.NoError(t, c.CloseOwnedCollections())

	// The next registration names one of them and a new one.
	c = open(anystore.IndexInfo{Name: "dx_w", Fields: []string{"w"}}, anystore.IndexInfo{Name: "dx_x", Fields: []string{"x"}})
	require.NotNil(t, c.Get(ctx, stampNotes, "r-v1"), "a read opens the collection too")
	assert.Equal(t, []string{"dx_w", "dx_x", "idx__addSeq"}, collNames())
	require.NoError(t, c.CloseOwnedCollections())

	c = open()
	write(c, "v2")
	assert.Equal(t, []string{"idx__addSeq"}, collNames())
}
