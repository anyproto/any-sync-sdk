package crdt

// Cross-peer convergence regressions: each test applies the same change
// set in two delivery orders (concurrent DAG branches arrive in either
// order) and asserts the final user-visible state is identical. The
// guarded invariants:
//
//  1. Compaction never claims a version for never-written fields — a
//     concurrent lower-version write to a fresh field lands regardless
//     of when peers compact.
//  2. A broad subtree $set/$unset merges per-leaf instead of gating
//     against any aggregate of the leaf versions below the path.
//  3. Splitting a collapsed `_ver` ancestor propagates its version onto
//     created intermediates, preserving coverage of unenumerated deeper
//     siblings.

import (
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// applyOrders runs the same changes in two orders on fresh controllers and
// requires both final records to be equal; returns the converged record.
func applyOrders(t *testing.T, orderA, orderB []Change) map[string]any {
	t.Helper()
	stA := newTestController(t)
	for _, ch := range orderA {
		require.NoError(t, stA.ApplyChange(ctx, ch))
	}
	recA := canonicalRecord(stA.Get(ctx, testDS, "r1"))

	stB := newTestController(t)
	for _, ch := range orderB {
		require.NoError(t, stB.ApplyChange(ctx, ch))
	}
	recB := canonicalRecord(stB.Get(ctx, testDS, "r1"))

	require.Equal(t, recA, recB, "delivery orders diverged")
	return recA
}

// Regression #1: a multi-field $set whose fields share one version must not
// claim authority over fresh fields — a concurrent older write to a field
// nobody wrote must land regardless of delivery order.
func TestConvergence_FreshFieldNotGatedBySharedVersionSiblings(t *testing.T) {
	g := newVersionGen()
	v1 := g.Next() // create {a:1}
	v2 := g.Next() // concurrent: $set e=9
	v3 := g.Next() // multi-field $set {x,y} — two siblings share v3

	arena := &anyenc.Arena{}
	create := makeUpsert(v1, "r1", Op{Type: OpSet, Payload: recordPayload(arena, map[string]any{"a": 1})})
	setE := makeChange(v2, "r1", Op{Type: OpSet, Path: []string{"e"}, Payload: arena.NewNumberInt(9)})
	setXY := makeChange(v3, "r1", Op{Type: OpSet, Payload: recordPayload(arena, map[string]any{"x": 1, "y": 2})})

	rec := applyOrders(t,
		[]Change{create, setXY, setE},
		[]Change{create, setE, setXY},
	)
	assert.Equal(t, float64(9), rec["e"], "fresh-field write must land in both orders")
}

// Regression #2: broad subtree $set racing per-leaf $sets converges to the
// per-leaf merge in every delivery order.
func TestConvergence_BroadSubtreeSetVsLeafWrites(t *testing.T) {
	g := newVersionGen()
	v1 := g.Next() // create
	v2 := g.Next() // $set a.c = 1
	v3 := g.Next() // $set a = {z:9} (broad, version between the leaf writes)
	v4 := g.Next() // $set a.d = 2

	arena := &anyenc.Arena{}
	create := makeUpsert(v1, "r1", Op{Type: OpSet, Payload: recordPayload(arena, map[string]any{"other": 1})})
	setAC := makeChange(v2, "r1", Op{Type: OpSet, Path: []string{"a", "c"}, Payload: arena.NewNumberInt(1)})
	setA := makeChange(v3, "r1", Op{Type: OpSet, Path: []string{"a"}, Payload: recordPayload(arena, map[string]any{"z": 9})})
	setAD := makeChange(v4, "r1", Op{Type: OpSet, Path: []string{"a", "d"}, Payload: arena.NewNumberInt(2)})

	rec := applyOrders(t,
		[]Change{create, setAC, setAD, setA},
		[]Change{create, setA, setAC, setAD},
	)
	// c@v2 superseded by the broad v3 replace; d@v4 beats it; z lands.
	assert.Equal(t, map[string]any{"z": float64(9), "d": float64(2)}, rec["a"])
}

// Regression #2b: a broad scalar $set with a surviving newer leaf is
// superseded per-leaf — the survivors keep the position an object, in both
// orders.
func TestConvergence_BroadScalarSetVsNewerLeaf(t *testing.T) {
	g := newVersionGen()
	v1 := g.Next() // create
	v2 := g.Next() // $set a = 5 (scalar, older)
	v3 := g.Next() // $set a.c = 7 (leaf, newer)

	arena := &anyenc.Arena{}
	create := makeUpsert(v1, "r1", Op{Type: OpSet, Payload: recordPayload(arena, map[string]any{"other": 1})})
	setScalar := makeChange(v2, "r1", Op{Type: OpSet, Path: []string{"a"}, Payload: arena.NewNumberInt(5)})
	setLeaf := makeChange(v3, "r1", Op{Type: OpSet, Path: []string{"a", "c"}, Payload: arena.NewNumberInt(7)})

	rec := applyOrders(t,
		[]Change{create, setScalar, setLeaf},
		[]Change{create, setLeaf, setScalar},
	)
	assert.Equal(t, map[string]any{"c": float64(7)}, rec["a"])
}

// Regression #2c: broad $unset racing a newer leaf write keeps the
// surviving leaf and removes the rest, in both orders.
func TestConvergence_BroadUnsetVsNewerLeaf(t *testing.T) {
	g := newVersionGen()
	v1 := g.Next() // create with a = {c:1, d:2}
	v2 := g.Next() // $unset a (broad)
	v3 := g.Next() // $set a.c = 7 (newer leaf)

	arena := &anyenc.Arena{}
	create := makeUpsert(v1, "r1", Op{Type: OpSet, Payload: recordPayload(arena, map[string]any{
		"a.c": 1, "a.d": 2,
	})})
	unsetA := makeChange(v2, "r1", Op{Type: OpUnset, Path: []string{"a"}})
	setAC := makeChange(v3, "r1", Op{Type: OpSet, Path: []string{"a", "c"}, Payload: arena.NewNumberInt(7)})

	rec := applyOrders(t,
		[]Change{create, unsetA, setAC},
		[]Change{create, setAC, unsetA},
	)
	// d@v1 is removed by the unset@v2; c@v3 survives it.
	assert.Equal(t, map[string]any{"c": float64(7)}, rec["a"])
}

// Regression #3: splitting a collapsed ancestor must preserve the collapsed
// version's coverage of unenumerated deeper siblings — an older concurrent
// write below the split point stays gated in both orders.
func TestConvergence_ExpansionPreservesAncestorCoverage(t *testing.T) {
	g := newVersionGen()
	v1 := g.Next() // create
	v2 := g.Next() // concurrent older: $set a.b.y = 9
	v3 := g.Next() // broad $set a = {b:{y:1}} (collapses _ver.a to v3)
	v4 := g.Next() // finer: $set a.b.x = 5 (splits the collapsed a)

	arena := &anyenc.Arena{}
	create := makeUpsert(v1, "r1", Op{Type: OpSet, Payload: recordPayload(arena, map[string]any{"other": 1})})
	setY := makeChange(v2, "r1", Op{Type: OpSet, Path: []string{"a", "b", "y"}, Payload: arena.NewNumberInt(9)})
	setA := makeChange(v3, "r1", Op{Type: OpSet, Path: []string{"a"}, Payload: recordPayload(arena, map[string]any{"b": map[string]any{"y": 1}})})
	setX := makeChange(v4, "r1", Op{Type: OpSet, Path: []string{"a", "b", "x"}, Payload: arena.NewNumberInt(5)})

	rec := applyOrders(t,
		[]Change{create, setA, setX, setY}, // split first, old write last
		[]Change{create, setY, setA, setX}, // old write first
	)
	// y was authoritatively written by the broad set at v3; the concurrent
	// older y@v2 must stay gated even after x@v4 split the collapsed entry.
	assert.Equal(t, map[string]any{"b": map[string]any{"y": float64(1), "x": float64(5)}}, rec["a"])
}
