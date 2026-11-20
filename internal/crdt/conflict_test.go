package crdt

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/lexid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// versionGen produces a stable, lexicographically-sortable sequence of
// versionIds via lexid — the same generator any-sync uses. Using lexid in
// tests catches subtle ordering bugs that simple "v1, v2, ..." strings hide
// (notably "v10" < "v5"; see the design memory note on lexid).
type versionGen struct {
	lx   *lexid.Lexid
	prev string
}

func newVersionGen() *versionGen {
	return &versionGen{
		lx: lexid.Must(lexid.CharsAllNoEscape, 4, 100),
	}
}

func (g *versionGen) Next() VersionId {
	g.prev = g.lx.Next(g.prev)
	return VersionId(g.prev)
}

// TestVersionGenIsMonotonic is a smoke test for the test helper itself.
func TestVersionGenIsMonotonic(t *testing.T) {
	g := newVersionGen()
	prev := VersionId("")
	for i := 0; i < 100; i++ {
		next := g.Next()
		assert.Truef(t, prev < next, "expected %q < %q", prev, next)
		prev = next
	}
}

// ----------------------------------------------------------------------------
// crdt-spec.md §15 conflict examples
// ----------------------------------------------------------------------------

// §15.1 — two devices rename the same record. Both writes should land in the
// expected order; the higher-version one wins.
func TestConflict_TwoDevicesRename(t *testing.T) {
	g := newVersionGen()
	vInsert := g.Next()
	vA := g.Next()
	vB := g.Next() // vB > vA

	// Insert is causally first (any-sync's DAG enforces this), so we only
	// permute the two concurrent renames. Modifies that arrive before their
	// insert are legitimately dropped in v1; that's a separate concern.
	for _, order := range [][]int{{0, 1, 2}, {0, 2, 1}} {
		st := newTestController(t)
		arena := &anyenc.Arena{}
		changes := []Change{
			makeUpsert(vInsert, "r1", Op{
				Type:    OpSet,
				Payload: recordPayload(arena, map[string]any{"name": "init"}),
			}),
			makeChange(vA, "r1", Op{
				Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("Alpha"),
			}),
			makeChange(vB, "r1", Op{
				Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("Beta"),
			}),
		}
		for _, i := range order {
			require.NoError(t, st.ApplyChange(ctx, changes[i]))
		}
		rec := st.Get(ctx, testDS, "r1")
		require.NotNil(t, rec)
		assert.Equal(t, "Beta", rec.GetString("name"), "order %v", order)
		assert.Equal(t, vB, GetRecordVersion(rec, "name"))
	}
}

// §15.2 — out-of-order delivery. An ancient $set arriving after a newer one
// is silently dropped.
func TestConflict_OutOfOrderDelivery(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}
	g := newVersionGen()
	vInsert := g.Next()
	vOld := g.Next()
	vNew := g.Next()

	require.NoError(t, st.ApplyChange(ctx, makeUpsert(vInsert, "r1", Op{
		Type: OpSet, Payload: recordPayload(arena, map[string]any{"name": "init"}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange(vNew, "r1", Op{
		Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("New"),
	})))
	// Stale arrives last.
	require.NoError(t, st.ApplyChange(ctx, makeChange(vOld, "r1", Op{
		Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("Old"),
	})))

	rec := st.Get(ctx, testDS, "r1")
	assert.Equal(t, "New", rec.GetString("name"))
	assert.Equal(t, vNew, GetRecordVersion(rec, "name"))
}

// §15.3 — delete races insert. Final state must always be the tombstone,
// regardless of delivery order.
func TestConflict_DeleteRacesInsert(t *testing.T) {
	g := newVersionGen()
	vInsert := g.Next()
	vDelete := g.Next()

	for _, deleteFirst := range []bool{false, true} {
		st := newTestController(t)
		arena := &anyenc.Arena{}
		insert := makeUpsert(vInsert, "r1", Op{
			Type: OpSet, Payload: recordPayload(arena, map[string]any{"name": "x"}),
		})
		del := Change{
			ObjectId: "obj1", Dataset: testDS, VersionId: vDelete, DataVersion: testDataVersion, Timestamp: 100,
			Records: []RecordChange{{Id: "r1", Ops: []Op{{Type: OpDelete}}}},
		}
		if deleteFirst {
			require.NoError(t, st.ApplyChange(ctx, del))
			require.NoError(t, st.ApplyChange(ctx, insert))
		} else {
			require.NoError(t, st.ApplyChange(ctx, insert))
			require.NoError(t, st.ApplyChange(ctx, del))
		}
		rec := st.Get(ctx, testDS, "r1")
		require.NotNil(t, rec)
		assert.True(t, isTombstone(rec), "deleteFirst=%v", deleteFirst)
	}
}

// §15.4 — $addToSet racing $set on the same field. Both orders converge to
// the $set's value because $addToSet is gated against a newer $set.
func TestConflict_AddToSetVsSet(t *testing.T) {
	g := newVersionGen()
	vInsert := g.Next()
	vAdd := g.Next() // older
	vSet := g.Next() // newer

	for _, addFirst := range []bool{true, false} {
		st := newTestController(t)
		arena := &anyenc.Arena{}
		require.NoError(t, st.ApplyChange(ctx, makeUpsert(vInsert, "r1", Op{
			Type: OpSet, Payload: recordPayload(arena, map[string]any{"tags": []any{}}),
		})))
		add := makeChange(vAdd, "r1", Op{
			Type: OpAddToSet, Path: []string{"tags"}, Payload: arena.NewString("urgent"),
		})
		set := makeChange(vSet, "r1", Op{
			Type: OpSet, Path: []string{"tags"}, Payload: goToAnyenc(arena, []string{"done"}),
		})
		if addFirst {
			require.NoError(t, st.ApplyChange(ctx, add))
			require.NoError(t, st.ApplyChange(ctx, set))
		} else {
			require.NoError(t, st.ApplyChange(ctx, set))
			require.NoError(t, st.ApplyChange(ctx, add))
		}
		rec := st.Get(ctx, testDS, "r1")
		tags := rec.GetArray("tags")
		require.Len(t, tags, 1, "addFirst=%v", addFirst)
		assert.Equal(t, "done", string(tags[0].GetStringBytes()))
	}
}

// §15.5 — concurrent $inc. Both increments apply regardless of order.
func TestConflict_ConcurrentInc(t *testing.T) {
	g := newVersionGen()
	vInsert := g.Next()
	vA := g.Next()
	vB := g.Next()

	// Insert pinned first; the two concurrent $inc calls permute. Per spec
	// §15.5, a $inc started before its target's insert can't compose with
	// the insert's base value, so we don't test that case.
	for _, order := range [][]int{{0, 1, 2}, {0, 2, 1}} {
		st := newTestController(t)
		arena := &anyenc.Arena{}
		changes := []Change{
			makeUpsert(vInsert, "r1", Op{
				Type: OpSet, Payload: recordPayload(arena, map[string]any{"views": 10}),
			}),
			makeChange(vA, "r1", Op{Type: OpInc, Path: []string{"views"}, Payload: arena.NewNumberInt(1)}),
			makeChange(vB, "r1", Op{Type: OpInc, Path: []string{"views"}, Payload: arena.NewNumberInt(1)}),
		}
		for _, i := range order {
			require.NoError(t, st.ApplyChange(ctx, changes[i]))
		}
		rec := st.Get(ctx, testDS, "r1")
		assert.Equal(t, 12, rec.GetInt("views"), "order %v", order)
	}
}

// ----------------------------------------------------------------------------
// $inc causally after $set: convergent under DAG delivery
// ----------------------------------------------------------------------------

// The only causally-legal delivery order for a sequence of (create → $set →
// $inc) on the same counter field is the sequence itself: each op references
// its predecessors via prevIds. The test exercises that order and verifies
// the intuitive result.
//
// Note: we don't permute this test because permutations like [$inc, $set]
// are not DAG-legal — if $inc's version is after $set in the local orderId,
// $inc's prevIds include $set by construction. Any permutation that puts
// $inc before $set would be a broken delivering peer, not a legitimate
// race the CRDT needs to handle.
func TestIncAfterSet_CausalConvergent(t *testing.T) {
	g := newVersionGen()
	vCreate := g.Next()
	vSet := g.Next()
	vInc := g.Next()

	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert(vCreate, "r1", Op{
		Type: OpSet, Payload: recordPayload(arena, map[string]any{"count": 0}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange(vSet, "r1", Op{
		Type: OpSet, Path: []string{"count"}, Payload: arena.NewNumberInt(100),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange(vInc, "r1", Op{
		Type: OpInc, Path: []string{"count"}, Payload: arena.NewNumberInt(1),
	})))

	rec := st.Get(ctx, testDS, "r1")
	// $set wrote 100 and bumped _ver.count to vSet.
	// $inc sees _ver.count = vSet < vInc, applies to the current value (100),
	// yielding 101. _ver.count stays at vSet (inc doesn't update _ver).
	assert.Equal(t, 101, rec.GetInt("count"))
	assert.Equal(t, vSet, GetRecordVersion(rec, "count"))
}

// ----------------------------------------------------------------------------
// Multiple concurrent $inc ops are commutative among themselves
// ----------------------------------------------------------------------------

// Three peers each issue a `$inc` on the same counter after seeing the
// creating $set. None of them saw the others' increments — they're siblings
// in the DAG. A fourth peer receives all three in *some* order; whatever
// that order is, the final value must be the same.
//
// We pin the create as op 0 (it's in every peer's ancestry) and permute
// the three $incs across all 3! = 6 orderings.
func TestMultipleIncs_CommutativeAfterCreate(t *testing.T) {
	g := newVersionGen()
	vCreate := g.Next()
	vA := g.Next() // concurrent $incs: each is a sibling of the others
	vB := g.Next()
	vC := g.Next()

	permutations := [][]int{
		{0, 1, 2, 3}, {0, 1, 3, 2}, {0, 2, 1, 3},
		{0, 2, 3, 1}, {0, 3, 1, 2}, {0, 3, 2, 1},
	}

	for _, p := range permutations {
		st := newTestController(t)
		arena := &anyenc.Arena{}
		changes := []Change{
			makeUpsert(vCreate, "r1", Op{
				Type: OpSet, Payload: recordPayload(arena, map[string]any{"count": 0}),
			}),
			makeChange(vA, "r1", Op{Type: OpInc, Path: []string{"count"}, Payload: arena.NewNumberInt(1)}),
			makeChange(vB, "r1", Op{Type: OpInc, Path: []string{"count"}, Payload: arena.NewNumberInt(2)}),
			makeChange(vC, "r1", Op{Type: OpInc, Path: []string{"count"}, Payload: arena.NewNumberInt(3)}),
		}
		for _, i := range p {
			require.NoError(t, st.ApplyChange(ctx, changes[i]))
		}
		rec := st.Get(ctx, testDS, "r1")
		assert.Equalf(t, 6, rec.GetInt("count"), "permutation %v", p)
	}
}

// ----------------------------------------------------------------------------
// $set + $inc concurrent on the SAME field: anti-pattern demo
// ----------------------------------------------------------------------------

// Two peers author a `$set` and a `$inc` on the same counter field without
// seeing each other. In the DAG these are siblings — both have the creating
// change as their common ancestor, and neither has the other in its prevIds
// chain. A third peer can receive them in either order, and the CRDT
// produces a different value for each order:
//
//	[create, set, inc] → 0 → 100 → 101
//	[create, inc, set] → 0 → 1   → 100 (the $set's value clobbers the
//	                                    inc's contribution because $inc
//	                                    didn't update _ver)
//
// This is NOT a CRDT bug — it's a schema-design anti-pattern. A field
// should be either a counter (only `$inc` after an initial `$set`) or a
// settable value (only `$set`), never both at once. Documenting the
// divergence here so the anti-pattern is concrete instead of hand-wavy.
//
// Whether a real any-sync deployment ever actually exposes this depends
// on how any-sync orders siblings during DAG traversal — if every peer
// derives the same total order, the result is consistent even though
// neither "value" matches a naive read of the user's intent.
func TestConcurrentSetAndIncOnSameField_AntiPattern(t *testing.T) {
	g := newVersionGen()
	vCreate := g.Next()
	vSet := g.Next() // concurrent with vInc
	vInc := g.Next()

	createOp := func(arena *anyenc.Arena) Change {
		return makeUpsert(vCreate, "r1", Op{
			Type: OpSet, Payload: recordPayload(arena, map[string]any{"count": 0}),
		})
	}
	setOp := func(arena *anyenc.Arena) Change {
		return makeChange(vSet, "r1", Op{
			Type: OpSet, Path: []string{"count"}, Payload: arena.NewNumberInt(100),
		})
	}
	incOp := func(arena *anyenc.Arena) Change {
		return makeChange(vInc, "r1", Op{
			Type: OpInc, Path: []string{"count"}, Payload: arena.NewNumberInt(1),
		})
	}

	// Order [create, set, inc]
	stA := newTestController(t)
	arenaA := &anyenc.Arena{}
	require.NoError(t, stA.ApplyChange(ctx, createOp(arenaA)))
	require.NoError(t, stA.ApplyChange(ctx, setOp(arenaA)))
	require.NoError(t, stA.ApplyChange(ctx, incOp(arenaA)))
	finalA := stA.Get(ctx, testDS, "r1").GetInt("count")

	// Order [create, inc, set]
	stB := newTestController(t)
	arenaB := &anyenc.Arena{}
	require.NoError(t, stB.ApplyChange(ctx, createOp(arenaB)))
	require.NoError(t, stB.ApplyChange(ctx, incOp(arenaB)))
	require.NoError(t, stB.ApplyChange(ctx, setOp(arenaB)))
	finalB := stB.Get(ctx, testDS, "r1").GetInt("count")

	assert.Equal(t, 101, finalA, "[create, set, inc] order")
	assert.Equal(t, 100, finalB, "[create, inc, set] order")
	assert.NotEqual(t, finalA, finalB,
		"anti-pattern: $set and $inc concurrent on same field produce order-dependent results")
}

// ----------------------------------------------------------------------------
// $set and $inc on DIFFERENT fields: always converges
// ----------------------------------------------------------------------------

// Sanity check: $set on "name" and $inc on "count" are independent; any
// permutation converges. This is the normal use case and it must always
// work.
func TestSetAndIncOnDifferentFields_Converges(t *testing.T) {
	g := newVersionGen()
	vCreate := g.Next()
	vSetName := g.Next()
	vIncCount := g.Next()

	permutations := [][]int{
		{0, 1, 2}, {0, 2, 1},
	}

	for _, p := range permutations {
		st := newTestController(t)
		arena := &anyenc.Arena{}
		changes := []Change{
			makeUpsert(vCreate, "r1", Op{
				Type: OpSet, Payload: recordPayload(arena, map[string]any{"name": "init", "count": 0}),
			}),
			makeChange(vSetName, "r1", Op{
				Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("updated"),
			}),
			makeChange(vIncCount, "r1", Op{
				Type: OpInc, Path: []string{"count"}, Payload: arena.NewNumberInt(5),
			}),
		}
		for _, i := range p {
			require.NoError(t, st.ApplyChange(ctx, changes[i]))
		}
		rec := st.Get(ctx, testDS, "r1")
		assert.Equalf(t, "updated", rec.GetString("name"), "permutation %v", p)
		assert.Equalf(t, 5, rec.GetInt("count"), "permutation %v", p)
	}
}

// ----------------------------------------------------------------------------
// Convergence: arbitrary permutations of arbitrary op sequences.
// ----------------------------------------------------------------------------

// generateOpSequence builds a deterministic mix of changes touching one
// record. Every change has a unique versionId (lexid-generated) and a
// well-defined LWW outcome.
type seqStep struct {
	change Change
}

// generateOpSequence builds a sequence containing only operations whose final
// state is delivery-order-independent: $set/$unset (LWW), $addToSet/$pull
// (commutative set), $inc (commutative counter). $incGated is excluded — it
// is intentionally non-convergent (LWW on the post-mutation value, see spec
// §5.6) and tested separately.
//
// The mix deliberately includes the shapes that historically escaped this
// fuzzer: multi-field $sets whose siblings share one version, broad
// subtree $set/$unset racing per-leaf writes under the same path, and
// fresh-field writes with mid-range versions (so broad or shared-version
// writes at HIGHER versions exist when they arrive late in a permutation).
func generateOpSequence(t *testing.T, arena *anyenc.Arena, g *versionGen, count int) []seqStep {
	t.Helper()
	steps := make([]seqStep, 0, count+1)
	steps = append(steps, seqStep{makeUpsert(g.Next(), "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "init", "tags": []any{}, "hits": 0}),
	})})
	for i := 0; i < count; i++ {
		v := g.Next()
		switch i % 8 {
		case 0:
			steps = append(steps, seqStep{makeChange(v, "r1", Op{
				Type: OpSet, Path: []string{"name"},
				Payload: arena.NewString("name-" + string(v)),
			})})
		case 1:
			steps = append(steps, seqStep{makeChange(v, "r1", Op{
				Type: OpAddToSet, Path: []string{"tags"},
				Payload: arena.NewString("tag-" + string(v)),
			})})
		case 2:
			steps = append(steps, seqStep{makeChange(v, "r1", Op{
				Type: OpSet, Path: []string{"meta", "color"},
				Payload: arena.NewString("color-" + string(v)),
			})})
		case 3:
			steps = append(steps, seqStep{makeChange(v, "r1", Op{
				Type: OpInc, Path: []string{"hits"},
				Payload: arena.NewNumberInt(1),
			})})
		case 4:
			// Multi-field $set: two siblings share this change's version
			// (one fresh per change, one contended across changes).
			steps = append(steps, seqStep{makeChange(v, "r1", Op{
				Type: OpSet,
				Payload: recordPayload(arena, map[string]any{
					"fresh-" + string(v): 1,
					"shared":             "shared-" + string(v),
				}),
			})})
		case 5:
			// Broad subtree replace racing the per-leaf meta.color writes.
			steps = append(steps, seqStep{makeChange(v, "r1", Op{
				Type: OpSet, Path: []string{"meta"},
				Payload: recordPayload(arena, map[string]any{
					"color": "broad-" + string(v),
					"size":  i,
				}),
			})})
		case 6:
			// Broad unset racing the same subtree.
			steps = append(steps, seqStep{makeChange(v, "r1", Op{
				Type: OpUnset, Path: []string{"meta"},
			})})
		case 7:
			// Fresh top-level field with a mid-range version.
			steps = append(steps, seqStep{makeChange(v, "r1", Op{
				Type: OpSet, Path: []string{"late-" + string(v)},
				Payload: arena.NewNumberInt(i),
			})})
		}
	}
	return steps
}

// canonicalRecord normalizes a record into a comparable representation: just
// the user-visible fields and their final values. _ver is ignored because two
// converged states can have differently-shaped _ver trees that encode the same
// per-field versions.
func canonicalRecord(rec *anyenc.Value) map[string]any {
	if rec == nil {
		return nil
	}
	out := map[string]any{}
	o, _ := rec.Object()
	o.Visit(func(k []byte, v *anyenc.Value) {
		key := string(k)
		if key == VersionsKey {
			return
		}
		out[key] = anyencToGo(v)
	})
	return out
}

func anyencToGo(v *anyenc.Value) any {
	if v == nil {
		return nil
	}
	switch v.Type() {
	case anyenc.TypeString:
		return string(v.GetStringBytes())
	case anyenc.TypeNumber:
		return v.GetFloat64()
	case anyenc.TypeTrue:
		return true
	case anyenc.TypeFalse:
		return false
	case anyenc.TypeNull:
		return nil
	case anyenc.TypeArray:
		arr, _ := v.Array()
		// $addToSet builds a set; compare as sorted-by-string.
		out := make([]any, 0, len(arr))
		for _, it := range arr {
			out = append(out, anyencToGo(it))
		}
		sort.SliceStable(out, func(i, j int) bool {
			return repr(out[i]) < repr(out[j])
		})
		return out
	case anyenc.TypeObject:
		o, _ := v.Object()
		m := map[string]any{}
		o.Visit(func(k []byte, vv *anyenc.Value) {
			m[string(k)] = anyencToGo(vv)
		})
		return m
	}
	return nil
}

func repr(v any) string {
	return fmt.Sprintf("%T:%v", v, v)
}

// TestConvergence_RandomPermutations applies the same set of changes in many
// different orders and asserts every permutation lands on the same final
// state.
func TestConvergence_RandomPermutations(t *testing.T) {
	const opCount = 24
	const permutations = 50

	arena := &anyenc.Arena{}
	g := newVersionGen()
	steps := generateOpSequence(t, arena, g, opCount)

	// Establish a reference state by applying in the canonical (creation)
	// order.
	refState := newTestController(t)
	for _, s := range steps {
		require.NoError(t, refState.ApplyChange(ctx, s.change))
	}
	ref := canonicalRecord(refState.Get(ctx, testDS, "r1"))

	// Now shuffle and re-apply many times.
	rng := rand.New(rand.NewSource(42))
	for p := 0; p < permutations; p++ {
		order := rng.Perm(len(steps))
		// The insert MUST come first or modifies on absent records get
		// skipped (spec says modify-on-absent is a no-op in v1). Pin it.
		insertIdx := indexOf(order, 0)
		order[0], order[insertIdx] = order[insertIdx], order[0]

		st := newTestController(t)
		for _, i := range order {
			require.NoError(t, st.ApplyChange(ctx, steps[i].change))
		}
		got := canonicalRecord(st.Get(ctx, testDS, "r1"))
		assert.Equalf(t, ref, got, "permutation %d %v", p, order)
	}
}

func indexOf(s []int, v int) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}
	return -1
}
