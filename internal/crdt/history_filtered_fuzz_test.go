package crdt

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/require"
)

// Delivery-order / branch-shape fuzz for the record-filtered replay
// fast path (docs/version-history-proposal.md §4.2). The base soundness
// test (TestFilteredReplayMatchesFullReplay) runs one ascending order;
// the proposal requires the claim to hold across delivery orders and
// branch-heavy DAGs before shipping.
//
// Two distinct claims are exercised:
//
//  1. SOUNDNESS (what RecordAt needs): for any delivery order D,
//     replaying the touching-subsequence of D produces a record
//     byte-identical to the full replay of D. Checked under arbitrary
//     permutations — stronger than production ever needs (tree
//     iteration is topological, so a device never sees a child before
//     its causal parent).
//
//  2. CONVERGENCE canary: two CAUSALLY VALID delivery orders of the
//     same change set converge. Valid means parents-before-children:
//     concurrent branches may interleave arbitrarily, but each
//     branch's internal order holds. Known, accepted non-convergence
//     (asserted around, not hidden):
//     - tombstone _ver residue: a sticky delete arriving before a
//       higher-versioned concurrent edit keeps the edit's version in
//       _ver.* — visible state (deleted) still converges;
//     - $inc concurrent with $set on the same field diverges by
//       construction (inc applies relative to whatever value was
//       current), so the concurrent generator uses $set/$delete only.
//       Flagged for grooming in the proposal.

// stripOrderArtifacts removes bookkeeping whose value legitimately
// depends on delivery order or apply count, not on the change set:
// _applySeq (per-apply counter) and _addSeq (stamped from the LAST
// applied change touching the row — "last" is order-dependent).
// _ver must converge (for live records) and stays in the comparison.
func stripOrderArtifacts(rec *anyenc.Value) *anyenc.Value {
	if rec == nil {
		return nil
	}
	if o, _ := rec.Object(); o != nil {
		o.Del(ApplySeqField)
		o.Del(AddSeqField)
	}
	return rec
}

func applyAll(t *testing.T, ctrl *Controller, changes []Change) {
	t.Helper()
	for _, ch := range changes {
		require.NoError(t, ctrl.ApplyChange(ctx, ch))
	}
}

func shuffled(changes []Change, rng *rand.Rand) []Change {
	out := append([]Change(nil), changes...)
	rng.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

func filterTouching(changes []Change, recordId string) []Change {
	var out []Change
	for _, ch := range changes {
		for _, rc := range ch.Records {
			if rc.Id == recordId {
				out = append(out, ch)
				break
			}
		}
	}
	return out
}

// concurrentWorkload models a branch-heavy DAG at the CRDT level: a
// shared create-prefix (all records exist before the fork), then two
// concurrent edit branches whose versionIds interleave on one lexid
// axis. Any merge of (prefix, then interleaved branches with internal
// order kept) is a causally valid delivery order.
type concurrentWorkload struct {
	prefix   []Change
	branches [2][]Change
}

func genConcurrentBranches(editsPerBranch, numRecords int, seed int64) concurrentWorkload {
	g := newVersionGen()
	rng := rand.New(rand.NewSource(seed))
	arena := &anyenc.Arena{}
	var w concurrentWorkload

	cid := 0
	stamp := func(ch Change) Change {
		ch.ChangeId = fmt.Sprintf("cid-%d", cid)
		ch.AddSeq = uint64(cid + 1)
		cid++
		return ch
	}

	for r := 0; r < numRecords; r++ {
		id := fmt.Sprintf("rec-%d", r)
		w.prefix = append(w.prefix, stamp(makeUpsert(g.Next(), id, Op{
			Type: OpSet,
			Payload: recordPayload(arena, map[string]any{
				"name":  "record " + id,
				"count": rng.Intn(1000),
			}),
		})))
	}

	// Interleave version minting across branches so cross-branch LWW
	// comparisons land on both sides of each other.
	for i := 0; i < editsPerBranch*2; i++ {
		branch := i % 2
		id := fmt.Sprintf("rec-%d", rng.Intn(numRecords))
		var ch Change
		if rng.Intn(150) == 0 {
			ch = makeChange(g.Next(), id, Op{Type: OpDelete})
		} else {
			// $set/$delete only: $inc concurrent with $set diverges by
			// construction (see header comment).
			ch = makeChange(g.Next(), id, Op{
				Type: OpSet, Path: []string{"name"},
				Payload: arena.NewString(fmt.Sprintf("edit %d by branch-%d", i, branch)),
			})
		}
		w.branches[branch] = append(w.branches[branch], stamp(ch))
	}
	return w
}

// mergeBranches returns prefix + a random causally-valid interleave of
// the two branches (each branch's internal order preserved).
func (w concurrentWorkload) mergeBranches(rng *rand.Rand) []Change {
	out := append([]Change(nil), w.prefix...)
	i, j := 0, 0
	a, b := w.branches[0], w.branches[1]
	for i < len(a) || j < len(b) {
		if i < len(a) && (j >= len(b) || rng.Intn(2) == 0) {
			out = append(out, a[i])
			i++
		} else {
			out = append(out, b[j])
			j++
		}
	}
	return out
}

func requireSameRecord(t *testing.T, target string, want, got *anyenc.Value, msg string) {
	t.Helper()
	if want == nil || got == nil {
		require.Equal(t, want == nil, got == nil, "%s: absence diverged for %s", msg, target)
		return
	}
	wantDead := want.Get(DeletedAtField) != nil
	gotDead := got.Get(DeletedAtField) != nil
	require.Equal(t, wantDead, gotDead, "%s: deletion state diverged for %s", msg, target)
	if wantDead {
		return // tombstone _ver residue is order-dependent by design
	}
	require.Equal(t, want.String(), got.String(), "%s: %s", msg, target)
}

// TestFilteredReplaySoundnessUnderPermutedDelivery: claim 1. Arbitrary
// permutations (even causally invalid ones) — filtered subsequence of D
// must equal full replay of D for the target record.
func TestFilteredReplaySoundnessUnderPermutedDelivery(t *testing.T) {
	for seed := int64(1); seed <= 3; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			changes := genHistoryWorkload(3_000, 60, true)
			rng := rand.New(rand.NewSource(seed))
			perm := shuffled(changes, rng)

			full, doneFull := newScratchController(t)
			defer doneFull()
			applyAll(t, full, perm)

			for _, target := range []string{"rec-0", "rec-7", "rec-31", "rec-59"} {
				part, donePart := newScratchController(t)
				applyAll(t, part, filterTouching(perm, target))
				requireSameRecord(t, target,
					stripOrderArtifacts(full.Get(ctx, testDS, target)),
					stripOrderArtifacts(part.Get(ctx, testDS, target)),
					"filtered vs full under permuted delivery")
				donePart()
			}
		})
	}
}

// TestFilteredReplayFuzzConcurrentBranches: claims 1 + 2 on a
// branch-heavy shape with causally valid delivery orders.
func TestFilteredReplayFuzzConcurrentBranches(t *testing.T) {
	for seed := int64(1); seed <= 3; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			w := genConcurrentBranches(1_500, 60, seed)
			rng := rand.New(rand.NewSource(seed * 100))

			orderA := w.mergeBranches(rng)
			orderB := w.mergeBranches(rng)

			fullA, doneA := newScratchController(t)
			defer doneA()
			applyAll(t, fullA, orderA)

			fullB, doneB := newScratchController(t)
			defer doneB()
			applyAll(t, fullB, orderB)

			for _, target := range []string{"rec-0", "rec-7", "rec-31", "rec-59"} {
				wantA := stripOrderArtifacts(fullA.Get(ctx, testDS, target))

				// Claim 2: two valid orders converge.
				requireSameRecord(t, target, wantA,
					stripOrderArtifacts(fullB.Get(ctx, testDS, target)),
					"full replays of two causally valid orders")

				// Claim 1 on each order.
				for name, order := range map[string][]Change{"A": orderA, "B": orderB} {
					fullSide := fullA
					if name == "B" {
						fullSide = fullB
					}
					part, donePart := newScratchController(t)
					applyAll(t, part, filterTouching(order, target))
					requireSameRecord(t, target,
						stripOrderArtifacts(fullSide.Get(ctx, testDS, target)),
						stripOrderArtifacts(part.Get(ctx, testDS, target)),
						"filtered vs full on order "+name)
					donePart()
				}
			}
		})
	}
}
