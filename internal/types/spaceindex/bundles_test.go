package spaceindex_test

import (
	"context"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/lexid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
)

var ctx = context.Background()

func newBundlesController(t *testing.T) *crdt.Controller {
	t.Helper()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ctrl, err := crdt.NewController(ctx, "spaceIndexObj", db, crdt.HandlerReg{
		Name:    spaceindex.BundlesDataset,
		Handler: spaceindex.BundlesHandler{},
		Schema:  spaceindex.BundlesSchema(),
	})
	require.NoError(t, err)
	return ctrl
}

// installChange is one device's whole install write: $set name +
// $set rootId + $addToSet roots, upsert — the exact shape the typed
// Ensure emits.
func installChange(version crdt.VersionId, bundleId, name, rootId string) crdt.Change {
	arena := &anyenc.Arena{}
	return crdt.Change{
		ObjectId:    "spaceIndexObj",
		Dataset:     spaceindex.BundlesDataset,
		VersionId:   version,
		DataVersion: spaceindex.BundlesHandlerVersion,
		Records: []crdt.RecordChange{{
			Id:     bundleId,
			Upsert: true,
			Ops: []crdt.Op{
				{Type: crdt.OpSet, Path: []string{spaceindex.FieldBundleName}, Payload: arena.NewString(name)},
				{Type: crdt.OpSet, Path: []string{spaceindex.FieldBundleRootId}, Payload: arena.NewString(rootId)},
				{Type: crdt.OpAddToSet, Path: []string{spaceindex.FieldBundleRoots}, Payload: arena.NewString(rootId)},
			},
		}},
	}
}

func rootsOf(t *testing.T, v *anyenc.Value) []string {
	t.Helper()
	arr := v.Get(spaceindex.FieldBundleRoots)
	require.NotNil(t, arr)
	items, err := arr.Array()
	require.NoError(t, err)
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, string(it.GetStringBytes()))
	}
	return out
}

// Two devices install the same bundle concurrently; whatever the
// delivery order, both replicas converge to the same winner and both
// claimed roots stay listed (losers must remain discoverable for
// merge + cleanup).
func TestBundlesConvergence_ConcurrentInstall(t *testing.T) {
	lx := lexid.Must(lexid.CharsAllNoEscape, 4, 100)
	vA := lx.Next("")
	vB := lx.Next(vA)

	installA := installChange(crdt.VersionId(vA), "bao/v1", "Bao", "rootA")
	installB := installChange(crdt.VersionId(vB), "bao/v1", "Bao", "rootB")

	peer1 := newBundlesController(t)
	require.NoError(t, peer1.ApplyChange(ctx, installA))
	require.NoError(t, peer1.ApplyChange(ctx, installB))

	peer2 := newBundlesController(t)
	require.NoError(t, peer2.ApplyChange(ctx, installB))
	require.NoError(t, peer2.ApplyChange(ctx, installA))

	rec1 := peer1.Get(ctx, spaceindex.BundlesDataset, "bao/v1")
	rec2 := peer2.Get(ctx, spaceindex.BundlesDataset, "bao/v1")
	require.NotNil(t, rec1)
	require.NotNil(t, rec2)

	// Same winner on both replicas — the higher-versioned install.
	assert.Equal(t, "rootB", rec1.GetString(spaceindex.FieldBundleRootId))
	assert.Equal(t, "rootB", rec2.GetString(spaceindex.FieldBundleRootId))
	// Both claims land regardless of order.
	assert.ElementsMatch(t, []string{"rootA", "rootB"}, rootsOf(t, rec1))
	assert.ElementsMatch(t, []string{"rootA", "rootB"}, rootsOf(t, rec2))
}

// Idempotent re-install from the same device (same root) leaves one
// claim entry.
func TestBundlesConvergence_ReinstallSameRoot(t *testing.T) {
	lx := lexid.Must(lexid.CharsAllNoEscape, 4, 100)
	v1 := lx.Next("")
	v2 := lx.Next(v1)

	ctrl := newBundlesController(t)
	require.NoError(t, ctrl.ApplyChange(ctx, installChange(crdt.VersionId(v1), "bao/v1", "Bao", "rootA")))
	require.NoError(t, ctrl.ApplyChange(ctx, installChange(crdt.VersionId(v2), "bao/v1", "Bao", "rootA")))

	rec := ctrl.Get(ctx, spaceindex.BundlesDataset, "bao/v1")
	require.NotNil(t, rec)
	assert.Equal(t, "rootA", rec.GetString(spaceindex.FieldBundleRootId))
	assert.Equal(t, []string{"rootA"}, rootsOf(t, rec))
}

func TestBundlesHandler_Validation(t *testing.T) {
	arena := &anyenc.Arena{}
	h := spaceindex.BundlesHandler{}
	str := func(s string) *anyenc.Value { return arena.NewString(s) }

	recWith := func(ops ...crdt.Op) *crdt.RecordChange {
		return &crdt.RecordChange{Id: "b1", Upsert: true, Ops: ops}
	}

	t.Run("valid install passes", func(t *testing.T) {
		rec := recWith(
			crdt.Op{Type: crdt.OpSet, Path: []string{spaceindex.FieldBundleRootId}, Payload: str("r1")},
			crdt.Op{Type: crdt.OpAddToSet, Path: []string{spaceindex.FieldBundleRoots}, Payload: str("r1")},
		)
		require.NoError(t, h.BeforeCreate(nil, rec, nil))
		for i := range rec.Ops {
			require.NoError(t, h.BeforeModify(nil, rec, &rec.Ops[i], nil))
		}
	})

	t.Run("empty bundle id rejected", func(t *testing.T) {
		rec := &crdt.RecordChange{Ops: []crdt.Op{{Type: crdt.OpSet, Path: []string{spaceindex.FieldBundleName}, Payload: str("x")}}}
		assert.ErrorIs(t, h.BeforeCreate(nil, rec, nil), crdt.ErrValidation)
	})

	t.Run("record delete rejected", func(t *testing.T) {
		assert.ErrorIs(t, h.BeforeDelete(nil, recWith(), nil), crdt.ErrValidation)
	})

	t.Run("rootId without matching roots claim rejected", func(t *testing.T) {
		rec := recWith(crdt.Op{Type: crdt.OpSet, Path: []string{spaceindex.FieldBundleRootId}, Payload: str("r1")})
		assert.ErrorIs(t, h.BeforeModify(nil, rec, &rec.Ops[0], nil), crdt.ErrValidation)
	})

	t.Run("rootId with mismatched roots claim rejected", func(t *testing.T) {
		rec := recWith(
			crdt.Op{Type: crdt.OpSet, Path: []string{spaceindex.FieldBundleRootId}, Payload: str("r1")},
			crdt.Op{Type: crdt.OpAddToSet, Path: []string{spaceindex.FieldBundleRoots}, Payload: str("other")},
		)
		assert.ErrorIs(t, h.BeforeModify(nil, rec, &rec.Ops[0], nil), crdt.ErrValidation)
	})

	t.Run("set on roots rejected", func(t *testing.T) {
		rec := recWith(crdt.Op{Type: crdt.OpSet, Path: []string{spaceindex.FieldBundleRoots}, Payload: str("r1")})
		assert.ErrorIs(t, h.BeforeModify(nil, rec, &rec.Ops[0], nil), crdt.ErrValidation)
	})

	t.Run("pull on roots rejected", func(t *testing.T) {
		rec := recWith(crdt.Op{Type: crdt.OpPull, Path: []string{spaceindex.FieldBundleRoots}, Payload: str("r1")})
		assert.ErrorIs(t, h.BeforeModify(nil, rec, &rec.Ops[0], nil), crdt.ErrValidation)
	})

	t.Run("root-level multi-field set rejected", func(t *testing.T) {
		rec := recWith(crdt.Op{Type: crdt.OpSet, Payload: arena.NewObject()})
		assert.ErrorIs(t, h.BeforeModify(nil, rec, &rec.Ops[0], nil), crdt.ErrValidation)
	})

	t.Run("unknown field rejected", func(t *testing.T) {
		rec := recWith(crdt.Op{Type: crdt.OpSet, Path: []string{"bogus"}, Payload: str("x")})
		assert.ErrorIs(t, h.BeforeModify(nil, rec, &rec.Ops[0], nil), crdt.ErrValidation)
	})

	t.Run("empty rootId rejected", func(t *testing.T) {
		rec := recWith(
			crdt.Op{Type: crdt.OpSet, Path: []string{spaceindex.FieldBundleRootId}, Payload: str("")},
		)
		assert.ErrorIs(t, h.BeforeModify(nil, rec, &rec.Ops[0], nil), crdt.ErrValidation)
	})
}

// Invalid ops are dropped per-op at apply time — a hostile or buggy
// writer cannot shrink the claim set through the DAG route.
func TestBundlesApply_InvalidOpsDropped(t *testing.T) {
	lx := lexid.Must(lexid.CharsAllNoEscape, 4, 100)
	v1 := lx.Next("")
	v2 := lx.Next(v1)

	ctrl := newBundlesController(t)
	require.NoError(t, ctrl.ApplyChange(ctx, installChange(crdt.VersionId(v1), "bao/v1", "Bao", "rootA")))

	arena := &anyenc.Arena{}
	attack := crdt.Change{
		ObjectId:    "spaceIndexObj",
		Dataset:     spaceindex.BundlesDataset,
		VersionId:   crdt.VersionId(v2),
		DataVersion: spaceindex.BundlesHandlerVersion,
		Records: []crdt.RecordChange{{
			Id:     "bao/v1",
			Upsert: true,
			Ops: []crdt.Op{
				// Replace the claim set — must be dropped (add-only).
				{Type: crdt.OpSet, Path: []string{spaceindex.FieldBundleRoots}, Payload: arena.NewArray()},
				// Pull a claim — must be dropped.
				{Type: crdt.OpPull, Path: []string{spaceindex.FieldBundleRoots}, Payload: arena.NewString("rootA")},
			},
		}},
	}
	require.NoError(t, ctrl.ApplyChange(ctx, attack))

	rec := ctrl.Get(ctx, spaceindex.BundlesDataset, "bao/v1")
	require.NotNil(t, rec)
	assert.Equal(t, []string{"rootA"}, rootsOf(t, rec))
	assert.Equal(t, "rootA", rec.GetString(spaceindex.FieldBundleRootId))
}
