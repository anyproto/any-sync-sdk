package properties_test

import (
	"context"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/lexid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
)

// membershipRegistry is the shared schema for the convergence cases:
// the two membership fields plus a string property on the definition
// under test.
func membershipRegistry(ownerId string) *types.StubRegistry {
	reg := &types.StubRegistry{}
	reg.Set(typeAny, anytype.FieldType, schema.KindString)
	reg.Set(typeAny, anytype.FieldCollections, schema.KindArray)
	reg.Set(ownerId, "p1", schema.KindString)
	return reg
}

// lexIds yields an increasing VersionId sequence.
func lexIds() func() crdt.VersionId {
	lx := lexid.Must(lexid.CharsAllNoEscape, 4, 100)
	prev := ""
	return func() crdt.VersionId { prev = lx.Next(prev); return crdt.VersionId(prev) }
}

// TestProperties_DetachConcurrentWriteReattach_Converges is the
// end-to-end proof of the orphan-resurrection decision
// (docs/06-data-structure.md §16): one peer removes a collection while
// another concurrently writes a value under it; after both peers see
// both changes (in opposite orders) they converge, the value survives
// as orphan, and re-adding reveals the identical value on both.
//
// This is the scenario that started the §16 thread. Nothing else tests
// it; a future apply-side membership guard would break this test.
func TestProperties_DetachConcurrentWriteReattach_Converges(t *testing.T) {
	ctx := context.Background()

	const userC = "userC"
	reg := membershipRegistry(userC)

	next := lexIds()
	vCreate := next()
	vDetach := next()
	vWrite := next() // vWrite > vDetach > vCreate

	a := &anyenc.Arena{}
	collArr := a.NewArray()
	collArr.SetArrayItem(0, a.NewString(userC))

	// Causally-first create: object is in userC and holds userC.p1.
	create := makeChange(vCreate, testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, anytype.FieldCollections}, Payload: collArr},
		crdt.Op{Type: crdt.OpSet, Path: []string{userC, "p1"}, Payload: a.NewString("old")},
	)
	// Bob removes userC. Alice writes a new value under userC — concurrent.
	detach := makeChange(vDetach, testObjectId, false,
		crdt.Op{Type: crdt.OpPull, Path: []string{typeAny, anytype.FieldCollections}, Payload: a.NewString(userC)},
	)
	write := makeChange(vWrite, testObjectId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{userC, "p1"}, Payload: a.NewString("new")},
	)

	// Two peers receive the concurrent pair in opposite orders.
	peerA := newPropsController(t, reg) // write before learning of detach
	for _, ch := range []crdt.Change{create, write, detach} {
		require.NoError(t, peerA.ApplyChange(ctx, ch))
	}
	peerB := newPropsController(t, reg) // detach before learning of write
	for _, ch := range []crdt.Change{create, detach, write} {
		require.NoError(t, peerB.ApplyChange(ctx, ch))
	}

	inUserC := func(rec *anyenc.Value) bool {
		for _, v := range rec.GetArray(typeAny, anytype.FieldCollections) {
			if string(v.GetStringBytes()) == userC {
				return true
			}
		}
		return false
	}
	assertState := func(ctrl *crdt.Controller, who string, wantAttached bool) {
		rec := ctrl.Get(ctx, properties.Dataset, testObjectId)
		require.NotNil(t, rec, who)
		assert.Equal(t, "new", rec.GetString(userC, "p1"), who+": orphan value converged to LWW winner")
		assert.Equal(t, wantAttached, inUserC(rec), who+": userC membership")
	}

	// Removed on both, value survives as orphan, both identical.
	assertState(peerA, "peerA", false)
	assertState(peerB, "peerB", false)

	// Re-add userC on both peers — orphan resurfaces unchanged.
	reattach := makeChange(next(), testObjectId, false,
		crdt.Op{Type: crdt.OpAddToSet, Path: []string{typeAny, anytype.FieldCollections}, Payload: a.NewString(userC)},
	)
	require.NoError(t, peerA.ApplyChange(ctx, reattach))
	require.NoError(t, peerB.ApplyChange(ctx, reattach))
	assertState(peerA, "peerA after reattach", true)
	assertState(peerB, "peerB after reattach", true)
}

// TestProperties_RetypeConcurrentWrite_Converges is the same tolerance
// for the scalar slot: a retype and a concurrent write under the old
// type converge, the old namespace's values stay put as orphans, and
// retyping back reveals them unchanged.
func TestProperties_RetypeConcurrentWrite_Converges(t *testing.T) {
	ctx := context.Background()

	const oldT = "oldT"
	reg := membershipRegistry(oldT)

	next := lexIds()
	vCreate := next()
	vRetype := next()
	vWrite := next()

	a := &anyenc.Arena{}
	create := makeChange(vCreate, testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, anytype.FieldType}, Payload: a.NewString(oldT)},
		crdt.Op{Type: crdt.OpSet, Path: []string{oldT, "p1"}, Payload: a.NewString("old")},
	)
	retype := makeChange(vRetype, testObjectId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, anytype.FieldType}, Payload: a.NewString("newT")},
	)
	write := makeChange(vWrite, testObjectId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{oldT, "p1"}, Payload: a.NewString("new")},
	)

	assertState := func(ctrl *crdt.Controller, who, wantType string) {
		rec := ctrl.Get(ctx, properties.Dataset, testObjectId)
		require.NotNil(t, rec, who)
		assert.Equal(t, "new", rec.GetString(oldT, "p1"), who+": orphan value converged to LWW winner")
		assert.Equal(t, wantType, rec.GetString(typeAny, anytype.FieldType), who+": type slot")
	}

	peerA := newPropsController(t, reg)
	for _, ch := range []crdt.Change{create, write, retype} {
		require.NoError(t, peerA.ApplyChange(ctx, ch))
	}
	peerB := newPropsController(t, reg)
	for _, ch := range []crdt.Change{create, retype, write} {
		require.NoError(t, peerB.ApplyChange(ctx, ch))
	}
	assertState(peerA, "peerA", "newT")
	assertState(peerB, "peerB", "newT")

	back := makeChange(next(), testObjectId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, anytype.FieldType}, Payload: a.NewString(oldT)},
	)
	require.NoError(t, peerA.ApplyChange(ctx, back))
	require.NoError(t, peerB.ApplyChange(ctx, back))
	assertState(peerA, "peerA after retype back", oldT)
	assertState(peerB, "peerB after retype back", oldT)
}
