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
)

// TestProperties_DetachConcurrentWriteReattach_Converges is the
// end-to-end proof of the orphan-resurrection decision
// (docs/06-data-structure.md §16): one peer detaches a type while
// another concurrently writes a value under it; after both peers see
// both changes (in opposite orders) they converge, the value survives
// as orphan, and re-attaching reveals the identical value on both.
//
// This is the scenario that started the §16 thread. Nothing else tests
// it; a future apply-side membership guard would break this test.
func TestProperties_DetachConcurrentWriteReattach_Converges(t *testing.T) {
	ctx := context.Background()

	// Shared schema: any.types is an array; userT declares a string p1.
	reg := &types.StubRegistry{}
	reg.Set(typeAny, "types", schema.KindArray)
	reg.Set("userT", "p1", schema.KindString)

	lx := lexid.Must(lexid.CharsAllNoEscape, 4, 100)
	prev := ""
	next := func() crdt.VersionId { prev = lx.Next(prev); return crdt.VersionId(prev) }
	vCreate := next()
	vDetach := next()
	vWrite := next() // vWrite > vDetach > vCreate

	a := &anyenc.Arena{}
	typesArr := a.NewArray()
	typesArr.SetArrayItem(0, a.NewString("userT"))

	// Causally-first create: object implements userT and holds userT.p1.
	create := makeChange(vCreate, testObjectId, "_base", true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, "types"}, Payload: typesArr},
		crdt.Op{Type: crdt.OpSet, Path: []string{"userT", "p1"}, Payload: a.NewString("old")},
	)
	// Bob detaches userT. Alice writes a new value under userT — concurrent.
	detach := makeChange(vDetach, testObjectId, "_base", false,
		crdt.Op{Type: crdt.OpPull, Path: []string{typeAny, "types"}, Payload: a.NewString("userT")},
	)
	write := makeChange(vWrite, testObjectId, "_base", false,
		crdt.Op{Type: crdt.OpSet, Path: []string{"userT", "p1"}, Payload: a.NewString("new")},
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

	hasUserT := func(rec *anyenc.Value) bool {
		for _, v := range rec.GetArray("_base", typeAny, "types") {
			if string(v.GetStringBytes()) == "userT" {
				return true
			}
		}
		return false
	}
	assertState := func(ctrl *crdt.Controller, who string, wantAttached bool) {
		rec := ctrl.Get(ctx, properties.Dataset, testObjectId)
		require.NotNil(t, rec, who)
		assert.Equal(t, "new", rec.GetString("_base", "userT", "p1"), who+": orphan value converged to LWW winner")
		assert.Equal(t, wantAttached, hasUserT(rec), who+": userT membership")
	}

	// Detached on both, value survives as orphan, both identical.
	assertState(peerA, "peerA", false)
	assertState(peerB, "peerB", false)

	// Re-attach userT on both peers — orphan resurfaces unchanged.
	reattach := makeChange(next(), testObjectId, "_base", false,
		crdt.Op{Type: crdt.OpAddToSet, Path: []string{typeAny, "types"}, Payload: a.NewString("userT")},
	)
	require.NoError(t, peerA.ApplyChange(ctx, reattach))
	require.NoError(t, peerB.ApplyChange(ctx, reattach))
	assertState(peerA, "peerA after reattach", true)
	assertState(peerB, "peerB after reattach", true)
}
