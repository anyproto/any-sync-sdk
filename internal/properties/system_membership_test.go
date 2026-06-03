package properties_test

import (
	"context"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// TestSystemPropertiesHandler_InboundIgnoresMembership pins the
// load-bearing asymmetry behind the orphan-resurrection decision
// (docs/06-data-structure.md §16): the inbound apply path does NOT
// consult any.types. A value written to a type the object doesn't
// implement lands as orphan on every peer, which is what makes
// concurrent detach-vs-write convergent.
//
// Compare TestPreValidate_TypeNotImplemented: the LOCAL path rejects
// the identical write. Inbound must accept it.
func TestSystemPropertiesHandler_InboundIgnoresMembership(t *testing.T) {
	ctx := context.Background()

	reg := defaultRegistry()                  // any.* props
	reg.Set("userT", "p1", schema.KindString) // userT is KNOWN to the registry…
	ctrl := newPropsController(t, reg)
	arena := &anyenc.Arena{}

	// …but the object never attaches userT (no any.types write). An
	// inbound $set on userT.p1 must still apply — membership is a
	// local-write concern only.
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange(
		"v1", testObjectId, "_base", true,
		crdt.Op{Type: crdt.OpSet, Path: []string{"userT", "p1"}, Payload: arena.NewString("orphan")},
	)))

	rec := ctrl.Get(ctx, properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, "orphan", rec.GetString("_base", "userT", "p1"),
		"inbound write to an unimplemented type must land as orphan, not drop")

	// The orphan value is real data the registry still validates by kind:
	// a kind-mismatched inbound write to the same unimplemented type still
	// drops per-op, proving the accept path is kind-checked, not blanket.
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange(
		"v2", testObjectId, "_base", false,
		crdt.Op{Type: crdt.OpSet, Path: []string{"userT", "p1"}, Payload: arena.NewNumberFloat64(5)},
	)))
	rec = ctrl.Get(ctx, properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, "orphan", rec.GetString("_base", "userT", "p1"),
		"kind-mismatched inbound write drops; prior orphan string survives")
}
