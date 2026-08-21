package subscribe

import (
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// A $setCreate creation-stamp offer projects as a plain $set of the
// POST-APPLY (min-gated) value — never the offer's own payload, which
// may have lost the min race — and is DROPPED when the snapshot is
// unavailable or lacks the field. Falling through to the generic
// $unset would tell viewers to erase author/createdAt whenever the
// post value is momentarily unreadable.
func TestProjectOp_SetCreateNeverUnsets(t *testing.T) {
	a := &anyenc.Arena{}
	op := crdt.Op{Type: crdt.OpSetCreate, Path: []string{"createdAt"}, Payload: a.NewNumberFloat64(100)}

	_, ok := projectOp(op, nil)
	assert.False(t, ok, "nil snapshot: offer dropped, not $unset")

	post := a.NewObject()
	_, ok = projectOp(op, post)
	assert.False(t, ok, "field missing from snapshot: offer dropped, not $unset")

	post.Set("createdAt", a.NewNumberFloat64(50))
	ev, ok := projectOp(op, post)
	require.True(t, ok)
	assert.Equal(t, crdt.OpSet, ev.Type, "ships as a plain $set")
	assert.Equal(t, []string{"createdAt"}, ev.Path)
	assert.Equal(t, float64(50), ev.Payload.GetFloat64(), "post-apply value, not the losing offer's")
}
