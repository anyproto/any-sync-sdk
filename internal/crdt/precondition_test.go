package crdt

import (
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChangedSince pins the precondition read: a record stamped above
// the value is a change, nothing above it is not, and while a reindex
// is pending the watermark restarts below the rows, so the answer comes
// from the rows rather than from the watermark.
func TestChangedSince(t *testing.T) {
	ctrl, _ := newApplySeqController(t, 100)
	arena := &anyenc.Arena{}

	res, err := ctrl.ApplyChangeWithResult(ctx, applySeqChange("v1", "r1",
		Op{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("a")}))
	require.NoError(t, err)

	changed, err := ctrl.ChangedSince(ctx, testDS, res.ApplySeq)
	require.NoError(t, err)
	assert.False(t, changed, "nothing above the highest stamp")

	changed, err = ctrl.ChangedSince(ctx, testDS, res.ApplySeq-1)
	require.NoError(t, err)
	assert.True(t, changed, "the record's stamp is above an older value")

	require.NoError(t, ctrl.ResetForReindex(ctx, nil))
	changed, err = ctrl.ChangedSince(ctx, testDS, res.ApplySeq-1)
	require.NoError(t, err)
	assert.True(t, changed, "a pending reindex zeroed the watermark; the rows still answer")
}
