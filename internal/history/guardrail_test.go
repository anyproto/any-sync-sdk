package history

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReplayGuardrailCountsDistinctRecords pins that ErrViewTooLarge
// bounds MATERIALIZED state, not replay work: a single record edited
// far past MaxRecords never trips (the editor keystroke profile — a
// small document with a long history must stay viewable), while a cut
// touching more DISTINCT records than the bound still does.
func TestReplayGuardrailCountsDistinctRecords(t *testing.T) {
	ctx := context.Background()

	// 200 edits of one record, bound 2: work >> bound, rows = 1.
	b := newTreeBuilder(t)
	var last string
	for i := 0; i < 200; i++ {
		last = b.add("notes", upsert(t, "n1", fmt.Sprintf(`{"title":"rev %d"}`, i)))
	}
	v, err := buildScratch(t, b.tree, ViewParams{
		ObjectId: testObjectId, Heads: []string{last}, Regs: testRegs(), MaxRecords: 2,
	})
	require.NoError(t, err, "a long edit history on one record must not trip the guardrail")
	require.NotNil(t, v.Record(ctx, "notes", "n1"))

	// 3 distinct records, bound 2: trips regardless of change count.
	b2 := newTreeBuilder(t)
	b2.add("notes", upsert(t, "n1", `{"title":"a"}`))
	b2.add("notes", upsert(t, "n2", `{"title":"b"}`))
	last = b2.add("notes", upsert(t, "n3", `{"title":"c"}`))
	_, err = buildScratch(t, b2.tree, ViewParams{
		ObjectId: testObjectId, Heads: []string{last}, Regs: testRegs(), MaxRecords: 2,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrViewTooLarge))
}
