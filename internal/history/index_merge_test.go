package history

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// TestRecIdsDeduped: a change with several ops on the same record
// stores the record id once in recIds and lists once per change.
func TestRecIdsDeduped(t *testing.T) {
	ctx := context.Background()
	ix := newTestIndex(t)
	ch := idxChange(1, "notes", "n1")
	ch.Records = append(ch.Records, ch.Records[0]) // second op batch on n1
	indexOne(t, ix, ch)

	out, _, err := ix.ListChanges(ctx, Filter{ObjectId: "obj-1", Dataset: "notes", RecordId: "n1"}, 10, "")
	require.NoError(t, err)
	require.Len(t, out, 1)

	hist, err := ix.historyColl(ctx, "obj-1", false)
	require.NoError(t, err)
	doc, err := hist.FindId(ctx, "o001")
	require.NoError(t, err)
	assert.Len(t, doc.Value().GetArray("recIds"), 1, "recIds must be deduped")
}

// TestListByTraceOrderWithPrefixRelatedOrderIds pins the keySep
// ordering property: lexid OrderIds of different lengths can be
// byte-prefixes of each other, and the trace key embeds the OrderId
// mid-key — so the separator must sort below the whole lexid alphabet
// (which starts at '!'), or descending pagination inverts for
// prefix-related ids. With any separator above '!' this test fails:
// "AB<sep>" would sort above "AB!X<sep>" and the older change would
// list first.
func TestListByTraceOrderWithPrefixRelatedOrderIds(t *testing.T) {
	ctx := context.Background()
	ix := newTestIndex(t)

	older := idxChange(1, "notes", "n1", func(c *crdt.Change) {
		c.VersionId = "AB"
		c.ChangeId = "cid-older"
		c.TraceIds = []string{"tr"}
	})
	newer := idxChange(2, "notes", "n2", func(c *crdt.Change) {
		c.VersionId = "AB!X" // lexid-newer AND a byte-extension of "AB"
		c.ChangeId = "cid-newer"
		c.TraceIds = []string{"tr"}
	})
	indexOne(t, ix, older)
	indexOne(t, ix, newer)

	out, _, err := ix.ListChanges(ctx, Filter{TraceId: "tr"}, 10, "")
	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.Equal(t, "cid-newer", string(out[0].Version), "descending: lexid-newer change first")
	assert.Equal(t, "cid-older", string(out[1].Version))

	// Same property must hold across a pagination boundary.
	page1, cur, err := ix.ListChanges(ctx, Filter{TraceId: "tr"}, 1, "")
	require.NoError(t, err)
	require.Len(t, page1, 1)
	assert.Equal(t, "cid-newer", string(page1[0].Version))
	require.NotEmpty(t, cur)
	page2, _, err := ix.ListChanges(ctx, Filter{TraceId: "tr"}, 1, cur)
	require.NoError(t, err)
	require.Len(t, page2, 1)
	assert.Equal(t, "cid-older", string(page2[0].Version))
}

// TestRecordFilterWithHostileRecordIds pins that record ids are NOT
// key material: they're matched as exact recIds array values, so ids
// containing the trace-key separator (space), ':' or even NUL filter
// correctly and never bleed into each other.
func TestRecordFilterWithHostileRecordIds(t *testing.T) {
	ctx := context.Background()
	ix := newTestIndex(t)
	hostile := []string{"a:b", "a:b:c", "a b", "a\x00b"}
	for i, rec := range hostile {
		indexOne(t, ix, idxChange(i+1, "notes", rec))
	}
	for _, rec := range hostile {
		out, _, err := ix.ListChanges(ctx, Filter{ObjectId: "obj-1", Dataset: "notes", RecordId: rec}, 10, "")
		require.NoError(t, err)
		require.Len(t, out, 1, "record %q must match exactly itself", rec)
		assert.Equal(t, rec, out[0].Touched[0].RecordId)
	}
}
