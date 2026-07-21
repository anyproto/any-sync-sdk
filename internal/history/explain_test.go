package history

import (
	"context"
	"fmt"
	"testing"

	"github.com/anyproto/any-store/v2/query"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// TestExplainQueryPlans pins that the history query shapes hit the
// intended any-store access paths on a realistically sized index:
//
//   - plain descending listing: reverse primary-key scan (no index);
//   - record filter: the recIds multikey index, NOT a collection scan —
//     the whole point of the SYN-88 merge;
//   - TouchedChangeIds (no sort): the recIds index.
func TestExplainQueryPlans(t *testing.T) {
	ctx := context.Background()
	ix := newTestIndex(t)

	// Chat-shaped: 20k changes, ~75% creates → ~15k distinct records,
	// so recIds equality is highly selective (~1–5 rows per record).
	created := 0
	for i := 0; i < 20_000; i++ {
		var rec string
		if created == 0 || i%4 != 0 {
			rec = fmt.Sprintf("msg-%d", created)
			created++
		} else {
			rec = fmt.Sprintf("msg-%d", created-1-(i%50)%created)
		}
		indexOne(t, ix, idxChange(i+1, "chat", rec, func(c *crdt.Change) {
			c.VersionId = crdt.VersionId(fmt.Sprintf("o%06d", i+1))
			c.ChangeId = fmt.Sprintf("cid-%06d", i+1)
		}))
	}

	hist, err := ix.historyColl(ctx, "obj-1", false)
	require.NoError(t, err)
	require.NotNil(t, hist)

	dsEq := query.Key{Path: []string{"ds"}, Filter: query.NewComp(query.CompOpEq, "chat")}
	recEq := query.Key{Path: recIdsPath, Filter: query.NewComp(query.CompOpEq, "msg-7")}

	// listDirect, no record filter: reverse PK scan.
	ex, err := hist.Find(query.And{dsEq}).Sort("-id").Limit(51).Explain(ctx)
	require.NoError(t, err)
	t.Logf("plain listing plan:\n%s", ex.Plan)
	for _, ie := range ex.Indexes {
		require.False(t, ie.Used, "plain listing should not need index %s", ie.Name)
	}

	// listDirect record filter: MUST ride the recIds multikey index.
	ex, err = hist.Find(query.And{recEq, dsEq}).Sort("-id").Limit(51).Explain(ctx)
	require.NoError(t, err)
	t.Logf("record filter plan:\n%s", ex.Plan)
	recIdsUsed := false
	for _, ie := range ex.Indexes {
		if ie.Used {
			recIdsUsed = true
			t.Logf("record filter uses index %s (cost %.1f)", ie.Name, ie.Cost)
		}
	}
	require.True(t, recIdsUsed,
		"record filter must use the recIds multikey index, not a collection scan:\n%s", ex.Plan)

	// TouchedChangeIds shape (no sort): recIds index as well.
	ex, err = hist.Find(query.And{recEq, dsEq}).Explain(ctx)
	require.NoError(t, err)
	t.Logf("touched-change-ids plan:\n%s", ex.Plan)
	recIdsUsed = false
	for _, ie := range ex.Indexes {
		recIdsUsed = recIdsUsed || ie.Used
	}
	require.True(t, recIdsUsed, "TouchedChangeIds must use the recIds index:\n%s", ex.Plan)
}
