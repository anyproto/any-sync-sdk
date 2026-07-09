package history

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// meta builds a descending-list entry. prev is the sole parent unless
// more are given (merge).
func meta(id, author string, ts int64, touched string, prev ...string) ChangeMeta {
	m := ChangeMeta{
		Version:   id,
		Author:    author,
		Timestamp: ts,
		Dataset:   "notes",
		PrevIds:   prev,
		GroupSize: 1,
	}
	if touched != "" {
		m.Touched = []TouchedRecord{{Dataset: "notes", RecordId: touched, Ops: []string{"$set"}}}
	}
	return m
}

func TestCoalesceLinearChain(t *testing.T) {
	// c3 <- c2 <- c1 (descending), same author, seconds apart.
	entries := []ChangeMeta{
		meta("c3", "alice", 1000, "n1", "c2"),
		meta("c2", "alice", 990, "n2", "c1"),
		meta("c1", "alice", 980, "n1", "c0"),
	}
	out := Coalesce(entries, CoalesceOpts{})
	require.Len(t, out, 1)
	g := out[0]
	assert.Equal(t, "c3", g.Version) // head = newest
	assert.Equal(t, 3, g.GroupSize)
	assert.Equal(t, []string{"c0"}, g.PrevIds) // oldest member's parents
	require.Len(t, g.Touched, 2)               // n1 deduped, n2
}

func TestCoalesceAuthorBreaks(t *testing.T) {
	entries := []ChangeMeta{
		meta("c3", "alice", 1000, "n1", "c2"),
		meta("c2", "bob", 990, "n1", "c1"),
		meta("c1", "alice", 980, "n1", "c0"),
	}
	out := Coalesce(entries, CoalesceOpts{})
	require.Len(t, out, 3)
	for _, e := range out {
		assert.Equal(t, 1, e.GroupSize)
	}
}

func TestCoalesceWindowBreaks(t *testing.T) {
	entries := []ChangeMeta{
		meta("c3", "alice", 10_000, "n1", "c2"),
		meta("c2", "alice", 1_000, "n1", "c1"), // 2.5h gap to c3
		meta("c1", "alice", 990, "n1", "c0"),
	}
	out := Coalesce(entries, CoalesceOpts{Window: 5 * time.Minute})
	require.Len(t, out, 2)
	assert.Equal(t, "c3", out[0].Version)
	assert.Equal(t, 1, out[0].GroupSize)
	assert.Equal(t, "c2", out[1].Version)
	assert.Equal(t, 2, out[1].GroupSize)
}

func TestCoalesceMergeNeverGroups(t *testing.T) {
	// c3 is a merge (two parents) — must not group with c2.
	entries := []ChangeMeta{
		meta("c3", "alice", 1000, "n1", "c2", "b1"),
		meta("c2", "alice", 990, "n1", "c1"),
		meta("c1", "alice", 980, "n1", "c0"),
	}
	out := Coalesce(entries, CoalesceOpts{})
	require.Len(t, out, 2)
	assert.Equal(t, "c3", out[0].Version)
	assert.Equal(t, 1, out[0].GroupSize)
	assert.Equal(t, 2, out[1].GroupSize) // c2+c1 still group
}

func TestCoalesceBranchPointBreaks(t *testing.T) {
	// c1 has two children in the page (a2 and b2): fork point, no
	// group may absorb c1.
	entries := []ChangeMeta{
		meta("a2", "alice", 1000, "n1", "c1"),
		meta("b2", "alice", 995, "n1", "c1"),
		meta("c1", "alice", 990, "n1", "c0"),
	}
	out := Coalesce(entries, CoalesceOpts{})
	require.Len(t, out, 3)
}

func TestCoalesceFilteredGapBreaks(t *testing.T) {
	// A record/dataset filter removed the change between c3 and c1:
	// c3.PrevIds points at an invisible change, so no group forms.
	entries := []ChangeMeta{
		meta("c3", "alice", 1000, "n1", "c2"),
		meta("c1", "alice", 980, "n1", "c0"),
	}
	out := Coalesce(entries, CoalesceOpts{})
	require.Len(t, out, 2)
}

func TestCoalesceUnionsTraces(t *testing.T) {
	e1 := meta("c2", "alice", 1000, "n1", "c1")
	e1.TraceIds = []string{"t1"}
	e2 := meta("c1", "alice", 990, "n1", "c0")
	e2.TraceIds = []string{"t1", "t2"}
	out := Coalesce([]ChangeMeta{e1, e2}, CoalesceOpts{})
	require.Len(t, out, 1)
	assert.ElementsMatch(t, []string{"t1", "t2"}, out[0].TraceIds)
}

func TestCoalesceEmptyAndSingle(t *testing.T) {
	assert.Empty(t, Coalesce(nil, CoalesceOpts{}))
	out := Coalesce([]ChangeMeta{meta("c1", "a", 1, "n1", "c0")}, CoalesceOpts{})
	require.Len(t, out, 1)
	assert.Equal(t, 1, out[0].GroupSize)
}
