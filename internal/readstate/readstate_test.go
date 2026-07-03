package readstate

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

// fixture DAG (versionIds = v01..): read gaps are changes the engine
// never saw as rows (self-authored / untracked).
type fakeChange struct {
	prev []string
	v    string
}

type fixture struct {
	*Engine
	seq     uint64
	changes map[string]fakeChange // resolver backing: "all changes on device"
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "readstate.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	f := &fixture{changes: map[string]fakeChange{}, seq: 1000}
	f.Engine = New(db, "space1", func(context.Context) (uint64, error) {
		f.seq++
		return f.seq, nil
	}, func(_ context.Context, _, changeId string) ([]string, string, bool, error) {
		ch, ok := f.changes[changeId]
		if !ok {
			return nil, "", false, nil
		}
		return ch.prev, ch.v, true, nil
	})
	return f
}

func (f *fixture) track(t *testing.T, tr Track) {
	t.Helper()
	f.changes[tr.ChangeId] = fakeChange{prev: tr.PrevIds, v: tr.VersionId}
	require.NoError(t, f.TrackChange(ctx, tr))
}

func mkTrack(obj, ch, v string, prev []string, tags ...string) Track {
	return Track{
		ObjectId: obj, ChangeId: ch, VersionId: v, PrevIds: prev,
		AddSeq: 1, ApplySeq: 1, RecordIds: []string{"rec-" + ch}, Tags: tags,
		Tracked: true,
	}
}

func TestTrack_InsertCountersTransitions(t *testing.T) {
	f := newFixture(t)
	tr := mkTrack("obj", "c1", "v01", nil, "message", "mention")
	tr.ApplySeq = 7
	f.track(t, tr)

	entries, _, err := f.UnreadEntries(ctx, "obj")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "c1", entries[0].ChangeId)
	assert.Equal(t, []string{"rec-c1"}, entries[0].RecordIds)

	counts, err := f.Counts(ctx, "obj")
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"message": 1, "mention": 1}, counts)

	trs, err := f.TransitionsSince(ctx, 0, 0)
	require.NoError(t, err)
	require.Len(t, trs, 1)
	assert.True(t, trs[0].Unread)
	assert.Equal(t, uint64(7), trs[0].StateSeq)
}

func TestTrack_SelfAuthoredAdvancesFrontier(t *testing.T) {
	f := newFixture(t)
	self := mkTrack("obj", "c1", "v01", nil, "message")
	self.SelfAuthored = true
	f.track(t, self)

	entries, _, err := f.UnreadEntries(ctx, "obj")
	require.NoError(t, err)
	assert.Empty(t, entries)
	heads, _, err := f.Frontier(ctx, "obj")
	require.NoError(t, err)
	assert.Equal(t, []string{"c1"}, heads)

	// Next self change replaces its prev in the frontier.
	self2 := mkTrack("obj", "c2", "v02", []string{"c1"}, "message")
	self2.SelfAuthored = true
	f.track(t, self2)
	heads, _, err = f.Frontier(ctx, "obj")
	require.NoError(t, err)
	assert.Equal(t, []string{"c2"}, heads)
}

func TestMarkRead_ClosureWithBranchAndGap(t *testing.T) {
	f := newFixture(t)
	// DAG: c1 <- c2(unread) <- gap(self) <- c4(unread)
	//        \<- b1(unread, concurrent branch off c1)
	f.track(t, mkTrack("obj", "c1", "v01", nil, "message"))
	f.track(t, mkTrack("obj", "c2", "v02", []string{"c1"}, "message"))
	self := mkTrack("obj", "gap", "v03", []string{"c2"}, "message")
	self.SelfAuthored = true
	f.track(t, self)
	f.track(t, mkTrack("obj", "c4", "v04", []string{"gap"}, "message"))
	f.track(t, mkTrack("obj", "b1", "v05", []string{"c1"}, "message"))

	// Mark c4: closure crosses the self-authored gap down to c2, c1.
	// Branch b1 stays unread.
	res, err := f.MarkRead(ctx, "obj", []string{"c4"})
	require.NoError(t, err)
	removedIds := entryIds(res.Removed)
	assert.ElementsMatch(t, []string{"c4", "c2", "c1"}, removedIds)

	entries, _, err := f.UnreadEntries(ctx, "obj")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "b1", entries[0].ChangeId)

	counts, err := f.Counts(ctx, "obj")
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"message": 1}, counts)

	// Now mark the branch too.
	res, err = f.MarkRead(ctx, "obj", []string{"b1"})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"b1"}, entryIds(res.Removed))
	counts, err = f.Counts(ctx, "obj")
	require.NoError(t, err)
	assert.Empty(t, counts)
}

func TestMarkReadUpTo_RangeAndReadAll(t *testing.T) {
	f := newFixture(t)
	for i := 1; i <= 5; i++ {
		prev := []string{fmt.Sprintf("c%d", i-1)}
		if i == 1 {
			prev = nil
		}
		f.track(t, mkTrack("obj", fmt.Sprintf("c%d", i), fmt.Sprintf("v%02d", i), prev, "message"))
	}
	res, err := f.MarkReadUpTo(ctx, "obj", "v03")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"c1", "c2", "c3"}, entryIds(res.Removed))
	assert.Equal(t, []string{"c3"}, res.Frontier)

	counts, err := f.Counts(ctx, "obj")
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"message": 2}, counts)

	// "" = read all.
	res, err = f.MarkReadUpTo(ctx, "obj", "")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"c4", "c5"}, entryIds(res.Removed))
	assert.Equal(t, []string{"c5"}, res.Frontier)
	counts, err = f.Counts(ctx, "obj")
	require.NoError(t, err)
	assert.Empty(t, counts)
}

func TestTrack_CoveredByFrontierBornRead(t *testing.T) {
	f := newFixture(t)
	f.track(t, mkTrack("obj", "c1", "v01", nil, "message"))
	_, err := f.MarkRead(ctx, "obj", []string{"c1"})
	require.NoError(t, err)

	// Replay of c1 (e.g. rebuild) must not resurrect an unread row.
	f.track(t, mkTrack("obj", "c1", "v01", nil, "message"))
	entries, _, err := f.UnreadEntries(ctx, "obj")
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestMarkRead_UnknownIdParksPendingAndResolves(t *testing.T) {
	f := newFixture(t)
	f.track(t, mkTrack("obj", "c1", "v01", nil, "message"))

	// Another device published head c2 we haven't synced yet.
	res, err := f.MarkRead(ctx, "obj", []string{"c2"})
	require.NoError(t, err)
	assert.Equal(t, []string{"c2"}, res.Pending)

	_, pending, err := f.Frontier(ctx, "obj")
	require.NoError(t, err)
	assert.Equal(t, []string{"c2"}, pending)

	// c2 arrives: it resolves the pending head and covers c1.
	f.track(t, mkTrack("obj", "c2", "v02", []string{"c1"}, "message"))
	entries, _, err := f.UnreadEntries(ctx, "obj")
	require.NoError(t, err)
	assert.Empty(t, entries)
	heads, pending, err := f.Frontier(ctx, "obj")
	require.NoError(t, err)
	assert.Empty(t, pending)
	assert.Equal(t, []string{"c2"}, heads)
	counts, err := f.Counts(ctx, "obj")
	require.NoError(t, err)
	assert.Empty(t, counts)
}

func TestTrack_SupersedeKey(t *testing.T) {
	f := newFixture(t)
	react := mkTrack("obj", "r1", "v01", nil, "reaction")
	react.Key = "reaction:+1:alice:msg1"
	f.track(t, react)

	counts, err := f.Counts(ctx, "obj")
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"reaction": 1}, counts)

	// Un-react: untracked change with the same key clears the entry.
	unreact := mkTrack("obj", "r2", "v02", []string{"r1"})
	unreact.Key = "reaction:+1:alice:msg1"
	unreact.Tracked = false
	f.track(t, unreact)

	entries, _, err := f.UnreadEntries(ctx, "obj")
	require.NoError(t, err)
	assert.Empty(t, entries)
	counts, err = f.Counts(ctx, "obj")
	require.NoError(t, err)
	assert.Empty(t, counts)
}

func TestClearRecords_DeleteDropsAndSheds(t *testing.T) {
	f := newFixture(t)
	solo := mkTrack("obj", "c1", "v01", nil, "message")
	solo.RecordIds = []string{"m1"}
	f.track(t, solo)
	multi := mkTrack("obj", "c2", "v02", []string{"c1"}, "message")
	multi.RecordIds = []string{"m1", "m2"}
	f.track(t, multi)

	require.NoError(t, f.ClearRecords(ctx, "obj", []string{"m1"}, 0))

	entries, _, err := f.UnreadEntries(ctx, "obj")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "c2", entries[0].ChangeId)
	assert.Equal(t, []string{"m2"}, entries[0].RecordIds)

	counts, err := f.Counts(ctx, "obj")
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"message": 1}, counts)

	trs, err := f.TransitionsSince(ctx, 1000, 0)
	require.NoError(t, err)
	// c1's became-read transition surfaced through the feed.
	var readTrs []Transition
	for _, tr := range trs {
		if !tr.Unread {
			readTrs = append(readTrs, tr)
		}
	}
	require.Len(t, readTrs, 1)
	assert.Equal(t, "c1", readTrs[0].ChangeId)
}

func TestTransitionsSince_CursorPaging(t *testing.T) {
	f := newFixture(t)
	for i := 1; i <= 3; i++ {
		tr := mkTrack("obj", fmt.Sprintf("c%d", i), fmt.Sprintf("v%02d", i), nil, "message")
		tr.ApplySeq = uint64(i * 10)
		f.track(t, tr)
	}
	trs, err := f.TransitionsSince(ctx, 10, 0)
	require.NoError(t, err)
	require.Len(t, trs, 2)
	assert.Equal(t, uint64(20), trs[0].StateSeq)
	assert.Equal(t, uint64(30), trs[1].StateSeq)

	trs, err = f.TransitionsSince(ctx, 10, 1)
	require.NoError(t, err)
	require.Len(t, trs, 1)
}

func entryIds(entries []Entry) []string {
	out := make([]string, 0, len(entries))
	for _, en := range entries {
		out = append(out, en.ChangeId)
	}
	return out
}
