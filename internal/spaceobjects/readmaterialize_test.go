package spaceobjects

import (
	"context"
	"errors"
	"fmt"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/readstate"
)

func TestDesiredFlagRecords(t *testing.T) {
	flags := map[string]string{"message": "unread", "mention": "unreadMention"}
	entries := []readstate.Entry{
		{Dataset: "chat", Tags: []string{"message"}, RecordIds: []string{"m1"}},
		{Dataset: "chat", Tags: []string{"message", "mention"}, RecordIds: []string{"m2"}},
		{Dataset: "other", Tags: []string{"message"}, RecordIds: []string{"x1"}},
		{Dataset: "chat", Tags: []string{"reaction"}, RecordIds: []string{"m1"}}, // undeclared tag
	}
	got := desiredFlagRecords(entries, "chat", flags)
	assert.Equal(t, map[string]struct{}{"m1": {}, "m2": {}}, got["unread"])
	assert.Equal(t, map[string]struct{}{"m2": {}}, got["unreadMention"])
}

func TestDesiredFlagRecords_EmptyEntriesStillClears(t *testing.T) {
	got := desiredFlagRecords(nil, "chat", map[string]string{"message": "unread"})
	require.Contains(t, got, "unread")
	assert.Empty(t, got["unread"])
}

func TestFlagFlipRecords(t *testing.T) {
	desired := map[string]struct{}{"a": {}, "b": {}}
	current := map[string]struct{}{"b": {}, "c": {}}
	recs := flagFlipRecords("unread", desired, current)
	require.Len(t, recs, 2)

	byId := map[string]crdt.Op{}
	for _, r := range recs {
		require.Len(t, r.Ops, 1)
		byId[r.Id] = r.Ops[0]
	}
	assert.Equal(t, crdt.OpSet, byId["a"].Type)   // newly unread
	assert.Equal(t, crdt.OpUnset, byId["c"].Type) // no longer unread
	assert.NotContains(t, byId, "b")              // unchanged

	assert.Empty(t, flagFlipRecords("unread", desired, desired))
}

// A pass that meets its Object closed runs once more; any other error,
// or a second close, ends it.
func TestProcessRetry(t *testing.T) {
	boom := errors.New("boom")
	for _, tc := range []struct {
		name  string
		errs  []error
		calls int
		want  error
	}{
		{"closed object", []error{fmt.Errorf("set: %w", object.ErrClosed), nil}, 2, nil},
		{"closed collection", []error{anystore.ErrCollectionClosed, nil}, 2, nil},
		{"closed twice", []error{object.ErrClosed, object.ErrClosed}, 2, object.ErrClosed},
		{"other error", []error{boom}, 1, boom},
		{"no error", []error{nil}, 1, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			err := processRetry(t.Context(), "o1", func(_ context.Context, id string) error {
				require.Equal(t, "o1", id)
				calls++
				return tc.errs[calls-1]
			})
			require.Equal(t, tc.calls, calls)
			if tc.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.want)
			}
		})
	}
}
