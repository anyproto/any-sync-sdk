package spaceobjects

import (
	"context"

	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/history"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// Raw-mode stores (tech space) carry internal bookkeeping only: no
// history surface, no history hook, no index rows. Regular stores get
// the hook and a working index.
func TestHistoryHookDisabledForRawMode(t *testing.T) {
	raw := &Store{customHandlers: []crdt.HandlerReg{
		{Name: "spaces", Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}},
	}}
	assert.Nil(t, raw.historyApplyHook(), "raw mode must not index history")
	_, err := raw.HistoryIndex(context.Background())
	require.ErrorIs(t, err, ErrHistoryUnavailable)

	regular := &Store{}
	assert.NotNil(t, regular.historyApplyHook())
}

// The apply hook must never open the index (that would nest collection
// DDL inside the apply tx): with no index published it defers the
// object to the pending-stale set, and the next successful
// HistoryIndex open flushes that set via MarkStale.
func TestHistoryHookDefersUntilIndexOpen(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, ":memory:", &anystore.Config{InMemory: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	s := &Store{db: db, spaceId: "sp-test"}
	hook := s.historyApplyHook()
	require.NotNil(t, hook)

	ch := &crdt.Change{
		SpaceId:   "sp-test",
		ObjectId:  "obj-x",
		Dataset:   "notes",
		ChangeId:  "cid-1",
		VersionId: "o1",
		Records:   []crdt.RecordChange{{Id: "r1", Upsert: true, Ops: []crdt.Op{{Type: crdt.OpSet}}}},
	}
	// Index not open yet: hook defers, writes nothing, no error.
	require.NoError(t, hook(ctx, ch, []string{"r1"}, nil))
	_, deferred := s.historyPendingStale.Load("obj-x")
	assert.True(t, deferred, "object queued for stale-marking while index unavailable")
	assert.Nil(t, s.historyIx.Load())

	// First out-of-tx open flushes the pending set.
	ix, err := s.HistoryIndex(ctx)
	require.NoError(t, err)
	stale, err := ix.IsStale(ctx, "obj-x")
	require.NoError(t, err)
	assert.True(t, stale, "pending object marked stale on open")
	_, still := s.historyPendingStale.Load("obj-x")
	assert.False(t, still, "pending entry consumed")

	// With the index open, the hook writes rows directly.
	ch2 := &crdt.Change{
		SpaceId:   "sp-test",
		ObjectId:  "obj-y",
		Dataset:   "notes",
		ChangeId:  "cid-2",
		VersionId: "o2",
		Records:   []crdt.RecordChange{{Id: "r1", Upsert: true, Ops: []crdt.Op{{Type: crdt.OpSet}}}},
	}
	require.NoError(t, hook(ctx, ch2, []string{"r1"}, nil))
	out, _, err := ix.ListChanges(ctx, history.Filter{ObjectId: "obj-y"}, 10, "")
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, "cid-2", out[0].Version)
}

// A transient open failure must not disable the index permanently: the
// error is returned but not cached, and a later call succeeds.
func TestHistoryIndexOpenRetries(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, ":memory:", &anystore.Config{InMemory: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	s := &Store{db: db, spaceId: "sp-test"}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.HistoryIndex(cancelled); err == nil {
		// Some anystore versions tolerate a cancelled ctx on collection
		// open; the retry contract is only observable when it fails.
		t.Skip("open with cancelled ctx did not fail; retry contract not exercisable")
	}
	require.Nil(t, s.historyIx.Load(), "failed open must not publish an index")

	ix, err := s.HistoryIndex(ctx)
	require.NoError(t, err, "second open must retry, not return a cached error")
	require.NotNil(t, ix)
}
