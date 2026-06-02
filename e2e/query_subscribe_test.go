package e2e

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/cheggaaa/mb/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestSDK_QuerySubscribe exercises the live-query Subscribe surface:
//
//   - Snapshot returns Initial + Total (with IncludeTotal).
//   - Subscribe returns Initial AND a live QuerySubscription.
//   - Writes after Subscribe emit SubscriptionEvent batches with the
//     post-apply doc and atomic ops.
//   - Limit+1 sentinel: a new arrival at the top of the sort emits an
//     Added (the new record) AND a Removed (the previous bottom-visible
//     row, demoted to sentinel) — no any-store query fires.
//   - Sub.Close() releases the mailbox cleanly (Err() == nil).
func TestSDK_QuerySubscribe(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Q"})
	require.NoError(t, err)

	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Movie"})
	require.NoError(t, err)
	titleProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Title", XKey: "title", Kind: space.PropertyKindString,
	})
	require.NoError(t, err)
	yearProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Year", XKey: "year", Kind: space.PropertyKindNumber,
	})
	require.NoError(t, err)

	// Seed two movies BEFORE subscribing — they show up in Initial.
	idA, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	_, err = sp.Properties().SetBase(ctx, idA, typeId, map[string]any{
		titleProp: "Aliens", yearProp: 1986,
	})
	require.NoError(t, err)

	idB, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	_, err = sp.Properties().SetBase(ctx, idB, typeId, map[string]any{
		titleProp: "Brazil", yearProp: 1985,
	})
	require.NoError(t, err)

	// Snapshot (no subscription) returns Initial + Total when requested.
	snap, err := sp.QueryObjects().
		Filter(map[string]any{typeId + "." + yearProp: map[string]any{"$gte": 1980}}).
		Sort(typeId+"."+yearProp).
		Snapshot(ctx, space.QueryOpts{IncludeTotal: true})
	require.NoError(t, err)
	assert.Nil(t, snap.Sub, "Snapshot must not return a live Sub")
	assert.GreaterOrEqual(t, snap.Total, 2, "Total should count both seeded movies")
	assert.GreaterOrEqual(t, len(snap.Initial), 2, "Initial should include both seeded movies")

	// A limited Snapshot must still report the unbounded total — Total is
	// independent of limit/offset.
	yearFilter := fmt.Sprintf(`{%q:{"$gte":1980}}`, typeId+"."+yearProp)
	limited, err := sp.QueryObjects().
		Filter(yearFilter).
		Sort(typeId+"."+yearProp).
		Limit(1).
		Snapshot(ctx, space.QueryOpts{IncludeTotal: true})
	require.NoError(t, err)
	assert.Len(t, limited.Initial, 1, "Initial must be capped at Limit")
	assert.Equal(t, 2, limited.Total, "Total must ignore Limit and count all matches")
	assert.True(t, limited.HasNext, "HasNext must be true: offset+len(Initial)=1 < Total=2")

	// The full (unbounded) Snapshot reaches the end → HasNext false.
	assert.False(t, snap.HasNext, "HasNext must be false when the page covers all matches")

	// A limit larger than the match count: the page is short (2 < 5), so
	// Total is the full match count and HasNext is false.
	short, err := sp.QueryObjects().
		Filter(yearFilter).
		Sort(typeId+"."+yearProp).
		Limit(5).
		Snapshot(ctx, space.QueryOpts{IncludeTotal: true})
	require.NoError(t, err)
	assert.Len(t, short.Initial, 2, "Initial holds all 2 matches (below the limit)")
	assert.Equal(t, 2, short.Total, "Total derived from the short page")
	assert.False(t, short.HasNext, "HasNext false: the short page reached the end")

	// Subscribe with limit=2 — the snapshot holds limit+1 internally.
	// Add a third movie that lands at the *top* of the sort (most recent
	// year) and verify a sentinel demotion: the new row arrives Added,
	// the previous bottom-visible row arrives Removed (now sentinel).
	res, err := sp.QueryObjects().
		Filter(map[string]any{typeId + "." + yearProp: map[string]any{"$gte": 1980}}).
		Sort("-"+typeId+"."+yearProp).
		Limit(2).
		Subscribe(ctx, space.QueryOpts{IncludeTotal: true})
	require.NoError(t, err)
	require.NotNil(t, res.Sub)
	t.Cleanup(func() { _ = res.Sub.Close() })
	assert.GreaterOrEqual(t, res.Total, 2)
	assert.LessOrEqual(t, len(res.Initial), 2, "Initial must be capped at Limit")

	// Add a third movie: 2024 — newer than both. With "-year" sort:
	// new arrival has the smallest tuple (sorted first), so it enters
	// visible at the top; the prior visible-bottom (Brazil 1985) gets
	// demoted to sentinel.
	idC, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	_, err = sp.Properties().SetBase(ctx, idC, typeId, map[string]any{
		titleProp: "Dune Part Two", yearProp: 2024,
	})
	require.NoError(t, err)

	// Drain events until we observe the Add(idC). We may receive a
	// preliminary event for "Create" before the SetBase lands, depending
	// on dispatch ordering — gather everything within a tight window.
	added, _ := drainSubEvents(t, res.Sub, 2*time.Second)
	require.NotEmpty(t, added, "expected at least one Added record from the new movie")
	require.Contains(t, addedIds(added), idC, "new movie id must appear in Added across the batch")

	// Closing the sub stops delivery without setting Err().
	require.NoError(t, res.Sub.Close())
	assert.NoError(t, res.Sub.Err(), "user-initiated Close must leave Err() nil")

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer closeCancel()
	_, err = res.Sub.Events().Wait(closeCtx)
	assert.True(t, errors.Is(err, mb.ErrClosed))
}

// drainSubEvents pulls events from sub until either window expires or
// we've collected an Added record. Returns the aggregated Added /
// Removed sets across the batch.
func drainSubEvents(t *testing.T, sub space.QuerySubscription, window time.Duration) ([]space.SubRecord, []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()
	var addedAll []space.SubRecord
	var removedAll []string
	for {
		evs, err := sub.Events().Wait(ctx)
		if err != nil {
			return addedAll, removedAll
		}
		for _, ev := range evs {
			addedAll = append(addedAll, ev.Added...)
			for _, r := range ev.Removed {
				removedAll = append(removedAll, r.Id)
			}
		}
		if len(addedAll) > 0 {
			return addedAll, removedAll
		}
	}
}

func addedIds(rs []space.SubRecord) []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.Id)
	}
	return out
}

// TestSDK_QuerySubscribe_ResubscribeAfterClose asserts the basic
// recovery contract: after the caller closes a Sub explicitly, a
// fresh Subscribe returns a snapshot reflecting every record that's
// still in the database — no data is lost, no records leak across
// sub instances.
func TestSDK_QuerySubscribe_ResubscribeAfterClose(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Resub"})
	require.NoError(t, err)
	typeId, titleProp := setupMovieType(t, ctx, sp)
	seedMovies(t, ctx, sp, typeId, titleProp, []string{"Aliens", "Brazil", "Casablanca"})

	// Subscribe, then close immediately.
	res1, err := sp.QueryObjects().
		Filter(map[string]any{"any.types": map[string]any{"$in": []any{typeId}}}).
		Snapshot(ctx, space.QueryOpts{IncludeTotal: true})
	require.NoError(t, err)
	beforeIds := idsOfInitial(res1.Initial)
	require.Len(t, beforeIds, 3)

	res2, err := sp.QueryObjects().
		Filter(map[string]any{"any.types": map[string]any{"$in": []any{typeId}}}).
		Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	require.NotNil(t, res2.Sub)
	subIds := idsOfInitial(res2.Initial)
	assert.ElementsMatch(t, beforeIds, subIds, "Subscribe.Initial must match Snapshot.Initial against the same DB state")

	require.NoError(t, res2.Sub.Close())
	assert.NoError(t, res2.Sub.Err())

	// Resubscribe — same data, same ids.
	res3, err := sp.QueryObjects().
		Filter(map[string]any{"any.types": map[string]any{"$in": []any{typeId}}}).
		Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = res3.Sub.Close() })
	assert.ElementsMatch(t, beforeIds, idsOfInitial(res3.Initial),
		"resubscribed snapshot must match the original snapshot — data must persist across sub lifetimes")
}

// TestSDK_QuerySubscribe_RestoreAfterRestart shuts the SDK down and
// reopens against the same DataDir + keys, then subscribes. The
// resubscribed snapshot must reflect every record written before
// shutdown — cold-restore through any-store, no events replayed.
func TestSDK_QuerySubscribe_RestoreAfterRestart(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	dataDir := t.TempDir()
	provider := newFixedSeedProvider(t)
	cfg := config.Config{
		Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}

	var typeId, titleProp string
	var wroteTitles []string

	// First boot — write data, then close.
	{
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		sdk, err := anysyncsdk.Open(ctx, cfg, provider)
		require.NoError(t, err)
		sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Restored"})
		require.NoError(t, err)
		typeId, titleProp = setupMovieType(t, ctx, sp)
		wroteTitles = []string{"Aliens", "Brazil", "Casablanca", "Dune"}
		seedMovies(t, ctx, sp, typeId, titleProp, wroteTitles)
		require.NoError(t, sdk.Close())
	}

	// Second boot — same DataDir, same keys. Subscribe and verify
	// every previously-written record is back.
	{
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		sdk, err := anysyncsdk.Open(ctx, cfg, provider)
		require.NoError(t, err)
		t.Cleanup(func() { _ = sdk.Close() })

		list, err := sdk.Spaces().List(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, list, "space index must rehydrate after restart")

		sp, err := sdk.Spaces().Get(ctx, list[0].Id)
		require.NoError(t, err)

		res, err := sp.QueryObjects().
			Filter(map[string]any{"any.types": map[string]any{"$in": []any{typeId}}}).
			Sort(typeId+"."+titleProp).
			Subscribe(ctx, space.QueryOpts{IncludeTotal: true})
		require.NoError(t, err)
		t.Cleanup(func() { _ = res.Sub.Close() })

		assert.Equal(t, len(wroteTitles), res.Total, "Total after restart must match the count of records written before shutdown")
		assert.Equal(t, len(wroteTitles), len(res.Initial), "Initial after restart must include every persisted record")
		gotTitles := extractTitles(t, res.Initial, typeId, titleProp)
		assert.ElementsMatch(t, wroteTitles, gotTitles, "Initial must reflect the exact set written before restart")
	}
}

// TestSDK_QuerySubscribe_DriftCloseAndResubscribe drives the held
// window past its drift budget by deleting most of its records,
// confirms the sub closes with ErrSubscriptionDrifted, and verifies
// that a follow-up Subscribe rebuilds a correct snapshot — the
// resubscribe path is the engine's primary recovery contract.
func TestSDK_QuerySubscribe_DriftCloseAndResubscribe(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Drift"})
	require.NoError(t, err)
	typeId, titleProp := setupMovieType(t, ctx, sp)

	// Seed 10 movies. Limit=10 → drift budget defaults to 30% → 3 losses
	// closes the sub. We'll delete 5 to be safely over the threshold.
	titles := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	ids := seedMovies(t, ctx, sp, typeId, titleProp, titles)
	require.Len(t, ids, 10)

	res, err := sp.QueryObjects().
		Filter(map[string]any{"any.types": map[string]any{"$in": []any{typeId}}}).
		Sort(typeId+"."+titleProp).
		Limit(10).
		Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	assert.Len(t, res.Initial, 10)

	// Delete the first 5 — well past 30% of limit.
	for _, id := range ids[:5] {
		require.NoError(t, sp.Objects().Delete(ctx, id))
	}

	// Drain whatever events arrived before the close. The mailbox
	// eventually closes; subsequent Wait returns mb.ErrClosed.
	closedCtx, closedCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer closedCancel()
	for {
		if _, err := res.Sub.Events().Wait(closedCtx); err != nil {
			assert.True(t, errors.Is(err, mb.ErrClosed), "expected mailbox closed, got %v", err)
			break
		}
	}
	assert.True(t, errors.Is(res.Sub.Err(), space.ErrSubscriptionDrifted),
		"sub must close with ErrSubscriptionDrifted after losing >30%% of limit; got %v", res.Sub.Err())

	// Resubscribe — fresh snapshot of the *current* state.
	res2, err := sp.QueryObjects().
		Filter(map[string]any{"any.types": map[string]any{"$in": []any{typeId}}}).
		Sort(typeId+"."+titleProp).
		Limit(10).
		Subscribe(ctx, space.QueryOpts{IncludeTotal: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = res2.Sub.Close() })

	assert.Equal(t, 5, res2.Total, "remaining-movies count must match (10 created - 5 deleted)")
	remainingTitles := titles[5:]
	assert.ElementsMatch(t, remainingTitles, extractTitles(t, res2.Initial, typeId, titleProp),
		"resubscribe must show only the still-alive records")
}

// setupMovieType creates a "Movie" type with a Title property and
// returns (typeId, titleProp).
func setupMovieType(t *testing.T, ctx context.Context, sp space.Space) (string, string) {
	t.Helper()
	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Movie"})
	require.NoError(t, err)
	titleProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Title", XKey: "title", Kind: space.PropertyKindString,
	})
	require.NoError(t, err)
	return typeId, titleProp
}

// seedMovies writes one Movie per title with titleProp set; returns
// the created object ids in order.
func seedMovies(t *testing.T, ctx context.Context, sp space.Space, typeId, titleProp string, titles []string) []string {
	t.Helper()
	ids := make([]string, 0, len(titles))
	for _, title := range titles {
		id, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
		require.NoError(t, err)
		_, err = sp.Properties().SetBase(ctx, id, typeId, map[string]any{titleProp: title})
		require.NoError(t, err)
		ids = append(ids, id)
	}
	return ids
}

// idsOfInitial pulls the `id` field from each row of a QueryResult.Initial.
func idsOfInitial(rows []*anyenc.Value) []string {
	out := make([]string, 0, len(rows))
	for _, v := range rows {
		out = append(out, string(v.GetStringBytes("id")))
	}
	return out
}

// extractTitles reads the title property off each Initial row at the
// canonical path `<typeId>.<titleProp>`. Returns the values in the
// order Initial provides them.
func extractTitles(t *testing.T, rows []*anyenc.Value, typeId, titleProp string) []string {
	t.Helper()
	out := make([]string, 0, len(rows))
	for _, v := range rows {
		titleVal := v.Get(typeId, titleProp)
		require.NotNil(t, titleVal, "row missing title at %s.%s: %s", typeId, titleProp, v.String())
		out = append(out, string(titleVal.GetStringBytes()))
	}
	return out
}
