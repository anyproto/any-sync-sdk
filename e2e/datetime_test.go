package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestSDK_DatetimeProperties: date properties and derived stamps are
// stored as any-store's native dateTime, which is what makes them
// orderable, index-keyable and computable by the date operators. Before
// this, `date` meant an ISO-8601 string and the stamps meant epoch
// numbers, and every date operator returned null against both.
func TestSDK_DatetimeProperties(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Dates"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Spaces().Create: %v", err)
	}
	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Task"})
	require.NoError(t, err)

	dueProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name:    "Due",
		Kind:    space.PropertyKindDatetime,
		XFormat: map[string]any{"type": "datetime"},
	})
	require.NoError(t, err)

	defs, err := sp.Types().Properties(ctx, typeId)
	require.NoError(t, err)
	var dueKind space.PropertyKind
	for _, d := range defs {
		if d.Id == dueProp {
			dueKind = d.Kind
		}
	}
	assert.Equal(t, space.PropertyKindDatetime, dueKind)

	seed := func(due time.Time) string {
		t.Helper()
		id, cerr := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
		require.NoError(t, cerr)
		_, serr := sp.Properties().Set(ctx, id, typeId, map[string]any{dueProp: due})
		require.NoError(t, serr, "a time.Time writes as a dateTime value")
		return id
	}
	early := time.Date(2026, 3, 1, 9, 30, 0, 0, time.UTC)
	late := time.Date(2026, 11, 17, 18, 0, 0, 0, time.UTC)
	earlyId := seed(early)
	lateId := seed(late)

	duePath := typeId + "." + dueProp

	// Read-back keeps the type — not a string, not an epoch number.
	row, err := sp.QueryObjects().Filter(map[string]any{"id": earlyId}).One(ctx)
	require.NoError(t, err)
	leaf := row.Get(typeId, dueProp)
	require.NotNil(t, leaf)
	require.Equal(t, anyenc.TypeDateTime, leaf.Type())
	gotMs, err := leaf.DateTimeMillis()
	require.NoError(t, err)
	assert.Equal(t, early.UnixMilli(), gotMs)

	// Ordering is chronological, which a string column only gets by
	// accident of ISO-8601 and an epoch column loses across peers.
	rows, err := sp.QueryObjects().
		Filter(map[string]any{duePath: map[string]any{"$exists": true}}).
		Sort("-" + duePath).All(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, lateId, string(rows[0].GetStringBytes("id")), "newest first")

	// The point of the exercise: date operators compute on real data.
	byYear := `[
		{"$match": {"` + duePath + `": {"$exists": true}}},
		{"$group": {"_id": {"$year": "$` + duePath + `"}, "count": {"$count": {}}}}
	]`
	agg, err := sp.AggregateObjects(byYear).All(ctx)
	require.NoError(t, err)
	require.Len(t, agg, 1, "both tasks fall in the same year")
	assert.EqualValues(t, 2026, agg[0].GetInt("id"))
	assert.EqualValues(t, 2, agg[0].GetInt("count"))

	// Derived stamps are dateTime too.
	createdAt := row.Get("createdAt")
	require.NotNil(t, createdAt)
	assert.Equal(t, anyenc.TypeDateTime, createdAt.Type(), "createdAt is an instant, not an epoch number")
	modifiedAt := row.Get("modifiedAt")
	require.NotNil(t, modifiedAt)
	assert.Equal(t, anyenc.TypeDateTime, modifiedAt.Type())
}
