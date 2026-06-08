package e2e

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_TechSpaceGenericQuery exercises the unified query surface over
// the tech space: clients read the space list through the same
// Query(objectId, dataset) mechanism regular spaces use, keyed by
// SpaceIndexObjectId(), with filters — no bespoke List call.
func TestE2E_TechSpaceGenericQuery(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("tech-space generic-query e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	provider := newFixedSeedProvider(t)
	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	sdk, err := anysyncsdk.Open(ctx, cfg, provider)
	require.NoError(t, err, "Open")
	t.Cleanup(func() { _ = sdk.Close() })

	svc := sdk.Spaces()
	a, err := svc.Create(ctx, space.CreateRequest{Name: "QAlpha"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create QAlpha: %v", err)
	}
	b, err := svc.Create(ctx, space.CreateRequest{Name: "QBeta"})
	require.NoError(t, err, "Create QBeta")

	indexId := svc.SpaceIndexObjectId()
	require.NotEmpty(t, indexId, "SpaceIndexObjectId")

	// Generic query over the spaces dataset returns both rows.
	docs, err := svc.Query(indexId, "spaces").All(ctx)
	require.NoError(t, err, "Query(spaces).All")
	ids := map[string]bool{}
	for _, d := range docs {
		ids[d.GetString("id")] = true
	}
	require.True(t, ids[a.Id()], "query should return QAlpha row")
	require.True(t, ids[b.Id()], "query should return QBeta row")

	// Filter narrows to one space by its name field.
	filtered, err := svc.Query(indexId, "spaces").
		Filter(map[string]any{"name": "QAlpha"}).All(ctx)
	require.NoError(t, err, "Query(spaces).Filter.All")
	require.Len(t, filtered, 1, "filter by name=QAlpha")
	require.Equal(t, a.Id(), filtered[0].GetString("id"))

	// Account-wide delete writes the synced remoteStatus=deleted, visible
	// through the same query surface.
	require.NoError(t, svc.Delete(ctx, b.Id()), "Delete QBeta")
	require.True(t, waitFor(ctx, 10*time.Second, 100*time.Millisecond, func() bool {
		got, qerr := svc.Query(indexId, "spaces").
			Filter(map[string]any{"id": b.Id()}).One(ctx)
		return qerr == nil && got != nil && got.GetString("remoteStatus") == "deleted"
	}), "deleted space should show remoteStatus=deleted via query")

	// Discovery: the tech-space `spaces` dataset is exposed as JSON Schema
	// with per-field classes (x-scope). localStatus is device-local.
	spacesSchema := findDataset(t, svc.Datasets(), "spaces")
	assert.Equal(t, "local", xScope(t, spacesSchema, "localStatus"))
	assert.Equal(t, "synced", xScope(t, spacesSchema, "remoteStatus"))

	// A regular space exposes its datasets too; `objects` is dynamic.
	sp, err := svc.Get(ctx, a.Id())
	require.NoError(t, err)
	objs := findDataset(t, sp.Datasets(), "objects")
	var doc map[string]any
	require.NoError(t, json.Unmarshal(objs.JSONSchema, &doc))
	assert.Equal(t, true, doc["additionalProperties"], "objects dataset is dynamic")
}

func findDataset(t *testing.T, dss []space.DatasetSchema, name string) space.DatasetSchema {
	t.Helper()
	for _, ds := range dss {
		if ds.Name == name {
			return ds
		}
	}
	t.Fatalf("dataset %q not found in discovery (%d datasets)", name, len(dss))
	return space.DatasetSchema{}
}

func xScope(t *testing.T, ds space.DatasetSchema, field string) string {
	t.Helper()
	var doc struct {
		Properties map[string]struct {
			XScope string `json:"x-scope"`
		} `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(ds.JSONSchema, &doc))
	return doc.Properties[field].XScope
}
