package spaceobjects

import (
	"context"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/payloads"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
)

// A system dataset rides the regular store path as an ungated
// built-in: registered on every controller next to payloads/bundles,
// listed by Schemas() without a type, stamped with its hardcoded
// DataVersion, owner-less (membership check is a no-op), and its
// local handler version survives into the controller's reindex set.
func TestStore_SystemDatasets(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "sys.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	store := NewStoreWithConfig(StoreConfig{
		DB:      db,
		SpaceId: "tech",
		SystemDatasets: []SystemDataset{{
			Reg:         crdt.HandlerReg{Name: "sysA", Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}, Version: 2},
			DataVersion: "sysA-v1",
		}},
		DisableHistory: true,
	})
	t.Cleanup(func() { _ = store.Close() })

	regs, shared, err := store.buildRegs()
	require.NoError(t, err)
	assert.Equal(t, []string{properties.Dataset}, shared, "objects stays the only shared collection")
	names := make([]string, 0, len(regs))
	for _, r := range regs {
		names = append(names, r.Name)
	}
	assert.Equal(t, []string{
		properties.Dataset, typetype.DatasetPropertyDefs, typetype.ShortIdsDataset, typetype.DatasetDefs,
		payloads.Dataset, spaceindex.BundlesDataset, "sysA",
	}, names, "system reg follows the shared built-ins")
	for _, r := range regs {
		if r.Name == "sysA" {
			assert.Equal(t, 2, r.Version, "local handler version preserved")
		}
	}

	var listed bool
	for _, ns := range store.Schemas() {
		if ns.Name == "sysA" {
			listed = true
			assert.Empty(t, ns.Owners, "system datasets are type-less")
			assert.True(t, ns.Schema.Dynamic)
		}
	}
	assert.True(t, listed, "Schemas() lists the system dataset")

	dv, err := store.DataVersion("sysA")
	require.NoError(t, err)
	assert.Equal(t, "sysA-v1", dv)

	_, owned := store.DatasetOwners("sysA")
	assert.False(t, owned, "no owner: membership check stays a no-op")

	assert.NotNil(t, store.Registry(), "regular path: registry present")
	_, err = store.HistoryIndex(ctx)
	require.ErrorIs(t, err, ErrHistoryUnavailable)
	assert.Nil(t, store.historyApplyHook())
}
