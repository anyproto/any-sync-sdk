package anysyncsdk

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/space"
)

// A property removed from a collection after objects were created with
// a value for it must not cost those objects their create on replay:
// the history view at the create version (a fresh replay through the
// current schema) keeps the type, the collections, the name and every
// still-declared value, and sheds only the removed property.
func TestSDK_CreateReplayShedsRemovedProperty(t *testing.T) {
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	sdk, err := Open(ctx, wmConfig(t, dataDir), wmProvider(t, dataDir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Shed"})
	require.NoError(t, err)

	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Task"})
	require.NoError(t, err)
	titleId, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{Name: "Title", Kind: space.PropertyKindString})
	require.NoError(t, err)
	collId, err := sp.Collections().Create(ctx, space.CollectionCreateParams{Name: "Linear"})
	require.NoError(t, err)
	stateId, err := sp.Collections().AddProperty(ctx, collId, space.PropertyDraft{Name: "State", Kind: space.PropertyKindString})
	require.NoError(t, err)
	goneId, err := sp.Collections().AddProperty(ctx, collId, space.PropertyDraft{Name: "Priority", Kind: space.PropertyKindNumber})
	require.NoError(t, err)

	objectId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
		Type:        typeId,
		Collections: []string{collId},
		InitialProperties: map[string]map[string]any{
			"any":  {"name": "Issue"},
			typeId: {titleId: "t"},
			collId: {stateId: "todo", goneId: 3},
		},
	})
	require.NoError(t, err)

	require.NoError(t, sp.Collections().RemoveProperty(ctx, collId, goneId))
	// An edit after the removal: head differs from the create, and the
	// value it carries is the only one the two views disagree on.
	_, err = sp.Properties().Set(ctx, objectId, collId, map[string]any{stateId: "done"})
	require.NoError(t, err)

	list, err := sp.History().ListChanges(ctx, objectId, space.HistoryFilter{}, 50, "")
	require.NoError(t, err)
	require.Len(t, list.Changes, 2)
	create := list.Changes[1].Version
	head := list.Changes[0].Version

	for version, state := range map[space.Version]string{create: "todo", head: "done"} {
		view, err := sp.History().ViewAt(ctx, objectId, version)
		require.NoError(t, err)
		rec, err := view.Record(ctx, properties.Dataset, objectId)
		require.NoError(t, err)
		require.NotNil(t, rec, "row present at %s", version)
		assert.Equal(t, typeId, rec.GetString("any", "type"), "type kept at %s", version)
		require.NotNil(t, rec.Get("any", "collections"), "collections kept at %s", version)
		assert.Equal(t, "Issue", rec.GetString("any", "name"), "name kept at %s", version)
		assert.Equal(t, "t", rec.GetString(typeId, titleId), "type value kept at %s", version)
		assert.Equal(t, state, rec.GetString(collId, stateId), "declared collection value kept at %s", version)
		assert.Nil(t, rec.Get(collId, goneId), "removed property shed at %s", version)
		require.NoError(t, view.Close())
	}
}
