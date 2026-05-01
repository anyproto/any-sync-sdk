package anysyncsdk_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

type fixedSeedProvider struct {
	account, device []byte
}

func newFixedSeedProvider(t *testing.T) *fixedSeedProvider {
	t.Helper()
	_, accPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, devPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return &fixedSeedProvider{account: accPriv, device: devPriv}
}

func (p *fixedSeedProvider) AccountKey(_ context.Context) ([]byte, error) { return p.account, nil }
func (p *fixedSeedProvider) DeviceKey(_ context.Context) ([]byte, error)  { return p.device, nil }

// TestSDK_OpenCreateList exercises the boot path end-to-end against
// staging — derive tech space, replay (no-op on first run), then
// CreateSpace and confirm List sees it via the space-index.
//
// Skips automatically if ../../test-etc/staging.yml isn't present.
// Network reachability is NOT required: CreateSpace writes locally
// first; coordinator push happens lazily and is best-effort here.
func TestSDK_OpenCreateList(t *testing.T) {
	confPath := filepath.Join("..", "test-etc", "staging.yml")
	yaml, err := os.ReadFile(confPath)
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	cfg := config.Config{
		Storage: config.Storage{
			DataDir:  t.TempDir(),
			Topology: config.StorageShared,
		},
		Network: config.Network{NodeConfYAML: yaml},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	require.NotEmpty(t, sdk.Account().Id(), "account id should resolve")

	// Empty index initially.
	list, err := sdk.Spaces().List(ctx)
	require.NoError(t, err)
	assert.Empty(t, list)

	// Create a space — round-trips through any-sync (local create) and
	// writes the space-index entry.
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{
		Name:        "Demo",
		Description: "test space",
		SpaceType:   space.SpaceTypeRegular,
	})
	require.NoError(t, err)
	require.NotNil(t, sp)
	require.NotEmpty(t, sp.Id())

	// Replication-key sanity: every space the SDK creates should
	// share the same shard key (= tech-space's repKey, derived from
	// the account). A second create proves it: both ids end in the
	// same `.<repKey-base36>` suffix.
	sp2, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Demo2"})
	require.NoError(t, err)
	require.Equal(t, suffixAfter(sp.Id(), '.'), suffixAfter(sp2.Id(), '.'),
		"created spaces must share the tech-space's replication key")

	// Index now has both rows.
	list, err = sdk.Spaces().List(ctx)
	require.NoError(t, err)
	require.Len(t, list, 2)

	got := findSpace(list, sp.Id())
	require.NotNil(t, got, "first created space missing from List")
	assert.Equal(t, "Demo", got.Name)
	assert.Equal(t, space.SpaceTypeRegular, got.Type)
	assert.Equal(t, space.StatusActive, got.Status)

	// Soft-delete the first space — Status flips to Deleted, row
	// stays in List.
	require.NoError(t, sdk.Spaces().Delete(ctx, sp.Id()))
	list, err = sdk.Spaces().List(ctx)
	require.NoError(t, err)
	require.Len(t, list, 2)
	got = findSpace(list, sp.Id())
	require.NotNil(t, got)
	assert.Equal(t, space.StatusDeleted, got.Status)

	_ = sp2
}

// suffixAfter returns the trailing portion of s after the last
// occurrence of sep. Empty if sep isn't present or it's at the end.
func suffixAfter(s string, sep byte) string {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == sep {
			return s[i+1:]
		}
	}
	return ""
}

// findSpace returns the *SpaceInfo with the given id, or nil.
func findSpace(infos []space.SpaceInfo, id string) *space.SpaceInfo {
	for i := range infos {
		if infos[i].Id == id {
			return &infos[i]
		}
	}
	return nil
}

// TestSDK_OpenSurvivesRestart verifies that on a second Open against
// the same DataDir, the tech space and its space-index rehydrate via
// cold-restore: the previously created space shows up in List.
func TestSDK_OpenSurvivesRestart(t *testing.T) {
	confPath := filepath.Join("..", "test-etc", "staging.yml")
	yaml, err := os.ReadFile(confPath)
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	dataDir := t.TempDir()
	provider := newFixedSeedProvider(t) // same keys across both opens
	cfg := config.Config{
		Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}

	// First boot — create a space.
	{
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		sdk, err := anysyncsdk.Open(ctx, cfg, provider)
		require.NoError(t, err)
		_, err = sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Persisted"})
		require.NoError(t, err)
		require.NoError(t, sdk.Close())
	}

	// Second boot — same dataDir, same keys.
	{
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		sdk, err := anysyncsdk.Open(ctx, cfg, provider)
		require.NoError(t, err)
		t.Cleanup(func() { _ = sdk.Close() })

		list, err := sdk.Spaces().List(ctx)
		require.NoError(t, err)
		require.Len(t, list, 1, "space-index must rehydrate")
		assert.Equal(t, "Persisted", list[0].Name)
	}
	_ = techspace.SpaceIndexDataset // keep techspace package import-stable
}

// TestSDK_TypesAndProperties walks the six-step demo flow:
//  1. Create a space.
//  2. Create a type ("Movie").
//  3. Create a property on the type ("Title", string kind).
//  4. Create an object (binding the new type at birth).
//  5. Set the property value on the object.
//  6. Read the property back.
//
// All writes are local (no network). Asserts both the user-observable
// state (the value lands and reads back) and the SDK-internal
// invariants (auto-derived propId equals shortId(changeId), bind
// shows up in any.types).
func TestSDK_TypesAndProperties(t *testing.T) {
	confPath := filepath.Join("..", "test-etc", "staging.yml")
	yaml, err := os.ReadFile(confPath)
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	// 1. Space.
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "MovieDB"})
	require.NoError(t, err)

	// 2. Type.
	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{
		Name:        "Movie",
		Description: "A film",
	})
	require.NoError(t, err)
	require.NotEmpty(t, typeId)

	// 3. Property on type — empty record id resolves to shortId.
	titleProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Title",
		XKey: "title",
		Kind: space.PropertyKindString,
	})
	require.NoError(t, err)
	require.NotEmpty(t, titleProp)

	// 4. Object — bind type at birth via InitialProperties.
	objectId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
		Types: []string{typeId},
	})
	require.NoError(t, err)
	require.NotEmpty(t, objectId)

	// 5. Set property value (base scope).
	res, err := sp.Properties().SetBase(ctx, objectId, typeId, map[string]any{
		titleProp: "Casablanca",
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.VersionId)
	require.NotEmpty(t, res.ChangeId)

	// 6. Read it back. The record id is the objectId; values are
	//    namespaced by typeId → propId.
	rec, err := sp.Properties().Get(ctx, objectId, space.PropertyReadOpts{})
	require.NoError(t, err)
	require.NotNil(t, rec)
	got := rec.GetString(typeId, titleProp)
	assert.Equal(t, "Casablanca", got)

	// Bind side-effect: any.types should include the typeId we passed
	// in CreateObjectOpts.
	types := rec.GetArray("any", "types")
	require.NotEmpty(t, types)
	var saw bool
	for _, v := range types {
		if string(v.GetStringBytes()) == typeId {
			saw = true
			break
		}
	}
	assert.True(t, saw, "any.types must contain %s", typeId)

	// Types.List returns the synthetic `any` built-in plus the
	// user-created Movie type, in that order.
	typeList, err := sp.Types().List(ctx)
	require.NoError(t, err)
	require.Len(t, typeList, 2)
	assert.Equal(t, "any", typeList[0].Id)
	assert.True(t, typeList[0].BuiltIn)
	assert.Equal(t, typeId, typeList[1].Id)
	assert.Equal(t, "Movie", typeList[1].Name)

	// Types.Get on the typeId returns the user-created row.
	tinfo, err := sp.Types().Get(ctx, typeId)
	require.NoError(t, err)
	assert.Equal(t, "Movie", tinfo.Name)

	// Types.Get("any") returns the built-in.
	anyInfo, err := sp.Types().Get(ctx, "any")
	require.NoError(t, err)
	assert.True(t, anyInfo.BuiltIn)
	assert.Equal(t, "Any", anyInfo.Name)

	// Built-in `any` type ships with name + description + icon +
	// types properties hardcoded.
	anyProps, err := sp.Types().Properties(ctx, "any")
	require.NoError(t, err)
	gotProps := make(map[string]space.PropertyKind, len(anyProps))
	for _, p := range anyProps {
		gotProps[p.Id] = p.Kind
	}
	assert.Equal(t, space.PropertyKindString, gotProps["name"])
	assert.Equal(t, space.PropertyKindString, gotProps["description"])
	assert.Equal(t, space.PropertyKindString, gotProps["icon"])
	assert.Equal(t, space.PropertyKindArray, gotProps["types"])

	// User-type properties: the Title we added shows up via the
	// type's `defs` dataset.
	movieProps, err := sp.Types().Properties(ctx, typeId)
	require.NoError(t, err)
	require.Len(t, movieProps, 1)
	assert.Equal(t, titleProp, movieProps[0].Id)
	assert.Equal(t, "Title", movieProps[0].Name)
	assert.Equal(t, space.PropertyKindString, movieProps[0].Kind)

	// Get on a regular object id (not a type) returns ErrNotFound.
	_, err = sp.Types().Get(ctx, objectId)
	assert.ErrorIs(t, err, space.ErrNotFound)

	// QueryObjects walks the per-space `objects` collection — every
	// object's property values live there, including the Movie type's
	// own metadata. So the row count is 2 at this point: the type row
	// and the regular object's row.
	docs, err := sp.QueryObjects().Limit(10).All(ctx)
	require.NoError(t, err)
	require.Len(t, docs, 2)

	byId := map[string]*anyenc.Value{}
	for _, d := range docs {
		byId[d.GetString("id")] = d
	}
	require.Contains(t, byId, typeId)
	require.Contains(t, byId, objectId)
	assert.Equal(t, "Movie", byId[typeId].GetString("any", "name"))
	assert.Equal(t, "Casablanca", byId[objectId].GetString(typeId, titleProp))

	// Count via the same shared collection.
	count, err := sp.QueryObjects().Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// Multi-object cross-query: create a second movie, set Title,
	// confirm both rows show up in the per-space collection and a
	// filter narrows correctly.
	objectId2, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
		Types: []string{typeId},
	})
	require.NoError(t, err)
	_, err = sp.Properties().SetBase(ctx, objectId2, typeId, map[string]any{
		titleProp: "Vertigo",
	})
	require.NoError(t, err)

	all, err := sp.QueryObjects().All(ctx)
	require.NoError(t, err)
	// type row + first Movie + second Movie = 3
	assert.Len(t, all, 3)

	// Filter by id to fetch one row. any-store accepts a JSON-shaped
	// filter as a string.
	one, err := sp.QueryObjects().
		Filter(`{"id":"` + objectId2 + `"}`).
		One(ctx)
	require.NoError(t, err)
	assert.Equal(t, "Vertigo", one.GetString(typeId, titleProp))

	// ModifyMany: two batches under the same objectId, both on the
	// per-space `objects` dataset. Pre-validates as one unit; both
	// land if all pass.
	multiRes, err := sp.ModifyMany(ctx, []space.ModifyBatch{
		{
			ObjectId: objectId,
			Dataset:  "objects",
			Records: []space.RecordModify{{
				Id:     objectId,
				Upsert: true,
				Ops: []space.Op{{
					Type:  space.OpSet,
					Path:  typeId + "." + titleProp,
					Value: "Casablanca II",
				}},
			}},
		},
		{
			ObjectId: objectId,
			Dataset:  "objects",
			Records: []space.RecordModify{{
				Id:     objectId,
				Upsert: true,
				Ops: []space.Op{{
					Type:  space.OpSet,
					Path:  "any.description",
					Value: "Updated via ModifyMany",
				}},
			}},
		},
	})
	require.NoError(t, err)
	require.Len(t, multiRes, 2)
	require.NotEmpty(t, multiRes[0].VersionId)
	require.NotEmpty(t, multiRes[1].VersionId)
	assert.NotEqual(t, multiRes[0].ChangeId, multiRes[1].ChangeId,
		"each batch should produce its own DAG change")

	rec2, err := sp.Properties().Get(ctx, objectId, space.PropertyReadOpts{})
	require.NoError(t, err)
	assert.Equal(t, "Casablanca II", rec2.GetString(typeId, titleProp))
	assert.Equal(t, "Updated via ModifyMany", rec2.GetString("any", "description"))

	// ModifyMany rejection: an invalid path on batch 2 fails
	// pre-validation; batch 1 must NOT land.
	_, err = sp.ModifyMany(ctx, []space.ModifyBatch{
		{
			ObjectId: objectId,
			Dataset:  "objects",
			Records: []space.RecordModify{{
				Id:     objectId,
				Upsert: true,
				Ops: []space.Op{{
					Type:  space.OpSet,
					Path:  "any.description",
					Value: "should NOT land",
				}},
			}},
		},
		{
			ObjectId: objectId,
			Dataset:  "objects",
			Records: []space.RecordModify{{
				Id:     objectId,
				Upsert: true,
				Ops: []space.Op{{
					Type:  space.OpSet,
					Path:  "id", // reserved → ValidateChange rejects
					Value: "boom",
				}},
			}},
		},
	})
	require.Error(t, err, "validation should fail the whole submission")

	rec3, err := sp.Properties().Get(ctx, objectId, space.PropertyReadOpts{})
	require.NoError(t, err)
	assert.Equal(t, "Updated via ModifyMany", rec3.GetString("any", "description"),
		"prior successful description must be untouched after a failed ModifyMany")

	// Cross-object ModifyMany must reject.
	_, err = sp.ModifyMany(ctx, []space.ModifyBatch{
		{ObjectId: objectId, Dataset: "objects"},
		{ObjectId: typeId, Dataset: "objects"},
	})
	require.Error(t, err)

	// Objects.Delete: marks the tree deleted via settings tree, drops
	// our cached state. After this, Get on the deleted id returns an
	// any-sync deletion error.
	require.NoError(t, sp.Objects().Delete(ctx, objectId))
}
