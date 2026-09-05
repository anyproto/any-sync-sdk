package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/space"
)

// secretModule is a reserved module: shared-only, declarable at runtime
// only by the consumer's own install (EnsureBundleRequest.SystemInstall).
func secretModule() handler.Module {
	return handler.Module{
		Name:        "secret",
		Canonical:   "secret_shared",
		SharedOnly:  true,
		Reserved:    true,
		DataVersion: "secret-v1",
		New: func(handler.ModuleInstance) handler.Dataset {
			return handler.Dataset{Schema: handler.Schema{Dynamic: true}}
		},
	}
}

// docType is a registered, hidden type with static parts: a body that
// shares the notes module and carries a namespaced notes instance, and
// a hidden part owning a static records dataset.
func docType() handler.Type {
	return handler.Type{
		Id: "doc", Name: "Document", Hidden: true,
		Datasets: []handler.Dataset{{
			Name: "doc_meta", DataVersion: "doc_meta-v1",
			Schema: handler.Schema{Fields: []handler.Field{
				{Id: "label", Schema: handler.Leaf(handler.PropertyKindString), MutableBy: handler.MutableByAnyone},
			}},
		}},
		Parts: []handler.Part{
			{Key: "body", Name: "Body", Pos: "a0", UI: map[string]any{"type": "document"},
				Datasets: []handler.PartDataset{{Module: "notes", Shared: true}, {Module: "notes", Key: "summary"}}},
			{Key: "meta", Hidden: true, Datasets: []handler.PartDataset{{Name: "doc_meta"}}},
		},
	}
}

// TestE2E_TypeParts_RegisteredStaticAndReserved covers the static
// side of parts on one device: a registered type's parts read back
// compiled, its module declarations own collections exactly like a
// runtime declaration (write gate, discovery, namespace), the type is
// hidden and immutable, and a reserved module is refused to runtime
// declarations and bundles alike — unless the bundle is the consumer's
// own install.
func TestE2E_TypeParts_RegisteredStaticAndReserved(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Types:   []handler.Type{docType()},
		Modules: []handler.Module{notesModule(), secretModule()},
	}, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "StaticParts"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatal(err)
	}

	// Hidden, registered, parts compiled from the declaration.
	info, err := sp.Types().Get(ctx, "doc")
	require.NoError(t, err)
	assert.True(t, info.Hidden)
	assert.True(t, info.BuiltIn)
	parts, err := sp.Types().Parts(ctx, "doc")
	require.NoError(t, err)
	require.Len(t, parts, 2)
	assert.Equal(t, "body", parts[0].Key)
	assert.Equal(t, "body", parts[0].Id)
	assert.Equal(t, map[string]any{"type": "document"}, parts[0].UI)
	require.Len(t, parts[0].Datasets, 2)
	assert.Equal(t, "notes_shared", parts[0].Datasets[0].Collection)
	assert.True(t, parts[0].Datasets[0].Shared)
	assert.Equal(t, "doc_summary", parts[0].Datasets[1].Collection)
	assert.Equal(t, "notes", parts[0].Datasets[1].Module)
	assert.True(t, parts[1].Hidden)
	require.Len(t, parts[1].Datasets, 1)
	assert.Equal(t, "doc_meta", parts[1].Datasets[0].Collection)
	assert.Equal(t, space.RecordsModule, parts[1].Datasets[0].Module)
	require.Len(t, parts[1].Datasets[0].Fields, 1)
	assert.Equal(t, "label", parts[1].Datasets[0].Fields[0].Key)
	defs, err := sp.Types().Datasets(ctx, "doc")
	require.NoError(t, err)
	assert.Len(t, defs, 3)
	_, err = sp.Types().AddPart(ctx, "doc", space.PartDraft{Key: "x", Datasets: []space.DatasetDraft{{Module: "notes", Key: "y"}}})
	require.ErrorIs(t, err, space.ErrTypeRegistered)

	// Discovery: the static declarations own their collections.
	seen := map[string]space.DatasetSchema{}
	for _, ds := range sp.Datasets() {
		seen[ds.Name] = ds
	}
	assert.Equal(t, []string{"doc"}, seen["notes_shared"].Owners)
	assert.Equal(t, []string{"doc"}, seen["doc_summary"].Owners)
	assert.Equal(t, "notes", seen["doc_summary"].Module)
	assert.Equal(t, []string{"doc"}, seen["doc_meta"].Owners)

	// The write gate: an object carrying doc holds all three; a bare
	// object none. The module namespace opens with the declaration.
	obj, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{"doc"}})
	require.NoError(t, err)
	write := func(objectId, dataset, field, value string) error {
		res, err := sp.Modify(ctx, space.ModifyBatch{
			ObjectId: objectId, Dataset: dataset,
			Records: []space.RecordModify{{Upsert: true, Ops: []space.Op{{Type: space.OpSet, Path: field, Value: value}}}},
		})
		if err != nil {
			return err
		}
		if len(res.Rejections) > 0 {
			return res.Rejections[0].ReasonErr
		}
		return nil
	}
	require.NoError(t, write(obj, "notes_shared", "text", "body"))
	require.NoError(t, write(obj, "doc_summary", "text", "tl;dr"))
	require.NoError(t, write(obj, "doc_meta", "label", "draft"))
	rows, err := sp.Query(obj, "doc_summary").All(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	_, err = sp.Properties().Set(ctx, obj, "notes", map[string]any{"pinned": true})
	require.NoError(t, err, "the module namespace is granted through the static declaration")
	bare, err := sp.Objects().Create(ctx, space.CreateObjectOpts{})
	require.NoError(t, err)
	require.ErrorIs(t, write(bare, "notes_shared", "text", "nope"), space.ErrDatasetNotDeclared)
	require.ErrorIs(t, write(bare, "doc_summary", "text", "nope"), space.ErrDatasetNotDeclared)
	require.ErrorIs(t, write(bare, "doc_meta", "label", "nope"), space.ErrDatasetNotDeclared)

	// Reserved module: refused on a user type's part, on a dataset
	// added to an existing part, and on a bundle — unless the bundle
	// is the consumer's own install.
	userType, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Room"})
	require.NoError(t, err)
	_, err = sp.Types().AddPart(ctx, userType, space.PartDraft{Key: "chat", Datasets: []space.DatasetDraft{{Module: "secret", Shared: true}}})
	require.ErrorIs(t, err, space.ErrModuleReserved)
	partId, err := sp.Types().AddPart(ctx, userType, space.PartDraft{Key: "notes", Datasets: []space.DatasetDraft{{Module: "notes", Shared: true}}})
	require.NoError(t, err)
	_, err = sp.Types().AddDataset(ctx, userType, partId, space.DatasetDraft{Module: "secret", Shared: true})
	require.ErrorIs(t, err, space.ErrModuleReserved)
	secretPart := []space.PartDraft{{Key: "chat", Datasets: []space.DatasetDraft{{Module: "secret", Shared: true}}}}
	_, _, err = sp.Bundles().Ensure(ctx, space.EnsureBundleRequest{Id: "room/v1", DerivedRoot: true, Parts: secretPart})
	require.ErrorIs(t, err, space.ErrModuleReserved)
	require.ErrorIs(t, err, space.ErrBundleBadRequest)
	_, err = sp.Bundles().Get(ctx, "room/v1")
	require.ErrorIs(t, err, space.ErrBundleUnknown, "a refused install leaves nothing behind")
	inst, installed, err := sp.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "room/v1", DerivedRoot: true, Parts: secretPart, Hidden: true, SystemInstall: true,
	})
	require.NoError(t, err, "the consumer's own install may declare the reserved module")
	require.True(t, installed)
	seen = map[string]space.DatasetSchema{}
	for _, ds := range sp.Datasets() {
		seen[ds.Name] = ds
	}
	assert.Equal(t, []string{inst.RootId}, seen["secret_shared"].Owners)
	require.NoError(t, write(inst.RootId, "secret_shared", "text", "hush"))
	root, err := sp.Types().Get(ctx, inst.RootId)
	require.NoError(t, err)
	assert.True(t, root.Hidden)
}
