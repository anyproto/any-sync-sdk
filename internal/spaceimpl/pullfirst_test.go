package spaceimpl

import (
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
)

// PullFirst names a regular space's spaceIndex tree and nothing for
// the tech space. It builds no store: a sync round calls it while the
// space may be mid-offload, where a rebuilt store would never close.
// The Service here has no app, so a store build would panic.
func TestPullFirstIsSpaceIndexAndBuildsNoStore(t *testing.T) {
	s := &Service{tsp: &techspace.Service{}, stores: map[string]*spaceobjects.Store{}}

	want, err := spaceIndexTreeId("space1")
	require.NoError(t, err)
	require.Equal(t, []string{want}, s.PullFirst(t.Context(), "space1"))
	require.Empty(t, s.stores)

	other, err := spaceIndexTreeId("space2")
	require.NoError(t, err)
	require.NotEqual(t, want, other, "the id is per space")

	require.Nil(t, s.PullFirst(t.Context(), s.tsp.SpaceId()), "the tech space has no spaceIndex")
}

// After the spaceIndex, PullFirst names the definitions the local rows
// know of: every root the spaceIndex's bundle rows name, then the live
// type and collection objects. It reads rows
// only — a space with no rows yet, a joiner's, gets the spaceIndex
// alone, and no collection is created for the asking.
func TestPullFirstNamesLocalDefinitions(t *testing.T) {
	ctx := t.Context()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "sdk.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	s := &Service{tsp: &techspace.Service{}, db: db, stores: map[string]*spaceobjects.Store{}}

	const spaceId = "space1"
	indexId, err := spaceIndexTreeId(spaceId)
	require.NoError(t, err)

	require.Equal(t, []string{indexId}, s.PullFirst(ctx, spaceId))
	names, err := db.GetCollectionNames(ctx)
	require.NoError(t, err)
	require.Empty(t, names, "asking creates no collection")

	row := func(coll anystore.Collection, id string, set func(a *anyenc.Arena, doc *anyenc.Value)) {
		t.Helper()
		a := &anyenc.Arena{}
		doc := a.NewObject()
		doc.Set("id", a.NewString(id))
		set(a, doc)
		require.NoError(t, coll.UpsertOne(ctx, doc))
	}
	anyType := func(marker string) func(a *anyenc.Arena, doc *anyenc.Value) {
		return func(a *anyenc.Arena, doc *anyenc.Value) {
			ns := a.NewObject()
			ns.Set("type", a.NewString(marker))
			doc.Set("any", ns)
		}
	}
	deleted := func(set func(a *anyenc.Arena, doc *anyenc.Value)) func(a *anyenc.Arena, doc *anyenc.Value) {
		return func(a *anyenc.Arena, doc *anyenc.Value) {
			set(a, doc)
			doc.Set("_deletedAt", a.NewNumberInt(1))
		}
	}

	bundles := mustColl(t, ctx, db, indexId+"_"+spaceindex.BundlesDataset)
	row(bundles, "wiki/v1", func(a *anyenc.Arena, doc *anyenc.Value) {
		// The register names one root; the claimed set also holds the
		// one the read path may prefer.
		doc.Set(spaceindex.FieldBundleRootId, a.NewString("root-wiki"))
		roots := a.NewArray()
		roots.SetArrayItem(0, a.NewString("root-wiki"))
		roots.SetArrayItem(1, a.NewString("root-wiki-canonical"))
		doc.Set(spaceindex.FieldBundleRoots, roots)
	})
	row(bundles, "empty/v1", func(*anyenc.Arena, *anyenc.Value) {})
	row(bundles, "gone/v1", deleted(func(a *anyenc.Arena, doc *anyenc.Value) {
		doc.Set(spaceindex.FieldBundleRootId, a.NewString("root-gone"))
	}))

	objects := mustColl(t, ctx, db, spaceId+"_"+spaceobjects.SpaceObjectsCollection)
	row(objects, "type-a", anyType("__type__"))
	row(objects, "type-dead", deleted(anyType("__type__")))
	row(objects, "coll-a", anyType("__collection__"))
	row(objects, "obj-1", anyType("type-a"))

	// Another space's rows stay its own.
	row(mustColl(t, ctx, db, "space2_"+spaceobjects.SpaceObjectsCollection), "type-b", anyType("__type__"))

	require.Equal(t, []string{indexId, "root-wiki", "root-wiki", "root-wiki-canonical", "type-a", "coll-a"}, s.PullFirst(ctx, spaceId))
	require.Empty(t, s.stores)
}

// Types and collections are fetched first in a regular space; the tech
// space holds neither.
func TestPullFirstTypes(t *testing.T) {
	s := &Service{tsp: &techspace.Service{}}
	require.ElementsMatch(t, []string{"type", "collection"}, s.PullFirstTypes("space1"))
	require.Nil(t, s.PullFirstTypes(s.tsp.SpaceId()))
}
