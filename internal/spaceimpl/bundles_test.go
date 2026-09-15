package spaceimpl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
	collectiontype "github.com/anyproto/any-sync-sdk/internal/types/collection"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
	"github.com/anyproto/any-sync-sdk/space"
)

// The structural gate runs before any lock, derive or write, so it is
// exercised without a fixture; the install/adopt flows are covered by
// the bundles e2e tests.

func entriesDraft() space.DatasetDraft {
	return space.DatasetDraft{
		Key:    "entries",
		IdRule: space.IdUser,
		Fields: []space.DatasetFieldDraft{
			{Key: "title", Kind: space.PropertyKindString, Required: true},
			{Key: "creator", Stamp: space.StampCreator},
		},
	}
}

func entriesPart(datasets ...space.DatasetDraft) []space.PartDraft {
	if len(datasets) == 0 {
		datasets = []space.DatasetDraft{entriesDraft()}
	}
	return []space.PartDraft{{Key: "entries", Datasets: datasets}}
}

func TestValidateEnsureRequest(t *testing.T) {
	newRoot := func(context.Context) (string, error) { return "root", nil }
	badKey := entriesDraft()
	badKey.Key = "_private"
	shared := entriesDraft()
	shared.Shared = true
	cases := []struct {
		name string
		req  space.EnsureBundleRequest
		tech bool
		bad  bool
	}{
		{name: "created root", req: space.EnsureBundleRequest{Id: "b", NewRoot: newRoot}},
		{name: "derived root", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true}},
		{name: "derived with parts", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Parts: entriesPart()}},
		{name: "both strategies", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, NewRoot: newRoot}, bad: true},
		{name: "no strategy", req: space.EnsureBundleRequest{Id: "b"}, bad: true},
		{name: "created root with parts", req: space.EnsureBundleRequest{Id: "b", NewRoot: newRoot, Parts: entriesPart()}},
		{name: "sdk-minted created root", req: space.EnsureBundleRequest{Id: "b", Parts: entriesPart()}},
		{name: "created root with a root type", req: space.EnsureBundleRequest{Id: "b", NewRoot: newRoot, RootType: "t"}, bad: true},
		{name: "created root with root collections", req: space.EnsureBundleRequest{Id: "b", NewRoot: newRoot, RootCollections: []string{"c"}}, bad: true},
		{name: "declaring root with a root type", req: space.EnsureBundleRequest{Id: "b", Parts: entriesPart(), RootType: "t"}, bad: true},
		{name: "declaring collection with a root type", req: space.EnsureBundleRequest{Id: "b", Collection: true, XKey: "shelf", RootType: "t"}, bad: true},
		{name: "derived root with a root type", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, RootType: "t"}},
		{name: "derived root with root collections", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, RootCollections: []string{"c1", "c2"}}},
		{name: "declaring root with root collections", req: space.EnsureBundleRequest{Id: "b", Parts: entriesPart(), RootCollections: []string{"c1"}}},
		{name: "root type names the type marker", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, RootType: typetype.MetaTypeMarker}, bad: true},
		{name: "root type names the collection marker", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, RootType: collectiontype.MetaMarker}, bad: true},
		{name: "root collection with an empty id", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, RootCollections: []string{""}}, bad: true},
		{name: "root collection names a marker", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, RootCollections: []string{collectiontype.MetaMarker}}, bad: true},
		{name: "root collection names `any`", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, RootCollections: []string{anytype.TypeId}}, bad: true},
		{name: "sdk-minted created root with root properties", req: space.EnsureBundleRequest{Id: "b", XKey: "wiki", RootProperties: map[string]map[string]any{"t": {"a": 1}}}},
		{name: "xkey alone declares a marker type", req: space.EnsureBundleRequest{Id: "b", XKey: "flag"}},
		{name: "xkey alone with metadata", req: space.EnsureBundleRequest{Id: "b", XKey: "flag", Hidden: true}},
		{name: "xkey on a caller-minted root", req: space.EnsureBundleRequest{Id: "b", NewRoot: newRoot, XKey: "flag"}},
		{name: "collection declared by xkey", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Collection: true, XKey: "shelf"}},
		{name: "collection declared by properties", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Collection: true, Properties: []space.PropertyDraft{{XKey: "pos", Kind: space.PropertyKindString}}}},
		{name: "collection with parts", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Collection: true, XKey: "shelf", Parts: entriesPart()}, bad: true},
		{name: "collection with a layout", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Collection: true, XKey: "shelf", Layout: map[string]any{"type": "chat"}}, bad: true},
		{name: "collection metadata without a declaration", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Collection: true, Hidden: true}, bad: true},
		{name: "seeded value that cannot be encoded", req: space.EnsureBundleRequest{Id: "b", XKey: "flag", RootProperties: map[string]map[string]any{"t": {"a": make(chan int)}}}, bad: true},
		{name: "seeded value under an empty type id", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, RootProperties: map[string]map[string]any{"": {"a": 1}}}, bad: true},
		{name: "tech xkey only", req: space.EnsureBundleRequest{Id: "b", XKey: "flag"}, tech: true},
		{name: "metadata without a declaration", req: space.EnsureBundleRequest{Id: "b", NewRoot: newRoot, Hidden: true}, bad: true},
		{name: "layout without a declaration", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Layout: map[string]any{"type": "chat"}}, bad: true},
		{name: "invalid dataset key", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Parts: entriesPart(badKey)}, bad: true},
		{name: "invalid part key", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Parts: []space.PartDraft{{Key: "Bad Key"}}}, bad: true},
		{name: "duplicate dataset key", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Parts: entriesPart(entriesDraft(), entriesDraft())}, bad: true},
		{name: "duplicate part key", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Parts: []space.PartDraft{{Key: "a"}, {Key: "a"}}}, bad: true},
		{name: "shared records", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Parts: entriesPart(shared)}, bad: true},
		{name: "tech derived with parts", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Parts: entriesPart()}, tech: true},
		{name: "tech caller-created root", req: space.EnsureBundleRequest{Id: "b", NewRoot: newRoot, Parts: entriesPart()}, tech: true, bad: true},
		{name: "tech sdk-minted created root", req: space.EnsureBundleRequest{Id: "b", Parts: entriesPart()}, tech: true},
		{name: "tech collection", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Collection: true, XKey: "shelf"}, tech: true},
		{name: "tech without parts", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true}, tech: true, bad: true},
		{name: "tech with a root type", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Parts: entriesPart(), RootType: "t"}, tech: true, bad: true},
		{name: "tech with root collections", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Parts: entriesPart(), RootCollections: []string{"c"}}, tech: true, bad: true},
		{name: "tech with root properties", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Parts: entriesPart(), RootProperties: map[string]map[string]any{"t": {"a": 1}}}, tech: true, bad: true},
	}
	b := newBundlesAPI(&spaceImpl{})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := b.validateEnsureRequest(tc.req, false)
			if err == nil && tc.tech {
				err = validateTechEnsureRequest(tc.req)
			}
			if tc.bad {
				require.ErrorIs(t, err, space.ErrBundleBadRequest)
				return
			}
			require.NoError(t, err)
		})
	}
}

// Ensure refuses a bad request before touching the store: a zero
// spaceImpl would panic on any store access.
func TestEnsure_BadRequestBeforeStore(t *testing.T) {
	b := newBundlesAPI(&spaceImpl{})
	_, _, err := b.Ensure(context.Background(), space.EnsureBundleRequest{Id: "b"})
	require.ErrorIs(t, err, space.ErrBundleBadRequest)
	tb := techBundles{b}
	_, _, err = tb.Ensure(context.Background(), space.EnsureBundleRequest{Id: "b", DerivedRoot: true})
	require.ErrorIs(t, err, space.ErrBundleBadRequest)
	_, _, err = tb.Ensure(context.Background(), space.EnsureBundleRequest{
		Id: "b", NewRoot: func(context.Context) (string, error) { return "r", nil },
		Parts: entriesPart(),
	})
	require.ErrorIs(t, err, space.ErrBundleBadRequest, "NewRoot is refused on the tech space")
}

func TestInstallRootMembers(t *testing.T) {
	req := space.EnsureBundleRequest{
		RootType:        "t1",
		RootCollections: []string{"c2", "c1", "c2"},
		RootProperties:  map[string]map[string]any{"c3": {"a": 1}, "c1": {"b": 2}},
	}
	m := installRootMembers(req)
	assert.Equal(t, "t1", m.Type)
	assert.Equal(t, []string{"c2", "c1", "c3"}, m.Collections,
		"RootCollections in order, then the seeded owners sorted, deduped")

	// A declaring root has no type of its own: the slot holds its
	// marker, and its own id is never listed — it implements itself.
	decl := req
	decl.RootType = ""
	decl.Parts = entriesPart()
	m = installRootMembers(decl)
	assert.Equal(t, typetype.MetaTypeMarker, m.Type)
	assert.Equal(t, []string{"c2", "c1", "c3"}, m.Collections)

	coll := space.EnsureBundleRequest{Collection: true, XKey: "shelf", RootCollections: []string{"c1"}}
	m = installRootMembers(coll)
	assert.Equal(t, collectiontype.MetaMarker, m.Type)
	assert.Equal(t, []string{"c1"}, m.Collections)

	// A bare root is nothing at all.
	m = installRootMembers(space.EnsureBundleRequest{})
	assert.Empty(t, m.Type)
	assert.Nil(t, m.Collections)

	// `any` is universal — seeding its values files the root nowhere.
	m = installRootMembers(space.EnsureBundleRequest{
		RootProperties: map[string]map[string]any{anytype.TypeId: {"description": "seeded"}}})
	assert.Nil(t, m.Collections)
	m = installRootMembers(space.EnsureBundleRequest{RootCollections: []string{anytype.TypeId, "c1"}})
	assert.Equal(t, []string{"c1"}, m.Collections)

	// A seeded owner that is already the type is not filed again.
	m = installRootMembers(space.EnsureBundleRequest{
		RootType: "t1", RootProperties: map[string]map[string]any{"t1": {"a": 1}}})
	assert.Equal(t, "t1", m.Type)
	assert.Nil(t, m.Collections)
}

func TestDeclares(t *testing.T) {
	bare := space.EnsureBundleRequest{Id: "b"}
	assert.False(t, bare.DeclaresType())
	assert.False(t, bare.DeclaresCollection())
	assert.False(t, bare.Declares())

	assert.True(t, space.EnsureBundleRequest{Id: "b", XKey: "flag"}.DeclaresType(), "an xKey alone is a marker type")
	assert.True(t, space.EnsureBundleRequest{Id: "b", Parts: entriesPart()}.DeclaresType())
	props := []space.PropertyDraft{{XKey: "a", Kind: space.PropertyKindString}}
	assert.True(t, space.EnsureBundleRequest{Id: "b", Properties: props}.DeclaresType())

	// Collection routes the same declaration to the other slot.
	c := space.EnsureBundleRequest{Id: "b", Collection: true, Properties: props}
	assert.False(t, c.DeclaresType())
	assert.True(t, c.DeclaresCollection())
	assert.True(t, c.Declares())
	assert.True(t, space.EnsureBundleRequest{Id: "b", Collection: true, XKey: "shelf"}.DeclaresCollection())
	assert.False(t, space.EnsureBundleRequest{Id: "b", Collection: true}.Declares(), "the flag alone declares nothing")
}
