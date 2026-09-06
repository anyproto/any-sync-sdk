package spaceimpl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
		{name: "created root with root types", req: space.EnsureBundleRequest{Id: "b", NewRoot: newRoot, RootTypes: []string{"t"}}, bad: true},
		{name: "sdk-minted created root with root types", req: space.EnsureBundleRequest{Id: "b", Parts: entriesPart(), RootTypes: []string{"t"}}},
		{name: "sdk-minted created root with root properties", req: space.EnsureBundleRequest{Id: "b", XKey: "wiki", RootProperties: map[string]map[string]any{"t": {"a": 1}}}},
		{name: "xkey alone declares a marker type", req: space.EnsureBundleRequest{Id: "b", XKey: "flag"}},
		{name: "xkey alone with metadata", req: space.EnsureBundleRequest{Id: "b", XKey: "flag", Hidden: true, Weight: 3}},
		{name: "xkey on a caller-minted root", req: space.EnsureBundleRequest{Id: "b", NewRoot: newRoot, XKey: "flag"}},
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
		{name: "tech without parts", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true}, tech: true, bad: true},
		{name: "tech with root types", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Parts: entriesPart(), RootTypes: []string{"t"}}, tech: true, bad: true},
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

func TestInstallRootTypes(t *testing.T) {
	req := space.EnsureBundleRequest{
		RootTypes:      []string{"t2", "t1", "t2"},
		RootProperties: map[string]map[string]any{"t3": {"a": 1}, "t1": {"b": 2}},
	}
	assert.Equal(t, []string{"t2", "t1", "t3"}, installRootTypes(req, ""))
	assert.Equal(t, []string{typetype.MetaTypeMarker, "root", "t2", "t1", "t3"}, installRootTypes(req, "root"),
		"self type: marker and own id first, then the union")
	assert.Equal(t, []string{typetype.MetaTypeMarker, "root"}, installRootTypes(space.EnsureBundleRequest{}, "root"))
	assert.Nil(t, installRootTypes(space.EnsureBundleRequest{}, ""))
	dup := space.EnsureBundleRequest{RootTypes: []string{"root", typetype.MetaTypeMarker}}
	assert.Equal(t, []string{typetype.MetaTypeMarker, "root"}, installRootTypes(dup, "root"), "requested types dedup against the self type")
	universal := space.EnsureBundleRequest{RootProperties: map[string]map[string]any{"any": {"description": "seeded"}}}
	assert.Equal(t, []string{typetype.MetaTypeMarker, "root"}, installRootTypes(universal, "root"),
		"`any` is universal — seeding its values attaches nothing")
	assert.Equal(t, []string{"t1"}, installRootTypes(space.EnsureBundleRequest{RootTypes: []string{"any", "t1"}}, ""),
		"`any` requested as a root type is dropped on every path")
	assert.Nil(t, installRootTypes(space.EnsureBundleRequest{RootTypes: []string{"any"}}, ""))
}

func TestDeclaresType(t *testing.T) {
	assert.False(t, space.EnsureBundleRequest{Id: "b"}.DeclaresType())
	assert.True(t, space.EnsureBundleRequest{Id: "b", XKey: "flag"}.DeclaresType(), "an xKey alone is a marker type")
	assert.True(t, space.EnsureBundleRequest{Id: "b", Parts: entriesPart()}.DeclaresType())
	assert.True(t, space.EnsureBundleRequest{Id: "b", Properties: []space.PropertyDraft{{XKey: "a", Kind: space.PropertyKindString}}}.DeclaresType())
}
