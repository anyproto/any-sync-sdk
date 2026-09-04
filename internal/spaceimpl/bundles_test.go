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
			err := b.validateEnsureRequest(tc.req)
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

func TestDerivedRootTypes(t *testing.T) {
	req := space.EnsureBundleRequest{
		RootTypes:      []string{"t2", "t1", "t2"},
		RootProperties: map[string]map[string]any{"t3": {"a": 1}, "t1": {"b": 2}},
	}
	assert.Equal(t, []string{"t2", "t1", "t3"}, derivedRootTypes(req, ""))
	assert.Equal(t, []string{typetype.MetaTypeMarker, "root", "t2", "t1", "t3"}, derivedRootTypes(req, "root"),
		"self type: marker and own id first, then the union")
	assert.Equal(t, []string{typetype.MetaTypeMarker, "root"}, derivedRootTypes(space.EnsureBundleRequest{}, "root"))
	assert.Nil(t, derivedRootTypes(space.EnsureBundleRequest{}, ""))
	dup := space.EnsureBundleRequest{RootTypes: []string{"root", typetype.MetaTypeMarker}}
	assert.Equal(t, []string{typetype.MetaTypeMarker, "root"}, derivedRootTypes(dup, "root"), "requested types dedup against the self type")
}
