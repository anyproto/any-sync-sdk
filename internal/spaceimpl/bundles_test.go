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
		Name:   "entries",
		IdRule: space.IdUser,
		Fields: []space.DatasetFieldDraft{
			{Key: "title", Kind: space.PropertyKindString, Required: true},
			{Key: "creator", Stamp: space.StampCreator},
		},
	}
}

func TestValidateEnsureRequest(t *testing.T) {
	newRoot := func(context.Context) (string, error) { return "root", nil }
	cases := []struct {
		name string
		req  space.EnsureBundleRequest
		tech bool
		bad  bool
	}{
		{name: "created root", req: space.EnsureBundleRequest{Id: "b", NewRoot: newRoot}},
		{name: "derived root", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true}},
		{name: "derived with datasets", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Datasets: []space.DatasetDraft{entriesDraft()}}},
		{name: "both strategies", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, NewRoot: newRoot}, bad: true},
		{name: "no strategy", req: space.EnsureBundleRequest{Id: "b"}, bad: true},
		{name: "datasets on created root", req: space.EnsureBundleRequest{Id: "b", NewRoot: newRoot, Datasets: []space.DatasetDraft{entriesDraft()}}, bad: true},
		{name: "invalid draft", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Datasets: []space.DatasetDraft{{Name: "_private"}}}, bad: true},
		{name: "duplicate name", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Datasets: []space.DatasetDraft{entriesDraft(), entriesDraft()}}, bad: true},
		{name: "tech derived with datasets", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Datasets: []space.DatasetDraft{entriesDraft()}}, tech: true},
		{name: "tech created root", req: space.EnsureBundleRequest{Id: "b", NewRoot: newRoot}, tech: true, bad: true},
		{name: "tech without datasets", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true}, tech: true, bad: true},
		{name: "tech with root types", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Datasets: []space.DatasetDraft{entriesDraft()}, RootTypes: []string{"t"}}, tech: true, bad: true},
		{name: "tech with root properties", req: space.EnsureBundleRequest{Id: "b", DerivedRoot: true, Datasets: []space.DatasetDraft{entriesDraft()}, RootProperties: map[string]map[string]any{"t": {"a": 1}}}, tech: true, bad: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateEnsureRequest(tc.req)
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
	require.ErrorIs(t, tb.ResolveLoser(context.Background(), "b", "r"), space.ErrUnsupported)
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
