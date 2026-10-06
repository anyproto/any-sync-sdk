package spaceimpl

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
)

// PullFirst names a regular space's spaceIndex tree and nothing for
// the tech space. It builds no store: a sync round calls it while the
// space may be mid-offload, where a rebuilt store would never close.
// The Service here has no app, so a store build would panic.
func TestPullFirstIsSpaceIndexAndBuildsNoStore(t *testing.T) {
	s := &Service{tsp: &techspace.Service{}, stores: map[string]*spaceobjects.Store{}}

	want, err := spaceIndexTreeId("space1")
	require.NoError(t, err)
	require.Equal(t, []string{want}, s.PullFirst("space1"))
	require.Empty(t, s.stores)

	other, err := spaceIndexTreeId("space2")
	require.NoError(t, err)
	require.NotEqual(t, want, other, "the id is per space")

	require.Nil(t, s.PullFirst(s.tsp.SpaceId()), "the tech space has no spaceIndex")
}
