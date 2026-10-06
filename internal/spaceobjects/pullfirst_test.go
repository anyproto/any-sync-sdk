package spaceobjects

import (
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
)

// PullFirst names the spaceIndex tree: the root the space-metadata
// watcher reads, derived from the space id alone.
func TestPullFirstIsSpaceIndex(t *testing.T) {
	root, err := objecttree.DeriveObjectTreeRoot(objecttree.ObjectTreeDerivePayload{
		ChangePayload: []byte(spaceindex.WellKnownDeriveSeed),
		SpaceId:       "space",
		IsEncrypted:   true,
	}, nil)
	require.NoError(t, err)

	s := &Store{spaceId: "space"}
	s.spaceIndexObjId = s.deriveSpaceIndexId()
	require.Equal(t, []string{root.Id}, s.PullFirst())

	require.Nil(t, (&Store{spaceId: "space"}).PullFirst(), "no derived id, nothing to pull first")
}
