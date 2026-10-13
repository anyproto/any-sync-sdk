package spaceimpl

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
)

// ReleaseTree builds no store for a space that has none: there is
// nothing to release. The Service here has no app, so a store build
// would panic.
func TestReleaseTreeBuildsNoStore(t *testing.T) {
	s := &Service{stores: map[string]*spaceobjects.Store{}}
	s.ReleaseTree("space1", "tree1")
	require.Empty(t, s.stores)
}
