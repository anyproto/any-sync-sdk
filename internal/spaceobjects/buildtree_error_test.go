package spaceobjects

import (
	"errors"
	"fmt"
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// A tree-open failure for an unknown or already-deleted tree joins
// space.ErrObjectNotFound so consumers match one SDK sentinel instead
// of any-sync's storage errors, while the underlying error stays in
// the chain for the internal callers that branch on it.
func TestTreeOpenError(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		notFound bool
	}{
		{"unknown tree", treestorage.ErrUnknownTreeId, true},
		{"deleted tree", spacestorage.ErrTreeStorageAlreadyDeleted, true},
		{"wrapped unknown tree", fmt.Errorf("get: %w", treestorage.ErrUnknownTreeId), true},
		{"unrelated", errors.New("disk on fire"), false},
	} {
		// Both tree-open routes classify identically: BuildTree reports a
		// missing or deleted tree, PutTree a deleted one.
		for _, op := range []string{"BuildTree", "PutTree"} {
			t.Run(op+"/"+tc.name, func(t *testing.T) {
				got := treeOpenError(op, "objA", tc.err)
				require.ErrorIs(t, got, tc.err)
				require.Contains(t, got.Error(), "objA")
				require.Contains(t, got.Error(), op)
				if tc.notFound {
					require.ErrorIs(t, got, space.ErrObjectNotFound)
				} else {
					require.NotErrorIs(t, got, space.ErrObjectNotFound)
				}
			})
		}
	}
}
