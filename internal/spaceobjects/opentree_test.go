package spaceobjects

import (
	"errors"
	"fmt"
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/stretchr/testify/require"
)

// openedTree is an ObjectTree that is only ever compared by identity.
type openedTree struct{ objecttree.ObjectTree }

func TestBuildTreeOrStored(t *testing.T) {
	// The two shapes any-sync reports a lost storage race in: the bare
	// sentinel from the stored-root pre-check, and the wrapped one from
	// the insert inside the deferred-creation transaction.
	bare := treestorage.ErrTreeExists
	wrapped := fmt.Errorf("init storage: %w", fmt.Errorf("create storage: %w", treestorage.ErrTreeExists))
	deleted := spacestorage.ErrTreeStorageAlreadyDeleted
	other := errors.New("peer unreachable")

	cases := []struct {
		name      string
		errs      []error // per call; a missing entry is success
		wantCalls int
		wantErr   error
	}{
		{name: "opens on the first call", wantCalls: 1},
		{name: "retries the bare sentinel", errs: []error{bare}, wantCalls: 2},
		{name: "retries the wrapped sentinel", errs: []error{wrapped}, wantCalls: 2},
		{name: "retries once only", errs: []error{bare, bare}, wantCalls: 2, wantErr: treestorage.ErrTreeExists},
		{name: "the retry's own error is returned", errs: []error{bare, deleted}, wantCalls: 2, wantErr: deleted},
		{name: "other errors are not retried", errs: []error{other}, wantCalls: 1, wantErr: other},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := &openedTree{}
			calls := 0
			got, err := buildTreeOrStored(func() (objecttree.ObjectTree, error) {
				calls++
				if calls <= len(tc.errs) {
					return nil, tc.errs[calls-1]
				}
				return want, nil
			})
			require.Equal(t, tc.wantCalls, calls)
			if tc.wantErr == nil {
				require.NoError(t, err)
				require.Same(t, want, got)
				return
			}
			require.ErrorIs(t, err, tc.wantErr)
			require.Nil(t, got)
		})
	}
}
