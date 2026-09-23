package spaceobjects

import (
	"context"
	"errors"
	"testing"

	anystorev1 "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/commonspace/headsync/headstorage"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

func TestDeriveChildGate(t *testing.T) {
	const child, parent = "child", "parent"
	entries := func(es ...headstorage.HeadsEntry) func(context.Context, string) (headstorage.HeadsEntry, error) {
		m := map[string]headstorage.HeadsEntry{}
		for _, e := range es {
			m[e.Id] = e
		}
		return func(_ context.Context, id string) (headstorage.HeadsEntry, error) {
			if e, ok := m[id]; ok {
				return e, nil
			}
			return headstorage.HeadsEntry{}, anystorev1.ErrDocNotFound
		}
	}
	cases := []struct {
		name        string
		entries     []headstorage.HeadsEntry
		want        error
		wantPresent bool
	}{
		{"parent absent, child absent", nil, space.ErrObjectNotFound, false},
		{"parent absent, child present", []headstorage.HeadsEntry{{Id: child}}, nil, true},
		{"parent signed", []headstorage.HeadsEntry{{Id: parent}}, nil, false},
		{"parent queued for deletion", []headstorage.HeadsEntry{{Id: parent, DeletedStatus: headstorage.DeletedStatusQueued}}, space.ErrObjectDeleted, false},
		{"parent deleted", []headstorage.HeadsEntry{{Id: parent, DeletedStatus: headstorage.DeletedStatusDeleted}}, space.ErrObjectDeleted, false},
		{"parent derived", []headstorage.HeadsEntry{{Id: parent, IsDerived: true}}, objecttree.ErrDerivedParent, false},
		{"parent derived, child present", []headstorage.HeadsEntry{{Id: child}, {Id: parent, IsDerived: true}}, nil, true},
		{"parent deleted, child present", []headstorage.HeadsEntry{{Id: child}, {Id: parent, DeletedStatus: headstorage.DeletedStatusDeleted}}, nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			present, err := deriveChildGate(context.Background(), entries(tc.entries...), child, parent)
			if tc.want == nil {
				require.NoError(t, err)
				require.Equal(t, tc.wantPresent, present)
				return
			}
			require.ErrorIs(t, err, tc.want)
		})
	}

	t.Run("storage error propagates", func(t *testing.T) {
		boom := errors.New("boom")
		getEntry := func(context.Context, string) (headstorage.HeadsEntry, error) { return headstorage.HeadsEntry{}, boom }
		_, err := deriveChildGate(context.Background(), getEntry, child, parent)
		require.ErrorIs(t, err, boom)
	})
}
