package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// A deleted child counts as a child until the coordinator makes its
// deletion final. The SDK asks for the deletion with no waiting period,
// so the coordinator's next deletion pass makes it final: within its
// pass interval, 20 seconds on the local networks. The tests wait for
// the child to leave the coordinator's list instead of for a time.

// TestE2E_ChildSpaces_DeleteParent: a parent with children is not
// deleted. Delete returns ErrSpaceHasChildren and changes nothing, for
// a child the owner's index holds and for one only the coordinator
// knows, and also while the child's deletion is not final. Once the
// children are gone the parents' deletion goes through and their
// members see them go.
func TestE2E_ChildSpaces_DeleteParent(t *testing.T) {
	t.Parallel()
	net := childSpacesNetwork(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	owner := net.account(t, ctx, "owner")
	writer := net.account(t, ctx, "writer")

	withOwn := owner.create(t, ctx, "Parent of the owner's child")
	withOthers := owner.create(t, ctx, "Parent of the writer's child")
	addMembers(t, ctx, withOwn, grant{writer, space.PermissionWriter})
	addMembers(t, ctx, withOthers, grant{writer, space.PermissionWriter})
	ownChild := owner.createChild(t, ctx, withOwn.Id(), "Owner's child")
	othersChild := writer.createChild(t, ctx, withOthers.Id(), "Writer's child")
	waitSigned(t, ctx, owner, withOwn.Id(), ownChild.Id())
	waitSigned(t, ctx, owner, withOthers.Id(), othersChild.Id())

	unchanged := func(t *testing.T, p space.Space) {
		t.Helper()
		row, ok := owner.row(ctx, p.Id())
		require.True(t, ok)
		assert.Equal(t, space.StatusActive, row.Status, "the refused parent stays active")
		assert.Equal(t, space.RefusalNone, row.Refusal)
		assert.Equal(t, coordinatorproto.SpaceStatus_SpaceStatusCreated, net.coordinatorStatus(t, ctx, owner, p.Id()))
		_, err := owner.spaces().Get(ctx, p.Id())
		assert.NoError(t, err, "the refused parent still loads")
	}

	t.Run("ChildInTheIndex", func(t *testing.T) {
		require.ErrorIs(t, owner.spaces().Delete(ctx, withOwn.Id()), space.ErrSpaceHasChildren)
		unchanged(t, withOwn)
	})
	t.Run("ChildOnlyTheCoordinatorKnows", func(t *testing.T) {
		_, held := owner.row(ctx, othersChild.Id())
		require.False(t, held)
		require.ErrorIs(t, owner.spaces().Delete(ctx, withOthers.Id()), space.ErrSpaceHasChildren)
		unchanged(t, withOthers)
		w, _ := writer.row(ctx, withOthers.Id())
		assert.Equal(t, space.StatusActive, w.Status, "the member keeps the parent")
	})

	require.NoError(t, owner.spaces().DeleteChild(ctx, ownChild.Id()))
	require.NoError(t, owner.spaces().DeleteChild(ctx, othersChild.Id()))

	t.Run("ChildDeletionNotFinal", func(t *testing.T) {
		pending := func() bool {
			return children(t, ctx, owner, withOthers.Id())[othersChild.Id()] == space.ChildStatusDeletionPending
		}
		before := pending()
		err := owner.spaces().Delete(ctx, withOthers.Id())
		if !before || !pending() {
			t.Logf("the child's deletion became final during the check; Delete returned %v", err)
			if err == nil {
				return
			}
		}
		require.ErrorIs(t, err, space.ErrSpaceHasChildren)
		unchanged(t, withOthers)
	})

	waitDeletionFinal(t, ctx, owner, withOwn.Id(), ownChild.Id())
	waitDeletionFinal(t, ctx, owner, withOthers.Id(), othersChild.Id())
	for _, p := range []space.Space{withOwn, withOthers} {
		row, _ := owner.row(ctx, p.Id())
		if row.Status != space.StatusDeleted {
			require.NoError(t, owner.spaces().Delete(ctx, p.Id()), "delete %s", p.Info().Name)
		}
		row, _ = owner.row(ctx, p.Id())
		assert.Equal(t, space.StatusDeleted, row.Status)
		assert.Equal(t, space.RefusalNone, row.Refusal)
		assert.NotEqual(t, coordinatorproto.SpaceStatus_SpaceStatusCreated, net.coordinatorStatus(t, ctx, owner, p.Id()))
		writer.waitRow(t, ctx, p.Id(), "the member sees the parent go", func(si space.SpaceInfo) bool {
			return si.Status == space.StatusDeleted
		})
		_, err := owner.spaces().Children(ctx, p.Id())
		assert.ErrorIs(t, err, space.ErrSpaceDeleted, "Children of the deleted %s", p.Info().Name)
	}
}

// TestE2E_ChildSpaces_DeleteParentOffline: a parent deleted offline
// leaves the account at once. Back online the coordinator refuses the
// deletion for a child the owner's device never held, and the deleted
// row says so. The parent's Owner deletes that child; once its deletion
// is final the parent's deletion goes through and the refusal clears.
func TestE2E_ChildSpaces_DeleteParentOffline(t *testing.T) {
	t.Parallel()
	net := childSpacesNetwork(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	proxied, routes := proxiedNetwork(t, net.yaml)
	stop := startTCPProxies(t, routes)
	defer func() { stop() }()
	owner := net.open(t, ctx, "owner behind proxies", newFixedSeedProvider(t), proxied)
	writer := net.account(t, ctx, "writer")

	parent := owner.create(t, ctx, "Parent")
	addMembers(t, ctx, parent, grant{writer, space.PermissionWriter})
	child := writer.createChild(t, ctx, parent.Id(), "Writer's child")
	waitSigned(t, ctx, owner, parent.Id(), child.Id())

	stop()
	require.NoError(t, owner.spaces().Delete(ctx, parent.Id()), "an offline delete is deferred")
	row, _ := owner.row(ctx, parent.Id())
	require.Equal(t, space.StatusDeleted, row.Status, "the parent leaves the account at once")
	stop = startTCPProxies(t, routes)

	owner.waitRow(t, ctx, parent.Id(), "the coordinator refuses the deferred delete", func(si space.SpaceInfo) bool {
		return si.Refusal == space.RefusalHasChildren
	})
	assert.Equal(t, coordinatorproto.SpaceStatus_SpaceStatusCreated, net.coordinatorStatus(t, ctx, owner, parent.Id()))
	listed := children(t, ctx, owner, parent.Id())
	require.Equal(t, space.ChildStatusActive, listed[child.Id()], "Children works on the locally deleted parent")

	require.NoError(t, owner.spaces().DeleteChild(ctx, child.Id()), "DeleteChild works on the locally deleted parent")
	owner.waitRow(t, ctx, parent.Id(), "the deferred delete goes through", func(si space.SpaceInfo) bool {
		return si.Refusal == space.RefusalNone
	})
	assert.NotEqual(t, coordinatorproto.SpaceStatus_SpaceStatusCreated, net.coordinatorStatus(t, ctx, owner, parent.Id()))
	writer.waitRow(t, ctx, parent.Id(), "the member sees the parent go", func(si space.SpaceInfo) bool {
		return si.Status == space.StatusDeleted
	})
}
