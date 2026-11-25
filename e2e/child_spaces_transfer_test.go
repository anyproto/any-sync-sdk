package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// childSpacesParentTransfer: once the parent's ownership moves
// to another account, the count of its children against the limit
// moves with it, and the new owner governs the children at once: it
// lists them, removes a member and deletes one. The old owner, now a
// Writer of the parent, is refused. A child refused for the limit is
// asked for again by the SDK, with no call, once a place is free.
func childSpacesParentTransfer(t *testing.T, net *childNet) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	old := net.account(t, ctx, "old owner")
	heir := net.account(t, ctx, "new owner")
	writer := net.account(t, ctx, "writer")
	member := net.account(t, ctx, "member")

	parent := old.createOrganization(t, ctx, "Parent")
	oldOther := old.createOrganization(t, ctx, "Old owner's other parent")
	heirOwn := heir.createOrganization(t, ctx, "New owner's own parent")
	addMembers(t, ctx, parent,
		grant{heir, space.PermissionAdmin},
		grant{writer, space.PermissionWriter},
		grant{member, space.PermissionReader})

	// Three children of the old owner's parent fill its limit.
	c1 := old.createChild(t, ctx, parent.Id(), "Old owner's child")
	c2 := writer.createChild(t, ctx, parent.Id(), "Writer's child")
	c3 := heir.createChild(t, ctx, parent.Id(), "New owner's child")
	for _, c := range []space.Space{c1, c2, c3} {
		waitSigned(t, ctx, old, parent.Id(), c.Id())
	}
	addMembers(t, ctx, c2, grant{member, space.PermissionWriter})
	addMembers(t, ctx, c3, grant{member, space.PermissionWriter})

	blocked := old.createChild(t, ctx, oldOther.Id(), "Over the old owner's limit")
	refused := old.waitRow(t, ctx, blocked.Id(), "the old owner is at its limit", func(si space.SpaceInfo) bool {
		return si.Refusal != space.RefusalNone
	})
	require.Equal(t, space.RefusalChildLimitReached, refused.Refusal)

	require.NoError(t, parent.ACL().OwnershipChange(ctx, heir.id(), space.PermissionWriter), "the parent changes owner")

	var (
		over      space.Space
		refusedAt time.Time
	)
	t.Run("LimitCountMoves", func(t *testing.T) {
		require.NoError(t, old.spaces().RetryPush(ctx, blocked.Id()), "the old owner's place is free")
		waitSigned(t, ctx, old, oldOther.Id(), blocked.Id())
		over = heir.createChild(t, ctx, heirOwn.Id(), "Over the new owner's limit")
		row := heir.waitRow(t, ctx, over.Id(), "the new owner is at its limit", func(si space.SpaceInfo) bool {
			return si.Refusal != space.RefusalNone
		})
		refusedAt = time.Now()
		assert.Equal(t, space.RefusalChildLimitReached, row.Refusal)
	})

	t.Run("NewOwnerGovernsAtOnce", func(t *testing.T) {
		listed := children(t, ctx, heir, parent.Id())
		for _, c := range []space.Space{c1, c2, c3} {
			assert.Equal(t, space.ChildStatusActive, listed[c.Id()], "%s listed", c.Info().Name)
		}
		require.NoError(t, heir.spaces().RemoveChildMembers(ctx, c2.Id(), []string{member.id()}))
		waitChildMembers(t, ctx, heir, c2.Id(), "member removed", func(m map[string]space.ChildMember) bool {
			return !activeAs(m, member.id(), space.PermissionWriter)
		})
		require.NoError(t, heir.spaces().DeleteChild(ctx, c1.Id()), "the new owner deletes the old owner's child")
		old.waitRow(t, ctx, c1.Id(), "the old owner sees its child go", func(si space.SpaceInfo) bool {
			return si.Status == space.StatusDeleted
		})
	})

	t.Run("OldOwnerRefused", func(t *testing.T) {
		_, err := old.spaces().Children(ctx, parent.Id())
		assert.ErrorIs(t, err, space.ErrParentPermissionDenied, "Children")
		assert.ErrorIs(t, old.spaces().RemoveChildMembers(ctx, c3.Id(), []string{member.id()}),
			space.ErrParentPermissionDenied, "RemoveChildMembers")
		assert.ErrorIs(t, old.spaces().ChangeChildOwner(ctx, c3.Id(), member.id(), space.PermissionWriter),
			space.ErrParentPermissionDenied, "ChangeChildOwner")
		assert.ErrorIs(t, old.spaces().DeleteChild(ctx, c2.Id()),
			space.ErrParentPermissionDenied, "DeleteChild")
		members := childMembers(t, ctx, heir, c3.Id())
		assert.True(t, activeAs(members, heir.id(), space.PermissionOwner), "%+v", members)
		assert.True(t, activeAs(members, member.id(), space.PermissionWriter), "%+v", members)
		assert.Equal(t, space.ChildStatusActive, children(t, ctx, heir, parent.Id())[c2.Id()])
	})

	// The deletion of the old owner's child frees a place of the new
	// owner. The SDK asks for the refused child again by itself on a
	// head-sync round of the space (every 30 s), once a wait is over: a
	// minute after the refusal, then two more after a second one. A place
	// freed less than three minutes after the refusal is asked for at the
	// latest at the second ask: two minutes and a head-sync period after
	// the first one, which can fall just before the place is free.
	t.Run("RefusedChildAskedAgainByItself", func(t *testing.T) {
		require.NotNil(t, over)
		waitDeletionFinal(t, ctx, heir, parent.Id(), c1.Id())
		freed := time.Since(refusedAt)
		require.Less(t, freed, 3*time.Minute, "the place is free before the SDK's third ask")
		const worst = 2*time.Minute + 30*time.Second
		if !waitFor(ctx, worst+30*time.Second, time.Second, func() bool {
			row, _ := heir.row(ctx, over.Id())
			return row.Refusal == space.RefusalNone
		}) {
			row, _ := heir.row(ctx, over.Id())
			t.Fatalf("the refused child is not asked for again: refusal %s", row.Refusal)
		}
		waitSigned(t, ctx, heir, heirOwn.Id(), over.Id())
	})
}
