package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// childSpacesRemovalFromTheParent: the parent's Owner, who is
// no member of the child and never loads it, lists the children, reads
// a child's members and removes one. The removal reaches the child's
// devices, and the parent's side, as a pending rotation; a new device of
// the child's Admin loads the child and rotates by itself, and the
// pending state ends.
// The removed member reads what was written before the rotation and
// nothing after it. A parent Admin removes the same way, and a device
// of the child's Owner, started again, rotates with no call on it. A
// parent Writer and an outsider are refused.
func childSpacesRemovalFromTheParent(t *testing.T, net *childNet) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	owner := net.account(t, ctx, "parent's owner")
	padmin := net.account(t, ctx, "parent's admin")
	pwriter := net.account(t, ctx, "parent's writer")
	outsider := net.account(t, ctx, "outsider")
	cowner := net.account(t, ctx, "child's owner")
	cadmin := net.account(t, ctx, "child's admin")
	m1 := net.account(t, ctx, "member removed by the parent's owner")
	m2 := net.account(t, ctx, "member removed by the parent's admin")
	m3 := net.account(t, ctx, "member who stays")

	parent := owner.create(t, ctx, "Parent")
	addMembers(t, ctx, parent,
		grant{padmin, space.PermissionAdmin},
		grant{pwriter, space.PermissionWriter},
		grant{cowner, space.PermissionWriter},
		grant{cadmin, space.PermissionReader},
		grant{m1, space.PermissionReader},
		grant{m2, space.PermissionReader},
		grant{m3, space.PermissionReader})

	child := cowner.createChild(t, ctx, parent.Id(), "Child")
	waitSigned(t, ctx, owner, parent.Id(), child.Id())
	handles := addMembers(t, ctx, child,
		grant{cadmin, space.PermissionAdmin},
		grant{m1, space.PermissionWriter},
		grant{m2, space.PermissionWriter},
		grant{m3, space.PermissionWriter})
	m1Child, m2Child, m3Child := handles[1], handles[2], handles[3]
	x0 := writeNote(t, ctx, child, "before the removals")
	for i, sp := range []space.Space{m1Child, m2Child, m3Child} {
		waitNote(t, ctx, []string{m1.name, m2.name, m3.name}[i], sp, x0, "before the removals")
	}

	// The parent's side.
	assert.Equal(t, space.ChildStatusActive, children(t, ctx, owner, parent.Id())[child.Id()])
	assert.Equal(t, space.ChildStatusActive, children(t, ctx, padmin, parent.Id())[child.Id()])
	members := childMembers(t, ctx, owner, child.Id())
	assert.True(t, activeAs(members, cowner.id(), space.PermissionOwner), "%+v", members)
	assert.True(t, activeAs(members, cadmin.id(), space.PermissionAdmin), "%+v", members)
	for _, m := range []*device{m1, m2, m3} {
		assert.True(t, activeAs(members, m.id(), space.PermissionWriter), "%s: %+v", m.name, members)
	}
	assert.NotContains(t, members, owner.id(), "the parent's owner is no member of the child")

	t.Run("RefusedCallers", func(t *testing.T) {
		for _, d := range []*device{pwriter, outsider} {
			_, err := d.spaces().Children(ctx, parent.Id())
			assert.ErrorIs(t, err, space.ErrParentPermissionDenied, "%s: Children", d.name)
			err = d.spaces().RemoveChildMembers(ctx, child.Id(), []string{m3.id()})
			assert.ErrorIs(t, err, space.ErrParentPermissionDenied, "%s: RemoveChildMembers", d.name)
		}
		assert.ErrorIs(t, owner.spaces().RemoveChildMembers(ctx, child.Id(), []string{cowner.id()}),
			space.ErrInsufficientPermissions, "the child's owner is not removed")
		assert.ErrorIs(t, owner.spaces().RemoveChildMembers(ctx, child.Id(), []string{newRandomIdentity(t)}),
			space.ErrAccountNotFound, "an identity the child does not hold")
		assert.True(t, activeAs(childMembers(t, ctx, owner, child.Id()), m3.id(), space.PermissionWriter))
	})

	// No device of the child's Owner or Admin runs: nobody rotates yet.
	cowner.stop()
	cadmin.stop()

	t.Run("RemovalByTheParentOwner", func(t *testing.T) {
		require.NoError(t, owner.spaces().RemoveChildMembers(ctx, child.Id(), []string{m1.id()}))
		waitChildMembers(t, ctx, owner, child.Id(), "m1 removed", func(m map[string]space.ChildMember) bool {
			return !activeAs(m, m1.id(), space.PermissionWriter)
		})
		waitRotationPending(t, ctx, m2.name, m2Child, true)
		waitRotationPending(t, ctx, m1.name, m1Child, true)
		waitChildRotationPending(t, ctx, owner, child.Id(), true)

		x1 := writeNote(t, ctx, m2Child, "after the removal, before the rotation")
		waitNote(t, ctx, m1.name+" (removed)", m1Child, x1, "after the removal, before the rotation")

		// A new device of the child's Admin loads the child and rotates.
		cadminB := net.device(t, ctx, cadmin, "child's admin, second device")
		cadminChild := cadminB.get(t, ctx, child.Id())
		waitRotationPending(t, ctx, m2.name, m2Child, false)
		waitRotationPending(t, ctx, cadminB.name, cadminChild, false)
		waitChildRotationPending(t, ctx, owner, child.Id(), false)

		x2 := writeNote(t, ctx, m2Child, "after the first rotation")
		waitNote(t, ctx, cadminB.name, cadminChild, x2, "after the first rotation")
		neverNote(t, ctx, m1.name+" (removed)", m1Child, x2, "after the first rotation", 15*time.Second)
		assert.True(t, readsNote(ctx, m1Child, x0, "before the removals"), "the removed member keeps what it read")
		cadminB.stop()
	})

	t.Run("RemovalByTheParentAdmin", func(t *testing.T) {
		require.NoError(t, padmin.spaces().RemoveChildMembers(ctx, child.Id(), []string{m2.id()}))
		waitChildMembers(t, ctx, padmin, child.Id(), "m2 removed", func(m map[string]space.ChildMember) bool {
			return !activeAs(m, m2.id(), space.PermissionWriter)
		})
		waitRotationPending(t, ctx, m3.name, m3Child, true)
		waitChildRotationPending(t, ctx, padmin, child.Id(), true)

		// The child's Owner comes back. Its device loads the account's
		// spaces at start, so it rotates with no call on it.
		cowner.start(t, ctx)
		t.Run("RestartedOwnerDeviceRotatesWithoutACall", func(t *testing.T) {
			if !waitFor(ctx, 30*time.Second, 250*time.Millisecond, func() bool {
				_ = m3Child.SyncHeads(ctx)
				pending, err := m3Child.ACL().RotationPending(ctx)
				return err == nil && !pending
			}) {
				t.Errorf("the child's Owner device, started again, did not rotate within 30s with no call on it")
			}
		})
		waitChildRotationPending(t, ctx, padmin, child.Id(), false)
		ownerChild := cowner.get(t, ctx, child.Id())
		waitRotationPending(t, ctx, m3.name, m3Child, false)
		waitRotationPending(t, ctx, cowner.name, ownerChild, false)

		x3 := writeNote(t, ctx, m3Child, "after the second rotation")
		waitNote(t, ctx, cowner.name, ownerChild, x3, "after the second rotation")
		neverNote(t, ctx, m2.name+" (removed)", m2Child, x3, "after the second rotation", 15*time.Second)
	})

	_, held := owner.row(ctx, child.Id())
	assert.False(t, held, "the parent's owner never holds the child")
}

// childSpacesOwnerAndDeletionFromTheParent: the parent's Owner
// hands a child to another member of it, naming each role the previous
// owner keeps; the new owner acts as the owner, and must be a member of
// the parent. A parent Admin is refused both. The parent's Owner then
// deletes the child it could never read; the child's owner cannot undo
// that, and every member's device sees the child go. The parent-side
// calls answer ErrSpaceUnknown for a child the network does not know and
// ErrSpaceDeleted for the deleted one, and DeleteChild ErrNotChildSpace
// for a top-level space of another account.
func childSpacesOwnerAndDeletionFromTheParent(t *testing.T, net *childNet) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	owner := net.account(t, ctx, "parent's owner")
	padmin := net.account(t, ctx, "parent's admin")
	cowner := net.account(t, ctx, "child's owner")
	cadmin := net.account(t, ctx, "child's admin")
	cmember := net.account(t, ctx, "child's writer")

	parent := owner.create(t, ctx, "Parent")
	addMembers(t, ctx, parent,
		grant{padmin, space.PermissionAdmin},
		grant{cowner, space.PermissionWriter},
		grant{cadmin, space.PermissionWriter},
		grant{cmember, space.PermissionReader})
	child := cowner.createChild(t, ctx, parent.Id(), "Child")
	waitSigned(t, ctx, owner, parent.Id(), child.Id())
	handles := addMembers(t, ctx, child, grant{cadmin, space.PermissionAdmin}, grant{cmember, space.PermissionWriter})
	cadminChild, cmemberChild := handles[0], handles[1]
	note := writeNote(t, ctx, child, "for the child's members")
	header := net.spaceHeader(t, ctx, cowner, child.Id())

	t.Run("Refused", func(t *testing.T) {
		assert.ErrorIs(t, padmin.spaces().ChangeChildOwner(ctx, child.Id(), cadmin.id(), space.PermissionAdmin),
			space.ErrParentPermissionDenied, "a parent Admin does not change the owner")
		assert.ErrorIs(t, padmin.spaces().DeleteChild(ctx, child.Id()),
			space.ErrParentPermissionDenied, "a parent Admin does not delete")
		for _, perm := range []space.Permission{space.PermissionNone, space.PermissionGuest, space.PermissionOwner} {
			assert.ErrorIs(t, owner.spaces().ChangeChildOwner(ctx, child.Id(), cadmin.id(), perm),
				space.ErrInsufficientPermissions, "the previous owner keeps %s", perm)
		}
		assert.ErrorIs(t, owner.spaces().ChangeChildOwner(ctx, child.Id(), cowner.id(), space.PermissionAdmin),
			space.ErrInsufficientPermissions, "the owner already")
		assert.ErrorIs(t, owner.spaces().ChangeChildOwner(ctx, child.Id(), padmin.id(), space.PermissionAdmin),
			space.ErrAccountNotFound, "no member of the child")
		assert.True(t, activeAs(childMembers(t, ctx, owner, child.Id()), cowner.id(), space.PermissionOwner))
	})

	t.Run("CallsOnAChildTheNetworkDoesNotKnow", func(t *testing.T) {
		unknown := unknownSpaceId(t, child.Id())
		_, err := owner.spaces().Children(ctx, unknown)
		assert.ErrorIs(t, err, space.ErrSpaceUnknown, "Children")
		parentSideCalls(t, ctx, owner, unknown, cadmin.id(), space.ErrSpaceUnknown)
		assert.ErrorIs(t, owner.spaces().DeleteChild(ctx, unknown), space.ErrSpaceUnknown, "DeleteChild")
	})

	t.Run("DeleteChildOfATopLevelSpace", func(t *testing.T) {
		foreign := cmember.create(t, ctx, "Top-level space of another account")
		var err error
		require.True(t, waitFor(ctx, 60*time.Second, time.Second, func() bool {
			err = owner.spaces().DeleteChild(ctx, foreign.Id())
			return !errors.Is(err, space.ErrSpaceUnknown)
		}), "the space reaches the network")
		assert.ErrorIs(t, err, space.ErrNotChildSpace)
		assert.Equal(t, coordinatorproto.SpaceStatus_SpaceStatusCreated, net.coordinatorStatus(t, ctx, cmember, foreign.Id()),
			"the space is not deleted")
	})

	// Each transfer names the role the previous owner keeps.
	type transfer struct {
		from, to *device
		toSpace  space.Space
		kept     space.Permission
	}
	for _, tr := range []transfer{
		{cowner, cadmin, cadminChild, space.PermissionAdmin},
		{cadmin, cmember, cmemberChild, space.PermissionWriter},
		{cmember, cowner, child, space.PermissionReader},
	} {
		require.NoError(t, owner.spaces().ChangeChildOwner(ctx, child.Id(), tr.to.id(), tr.kept),
			"hand the child from %s to %s", tr.from.name, tr.to.name)
		waitChildMembers(t, ctx, owner, child.Id(), tr.to.name+" owns the child", func(m map[string]space.ChildMember) bool {
			return activeAs(m, tr.to.id(), space.PermissionOwner) && activeAs(m, tr.from.id(), tr.kept)
		})
		require.True(t, waitFor(ctx, 60*time.Second, 250*time.Millisecond, func() bool {
			_ = tr.toSpace.SyncHeads(ctx)
			me, err := tr.toSpace.Members().Me(ctx)
			return err == nil && me.Permission == space.PermissionOwner
		}), "%s's device sees itself the owner", tr.to.name)
	}

	// The new owner acts as the owner: only an Owner grants Admin.
	require.NoError(t, owner.spaces().ChangeChildOwner(ctx, child.Id(), cadmin.id(), space.PermissionAdmin))
	waitChildMembers(t, ctx, owner, child.Id(), "the child's admin owns the child", func(m map[string]space.ChildMember) bool {
		return activeAs(m, cadmin.id(), space.PermissionOwner)
	})
	require.True(t, waitFor(ctx, 60*time.Second, 250*time.Millisecond, func() bool {
		_ = cadminChild.SyncHeads(ctx)
		me, err := cadminChild.Members().Me(ctx)
		return err == nil && me.Permission == space.PermissionOwner
	}))
	t.Run("NewOwnerGrantsAdmin", func(t *testing.T) {
		padminChild := addMembers(t, ctx, cadminChild, grant{padmin, space.PermissionAdmin})[0]
		waitNote(t, ctx, padmin.name, padminChild, note, "for the child's members")
		assert.True(t, activeAs(childMembers(t, ctx, owner, child.Id()), padmin.id(), space.PermissionAdmin))
	})

	t.Run("NewOwnerIsAMemberOfTheParent", func(t *testing.T) {
		require.NoError(t, parent.ACL().RemoveAccounts(ctx, []string{cmember.id()}), "the child's writer leaves the parent")
		err := owner.spaces().ChangeChildOwner(ctx, child.Id(), cmember.id(), space.PermissionAdmin)
		assert.ErrorIs(t, err, space.ErrNotParentMember, "the parent's Owner hands the child to an outsider")
		err = cadminChild.ACL().OwnershipChange(ctx, cmember.id(), space.PermissionAdmin)
		assert.ErrorIs(t, err, space.ErrNotParentMember, "the child's Owner hands the child to an outsider")
		members := childMembers(t, ctx, owner, child.Id())
		assert.True(t, activeAs(members, cadmin.id(), space.PermissionOwner), "%+v", members)
		assert.True(t, activeAs(members, cmember.id(), space.PermissionReader), "%+v", members)
	})

	// The child goes back to its creator, then the parent's Owner
	// deletes the child it could never read.
	require.NoError(t, owner.spaces().ChangeChildOwner(ctx, child.Id(), cowner.id(), space.PermissionAdmin))
	waitChildMembers(t, ctx, owner, child.Id(), "the creator owns the child again", func(m map[string]space.ChildMember) bool {
		return activeAs(m, cowner.id(), space.PermissionOwner)
	})
	_, held := owner.row(ctx, child.Id())
	require.False(t, held, "the parent's owner holds no row of the child")
	require.NoError(t, owner.spaces().DeleteChild(ctx, child.Id()))
	listed, stillListed := children(t, ctx, owner, parent.Id())[child.Id()]
	assert.True(t, !stillListed || listed == space.ChildStatusDeletionPending, "listed as %s", listed)

	// The two ways an owner takes a deletion back: the status change to
	// created, and a forced sign of the space's header.
	undo := func(t *testing.T) {
		t.Helper()
		revert := net.coordinatorCall(ctx, cowner, func(cl coordinatorproto.DRPCCoordinatorClient) error {
			_, err := cl.SpaceStatusChange(ctx, &coordinatorproto.SpaceStatusChangeRequest{SpaceId: child.Id()})
			return err
		})
		assert.Error(t, revert, "the child's owner reverts the deletion")
		forced := net.coordinatorCall(ctx, cowner, func(cl coordinatorproto.DRPCCoordinatorClient) error {
			_, err := cl.SpaceSign(ctx, &coordinatorproto.SpaceSignRequest{SpaceId: child.Id(), Header: header, ForceRequest: true})
			return err
		})
		assert.Error(t, forced, "the child's owner signs the deleted child again")
		t.Logf("revert: %v; forced sign: %v", revert, forced)
		assert.NotEqual(t, coordinatorproto.SpaceStatus_SpaceStatusCreated, net.coordinatorStatus(t, ctx, cowner, child.Id()))
	}
	t.Run("DeletionNotUndoneByTheChildOwner", func(t *testing.T) {
		status := net.coordinatorStatus(t, ctx, cowner, child.Id())
		t.Logf("the child's status before the attempts: %s", status)
		undo(t)
	})

	for _, d := range []*device{cowner, cadmin, cmember, padmin} {
		d.waitRow(t, ctx, child.Id(), "the deleted child goes", func(si space.SpaceInfo) bool {
			return si.Status == space.StatusDeleted
		})
		_, err := d.spaces().Get(ctx, child.Id())
		assert.ErrorIs(t, err, space.ErrSpaceDeleted, "%s: Get of the deleted child", d.name)
	}
	_, held = owner.row(ctx, child.Id())
	assert.False(t, held, "the parent's owner never held the child")

	t.Run("CallsOnTheDeletedChild", func(t *testing.T) {
		parentSideCalls(t, ctx, owner, child.Id(), cadmin.id(), space.ErrSpaceDeleted)
		assert.NoError(t, owner.spaces().DeleteChild(ctx, child.Id()), "a deleted child is deleted already")
	})

	t.Run("FinalDeletionNotUndoneByTheChildOwner", func(t *testing.T) {
		waitDeletionFinal(t, ctx, owner, parent.Id(), child.Id())
		assert.Equal(t, coordinatorproto.SpaceStatus_SpaceStatusDeleted, net.coordinatorStatus(t, ctx, cowner, child.Id()))
		undo(t)
	})
}

// parentSideCalls checks that the calls made on a child from the parent's
// side, other than DeleteChild, answer want for childId: the child's
// members and pending rotation, a removal and an owner change naming
// identity.
func parentSideCalls(t *testing.T, ctx context.Context, d *device, childId, identity string, want error) {
	t.Helper()
	_, err := d.spaces().ChildMembers(ctx, childId)
	assert.ErrorIs(t, err, want, "ChildMembers")
	_, err = d.spaces().ChildRotationPending(ctx, childId)
	assert.ErrorIs(t, err, want, "ChildRotationPending")
	assert.ErrorIs(t, d.spaces().RemoveChildMembers(ctx, childId, []string{identity}), want, "RemoveChildMembers")
	assert.ErrorIs(t, d.spaces().ChangeChildOwner(ctx, childId, identity, space.PermissionAdmin), want, "ChangeChildOwner")
}
