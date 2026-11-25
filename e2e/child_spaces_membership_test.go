package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// childSpacesMembership: only members of the parent become
// members of a child. Members of the parent are added by direct add, by
// the child's Owner and by its Admin, join on the Admin's invite, and
// read the child. An account outside the parent is refused on every way
// in with ErrNotParentMember: a direct add (nobody of the batch is
// added), a join request (refused when posted, no row on any of its
// devices), and the acceptance of a request whose author left the
// parent since. The organization and the child have no guest role: the
// calls that name it return ErrNoGuestRole, for the Owner and for an
// Admin, and no account becomes a guest.
func childSpacesMembership(t *testing.T, net *childNet) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	owner := net.account(t, ctx, "owner")
	member := net.account(t, ctx, "member of the parent")
	other := net.account(t, ctx, "other member of the parent")
	joiner := net.account(t, ctx, "member of the parent who joins")
	leaver := net.account(t, ctx, "member who leaves the parent")
	outsider := net.account(t, ctx, "outsider")
	outsiderB := net.device(t, ctx, outsider, "outsider's second device")

	parent := owner.createOrganization(t, ctx, "Parent")
	addMembers(t, ctx, parent,
		grant{member, space.PermissionWriter},
		grant{other, space.PermissionReader},
		grant{joiner, space.PermissionReader},
		grant{leaver, space.PermissionWriter})
	child := owner.createChild(t, ctx, parent.Id(), "Child")
	waitSigned(t, ctx, owner, parent.Id(), child.Id())
	note := writeNote(t, ctx, child, "for the members of the child")

	// A member of the parent, by direct add of the child's Owner.
	memberChild := addMembers(t, ctx, child, grant{member, space.PermissionAdmin})[0]
	waitNote(t, ctx, member.name, memberChild, note, "for the members of the child")
	joined := member.waitRow(t, ctx, child.Id(), "the joined child names its parent", func(si space.SpaceInfo) bool {
		return si.ParentSpaceId != ""
	})
	assert.Equal(t, parent.Id(), joined.ParentSpaceId)

	notIn := func(t *testing.T, identities ...string) {
		t.Helper()
		listed, err := child.Members().List(ctx)
		require.NoError(t, err)
		for _, m := range listed {
			for _, id := range identities {
				if m.Identity == id {
					assert.NotEqual(t, space.MemberStatusActive, m.Status, "%s is no member of the child", id)
				}
			}
		}
		onNodes := childMembers(t, ctx, owner, child.Id())
		for _, id := range identities {
			if m, ok := onNodes[id]; ok {
				assert.NotEqual(t, space.MemberStatusActive, m.Status, "%s is no member of the child on the nodes", id)
			}
		}
	}

	t.Run("DirectAddOfAnOutsider", func(t *testing.T) {
		err := child.ACL().AddAccounts(ctx, []space.MemberAdd{
			{Identity: leaver.id(), Permission: space.PermissionWriter},
			{Identity: outsider.id(), Permission: space.PermissionWriter},
		})
		require.ErrorIs(t, err, space.ErrNotParentMember)
		notIn(t, leaver.id(), outsider.id())
	})

	t.Run("DirectAddByAChildAdmin", func(t *testing.T) {
		otherChild := addMembers(t, ctx, memberChild, grant{other, space.PermissionReader})[0]
		waitNote(t, ctx, other.name, otherChild, note, "for the members of the child")
	})

	// The invite of the child's Admin.
	inv, err := memberChild.ACL().CreateInvite(ctx)
	require.NoError(t, err, "a child's Admin creates an invite")
	token, err := space.EncodeInvite(inv)
	require.NoError(t, err)

	t.Run("JoinOnTheInviteOfAChildAdmin", func(t *testing.T) {
		_, err := joiner.spaces().Join(ctx, space.JoinRequest{Invite: token})
		require.ErrorIs(t, err, space.ErrJoinPending)
		req := awaitJoinRequest(t, ctx, memberChild, joiner.id())
		require.NoError(t, memberChild.ACL().AcceptRequest(ctx, req.RecordId, space.PermissionWriter))
		joiner.waitRow(t, ctx, child.Id(), "the accepted join", func(si space.SpaceInfo) bool {
			return si.Status == space.StatusActive
		})
		waitNote(t, ctx, joiner.name, joiner.get(t, ctx, child.Id()), note, "for the members of the child")
	})

	t.Run("JoinRequestOfAnOutsider", func(t *testing.T) {
		_, err := outsider.spaces().Join(ctx, space.JoinRequest{Invite: token})
		require.ErrorIs(t, err, space.ErrNotParentMember)
		for _, d := range []*device{outsider, outsiderB} {
			require.NoError(t, d.spaces().SyncSpaceList(ctx))
			_, held := d.row(ctx, child.Id())
			assert.False(t, held, "%s: a refused join writes no row", d.name)
		}
		_ = child.SyncHeads(ctx)
		reqs, err := child.Members().JoinRequests(ctx)
		require.NoError(t, err)
		for _, r := range reqs {
			assert.NotEqual(t, outsider.id(), r.Identity, "the refused request is not in the ACL")
		}
	})

	// pendingRequest is the join request the child's ACL is left with.
	var pendingRequest string
	t.Run("AcceptedAfterTheRequesterLeftTheParent", func(t *testing.T) {
		_, err := leaver.spaces().Join(ctx, space.JoinRequest{Invite: token})
		require.ErrorIs(t, err, space.ErrJoinPending, "a member of the parent posts a join request")
		req := awaitJoinRequest(t, ctx, child, leaver.id())
		pendingRequest = req.RecordId

		require.NoError(t, parent.ACL().RemoveAccounts(ctx, []string{leaver.id()}), "the requester leaves the parent")
		err = child.ACL().AcceptRequest(ctx, req.RecordId, space.PermissionWriter)
		require.ErrorIs(t, err, space.ErrNotParentMember)
		notIn(t, leaver.id())
	})

	t.Run("NoGuestRole", func(t *testing.T) {
		require.NotEmpty(t, pendingRequest, "the child's ACL holds a join request")
		guestIdentity := newRandomIdentity(t)
		// other is a Reader of the organization and of the child. Only
		// the child's ACL holds a join request to accept.
		for _, by := range []struct {
			who     string
			sp      space.Space
			request string
		}{
			{"the organization's Owner", parent, ""},
			{"the child's Owner", child, pendingRequest},
			{"a child's Admin", memberChild, pendingRequest},
		} {
			acl := by.sp.ACL()
			_, err := acl.CreateGuestKey(ctx)
			assert.ErrorIs(t, err, space.ErrNoGuestRole, "%s: CreateGuestKey", by.who)
			assert.ErrorIs(t, acl.RevokeGuestKey(ctx), space.ErrNoGuestRole, "%s: RevokeGuestKey", by.who)
			err = acl.AddAccounts(ctx, []space.MemberAdd{
				{Identity: outsider.id(), Permission: space.PermissionGuest},
				{Identity: guestIdentity, Permission: space.PermissionGuest},
			})
			assert.ErrorIs(t, err, space.ErrNoGuestRole, "%s: AddAccounts of guests", by.who)
			err = acl.ChangePermissions(ctx, []space.PermissionChange{{Identity: other.id(), Permission: space.PermissionGuest}})
			assert.ErrorIs(t, err, space.ErrNoGuestRole, "%s: ChangePermissions to guest", by.who)
			if by.request != "" {
				assert.ErrorIs(t, acl.AcceptRequest(ctx, by.request, space.PermissionGuest), space.ErrNoGuestRole,
					"%s: AcceptRequest as a guest", by.who)
			}
		}

		notIn(t, outsider.id(), guestIdentity)
		for id, m := range childMembers(t, ctx, owner, child.Id()) {
			assert.NotEqual(t, space.PermissionGuest, m.Permission, "no guest in the child: %s", id)
		}
		_ = parent.SyncHeads(ctx)
		inParent, err := parent.Members().List(ctx)
		require.NoError(t, err)
		for _, m := range inParent {
			assert.NotEqual(t, space.PermissionGuest, m.Permission, "no guest in the organization: %s", m.Identity)
			assert.NotEqual(t, guestIdentity, m.Identity, "the guest identity is not in the organization")
		}
		for _, d := range []*device{outsider, outsiderB} {
			require.NoError(t, d.spaces().SyncSpaceList(ctx))
			for _, id := range []string{parent.Id(), child.Id()} {
				_, held := d.row(ctx, id)
				assert.False(t, held, "%s: a refused add writes no row of %s", d.name, id)
			}
		}
	})
}
