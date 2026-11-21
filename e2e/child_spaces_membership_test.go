package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_ChildSpaces_Membership: only members of the parent become
// members of a child. Members of the parent are added by direct add, by
// the child's Owner and by its Admin, join on the Admin's invite, and
// read the child. An account outside the parent is refused on every way
// in with ErrNotParentMember: a direct add (nobody of the batch is
// added), a join request (refused when posted, no row on any of its
// devices), the acceptance of a request whose author left the parent
// since, and a guest identity added by anyone but the child's Owner.
// The Owner's guest key gives an outsider read-only access.
func TestE2E_ChildSpaces_Membership(t *testing.T) {
	t.Parallel()
	net := childSpacesNetwork(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	owner := net.account(t, ctx, "owner")
	member := net.account(t, ctx, "member of the parent")
	other := net.account(t, ctx, "other member of the parent")
	joiner := net.account(t, ctx, "member of the parent who joins")
	leaver := net.account(t, ctx, "member who leaves the parent")
	outsider := net.account(t, ctx, "outsider")
	outsiderB := net.device(t, ctx, outsider, "outsider's second device")

	parent := owner.create(t, ctx, "Parent")
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

	t.Run("AcceptedAfterTheRequesterLeftTheParent", func(t *testing.T) {
		_, err := leaver.spaces().Join(ctx, space.JoinRequest{Invite: token})
		require.ErrorIs(t, err, space.ErrJoinPending, "a member of the parent posts a join request")
		req := awaitJoinRequest(t, ctx, child, leaver.id())

		require.NoError(t, parent.ACL().RemoveAccounts(ctx, []string{leaver.id()}), "the requester leaves the parent")
		err = child.ACL().AcceptRequest(ctx, req.RecordId, space.PermissionWriter)
		require.ErrorIs(t, err, space.ErrNotParentMember)
		notIn(t, leaver.id())
	})

	t.Run("GuestIdentityOnlyFromTheChildOwner", func(t *testing.T) {
		guestIdentity := newRandomIdentity(t)
		err := memberChild.ACL().AddAccounts(ctx, []space.MemberAdd{
			{Identity: guestIdentity, Permission: space.PermissionGuest},
		})
		require.ErrorIs(t, err, space.ErrNotParentMember, "a child's Admin adds a guest from outside the parent")
		notIn(t, guestIdentity)
		_, err = memberChild.ACL().CreateGuestKey(ctx)
		assert.Error(t, err, "a child's Admin creates no guest key")
	})

	t.Run("GuestKeyForAnOutsider", func(t *testing.T) {
		guest, err := child.ACL().CreateGuestKey(ctx)
		require.NoError(t, err)
		guestToken, err := space.EncodeInvite(guest)
		require.NoError(t, err)
		guestChild, err := outsider.spaces().JoinGuest(ctx, guestToken)
		if err != nil {
			require.ErrorIs(t, err, space.ErrGuestJoinPending)
		}
		row := outsider.waitRow(t, ctx, child.Id(), "the guest's copy of the child is active", func(si space.SpaceInfo) bool {
			return si.Status == space.StatusActive
		})
		if guestChild == nil {
			guestChild = outsider.get(t, ctx, child.Id())
		}
		waitNote(t, ctx, outsider.name, guestChild, note, "for the members of the child")
		_, err = guestChild.Types().Create(ctx, space.TypeCreateParams{Name: "written by a guest"})
		assert.ErrorIs(t, err, space.ErrReadOnlySpace, "a guest only reads")
		row = outsider.waitRow(t, ctx, child.Id(), "the guest's row names the parent", func(si space.SpaceInfo) bool {
			return si.ParentSpaceId != ""
		})
		assert.Equal(t, parent.Id(), row.ParentSpaceId)
		assert.Equal(t, space.PermissionGuest, guestChild.Info().OwnRole)
	})
}
