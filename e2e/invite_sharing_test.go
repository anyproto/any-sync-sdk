package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// Direct-added members never held the join token. Sharing must work from
// replicated space state, including after a writer becomes a reader.
func TestE2E_MembersShareRequestInvite(t *testing.T) {
	yaml, _, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	owner := openSDKAt(t, ctx, yaml, t.TempDir(), newFixedSeedProvider(t), "owner")
	defer owner.Close()
	member := openSDKAt(t, ctx, yaml, t.TempDir(), newFixedSeedProvider(t), "member")
	defer member.Close()
	applicant := openSDKAt(t, ctx, yaml, t.TempDir(), newFixedSeedProvider(t), "applicant")
	defer applicant.Close()
	sp, err := owner.Spaces().Create(ctx, space.CreateRequest{Name: "Invite sharing"})
	require.NoError(t, err)
	inv, err := sp.ACL().CreateInvite(ctx)
	require.NoError(t, err)
	token, err := space.EncodeInvite(inv)
	require.NoError(t, err)
	same, err := sp.ACL().CreateInvite(ctx)
	require.NoError(t, err)
	sameToken, err := space.EncodeInvite(same)
	require.NoError(t, err)
	require.Equal(t, token, sameToken, "repeated creation must preserve the link")
	require.NoError(t, sp.ACL().AddAccounts(ctx, []space.MemberAdd{{Identity: member.Account().Id(), Permission: space.PermissionWriter}}))
	var joined space.Space
	require.Eventually(t, func() bool {
		joined, err = member.Spaces().AcceptInvite(ctx, sp.Id())
		return err == nil
	}, 90*time.Second, time.Second)

	assertShared := func() {
		t.Helper()
		require.Eventually(t, func() bool {
			_ = joined.SyncHeads(ctx)
			invs, readErr := joined.Members().Invites(ctx)
			return readErr == nil && len(invs) == 1 && invs[0].Key != nil
		}, 45*time.Second, time.Second, "member must recover the owner's invite from the space")
		shared, shareErr := joined.ACL().CreateInvite(ctx)
		require.NoError(t, shareErr)
		got, encodeErr := space.EncodeInvite(shared)
		require.NoError(t, encodeErr)
		require.Equal(t, token, got, "sharing must reuse the existing invite")
	}
	assertShared()
	require.NoError(t, sp.ACL().ChangePermissions(ctx, []space.PermissionChange{{Identity: member.Account().Id(), Permission: space.PermissionReader}}))
	require.Eventually(t, func() bool {
		_ = joined.SyncHeads(ctx)
		me, readErr := joined.Members().Me(ctx)
		return readErr == nil && me.Permission == space.PermissionReader
	}, 45*time.Second, time.Second)
	assertShared()

	// The forwarded token requests access; it does not approve it.
	_, err = applicant.Spaces().Join(ctx, space.JoinRequest{Invite: token})
	require.ErrorIs(t, err, space.ErrJoinPending)
	request := awaitJoinRequest(t, ctx, sp, applicant.Account().Id())
	require.Eventually(t, func() bool {
		_ = joined.SyncHeads(ctx)
		requests, readErr := joined.Members().JoinRequests(ctx)
		return readErr == nil && len(requests) == 1
	}, 45*time.Second, time.Second)
	require.ErrorIs(t, joined.ACL().AcceptRequest(ctx, request.RecordId, space.PermissionReader), space.ErrInsufficientPermissions)
	require.ErrorIs(t, joined.ACL().RevokeAllInvites(ctx), space.ErrInsufficientPermissions)

	// A stale shared key must disappear from the public invite surface.
	require.NoError(t, sp.ACL().RevokeAllInvites(ctx))
	require.Eventually(t, func() bool {
		_ = joined.SyncHeads(ctx)
		invs, readErr := joined.Members().Invites(ctx)
		return readErr == nil && len(invs) == 0
	}, 45*time.Second, time.Second)
	_, err = joined.ACL().CreateInvite(ctx)
	require.ErrorIs(t, err, space.ErrInsufficientPermissions)
	// Rotation invalidates old custody and shares the replacement.
	inv, err = sp.ACL().CreateInvite(ctx)
	require.NoError(t, err)
	rotated, err := space.EncodeInvite(inv)
	require.NoError(t, err)
	require.NotEqual(t, token, rotated)
	token = rotated
	assertShared()

}
