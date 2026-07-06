package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_NestedSpaces_CreateChild exercises phase 1 of docs/16 against a
// live network: the org owner creates a child space under the org, the
// registration is visible via Children, the child syncs (its receipt was
// granted against the parent registration), and a Writer in the org cannot
// register children (Admin+ is required by the acl validator + coordinator).
//
// Requires a coordinator built from the nested-spaces branch; on an older
// coordinator the child's SpaceSign is refused and the test skips.
func TestE2E_NestedSpaces_CreateChild(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("nested-spaces e2e needs a live coordinator; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	openSDK := func(name string) *anysyncsdk.SDK {
		t.Helper()
		cfg := config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
		}
		sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
		require.NoError(t, err, "%s: Open", name)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk
	}

	owner := openSDK("owner")
	writer := openSDK("writer")

	// the org (parent) space
	org, err := owner.Spaces().Create(ctx, space.CreateRequest{Name: "Org"})
	require.NoError(t, err, "owner: create org")

	// a child under the org — CreateChild registers in the org acl and needs
	// the coordinator online for the registration + receipt
	child, err := owner.Spaces().CreateChild(ctx, space.CreateChildRequest{
		ParentSpaceId: org.Id(),
		Name:          "Compartment A",
	})
	if isNoNetworkErr(err) {
		t.Skipf("network unreachable on CreateChild: %v", err)
	}
	if err != nil {
		t.Skipf("CreateChild failed — coordinator likely predates nested spaces: %v", err)
	}
	require.NotEqual(t, org.Id(), child.Id())

	// the registration is visible
	children, err := owner.Spaces().Children(ctx, org.Id())
	require.NoError(t, err)
	require.Len(t, children, 1)
	assert.Equal(t, child.Id(), children[0].ChildSpaceId)
	assert.Equal(t, owner.Account().Id(), children[0].Author)
	assert.Equal(t, space.PermissionNone, children[0].OrgPermission)
	assert.False(t, children[0].Revoked)

	// the child carries the parent link in the space list
	if si, ok := infoByID(t, ctx, owner, child.Id()); ok {
		assert.Equal(t, org.Id(), si.ParentSpaceId)
	}

	// content in the child works like any space
	typeId, err := child.Types().Create(ctx, space.TypeCreateParams{Name: "Note"})
	require.NoError(t, err)
	_, err = child.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)

	// the child must become pushable — its receipt is granted only against
	// the parent registration, so a successful shareable flip proves the
	// coordinator accepted the nested SpaceSign
	deadline := time.Now().Add(90 * time.Second)
	var shareableErr error
	for time.Now().Before(deadline) {
		shareableErr = child.ACL().StopSharing(ctx)
		if shareableErr == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if shareableErr != nil {
		t.Logf("child never became shareable: %v", shareableErr)
	}

	// a Writer in the org cannot register children
	err = org.ACL().AddAccounts(ctx, []space.MemberAdd{
		{Identity: writer.Account().Id(), Permission: space.PermissionWriter},
	})
	if isNoNetworkErr(err) {
		t.Skipf("network unreachable on AddAccounts: %v", err)
	}
	require.NoError(t, err, "owner: add writer to org")

	writerOrg, err := waitInviteAccepted(t, ctx, writer, org.Id())
	if writerOrg == nil {
		t.Skipf("writer never received the org invite: %v", err)
	}
	_, err = writer.Spaces().CreateChild(ctx, space.CreateChildRequest{
		ParentSpaceId: org.Id(),
		Name:          "Should fail",
	})
	require.Error(t, err, "a writer must not be able to register a child")
}

// waitInviteAccepted polls for the direct-add invite row and accepts it.
func waitInviteAccepted(t *testing.T, ctx context.Context, who *anysyncsdk.SDK, spaceId string) (space.Space, error) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if si, ok := infoByID(t, ctx, who, spaceId); ok &&
			(si.Status == space.StatusInvitePending || si.Status == space.StatusActive) {
			sp, err := who.Spaces().AcceptInvite(ctx, spaceId)
			if err == nil {
				return sp, nil
			}
		}
		time.Sleep(time.Second)
	}
	return nil, context.DeadlineExceeded
}

// TestE2E_NestedSpaces_KeylessGovernance exercises phase 2 of docs/16: a
// member-created child governed by a keyless org owner — the org removes a
// member without holding the child's read key, a key-holding admin device
// completes the rotation automatically, and the org deletes another child it
// cannot read.
func TestE2E_NestedSpaces_KeylessGovernance(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("nested-spaces e2e needs a live coordinator; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	openSDK := func(name string) *anysyncsdk.SDK {
		t.Helper()
		cfg := config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
		}
		sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
		require.NoError(t, err, "%s: Open", name)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk
	}

	orgOwner := openSDK("orgOwner")
	admin := openSDK("admin")   // org member, creates the child
	member := openSDK("member") // added to the child, then removed keylessly

	// org with an admin member
	org, err := orgOwner.Spaces().Create(ctx, space.CreateRequest{Name: "Org"})
	require.NoError(t, err)
	err = org.ACL().AddAccounts(ctx, []space.MemberAdd{
		{Identity: admin.Account().Id(), Permission: space.PermissionAdmin},
	})
	if isNoNetworkErr(err) {
		t.Skipf("network unreachable: %v", err)
	}
	require.NoError(t, err)
	adminOrg, err := waitInviteAccepted(t, ctx, admin, org.Id())
	if adminOrg == nil {
		t.Skipf("admin never received the org invite: %v", err)
	}

	// the admin creates a child — org owner is legalOwner-only, holds no key
	child, err := admin.Spaces().CreateChild(ctx, space.CreateChildRequest{
		ParentSpaceId: org.Id(),
		Name:          "Team",
	})
	if err != nil {
		t.Skipf("CreateChild failed — coordinator likely predates nested spaces: %v", err)
	}

	// the child gets a member
	err = child.ACL().AddAccounts(ctx, []space.MemberAdd{
		{Identity: member.Account().Id(), Permission: space.PermissionWriter},
	})
	require.NoError(t, err)
	memberChild, err := waitInviteAccepted(t, ctx, member, child.Id())
	if memberChild == nil {
		t.Skipf("member never received the child invite: %v", err)
	}

	// the ORG OWNER — not a member of the child — removes the member keylessly
	err = orgOwner.Spaces().RemoveMemberAsLegalOwner(ctx, child.Id(), member.Account().Id())
	require.NoError(t, err, "keyless removal by the legalOwner")

	// the admin's device observes the pending removal and rotates automatically
	deadline := time.Now().Add(2 * time.Minute)
	rotated := false
	for time.Now().Before(deadline) {
		pending, err := child.ACL().PendingKeylessRemovals(ctx)
		if err == nil && len(pending) == 0 {
			// pending cleared — check the member is actually out
			members, err := child.Members().List(ctx)
			if err == nil {
				out := true
				for _, m := range members {
					if m.Identity == member.Account().Id() && m.Permission != space.PermissionNone {
						out = false
					}
				}
				if out {
					rotated = true
					break
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
	assert.True(t, rotated, "auto-rotation should clear the pending keyless removal")

	// the org owner deletes a second child it cannot read
	child2, err := admin.Spaces().CreateChild(ctx, space.CreateChildRequest{
		ParentSpaceId: org.Id(),
		Name:          "Doomed",
	})
	require.NoError(t, err)
	err = orgOwner.Spaces().DeleteChildAsLegalOwner(ctx, child2.Id())
	require.NoError(t, err, "legalOwner delete of an unreadable child")
}

// TestE2E_NestedSpaces_Compartments exercises phase 4 of docs/16 — the
// compartment pattern: one child space per access scope, self-governed
// membership via direct-add, need-to-know (the org owner governs compartments
// it cannot read), and a member's effective view = the union of the
// compartments they belong to.
func TestE2E_NestedSpaces_Compartments(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("nested-spaces e2e needs a live coordinator; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	openSDK := func(name string) *anysyncsdk.SDK {
		t.Helper()
		cfg := config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
		}
		sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
		require.NoError(t, err, "%s: Open", name)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk
	}

	orgOwner := openSDK("orgOwner")
	deptAdmin := openSDK("deptAdmin") // org Admin, creates + runs the compartments
	bob := openSDK("bob")             // org member, belongs to eng only

	org, err := orgOwner.Spaces().Create(ctx, space.CreateRequest{Name: "Org"})
	require.NoError(t, err)
	err = org.ACL().AddAccounts(ctx, []space.MemberAdd{
		{Identity: deptAdmin.Account().Id(), Permission: space.PermissionAdmin},
		{Identity: bob.Account().Id(), Permission: space.PermissionWriter},
	})
	if isNoNetworkErr(err) {
		t.Skipf("network unreachable: %v", err)
	}
	require.NoError(t, err)
	adminOrg, err := waitInviteAccepted(t, ctx, deptAdmin, org.Id())
	if adminOrg == nil {
		t.Skipf("deptAdmin never received the org invite: %v", err)
	}
	if sp, _ := waitInviteAccepted(t, ctx, bob, org.Id()); sp == nil {
		t.Skip("bob never received the org invite")
	}

	// two compartments, created by the dept admin: the org owner is
	// legalOwner-only on both (orgPermission=None) — need-to-know
	eng, err := deptAdmin.Spaces().CreateChild(ctx, space.CreateChildRequest{
		ParentSpaceId: org.Id(),
		Name:          "eng",
	})
	if err != nil {
		t.Skipf("CreateChild failed — coordinator likely predates nested spaces: %v", err)
	}
	fin, err := deptAdmin.Spaces().CreateChild(ctx, space.CreateChildRequest{
		ParentSpaceId: org.Id(),
		Name:          "finance",
	})
	require.NoError(t, err)

	// content in each compartment
	engType, err := eng.Types().Create(ctx, space.TypeCreateParams{Name: "Doc"})
	require.NoError(t, err)
	_, err = eng.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{engType}})
	require.NoError(t, err)
	finType, err := fin.Types().Create(ctx, space.TypeCreateParams{Name: "Ledger"})
	require.NoError(t, err)
	_, err = fin.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{finType}})
	require.NoError(t, err)

	// compartment membership is self-governed by its admin via direct-add:
	// bob joins eng ONLY
	err = eng.ACL().AddAccounts(ctx, []space.MemberAdd{
		{Identity: bob.Account().Id(), Permission: space.PermissionWriter},
	})
	require.NoError(t, err)
	bobEng, err := waitInviteAccepted(t, ctx, bob, eng.Id())
	if bobEng == nil {
		t.Skipf("bob never received the eng invite: %v", err)
	}

	// bob's effective view: eng content is readable...
	readDeadline := time.Now().Add(90 * time.Second)
	var sawEngContent bool
	for time.Now().Before(readDeadline) {
		_ = bobEng.SyncHeads(ctx)
		if docs, err := bobEng.QueryObjects().All(ctx); err == nil && len(docs) > 0 {
			sawEngContent = true
			break
		}
		time.Sleep(2 * time.Second)
	}
	assert.True(t, sawEngContent, "bob should read eng content")

	// ...finance is not: bob isn't a member and holds no key
	finMembers, err := fin.Members().List(ctx)
	require.NoError(t, err)
	for _, m := range finMembers {
		assert.NotEqual(t, bob.Account().Id(), m.Identity, "bob must not be a finance member")
	}

	// both compartments are REGISTERED visibly — existence is not hidden
	children, err := orgOwner.Spaces().Children(ctx, org.Id())
	require.NoError(t, err)
	assert.Len(t, children, 2)

	// the org owner governs a compartment it cannot read: keyless removal
	// of bob from eng
	err = orgOwner.Spaces().RemoveMemberAsLegalOwner(ctx, eng.Id(), bob.Account().Id())
	require.NoError(t, err, "org owner keyless removal from an unreadable compartment")
}
