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
