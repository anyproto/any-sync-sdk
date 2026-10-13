package e2e

import (
	"bytes"
	"context"
	"io"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_AccountIcon_ContactFetch is the end-to-end proof of avatars
// against a live local network:
//
//   - the owner stores an avatar; it reads back through the directory
//     and survives a name update, while UpdateMetadata refuses an
//     IconCID of its own;
//   - a co-member resolves the owner's profile with the avatar
//     reference once the file is durable, and fetches the bytes by
//     rootCid through the public read path — never joining the owner's
//     tech space;
//   - deleting the avatar clears it on both sides.
//
// The local object store is private, so the public read surface is
// the same stand-in the files download e2e uses (newBlobCDN), bound to
// the owner's tech space and handed to the joiner as its public base.
func TestE2E_AccountIcon_ContactFetch(t *testing.T) {
	t.Parallel()
	netYaml, fileV2Peers, _ := loadLocalFilesV2Network(t)
	if testing.Short() {
		t.Skip("avatar e2e is slow; rerun without -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	mkSDK := func(name string, files config.Files) *anysyncsdk.SDK {
		t.Helper()
		cfg := config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: netYaml},
			Files:   files,
		}
		sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
		require.NoError(t, err, "%s: Open", name)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk
	}

	owner := mkSDK("owner", config.Files{})
	ownerId := owner.Account().Id()
	cdn := newBlobCDN(t, ctx, owner, fileV2Peers, owner.TechSpaceId())
	t.Cleanup(cdn.Close)
	joiner := mkSDK("joiner", config.Files{PublicReadBaseUrl: cdn.URL})
	require.NotEqual(t, ownerId, joiner.Account().Id())

	// Under the inline cutoff on purpose: the avatar path pins the full
	// tier regardless, so this is the interesting size.
	icon := make([]byte, 2048)
	_, _ = mrand.New(mrand.NewSource(146)).Read(icon)

	meta, err := owner.Account().SetIcon(ctx, bytes.NewReader(icon), space.IconOpts{Name: "avatar.png", Mime: "image/png"})
	if err != nil && isNoNetworkErr(err) {
		t.Skipf("network unreachable: %v", err)
	}
	require.NoError(t, err)
	require.NotEmpty(t, meta.IconCID, "SetIcon returns the avatar's content address")

	// Own read-back, through the directory surface.
	rd, err := owner.Identities().OpenIcon(ctx, ownerId)
	require.NoError(t, err)
	got, err := io.ReadAll(rd)
	require.NoError(t, rd.Close())
	require.NoError(t, err)
	assert.Equal(t, icon, got)

	// A name update keeps the avatar; the icon is not UpdateMetadata's.
	require.NoError(t, owner.Account().UpdateMetadata(ctx, space.AccountMetadata{Name: "Owner Name"}))
	stored, present, err := owner.Account().Metadata(ctx)
	require.NoError(t, err)
	require.True(t, present)
	assert.Equal(t, space.AccountMetadata{Name: "Owner Name", IconCID: meta.IconCID}, stored)
	err = owner.Account().UpdateMetadata(ctx, space.AccountMetadata{Name: "x", IconCID: "bafy"})
	assert.ErrorIs(t, err, space.ErrProfileIconReadOnly)

	// Oversized input is refused before anything is stored.
	_, err = owner.Account().SetIcon(ctx, bytes.NewReader(make([]byte, space.MaxIconSize+1)), space.IconOpts{})
	assert.ErrorIs(t, err, space.ErrIconTooLarge)
	stored, _, err = owner.Account().Metadata(ctx)
	require.NoError(t, err)
	assert.Equal(t, meta.IconCID, stored.IconCID, "a refused upload leaves the avatar alone")

	// Share a space so the joiner holds the owner's symkey.
	sp, err := owner.Spaces().Create(ctx, space.CreateRequest{Name: "Avatar E2E"})
	require.NoError(t, err)
	spaceId := sp.Id()
	inv, err := sp.ACL().CreateInvite(ctx)
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable: %v", err)
		}
		t.Fatalf("CreateInvite: %v", err)
	}
	token, err := space.EncodeInvite(inv)
	require.NoError(t, err)
	cancelSub := sp.Members().Subscribe(func(space.MemberEvent) {})
	t.Cleanup(cancelSub)

	_, err = joiner.Spaces().Join(ctx, space.JoinRequest{Invite: token})
	require.ErrorIs(t, err, spaceimpl.ErrJoinPending)
	var joinReq space.JoinRequestInfo
	require.Eventually(t, func() bool {
		_ = sp.SyncHeads(ctx)
		reqs, err := sp.Members().JoinRequests(ctx)
		if err != nil || len(reqs) == 0 {
			return false
		}
		joinReq = reqs[0]
		return true
	}, 120*time.Second, 2*time.Second, "owner never saw the join request")
	require.NoError(t, sp.ACL().AcceptRequest(ctx, joinReq.RecordId, space.PermissionWriter))

	// The joiner resolves the owner's profile; the avatar reference
	// arrives once the file's backup is confirmed and the profile is
	// republished with it.
	require.Eventually(t, func() bool {
		jsp, err := joiner.Spaces().Get(ctx, spaceId)
		if err != nil {
			return false
		}
		jsp.Members().Subscribe(func(space.MemberEvent) {})()
		info, ok, _ := joiner.Identities().Get(ctx, ownerId)
		return ok && info.Name == "Owner Name" && info.IconCID == meta.IconCID
	}, 180*time.Second, 2*time.Second, "joiner directory should resolve the owner's name and avatar cid")

	rd, err = joiner.Identities().OpenIcon(ctx, ownerId)
	require.NoError(t, err, "a contact opens the avatar by rootCid")
	got, err = io.ReadAll(rd)
	require.NoError(t, rd.Close())
	require.NoError(t, err)
	assert.Equal(t, icon, got, "the contact decrypts the owner's avatar with the key from the profile")
	assert.Positive(t, cdn.Requests(), "the contact fetched through the public read path")

	// Cached: a second open serves locally.
	before := cdn.Requests()
	rd, err = joiner.Identities().OpenIcon(ctx, ownerId)
	require.NoError(t, err)
	_, err = io.ReadAll(rd)
	require.NoError(t, rd.Close())
	require.NoError(t, err)
	assert.Equal(t, before, cdn.Requests(), "a cached avatar is not refetched")

	// Delete: the owner has none, and the joiner's directory follows.
	require.NoError(t, owner.Account().DeleteIcon(ctx))
	stored, _, err = owner.Account().Metadata(ctx)
	require.NoError(t, err)
	assert.Empty(t, stored.IconCID)
	_, err = owner.Identities().OpenIcon(ctx, ownerId)
	assert.ErrorIs(t, err, space.ErrNotFound)
	require.Eventually(t, func() bool {
		info, ok, _ := joiner.Identities().Get(ctx, ownerId)
		return ok && info.Name == "Owner Name" && info.IconCID == ""
	}, 90*time.Second, 2*time.Second, "joiner directory should drop the avatar cid")
	_, err = joiner.Identities().OpenIcon(ctx, ownerId)
	assert.ErrorIs(t, err, space.ErrNotFound)
}
