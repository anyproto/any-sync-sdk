package e2e

import (
	"bytes"
	"context"
	"crypto/ed25519"
	crand "crypto/rand"
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

	// closed records the SDKs the test closed itself: Close is not
	// idempotent, so the cleanup skips them.
	closed := map[*anysyncsdk.SDK]bool{}
	mkSDK := func(name, dataDir string, provider *fixedSeedProvider, files config.Files) *anysyncsdk.SDK {
		t.Helper()
		cfg := config.Config{
			Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: netYaml},
			Files:   files,
		}
		sdk, err := anysyncsdk.Open(ctx, cfg, provider)
		require.NoError(t, err, "%s: Open", name)
		t.Cleanup(func() {
			if !closed[sdk] {
				_ = sdk.Close()
			}
		})
		return sdk
	}

	ownerKeys := newFixedSeedProvider(t)
	owner := mkSDK("owner", t.TempDir(), ownerKeys, config.Files{})
	ownerId := owner.Account().Id()
	cdn := newBlobCDN(t, ctx, owner, fileV2Peers, owner.TechSpaceId())
	t.Cleanup(cdn.Close)
	joiner := mkSDK("joiner", t.TempDir(), newFixedSeedProvider(t), config.Files{PublicReadBaseUrl: cdn.URL})
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

	// A second device of the owner (same account key, its own device
	// key): it syncs the profile row and then the avatar's file row,
	// serves the avatar itself, and its boot republish must not drop
	// the avatar for the contact.
	_, device2, err := ed25519.GenerateKey(crand.Reader)
	require.NoError(t, err)
	device2Keys := &fixedSeedProvider{account: ownerKeys.account, device: device2}
	device2Dir := t.TempDir()
	// From here on the contact must never lose the avatar: not while
	// the second device syncs the row (its mirror must wait for the
	// file row rather than publish without the icon) and not across its
	// restart. The directory refreshes on a 60 s tick, so the monitor
	// samples what the joiner shows rather than every put.
	monitorCtx, cancelMonitor := context.WithCancel(ctx)
	monitorDone := make(chan struct{})
	dropped := make(chan space.IdentityInfo, 1)
	go func() {
		defer close(monitorDone)
		tick := time.NewTicker(2 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-monitorCtx.Done():
				return
			case <-tick.C:
			}
			info, ok, _ := joiner.Identities().Get(monitorCtx, ownerId)
			if ok && info.IconCID != meta.IconCID {
				select {
				case dropped <- info:
				default:
				}
			}
		}
	}()
	stopMonitor := func() {
		cancelMonitor()
		<-monitorDone
	}
	t.Cleanup(stopMonitor) // a failed require must not leave it reading a closing joiner
	owner2 := mkSDK("owner-2", device2Dir, device2Keys, config.Files{PublicReadBaseUrl: cdn.URL})
	require.Eventually(t, func() bool {
		m, present, err := owner2.Account().Metadata(ctx)
		return err == nil && present && m.IconCID == meta.IconCID
	}, 120*time.Second, 2*time.Second, "the second device should sync the profile row with the avatar")
	require.Eventually(t, func() bool {
		rd, err := owner2.Account().OpenIcon(ctx)
		if err != nil {
			return false
		}
		got, err = io.ReadAll(rd)
		_ = rd.Close()
		return err == nil && bytes.Equal(got, icon)
	}, 120*time.Second, 2*time.Second, "the second device should serve the avatar once its file row syncs")
	// Restart it: now the boot republish runs with the synced row, and
	// the contact — whose directory refreshes on a 60 s tick — must
	// keep the avatar across that.
	closed[owner2] = true
	require.NoError(t, owner2.Close())
	owner2 = mkSDK("owner-2 again", device2Dir, device2Keys, config.Files{PublicReadBaseUrl: cdn.URL})
	select {
	case info := <-dropped:
		t.Fatalf("the contact lost the avatar while the second device came up or restarted: %+v", info)
	case <-time.After(75 * time.Second):
	}
	stopMonitor()
	select {
	case info := <-dropped:
		t.Fatalf("the contact lost the avatar right before the window closed: %+v", info)
	default:
	}
	rd, err = owner2.Account().OpenIcon(ctx)
	require.NoError(t, err)
	_ = rd.Close()

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
	require.Eventually(t, func() bool {
		m, _, err := owner2.Account().Metadata(ctx)
		return err == nil && m.IconCID == ""
	}, 90*time.Second, 2*time.Second, "the second device should see the avatar removed")
}
