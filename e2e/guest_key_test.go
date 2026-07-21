package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_GuestKeyLifecycle drives the whole public-access (guest key)
// mechanic against a live network:
//
//  1. Alice creates a space with a type, an object, and a property
//     value, then mints a guest key (CreateGuestKey — ACL AccountsAdd
//     with Guest permission + owner-side custody).
//  2. CreateGuestKey is idempotent: a second call returns the same key.
//  3. Bob (distinct account, never on the ACL himself) adds the space
//     via JoinGuest with the encoded guest invite — no join request, no
//     approval — and converges on Alice's content. This is also the
//     protocol spike: Bob's device pulls and decrypts the space while
//     handshaking with his real peer key and signing space-level as the
//     shared guest identity.
//  4. Read-only: Bob's creates and property writes fail with
//     ErrReadOnlySpace; his Info reports OwnRole=guest.
//  5. Alice revokes; Bob's row flips to StatusGuestRevoked while his
//     local copy stays readable.
//  6. A fresh CreateGuestKey mints a different identity (old invites
//     are dead), and Bob can drop the space locally with Delete.
//
// Skips when no any-sync network is reachable; slow (multiple headsync
// windows) — run without -short.
func TestE2E_GuestKeyLifecycle(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("guest-key e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
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

	alice := openSDK("alice")
	bob := openSDK("bob")
	require.NotEqual(t, alice.Account().Id(), bob.Account().Id())

	sp, err := alice.Spaces().Create(ctx, space.CreateRequest{Name: "GuestLib"})
	require.NoError(t, err, "alice: Spaces().Create")

	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Doc"})
	require.NoError(t, err)
	propId, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Title", XKey: "title", Kind: space.PropertyKindString,
	})
	require.NoError(t, err)
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	_, err = sp.Properties().Set(ctx, objId, typeId, map[string]any{propId: "Casablanca"})
	require.NoError(t, err)

	// Mint the guest key. Same freshly-bootstrapped-network caveat as
	// CreateInvite: retry while the coordinator/consensus catch up.
	var inv space.Invite
	if !waitFor(ctx, 60*time.Second, time.Second, func() bool {
		inv, err = sp.ACL().CreateGuestKey(ctx)
		return err == nil
	}) {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on CreateGuestKey: %v", err)
		}
		t.Fatalf("alice: CreateGuestKey: %v", err)
	}
	require.Equal(t, space.InviteKindGuest, inv.Kind)
	require.Equal(t, sp.Id(), inv.SpaceId)
	keyBytes, err := inv.InviteKey.Marshall()
	require.NoError(t, err)

	// Idempotent: the custody row returns the same key.
	inv2, err := sp.ACL().CreateGuestKey(ctx)
	require.NoError(t, err, "alice: second CreateGuestKey")
	key2Bytes, err := inv2.InviteKey.Marshall()
	require.NoError(t, err)
	assert.Equal(t, keyBytes, key2Bytes, "CreateGuestKey must be idempotent")

	token, err := space.EncodeInvite(inv)
	require.NoError(t, err)

	// Bob joins as guest. Immediate load or pending — both are success.
	bobSpace, err := bob.Spaces().JoinGuest(ctx, token)
	if err != nil {
		require.True(t, errors.Is(err, space.ErrGuestJoinPending),
			"bob: JoinGuest must return the space or ErrGuestJoinPending, got %v", err)
	}
	if !waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = bob.Spaces().SyncSpaceList(ctx)
		infos, listErr := bob.Spaces().List(ctx)
		if listErr != nil {
			return false
		}
		for _, si := range infos {
			if si.Id == sp.Id() && si.Status == space.StatusActive {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("bob's guest space never flipped to active")
	}
	if bobSpace == nil {
		require.True(t, waitFor(ctx, 60*time.Second, time.Second, func() bool {
			bobSpace, err = bob.Spaces().Get(ctx, sp.Id())
			return err == nil
		}), "bob: Get after guest load: %v", err)
	}

	// Content convergence: type, object, property value.
	converged := waitFor(ctx, 3*time.Minute, 2*time.Second, func() bool {
		_ = bobSpace.SyncHeads(ctx)
		if !containsString(userTypeIds(bobSpace, ctx), typeId) {
			return false
		}
		rec, err := bobSpace.Properties().Get(ctx, objId)
		if err != nil || rec == nil {
			return false
		}
		return rec.GetString(typeId, propId) == "Casablanca"
	})
	require.True(t, converged, "bob: guest content never converged")

	// Read-only enforcement, typed.
	_, err = bobSpace.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	assert.ErrorIs(t, err, space.ErrReadOnlySpace, "guest Objects().Create must be gated")
	_, err = bobSpace.Properties().Set(ctx, objId, typeId, map[string]any{propId: "Vertigo"})
	assert.ErrorIs(t, err, space.ErrReadOnlySpace, "guest Properties().Set must be gated")

	// OwnRole mirrors the shared guest identity's permission.
	if !waitFor(ctx, 60*time.Second, time.Second, func() bool {
		return bobSpace.Info().OwnRole == space.PermissionGuest
	}) {
		t.Fatalf("bob: OwnRole never mirrored to guest, got %v", bobSpace.Info().OwnRole)
	}

	// Revoke. Bob's mirror flips the row to StatusGuestRevoked on the
	// next ACL sync; his local copy stays readable.
	require.NoError(t, sp.ACL().RevokeGuestKey(ctx), "alice: RevokeGuestKey")
	if !waitFor(ctx, 2*time.Minute, 2*time.Second, func() bool {
		_ = bobSpace.SyncHeads(ctx)
		infos, listErr := bob.Spaces().List(ctx)
		if listErr != nil {
			return false
		}
		for _, si := range infos {
			if si.Id == sp.Id() {
				return si.Status == space.StatusGuestRevoked
			}
		}
		return false
	}) {
		t.Fatalf("bob's guest space never flipped to revoked")
	}
	rec, err := bobSpace.Properties().Get(ctx, objId)
	require.NoError(t, err, "bob: local read after revoke")
	require.NotNil(t, rec)
	assert.Equal(t, "Casablanca", rec.GetString(typeId, propId),
		"bob: local copy must stay readable after revoke")

	// Fresh key = fresh identity; the old invite is dead for good.
	var inv3 space.Invite
	if !waitFor(ctx, 60*time.Second, time.Second, func() bool {
		inv3, err = sp.ACL().CreateGuestKey(ctx)
		return err == nil
	}) {
		t.Fatalf("alice: CreateGuestKey after revoke: %v", err)
	}
	key3Bytes, err := inv3.InviteKey.Marshall()
	require.NoError(t, err)
	assert.NotEqual(t, keyBytes, key3Bytes, "re-created guest key must be a fresh identity")

	// Bob drops the space. The row stays in List with the (non-terminal)
	// guest delete marker — assert it is present AND deleted.
	require.NoError(t, bob.Spaces().Delete(ctx, sp.Id()), "bob: Delete guest space")
	infos, err := bob.Spaces().List(ctx)
	require.NoError(t, err)
	var found bool
	for _, si := range infos {
		if si.Id == sp.Id() {
			found = true
			assert.Equal(t, space.StatusDeleted, si.Status, "guest space must be locally deleted")
		}
	}
	require.True(t, found, "deleted guest space must stay listed while deleted")

	// Re-join after delete: the guest delete marker is non-terminal, and
	// the fresh invite carries the ROTATED identity, so this also covers
	// the key-refresh path (row still holds the revoked key). Content
	// must re-converge from scratch — the delete offloaded local state.
	token3, err := space.EncodeInvite(inv3)
	require.NoError(t, err)
	if _, err := bob.Spaces().JoinGuest(ctx, token3); err != nil {
		require.True(t, errors.Is(err, space.ErrGuestJoinPending),
			"bob: re-JoinGuest must return the space or ErrGuestJoinPending, got %v", err)
	}
	if !waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = bob.Spaces().SyncSpaceList(ctx)
		infos, listErr := bob.Spaces().List(ctx)
		if listErr != nil {
			return false
		}
		for _, si := range infos {
			if si.Id == sp.Id() && si.Status == space.StatusActive {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("bob's re-joined guest space never flipped back to active")
	}
	var bobSpace2 space.Space
	require.True(t, waitFor(ctx, 60*time.Second, time.Second, func() bool {
		bobSpace2, err = bob.Spaces().Get(ctx, sp.Id())
		return err == nil
	}), "bob: Get after re-join: %v", err)
	reconverged := waitFor(ctx, 3*time.Minute, 2*time.Second, func() bool {
		_ = bobSpace2.SyncHeads(ctx)
		rec, rErr := bobSpace2.Properties().Get(ctx, objId)
		return rErr == nil && rec != nil && rec.GetString(typeId, propId) == "Casablanca"
	})
	require.True(t, reconverged, "bob: content never re-converged after rejoin")
	_, err = bobSpace2.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	assert.ErrorIs(t, err, space.ErrReadOnlySpace, "re-joined guest space must stay read-only")
}
