package anysyncsdk_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// strPtr wraps a literal for the pointer-to-string semantics of
// space.SetMetadataRequest. Saves a t.Helper() boilerplate at every
// call site below.
func strPtr(s string) *string { return &s }

// TestSpaceIndex_RoundTrip exercises the single-peer happy path:
// Create stamps name/description/icon into the spaceIndex object
// AND surfaces them in sp.Info(); SetMetadata then renames the
// space and (after the apply→mirror→tech-space-write round-trip)
// the same Info() read reflects the new name.
//
// Skips when staging is unavailable; uses the shared seed provider
// for a one-shot SDK boot.
func TestSpaceIndex_RoundTrip(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{
		Name:        "Original",
		Description: "first description",
		IconCID:     "icon-A",
	})
	require.NoError(t, err)

	// The Create path seeds the spaceIndex object + tsp row in lockstep,
	// so Info() reflects the initial values without waiting.
	info := sp.Info()
	assert.Equal(t, "Original", info.Name)
	assert.Equal(t, "first description", info.Description)
	assert.Equal(t, "icon-A", info.IconCID)

	// SpaceIndexObjectId is the deterministic derived id — non-empty
	// on every peer that has the space loaded.
	assert.NotEmpty(t, sp.SpaceIndexObjectId(), "SpaceIndexObjectId must be populated post-Create")

	// Rename via the new SetMetadata API. Description and icon should
	// stay untouched (nil pointers leave-as-is).
	require.NoError(t, sp.SetMetadata(ctx, space.SetMetadataRequest{
		Name: strPtr("Renamed"),
	}))

	// The watcher mirrors the in-space spaceIndex apply into the
	// tech-space row asynchronously (subscription delivery, not in-
	// line). Poll until Info() reflects the new name.
	require.True(t, waitFor(ctx, 5*time.Second, 50*time.Millisecond, func() bool {
		return sp.Info().Name == "Renamed"
	}), "sp.Info().Name never flipped to Renamed; last=%q", sp.Info().Name)

	// Untouched fields must be preserved.
	info = sp.Info()
	assert.Equal(t, "Renamed", info.Name)
	assert.Equal(t, "first description", info.Description)
	assert.Equal(t, "icon-A", info.IconCID)

	// Service.List sees the same row.
	list, err := sdk.Spaces().List(ctx)
	require.NoError(t, err)
	var found bool
	for _, row := range list {
		if row.Id == sp.Id() {
			found = true
			assert.Equal(t, "Renamed", row.Name)
			assert.Equal(t, "first description", row.Description)
			break
		}
	}
	assert.True(t, found, "Service.List did not contain the renamed space")

	// Patch a different field — description — and confirm name stays
	// the converged "Renamed".
	require.NoError(t, sp.SetMetadata(ctx, space.SetMetadataRequest{
		Description: strPtr("second description"),
	}))
	require.True(t, waitFor(ctx, 5*time.Second, 50*time.Millisecond, func() bool {
		return sp.Info().Description == "second description"
	}))
	assert.Equal(t, "Renamed", sp.Info().Name)
}

// TestE2E_SpaceIndexMultiPeerConvergence is the convergence story
// PROMPT.md calls out: Alice creates a space, shares with Bob via
// RequestToJoin, both see name="Original"; Alice renames to
// "Updated"; Bob's sp.Info() AND Service.List must eventually show
// the new value too.
//
// Long-running (~60-90s) and network-bound; skipped without an
// any-sync config. Mirrors the test scaffolding from
// invite_sync_test.TestE2E_AliceBobInviteAndContent.
func TestE2E_SpaceIndexMultiPeerConvergence(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("multi-peer spaceIndex convergence is slow (~60-90s); rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
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

	// Bob's profile up-front so the owner-side member view renders his
	// display name once the join request lands. Matches the pattern
	// used in TestE2E_AliceBobInviteAndContent.
	require.NoError(t, bob.Account().UpdateMetadata(ctx, space.AccountMetadata{Name: "Bob"}))

	// Alice creates the space with explicit metadata.
	aliceSp, err := alice.Spaces().Create(ctx, space.CreateRequest{
		Name:        "Original",
		Description: "initial",
		IconCID:     "icon-init",
	})
	require.NoError(t, err)

	// Alice mints the invite (with retry — fresh local networks need a
	// moment for the consensus log to exist).
	var inv space.Invite
	if !waitFor(ctx, 30*time.Second, 1*time.Second, func() bool {
		inv, err = aliceSp.ACL().CreateInvite(ctx)
		return err == nil
	}) {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on CreateInvite: %v", err)
		}
		t.Fatalf("alice: CreateInvite: %v", err)
	}
	token, err := space.EncodeInvite(inv)
	require.NoError(t, err)

	// Bob joins. ErrJoinPending is the success path.
	_, err = bob.Spaces().Join(ctx, space.JoinRequest{
		Invite:   token,
		Metadata: space.AccountMetadata{Name: "Bob"},
	})
	require.True(t, errors.Is(err, spaceimpl.ErrJoinPending),
		"bob: Join must return ErrJoinPending, got %v", err)

	// Alice sees the request, accepts.
	var joinReq space.JoinRequestInfo
	require.True(t, waitFor(ctx, 90*time.Second, 1*time.Second, func() bool {
		reqs, _ := aliceSp.Members().JoinRequests(ctx)
		if len(reqs) > 0 {
			joinReq = reqs[0]
			return true
		}
		return false
	}), "alice never saw bob's join request")
	require.NoError(t, aliceSp.ACL().AcceptRequest(ctx, joinReq.RecordId, space.PermissionWriter))

	// Bob acquires a handle to the space.
	var bobSp space.Space
	require.True(t, waitFor(ctx, 60*time.Second, 1*time.Second, func() bool {
		bobSp, err = bob.Spaces().Get(ctx, aliceSp.Id())
		return err == nil
	}), "bob: Spaces().Get(%s) never succeeded: %v", aliceSp.Id(), err)

	// Step 1: Bob must converge on Alice's initial metadata (the
	// spaceIndex tree pulled via background sync, then the watcher
	// mirrors into Bob's tech-space row).
	require.True(t, waitFor(ctx, 3*time.Minute, 1*time.Second, func() bool {
		return bobSp.Info().Name == "Original"
	}), "bob never converged on initial name; last=%q", bobSp.Info().Name)
	assert.Equal(t, "initial", bobSp.Info().Description)
	assert.Equal(t, "icon-init", bobSp.Info().IconCID)

	// Bob's Service.List must also reflect the converged name.
	bobList, err := bob.Spaces().List(ctx)
	require.NoError(t, err)
	var listed bool
	for _, row := range bobList {
		if row.Id == aliceSp.Id() {
			listed = true
			assert.Equal(t, "Original", row.Name)
		}
	}
	assert.True(t, listed, "bob's Service.List did not contain the shared space")

	// Step 2: Alice renames the space. Bob's existing handle must
	// eventually surface the new name.
	require.NoError(t, aliceSp.SetMetadata(ctx, space.SetMetadataRequest{
		Name: strPtr("Updated"),
	}))

	// Alice's own Info — local mirror should converge very quickly.
	require.True(t, waitFor(ctx, 10*time.Second, 50*time.Millisecond, func() bool {
		return aliceSp.Info().Name == "Updated"
	}), "alice did not see her own rename")

	// Bob's converged view — requires DAG sync of the spaceIndex tree
	// across the network. Generous deadline.
	require.True(t, waitFor(ctx, 3*time.Minute, 1*time.Second, func() bool {
		return bobSp.Info().Name == "Updated"
	}), "bob never converged on Updated; last=%q", bobSp.Info().Name)

	// Service.List on Bob must also reflect the rename.
	require.True(t, waitFor(ctx, 30*time.Second, 500*time.Millisecond, func() bool {
		list, err := bob.Spaces().List(ctx)
		if err != nil {
			return false
		}
		for _, row := range list {
			if row.Id == aliceSp.Id() {
				return row.Name == "Updated"
			}
		}
		return false
	}), "bob's Service.List never reflected the rename")
}
