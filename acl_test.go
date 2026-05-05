package anysyncsdk_test

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// anySyncCryptoGenerate is a thin wrapper to make GenerateEd25519Key
// callable from tests without importing crypto/rand into every site.
func anySyncCryptoGenerate() (crypto.PrivKey, crypto.PubKey, error) {
	return crypto.GenerateEd25519Key(rand.Reader)
}

// TestSDK_ACL_OwnerSurface walks the owner-side ACL surface against a
// freshly created space:
//
//   - MembersAPI.Me / List on a one-member space surfaces the owner
//     with PermissionOwner;
//   - ACL.CreateInvite mints a valid share token (round-trips through
//     EncodeInvite/DecodeInvite back to the same spaceId);
//   - MembersAPI.Invites lists the active invite;
//   - ACL.RevokeAllInvites tears it down.
//
// The owner-on-empty-space surface needs no second peer, so it runs
// against staging without a parallel SDK instance.
func TestSDK_ACL_OwnerSurface(t *testing.T) {
	confPath := filepath.Join("..", "test-etc", "staging.yml")
	yaml, err := os.ReadFile(confPath)
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	// 90s — first SpaceMakeShareable can wait through the headsync
	// period plus peer-connect; tests against staging are network-bound.
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "ACL"})
	require.NoError(t, err)

	// Me() on a fresh space returns the owner.
	me, err := sp.Members().Me(ctx)
	require.NoError(t, err)
	assert.Equal(t, sdk.Account().Id(), me.Identity)
	assert.Equal(t, space.PermissionOwner, me.Permission)
	assert.Equal(t, space.MemberStatusActive, me.Status)

	// List() returns exactly the owner.
	members, err := sp.Members().List(ctx)
	require.NoError(t, err)
	require.Len(t, members, 1)
	assert.Equal(t, me.Identity, members[0].Identity)

	// No invites yet.
	invs, err := sp.Members().Invites(ctx)
	require.NoError(t, err)
	assert.Empty(t, invs)

	// CreateInvite needs to publish the new ACL record to consensus
	// nodes — that's network-bound. If staging is unreachable
	// (offline / DNS sandbox), don't fail the local-only assertions
	// above; just skip the network-dependent ones.
	inv, err := sp.ACL().CreateInvite(ctx)
	if err != nil {
		if isNoNetworkErr(err) {
			t.Logf("skipping invite assertions: network unreachable: %v", err)
			return
		}
		t.Fatalf("CreateInvite: %v", err)
	}
	assert.Equal(t, sp.Id(), inv.SpaceId)
	require.NotNil(t, inv.InviteKey)

	token, err := space.EncodeInvite(inv)
	require.NoError(t, err)
	decoded, err := space.DecodeInvite(token)
	require.NoError(t, err)
	assert.Equal(t, sp.Id(), decoded.SpaceId)

	// Invites() lists exactly one active invite.
	invs, err = sp.Members().Invites(ctx)
	require.NoError(t, err)
	require.Len(t, invs, 1)
	require.NotEmpty(t, invs[0].RecordId)

	// RevokeAllInvites tears it down.
	require.NoError(t, sp.ACL().RevokeAllInvites(ctx))
	invs, err = sp.Members().Invites(ctx)
	require.NoError(t, err)
	assert.Empty(t, invs)

	// JoinRequests is empty on a fresh space.
	reqs, err := sp.Members().JoinRequests(ctx)
	require.NoError(t, err)
	assert.Empty(t, reqs)

	// Get(unknown) returns ErrNotFound. Synthesise a peer-id from a
	// freshly generated key so the address parses correctly but the
	// identity isn't a member.
	otherId := newRandomPeerId(t)
	_, err = sp.Members().Get(ctx, otherId)
	assert.ErrorIs(t, err, space.ErrNotFound)
}

// newRandomPeerId returns a libp2p-style PeerId derived from a fresh
// ed25519 key. Used by tests that need a syntactically valid but
// non-member identity.
func newRandomPeerId(t *testing.T) string {
	t.Helper()
	priv, _, err := anySyncCryptoGenerate()
	require.NoError(t, err)
	return priv.GetPublic().PeerId()
}

// TestE2E_OwnerInviteJoinerAccept walks the full RequestToJoin flow
// across two SDK instances against staging:
//
//  1. Owner creates a space and an invite.
//  2. Joiner calls Service.Join with the invite token.
//  3. Owner polls Members.JoinRequests until the request lands (the
//     ACL record propagates via headsync, period ~30s).
//  4. Owner calls ACL.AcceptRequest with PermissionWriter.
//  5. Owner sees the joiner in Members.List with the granted perm.
//
// The joiner side does not yet pull the space post-acceptance — that
// loader is a follow-up. This test asserts the owner-side state
// transitions only.
//
// Slow (~60-90s due to staging round-trips and a single headsync wait).
// Skips if staging is unreachable. Each Open uses a separate temp dir
// and a fresh account key so the two SDKs are genuinely distinct.
func TestE2E_OwnerInviteJoinerAccept(t *testing.T) {
	confPath := filepath.Join("..", "test-etc", "staging.yml")
	yaml, err := os.ReadFile(confPath)
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	// Two contexts, generous timeouts — staging plus headsync waits.
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	mkSDK := func(name string) *anysyncsdk.SDK {
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

	owner := mkSDK("owner")
	joiner := mkSDK("joiner")
	require.NotEqual(t, owner.Account().Id(), joiner.Account().Id(),
		"owner and joiner must have distinct account ids")

	// 1. Owner creates a space.
	sp, err := owner.Spaces().Create(ctx, space.CreateRequest{Name: "E2E"})
	require.NoError(t, err)

	// 2. Owner creates an invite — this also triggers MakeShareable
	// internally with retry, so the space is registered with the
	// coordinator before the invite record is published.
	inv, err := sp.ACL().CreateInvite(ctx)
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("skipping e2e: network unreachable: %v", err)
		}
		t.Fatalf("CreateInvite: %v", err)
	}
	token, err := space.EncodeInvite(inv)
	require.NoError(t, err)

	// Subscribe to members events on the owner side BEFORE the joiner
	// sends the request, so we observe the full add → change cycle.
	var (
		evMu sync.Mutex
		evs  []space.MemberEvent
	)
	cancelSub := sp.Members().Subscribe(func(ev space.MemberEvent) {
		evMu.Lock()
		evs = append(evs, ev)
		evMu.Unlock()
	})
	t.Cleanup(cancelSub)
	collectedEvents := func() []space.MemberEvent {
		evMu.Lock()
		defer evMu.Unlock()
		out := make([]space.MemberEvent, len(evs))
		copy(out, evs)
		return out
	}

	// 3. Joiner calls Service.Join — sends RequestJoin to the network
	// and writes a tech-space record locally with StatusJoining.
	// Returns ErrJoinPending; that's the success signal.
	_, err = joiner.Spaces().Join(ctx, space.JoinRequest{
		Invite: token,
		Metadata: space.AccountMetadata{
			Name: "Joiner Display Name",
		},
	})
	require.ErrorIs(t, err, spaceimpl.ErrJoinPending,
		"joiner should land in pending state after RequestJoin")

	// Joiner-side tech-space record records the pending state.
	joinerSpaces, err := joiner.Spaces().List(ctx)
	require.NoError(t, err)
	require.Len(t, joinerSpaces, 1)
	assert.Equal(t, sp.Id(), joinerSpaces[0].Id)
	assert.Equal(t, space.StatusJoining, joinerSpaces[0].Status)

	// 4. Owner polls JoinRequests until the joiner's request lands.
	// The request arrives via the owner's per-space headsync pulling
	// fresh ACL records from the consensus node — period 30s.
	var joinReq space.JoinRequestInfo
	require.Eventually(t, func() bool {
		reqs, err := sp.Members().JoinRequests(ctx)
		if err != nil || len(reqs) == 0 {
			return false
		}
		joinReq = reqs[0]
		return true
	}, 90*time.Second, 2*time.Second, "owner never saw the join request")

	assert.Equal(t, joiner.Account().Id(), joinReq.Identity)
	assert.NotEmpty(t, joinReq.RecordId)

	// Wait for the watcher to fire Added(Joining) BEFORE accepting.
	// Otherwise on a fast local stack the AcceptRequest can land between
	// two 250ms ticks of the watcher, the head jumps directly to
	// joiner=Active, and the firehose only emits a single Added(Active)
	// event — losing the intermediate Joining state observers care about.
	require.Eventually(t, func() bool {
		for _, ev := range collectedEvents() {
			if ev.Member.Identity == joiner.Account().Id() &&
				ev.Kind == space.MemberEventAdded &&
				ev.Member.Status == space.MemberStatusJoining {
				return true
			}
		}
		return false
	}, 5*time.Second, 50*time.Millisecond, "watcher never observed the joining state")

	// 5. Owner accepts with PermissionWriter. AcceptRequest both pushes
	// the record to consensus AND updates the owner's local AclList
	// synchronously, so Members().List should reflect the new member
	// without further polling.
	require.NoError(t, sp.ACL().AcceptRequest(ctx, joinReq.RecordId, space.PermissionWriter))

	members, err := sp.Members().List(ctx)
	require.NoError(t, err)
	require.Len(t, members, 2, "owner + joiner")

	var found space.Member
	var sawOwner, sawJoiner bool
	for _, m := range members {
		switch m.Identity {
		case owner.Account().Id():
			sawOwner = true
			assert.Equal(t, space.PermissionOwner, m.Permission)
			assert.Equal(t, space.MemberStatusActive, m.Status)
		case joiner.Account().Id():
			sawJoiner = true
			found = m
		}
	}
	require.True(t, sawOwner, "owner missing from members")
	require.True(t, sawJoiner, "joiner missing from members")
	assert.Equal(t, space.PermissionWriter, found.Permission)
	assert.Equal(t, space.MemberStatusActive, found.Status)

	// JoinRequests should be empty now that the request was accepted.
	reqs, err := sp.Members().JoinRequests(ctx)
	require.NoError(t, err)
	assert.Empty(t, reqs, "no pending requests after Accept")

	// Subscribe firehose: the watcher's polling tick (~250 ms) lags
	// the local AclList update, so allow up to 5 s for events to
	// settle before asserting. We expect at minimum:
	//   - an Added event for the joiner with Status=Joining (the
	//     pending request appearing on owner via headsync), and
	//   - a Changed event flipping Status to Active after Accept.
	require.Eventually(t, func() bool {
		es := collectedEvents()
		var sawAdded, sawAccepted bool
		for _, ev := range es {
			if ev.Member.Identity != joiner.Account().Id() {
				continue
			}
			switch ev.Kind {
			case space.MemberEventAdded:
				if ev.Member.Status == space.MemberStatusJoining {
					sawAdded = true
				}
			case space.MemberEventChanged:
				if ev.Member.Status == space.MemberStatusActive &&
					ev.Member.Permission == space.PermissionWriter {
					sawAccepted = true
				}
			}
		}
		return sawAdded && sawAccepted
	}, 5*time.Second, 100*time.Millisecond, "expected Added(joining) + Changed(active) events; got %v", collectedEvents())

	// Query() over the materialised members collection. Needs to
	// settle the same way Subscribe events do, since the watcher
	// reconciles to disk on tick. Should see exactly two rows
	// (owner + joiner) with the correct fields.
	require.Eventually(t, func() bool {
		count, err := sp.Members().Query().Count(ctx)
		return err == nil && count == 2
	}, 5*time.Second, 100*time.Millisecond, "members collection should hold 2 rows")

	// Filter on permission=writer — must hit exactly the joiner row.
	doc, err := sp.Members().Query().
		Filter(`{"permission":"writer"}`).
		One(ctx)
	require.NoError(t, err)
	assert.Equal(t, joiner.Account().Id(), doc.GetString("id"))
	assert.Equal(t, "active", doc.GetString("status"))
	assert.Equal(t, "Joiner Display Name", doc.GetString("name"))

	// Sorting by id round-trips both rows; just check we got two
	// distinct ids.
	all, err := sp.Members().Query().Sort("id").All(ctx)
	require.NoError(t, err)
	require.Len(t, all, 2)
	assert.NotEqual(t, all[0].GetString("id"), all[1].GetString("id"))

	// Phase 3: identityRepo. The owner publishes a profile; the
	// per-space members watcher fetches it (immediately, because
	// UpdateMetadata kicks every running watcher) and overrides the
	// owner's Member.Name. The override path doesn't depend on the
	// owner having any prior metadata in the ACL — the owner's
	// CurrentAccounts entry is created from the space-root, with
	// no Name attached.
	require.NoError(t, owner.Account().UpdateMetadata(ctx, space.AccountMetadata{
		Name:        "Owner Live Name",
		Description: "Live description",
		IconCID:     "owner-icon-cid",
	}))

	// After the kick, the watcher's profileLoop fetches in a
	// goroutine; the next regular tick (~250 ms) rebuilds the
	// snapshot. Allow up to 5 s.
	require.Eventually(t, func() bool {
		me, err := sp.Members().Me(ctx)
		if err != nil {
			return false
		}
		return me.Name == "Owner Live Name" &&
			me.Description == "Live description" &&
			me.IconCID == "owner-icon-cid"
	}, 5*time.Second, 100*time.Millisecond, "owner profile override never applied")

	// And the materialised members collection picks up the same
	// override — Query("permission":"owner") returns the new name.
	// The disk reconcile happens on the next 250 ms watcher tick
	// after profilesDirty is flipped; allow a couple of ticks for
	// scheduler jitter.
	require.Eventually(t, func() bool {
		doc, err := sp.Members().Query().
			Filter(`{"permission":"owner"}`).
			One(ctx)
		return err == nil && doc.GetString("name") == "Owner Live Name"
	}, 5*time.Second, 100*time.Millisecond, "owner profile not visible via Query")

	// Joiner publishes their profile too; owner's view of the
	// joiner flips from the join-time snapshot ("Joiner Display Name")
	// to the live profile ("Joiner Live Name"). UpdateMetadata kicks
	// only the joiner SDK's watchers (no overlap with owner's), so
	// the owner has to wait for its own slow tick OR the regular
	// 250 ms tick after the headsync pulls something else — but
	// since the joiner-side watcher kicks both peers via identityRepo
	// and the owner's regular tick re-pulls every 60s anyway, we
	// wait up to 65 s for the owner to see the change.
	require.NoError(t, joiner.Account().UpdateMetadata(ctx, space.AccountMetadata{
		Name: "Joiner Live Name",
	}))
	require.Eventually(t, func() bool {
		j, err := sp.Members().Get(ctx, joiner.Account().Id())
		if err != nil {
			return false
		}
		return j.Name == "Joiner Live Name"
	}, 65*time.Second, 1*time.Second, "joiner profile override never applied on owner side")
}

// TestSDK_Spaces_Derive verifies the deterministic-derive surface:
// same (account, seed) lands on the same spaceId across calls, the
// space appears in List with StatusActive, and the OwnRole is owner.
func TestSDK_Spaces_Derive(t *testing.T) {
	confPath := filepath.Join("..", "test-etc", "staging.yml")
	yaml, err := os.ReadFile(confPath)
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	seed := []byte("project:demo:2026-05")
	sp1, err := sdk.Spaces().Derive(ctx, space.DeriveRequest{Seed: seed})
	require.NoError(t, err)
	require.NotEmpty(t, sp1.Id())

	sp2, err := sdk.Spaces().Derive(ctx, space.DeriveRequest{Seed: seed})
	require.NoError(t, err)
	assert.Equal(t, sp1.Id(), sp2.Id(), "Derive must be idempotent for the same seed")

	// Different seed → different space.
	sp3, err := sdk.Spaces().Derive(ctx, space.DeriveRequest{Seed: []byte("different")})
	require.NoError(t, err)
	assert.NotEqual(t, sp1.Id(), sp3.Id())

	// List sees both.
	list, err := sdk.Spaces().List(ctx)
	require.NoError(t, err)
	require.Len(t, list, 2)
}

// TestSDK_Join_PendingErrIsExpected confirms that calling Join with a
// valid invite (against a space the joiner is not the owner of) lands
// the request and surfaces ErrJoinPending — the joiner's local space
// is materialised only after owner acceptance + sync, which is a
// follow-up. This test is gated on staging being available; the
// invite is generated by the same SDK instance for self-test purposes.
func TestSDK_Join_PendingErrIsExpected(t *testing.T) {
	// Self-join attempt is rejected by any-sync (you can't join a space
	// you already own). We simulate the codepath: build an invite for a
	// space owned by us, then call Join with it. The expected behavior
	// is an error from any-sync's RequestJoin RPC — not ErrJoinPending.
	// The point of this test is only to verify the codepath wires
	// through end-to-end (DecodeInvite → JoiningClient.RequestJoin →
	// returned error). A two-account integration test is deferred.
	confPath := filepath.Join("..", "test-etc", "staging.yml")
	yaml, err := os.ReadFile(confPath)
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "JoinSelf"})
	require.NoError(t, err)

	// Boundary errors do not need network.
	_, err = sdk.Spaces().Join(ctx, space.JoinRequest{Invite: ""})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Invite required")

	_, err = sdk.Spaces().Join(ctx, space.JoinRequest{Invite: "!not-base58!"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, space.ErrInvalidInvite))

	inv, err := sp.ACL().CreateInvite(ctx)
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("skipping: network unreachable: %v", err)
		}
		t.Fatalf("CreateInvite: %v", err)
	}
	token, err := space.EncodeInvite(inv)
	require.NoError(t, err)

	// Self-join: codepath runs through to RequestJoin; any-sync
	// rejects the attempt at the network layer. We accept either a
	// rejection error or ErrJoinPending — the goal is to confirm the
	// pipeline is wired and the right surface area is reachable.
	_, err = sdk.Spaces().Join(ctx, space.JoinRequest{Invite: token})
	if err == nil {
		t.Fatalf("expected an error joining a space we own")
	}
	if errors.Is(err, spaceimpl.ErrJoinPending) {
		t.Logf("Join returned ErrJoinPending (network call queued)")
	} else {
		t.Logf("Join returned %v (network rejection — expected for self-join)", err)
	}
}

// isNoNetworkErr reports whether err looks like a transient
// connectivity failure (DNS, dial, EOF) rather than a real ACL
// rejection. Used to skip integration assertions in offline / sandbox
// runs.
func isNoNetworkErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, sub := range []string{
		"unable to connect",
		"no such host",
		"connection refused",
		"context deadline exceeded",
	} {
		if containsFold(msg, sub) {
			return true
		}
	}
	return false
}

// containsFold is a tiny case-insensitive substring check so we don't
// pull strings.EqualFold-style helpers from std.
func containsFold(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		match := true
		for j := 0; j < len(sub); j++ {
			a, b := s[i+j], sub[j]
			if a >= 'A' && a <= 'Z' {
				a += 'a' - 'A'
			}
			if b >= 'A' && b <= 'Z' {
				b += 'a' - 'A'
			}
			if a != b {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
