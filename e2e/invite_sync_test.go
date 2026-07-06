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
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_AliceBobInviteAndContent layers the multi-identity invite
// flow on top of the cold-sync convergence the previous test
// established. The path under test:
//
//  1. Alice creates a space, populates it with one user-defined type
//     and two objects (each with a property value).
//  2. Alice mints a RequestToJoin invite.
//  3. Bob (distinct keys) calls Spaces().Join with that invite —
//     returns ErrJoinPending.
//  4. Alice waits for the request to land via headsync, then accepts
//     it as PermissionWriter.
//  5. Bob waits for Members.Me() to flip to Active.
//  6. Bob then enumerates Types() / QueryObjects() / Properties() and
//     must see Alice's content.
//
// Step (6) is the part that the previous SDK implementation broke —
// the joiner converged on members but never on user content. With
// the listener-wiring + decrypt-via-IterateAfterAddSeq fixes in
// place, this test passes under the same infrastructure as
// TestE2E_ColdSyncSameKey.
//
// Skips when no any-sync network is reachable (loadAnySyncNetwork
// shared with the cold-sync test). Slow (~60-90s end-to-end) because
// the join flow needs at minimum two headsync ticks (Alice sees
// request, Bob sees acceptance).
func TestE2E_AliceBobInviteAndContent(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("invite-sync e2e is slow (~60-90s); rerun without -short")
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
	require.NotEqual(t, alice.Account().Id(), bob.Account().Id(),
		"alice and bob must have distinct account ids")

	// Bob publishes a profile up-front so Alice's identityRepo lookup
	// has something to find when the join request lands. Mirrors what
	// TestE2E_OwnerInviteJoinerAccept does — the same setup is needed
	// here for the owner-side member view to render Bob's display
	// name. (We don't assert on the name in this test, but we keep
	// the publish in case the assertions grow.)
	require.NoError(t, bob.Account().UpdateMetadata(ctx, space.AccountMetadata{
		Name: "Bob",
	}))

	// Alice creates the space + content BEFORE Bob joins. The CRDT
	// gate inside the SDK (DataVersion / known-shortIds set) is what
	// keeps property-value writes parked until the type definition
	// they reference catches up — this test exercises that path on
	// the joiner side, where Bob's controller has to receive the
	// type, the property def, the object, and the property value, in
	// any sync-imposed order.
	sp, err := alice.Spaces().Create(ctx, space.CreateRequest{Name: "AliceLib"})
	require.NoError(t, err, "alice: Spaces().Create")

	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Movie"})
	require.NoError(t, err)

	propId, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Title",
		XKey: "title",
		Kind: space.PropertyKindString,
	})
	require.NoError(t, err)

	objs := make([]objFix, 0, 2)
	for _, title := range []string{"Casablanca", "Vertigo"} {
		objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
		require.NoError(t, err)
		_, err = sp.Properties().Set(ctx, objId, typeId, map[string]any{propId: title})
		require.NoError(t, err)
		objs = append(objs, objFix{id: objId, title: title})
	}
	t.Logf("alice: spaceId=%s typeId=%s propId=%s objs=%v", sp.Id(), typeId, propId, objs)

	// Mint the invite. On a freshly-bootstrapped local network the
	// consensus node may not yet have a log for the space the very
	// first time we try (the ACL record publish surfaces this as
	// "log not found"). Retry briefly — this only matters in tests
	// against a freshly started local infra; against staging the
	// space's log has long since been created.
	var inv space.Invite
	if !waitFor(ctx, 30*time.Second, 1*time.Second, func() bool {
		inv, err = sp.ACL().CreateInvite(ctx)
		return err == nil
	}) {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on CreateInvite: %v", err)
		}
		t.Fatalf("alice: CreateInvite: %v", err)
	}
	token, err := space.EncodeInvite(inv)
	require.NoError(t, err)

	// Bob sends the join request. ErrJoinPending is the success path.
	_, err = bob.Spaces().Join(ctx, space.JoinRequest{
		Invite:   token,
		Metadata: space.AccountMetadata{Name: "Bob"},
	})
	require.True(t, errors.Is(err, spaceimpl.ErrJoinPending),
		"bob: Join must return ErrJoinPending, got %v", err)

	// Alice waits for the request to land via her headsync. Kicking a
	// diff round each poll pulls the ACL record now instead of waiting
	// for the periodic tick.
	var joinReq space.JoinRequestInfo
	if !waitFor(ctx, 90*time.Second, 1*time.Second, func() bool {
		_ = sp.SyncHeads(ctx)
		reqs, _ := sp.Members().JoinRequests(ctx)
		if len(reqs) > 0 {
			joinReq = reqs[0]
			return true
		}
		return false
	}) {
		t.Fatalf("alice never saw bob's join request")
	}
	require.Equal(t, bob.Account().Id(), joinReq.Identity)

	// Alice accepts. The accept also pushes through ACL sync; bob's
	// next headsync tick picks it up.
	require.NoError(t, sp.ACL().AcceptRequest(ctx, joinReq.RecordId, space.PermissionWriter),
		"alice: AcceptRequest")

	// Bob's join controller drives the rest autonomously: its ACL
	// waiter (kicked when Join ran) detects Alice's accept, pulls the
	// space, and flips the tech-space row to active — no caller-side
	// Get() retry loop. Wait for that flip to surface in List.
	if !waitFor(ctx, 90*time.Second, 1*time.Second, func() bool {
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
		t.Fatalf("bob's space never flipped to active autonomously")
	}
	// Get now succeeds first try — the controller already loaded it.
	bobSpace, err := bob.Spaces().Get(ctx, sp.Id())
	require.NoError(t, err, "bob: Spaces().Get(%s) after autonomous flip", sp.Id())

	if !waitFor(ctx, 90*time.Second, 1*time.Second, func() bool {
		_ = bobSpace.SyncHeads(ctx)
		me, err := bobSpace.Members().Me(ctx)
		if err != nil {
			return false
		}
		return me.Status == space.MemberStatusActive
	}) {
		t.Fatalf("bob never reached MemberStatusActive")
	}

	// At this point Bob should be a writer on Alice's space. The
	// content sync window is identical to the cold-sync test: types,
	// objects, AND per-object property values must all converge.
	//
	// The value check belongs INSIDE this wait, not after it. Each
	// object reaches Bob as two separate DAG changes — the `any.types`
	// bootstrap and the `Properties().Set` value — both parked behind the type
	// def's shortId (the DataVersion gate). When the type def lands they
	// drain in one pass but apply non-atomically per object: the
	// `any.types` replay creates the object's row (so it shows up in
	// QueryObjects) microseconds before the value replay lands. Gating
	// only on object presence and then reading the value once races that
	// gap — the value is correct on the very next read, but a one-shot
	// assert flakes ~1/25. Poll until the values are present too.
	var lastTypes []string
	var lastObjs []string
	var lastVals []string
	converged := waitFor(ctx, 3*time.Minute, 2*time.Second, func() bool {
		_ = bobSpace.SyncHeads(ctx)
		typeIds := userTypeIds(bobSpace, ctx)
		lastTypes = typeIds
		if !containsString(typeIds, typeId) {
			return false
		}
		docs, err := bobSpace.QueryObjects().All(ctx)
		if err != nil {
			return false
		}
		ids := make([]string, 0, len(docs))
		for _, d := range docs {
			ids = append(ids, d.GetString("id"))
		}
		lastObjs = ids
		for _, want := range objs {
			if !containsString(ids, want.id) {
				return false
			}
		}
		lastVals = lastVals[:0]
		for _, want := range objs {
			rec, err := bobSpace.Properties().Get(ctx, want.id)
			if err != nil || rec == nil {
				return false
			}
			got := rec.GetString(typeId, propId)
			lastVals = append(lastVals, got)
			if got != want.title {
				return false
			}
		}
		return true
	})
	if !converged {
		t.Fatalf("bob: content never converged\n  want type=%s objects=%v values=%v\n  got types=%v objects=%v values=%v",
			typeId, objIds(objs), objTitles(objs), lastTypes, lastObjs, lastVals)
	}

	// Re-assert the values for a clear failure message if the wait above
	// is ever loosened — by here they are guaranteed present.
	for _, want := range objs {
		rec, err := bobSpace.Properties().Get(ctx, want.id)
		require.NoError(t, err, "bob: Properties().Get(%s)", want.id)
		require.NotNil(t, rec, "bob: nil property record for %s", want.id)
		assert.Equal(t, want.title, rec.GetString(typeId, propId),
			"bob: property value mismatch on %s", want.id)
	}

	// Bob is a writer — he should also be able to see himself in the
	// member list of the now-shared space and be listed alongside
	// Alice. A tighter assertion than just Me().Active.
	members, err := bobSpace.Members().List(ctx)
	require.NoError(t, err)
	var sawAlice, sawBob bool
	for _, m := range members {
		switch m.Identity {
		case alice.Account().Id():
			sawAlice = true
		case bob.Account().Id():
			sawBob = true
			assert.Equal(t, space.PermissionWriter, m.Permission)
		}
	}
	assert.True(t, sawAlice, "bob's members view missing alice")
	assert.True(t, sawBob, "bob's members view missing himself")
}

type objFix struct {
	id    string
	title string
}

func objIds(objs []objFix) []string {
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		out = append(out, o.id)
	}
	return out
}

func objTitles(objs []objFix) []string {
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		out = append(out, o.title)
	}
	return out
}
