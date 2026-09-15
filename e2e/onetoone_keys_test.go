package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
	"github.com/anyproto/any-sync-sdk/space"
)

// The one-to-one identity-key exchange (docs/13-one-to-one-spaces.md
// § Key exchange inside the space). A 1-1 ACL is immutable and carries
// no per-writer metadata, and the coordinator inbox invite ships the
// INITIATOR's metadata symkey one way only, so the rows are the only
// path by which the initiator can resolve the acceptor's profile. Each
// participant publishes its own symkey as a row of the `identityKeys`
// dataset on the space's derived spaceIndex object, and a watcher folds
// the peer's row into the identities directory.
//
// These tests assert the exchange through public surfaces only: the
// windowed query over the spaceIndex object for the rows themselves,
// and Identities()/List for the consequence (the peer's profile
// resolving, which is only possible with the peer's symkey in hand).

// openSDKAt opens an SDK on an EXPLICIT data dir, so a test can close a
// device and reopen the same one. No cleanup is registered — the caller
// owns the handle (the reopen test closes and reopens it mid-run).
func openSDKAt(t *testing.T, ctx context.Context, yaml []byte, dir string, p *fixedSeedProvider, name string) *anysyncsdk.SDK {
	t.Helper()
	cfg := config.Config{
		Storage: config.Storage{DataDir: dir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	sdk, err := anysyncsdk.Open(ctx, cfg, p)
	require.NoError(t, err, "%s: Open", name)
	return sdk
}

// identityKeyRows reads the identityKeys rows off a 1-1's derived
// spaceIndex object through the public query surface: identity → symKey.
// Returns nil while the tree hasn't arrived on this device yet, so it is
// safe to call from a poll loop.
func identityKeyRows(ctx context.Context, sp space.Space) map[string]string {
	res, err := sp.Query(sp.SpaceIndexObjectId(), spaceindex.IdentityKeysDataset).Snapshot(ctx, space.QueryOpts{})
	if err != nil {
		return nil
	}
	out := make(map[string]string, len(res.Initial))
	for _, d := range res.Initial {
		if id := d.GetString("id"); id != "" {
			out[id] = d.GetString(spaceindex.FieldIdentityKeySymKey)
		}
	}
	return out
}

// accountMetaSymKey derives the metadata symkey an account WOULD
// publish, straight from the test's account seed and through the same
// derivation the SDK runs. It pins the exchanged secret to the key that
// decrypts that account's identityRepo profile, rather than to "some
// non-empty string".
func accountMetaSymKey(t *testing.T, p *fixedSeedProvider) string {
	t.Helper()
	signKey, err := crypto.NewSigningEd25519PrivKeyFromBytes(p.account)
	require.NoError(t, err)
	k, err := space.DeriveAccountMetadataSymKey(signKey)
	require.NoError(t, err)
	s, err := space.MarshalSymKey(k)
	require.NoError(t, err)
	return s
}

// pollSynced re-checks fn every 2s until it holds or the deadline
// passes, forcing a head-sync round on each given space first — the 1-1
// trees converge through the periodic timer otherwise, which is far
// slower than a test wants to wait.
func pollSynced(ctx context.Context, d time.Duration, sps []space.Space, fn func() bool) bool {
	deadline := time.Now().Add(d)
	for {
		for _, sp := range sps {
			if sp != nil {
				_ = sp.SyncHeads(ctx)
			}
		}
		if fn() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(2 * time.Second)
	}
}

// oneToOnePair is a converged two-account 1-1: both participants have
// published an identityRepo profile, both hold the space, and both
// identityKeys rows are readable on both sides.
type oneToOnePair struct {
	alice, bob         *anysyncsdk.SDK
	aliceProv, bobProv *fixedSeedProvider
	aliceDir, bobDir   string
	aliceID, bobID     string
	aliceSp, bobSp     space.Space
	spaceId            string
}

// closeAlice / closeBob close a device at most once. SDK.Close is NOT
// idempotent (a second call panics closing an already-closed channel),
// and the reopen test closes alice mid-run, so the fixture owns the
// handle and the cleanup closes whatever is open at the end.
func (p *oneToOnePair) closeAlice() {
	if p.alice != nil {
		_ = p.alice.Close()
		p.alice = nil
	}
}

func (p *oneToOnePair) closeBob() {
	if p.bob != nil {
		_ = p.bob.Close()
		p.bob = nil
	}
}

// setupOneToOnePair runs the out-of-band handshake — alice initiates,
// bob discovers with RegisterIncoming and accepts — and waits until the
// two identityKeys rows converge on both devices.
//
// The RegisterIncoming display hint is deliberately EMPTY: a hint would
// put the peer's name on the row directly, and every name assertion
// below is meant to prove the name came from identityRepo, i.e. that
// the peer's symkey reached this side.
func setupOneToOnePair(t *testing.T, ctx context.Context, yaml []byte, aliceName, bobName string) *oneToOnePair {
	t.Helper()
	p := &oneToOnePair{
		aliceProv: newFixedSeedProvider(t),
		bobProv:   newFixedSeedProvider(t),
		aliceDir:  t.TempDir(),
		bobDir:    t.TempDir(),
	}
	p.alice = openSDKAt(t, ctx, yaml, p.aliceDir, p.aliceProv, "alice")
	t.Cleanup(p.closeAlice)
	p.bob = openSDKAt(t, ctx, yaml, p.bobDir, p.bobProv, "bob")
	t.Cleanup(p.closeBob)
	p.aliceID, p.bobID = p.alice.Account().Id(), p.bob.Account().Id()
	require.NotEqual(t, p.aliceID, p.bobID)

	// Profiles first: the key exchange is only observable through a
	// profile the peer can decrypt once it holds the key.
	require.NoError(t, p.alice.Account().UpdateMetadata(ctx, space.AccountMetadata{Name: aliceName}))
	require.NoError(t, p.bob.Account().UpdateMetadata(ctx, space.AccountMetadata{Name: bobName}))

	aliceSp, err := p.alice.Spaces().OneToOne(ctx, p.bobID)
	require.NoError(t, err, "alice: OneToOne")
	p.aliceSp, p.spaceId = aliceSp, aliceSp.Id()

	require.NoError(t, p.bob.Spaces().RegisterIncoming(ctx, p.aliceID, space.AccountMetadata{}))
	bobSp, err := p.bob.Spaces().AcceptOneToOne(ctx, p.spaceId)
	require.NoError(t, err, "bob: AcceptOneToOne")
	require.Equal(t, p.spaceId, bobSp.Id(), "both sides derive the same 1-1 id")
	p.bobSp = bobSp

	// Both rows on both sides. Alice's own row is written by the post-load
	// seed pass on initiate, Bob's by the same pass on accept; each side
	// then receives the peer's row through ordinary space sync.
	both := []space.Space{p.aliceSp, p.bobSp}
	converged := pollSynced(ctx, 3*time.Minute, both, func() bool {
		a, b := identityKeyRows(ctx, p.aliceSp), identityKeyRows(ctx, p.bobSp)
		return len(a) == 2 && len(b) == 2 &&
			a[p.aliceID] != "" && a[p.bobID] != "" &&
			a[p.aliceID] == b[p.aliceID] && a[p.bobID] == b[p.bobID]
	})
	require.True(t, converged, "identityKeys rows never converged on both sides of the 1-1 (space %s)", p.spaceId)
	return p
}

// TestE2E_OneToOne_KeyExchangeBothDirections is the regression this
// dataset exists for: after an out-of-band accept BOTH participants
// resolve each other's profile. The acceptor→initiator direction was
// impossible before — Accept sends no inbox notification back, so
// Alice never saw Bob's metadata symkey and her space list showed him
// identity-only forever.
func TestE2E_OneToOne_KeyExchangeBothDirections(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("1-1 key-exchange e2e needs cross-account sync; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	p := setupOneToOnePair(t, ctx, yaml, "Alice Keys", "Bob Keys")

	// The rows themselves: exactly one per participant, keyed by the
	// participant's account identity, carrying that account's derived
	// metadata symkey, and byte-identical on both replicas.
	aliceRows, bobRows := identityKeyRows(ctx, p.aliceSp), identityKeyRows(ctx, p.bobSp)
	require.Len(t, aliceRows, 2, "alice sees exactly two identityKeys rows")
	require.Len(t, bobRows, 2, "bob sees exactly two identityKeys rows")
	assert.Equal(t, accountMetaSymKey(t, p.aliceProv), aliceRows[p.aliceID],
		"alice's row carries her account metadata symkey, not some other secret")
	assert.Equal(t, accountMetaSymKey(t, p.bobProv), aliceRows[p.bobID],
		"bob's row carries his account metadata symkey")
	assert.Equal(t, aliceRows[p.aliceID], bobRows[p.aliceID], "alice's key converged byte-identically")
	assert.Equal(t, aliceRows[p.bobID], bobRows[p.bobID], "bob's key converged byte-identically")

	// Direction 1 (already worked via the inbox invite, asserted as the
	// control): bob resolves alice.
	require.True(t, pollSynced(ctx, 90*time.Second, []space.Space{p.aliceSp, p.bobSp}, func() bool {
		info, ok, _ := p.bob.Identities().Get(ctx, p.aliceID)
		return ok && info.Name == "Alice Keys"
	}), "bob should resolve alice's profile from her published key")

	// Direction 2 (the fix): alice resolves bob. Her only possible source
	// is bob's identityKeys row — no inbox invite ever travels this way.
	require.True(t, pollSynced(ctx, 90*time.Second, []space.Space{p.aliceSp, p.bobSp}, func() bool {
		info, ok, _ := p.alice.Identities().Get(ctx, p.bobID)
		return ok && info.Name == "Bob Keys"
	}), "alice should resolve bob's profile from the identityKeys row he published in the space")

	// The consequence clients actually render: a 1-1 shows the friend by
	// name in the space list, on both sides.
	require.Eventually(t, func() bool {
		si, ok := infoByID(t, ctx, p.alice, p.spaceId)
		return ok && si.Name == "Bob Keys"
	}, 30*time.Second, time.Second, "alice's space list should name the 1-1 after bob")
	si, ok := infoByID(t, ctx, p.bob, p.spaceId)
	require.True(t, ok)
	assert.Equal(t, "Alice Keys", si.Name, "bob's space list should name the 1-1 after alice")

	// The directory records the 1-1 as a sighting for the peer.
	info, ok, err := p.alice.Identities().Get(ctx, p.bobID)
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, containsStr(info.SpaceIds, p.spaceId), "the 1-1 is a sighting of the peer")
}

// TestE2E_OneToOne_KeyPublishIsIdempotent pins the writer's no-churn
// rule: the symkey is derived deterministically from the account key, so
// every load of the 1-1 — on this device or another of the same account
// — must find its own row already correct and write nothing. A writer
// that re-published per load would grow the DAG forever and re-fire the
// peer's watcher on every restart.
func TestE2E_OneToOne_KeyPublishIsIdempotent(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("1-1 key-exchange e2e needs cross-account sync; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	p := setupOneToOnePair(t, ctx, yaml, "Alice Idem", "Bob Idem")
	before := identityKeyRows(ctx, p.aliceSp)
	require.Len(t, before, 2)

	// Count the identityKeys changes on the spaceIndex object before the
	// restart: one per participant, and that must not grow.
	indexObjId := p.aliceSp.SpaceIndexObjectId()
	changesBefore := listIdentityKeyChanges(t, ctx, p.alice, p.spaceId, indexObjId)
	assert.Len(t, changesBefore, 2, "one identityKeys change per participant")

	// Same device, same data dir, same account+device seeds: close and
	// reopen, then load the 1-1 again (which re-runs the seed pass).
	p.closeAlice()
	p.alice = openSDKAt(t, ctx, yaml, p.aliceDir, p.aliceProv, "alice-reopened")
	reopened, err := p.alice.Spaces().Get(ctx, p.spaceId)
	require.NoError(t, err, "alice: reload the 1-1 after reopen")
	p.aliceSp = reopened

	// Give the post-load seed pass time to run (and to misbehave).
	time.Sleep(10 * time.Second)

	after := identityKeyRows(ctx, p.aliceSp)
	assert.Equal(t, before, after, "reopening must not change either identityKeys row")
	changesAfter := listIdentityKeyChanges(t, ctx, p.alice, p.spaceId, indexObjId)
	assert.Equal(t, len(changesBefore), len(changesAfter),
		"reopening must not add an identityKeys change to the DAG")

	// The peer still reads alice's original key — nothing was rewritten.
	bobView := identityKeyRows(ctx, p.bobSp)
	assert.Equal(t, before[p.aliceID], bobView[p.aliceID], "alice's key is unchanged for the peer")
}

// listIdentityKeyChanges returns every identityKeys change on an object,
// newest first. The version-history surface is the cheapest public way
// to prove a write did NOT happen.
func listIdentityKeyChanges(t *testing.T, ctx context.Context, sdk *anysyncsdk.SDK, spaceId, objectId string) []space.ChangeMeta {
	t.Helper()
	sp, err := sdk.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)
	list, err := sp.History().ListChanges(ctx, objectId, space.HistoryFilter{Dataset: spaceindex.IdentityKeysDataset}, 100, "")
	require.NoError(t, err)
	return list.Changes
}

// TestE2E_OneToOne_ForeignKeyRowRefused is the authorization negative.
// Two fences stand behind a row: the public write surface treats
// identityKeys as SDK-internal (so a client can't hand-craft a row at
// all), and — for anything that gets past a client, including a peer's
// inbound change — the handler admits a row only from the identity it is
// keyed by. This test pins the outer fence and the outcome; the handler
// rule itself is pinned by the internal/types/spaceindex unit tests.
func TestE2E_OneToOne_ForeignKeyRowRefused(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("1-1 key-exchange e2e needs cross-account sync; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	p := setupOneToOnePair(t, ctx, yaml, "Alice Auth", "Bob Auth")
	aliceKey := identityKeyRows(ctx, p.aliceSp)[p.aliceID]
	require.NotEmpty(t, aliceKey)

	// Bob tries to overwrite Alice's row through the public write path.
	_, err = p.bobSp.Modify(ctx, space.ModifyBatch{
		ObjectId: p.bobSp.SpaceIndexObjectId(),
		Dataset:  spaceindex.IdentityKeysDataset,
		Records: []space.RecordModify{{
			Id:     p.aliceID,
			Upsert: true,
			Ops:    []space.Op{{Type: space.OpSet, Path: spaceindex.FieldIdentityKeySymKey, Value: "spoofed-key"}},
		}},
	})
	require.Error(t, err, "identityKeys must not be writable through the public Modify surface")
	assert.Contains(t, err.Error(), "SDK-internal", "the refusal is the public-dataset fence")

	// A delete is refused by the same fence, ahead of the handler rule
	// (a raw delete would otherwise be signed into the DAG before the
	// apply-time rejection, and the tombstone is sticky).
	_, err = p.bobSp.Delete(ctx, space.DeleteBatch{
		ObjectId:  p.bobSp.SpaceIndexObjectId(),
		Dataset:   spaceindex.IdentityKeysDataset,
		RecordIds: []string{p.aliceID},
	})
	require.Error(t, err, "identityKeys rows must not be deletable through the public surface")

	// Nothing moved: alice's key is intact on both replicas — neither
	// refused call left a change behind for sync to carry.
	time.Sleep(5 * time.Second)
	_ = p.aliceSp.SyncHeads(ctx)
	_ = p.bobSp.SyncHeads(ctx)
	assert.Equal(t, aliceKey, identityKeyRows(ctx, p.bobSp)[p.aliceID], "bob's replica of alice's key is untouched")
	assert.Equal(t, aliceKey, identityKeyRows(ctx, p.aliceSp)[p.aliceID], "alice's own key is untouched")
}

// TestE2E_OneToOne_SecondDeviceResolvesPeer proves the exchange lands
// account-wide, not device-wide: a second device of Alice's account,
// opened after the 1-1 converged, resolves Bob's profile on its own —
// Bob does nothing, and the second device never accepts anything. The
// peer's symkey reaches it either through the synced identities
// directory or through the space's identityKeys row it reads on load.
func TestE2E_OneToOne_SecondDeviceResolvesPeer(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("1-1 key-exchange e2e needs cross-account sync; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	p := setupOneToOnePair(t, ctx, yaml, "Alice Multi", "Bob Multi")

	// Alice's second device: same account seed, fresh device key.
	alice2 := openSDKAt(t, ctx, yaml, t.TempDir(), sameAccountFreshDevice(t, p.aliceProv), "alice-2")
	t.Cleanup(func() { _ = alice2.Close() })
	require.Equal(t, p.aliceID, alice2.Account().Id())

	// The 1-1 arrives as an active row through tech-space sync; the
	// second device adopts it without re-accepting.
	var alice2Sp space.Space
	require.Eventually(t, func() bool {
		_ = alice2.Spaces().SyncSpaceList(ctx)
		si, ok := infoByID(t, ctx, alice2, p.spaceId)
		if !ok || si.Status != space.StatusActive {
			return false
		}
		sp, err := alice2.Spaces().Get(ctx, p.spaceId)
		if err != nil {
			return false
		}
		alice2Sp = sp
		return true
	}, 2*time.Minute, 2*time.Second, "alice's second device never adopted the 1-1")

	// Both rows are readable there, and bob's profile resolves — with no
	// action on bob's side and no second accept.
	require.True(t, pollSynced(ctx, 90*time.Second, []space.Space{alice2Sp, p.bobSp}, func() bool {
		rows := identityKeyRows(ctx, alice2Sp)
		return len(rows) == 2 && rows[p.bobID] != ""
	}), "the second device should read both identityKeys rows")
	assert.Equal(t, accountMetaSymKey(t, p.bobProv), identityKeyRows(ctx, alice2Sp)[p.bobID],
		"the second device reads bob's real metadata symkey")

	require.True(t, pollSynced(ctx, 90*time.Second, []space.Space{alice2Sp, p.bobSp}, func() bool {
		info, ok, _ := alice2.Identities().Get(ctx, p.bobID)
		return ok && info.Name == "Bob Multi"
	}), "the second device should resolve bob's profile from the account-wide key")

	si, ok := infoByID(t, ctx, alice2, p.spaceId)
	require.True(t, ok)
	assert.Equal(t, "Bob Multi", si.Name, "the second device names the 1-1 after the peer")
}
