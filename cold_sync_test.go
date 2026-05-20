package anysyncsdk_test

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_ColdSyncSameKey is the simplest sync e2e: one account, two
// devices. Device A creates two spaces, each with a user-defined type
// and a couple of objects. Device B then opens against a fresh DataDir
// using the SAME account / device keys (modeling "user signs into a
// new device") and must end up seeing the spaces, the types, and the
// objects.
//
// Why this is a useful first test: it isolates the cold-pull path
// from the ACL/invite plumbing entirely. There's only one identity,
// so ACL state never changes after space creation; the only thing
// that has to work is "ask sync nodes for everything tied to this
// account and replay it locally". If this fails we know the plain
// pull path is broken; if it passes but the Alice/Bob test fails we
// know the gap is specific to the post-acceptance flow.
//
// Device A is kept alive throughout: it is the only place in the
// network that has just-pushed the user trees, and we don't want
// device A to TTL its space cache or quit while device B is still
// trying to fetch.
func TestE2E_ColdSyncSameKey(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("cold-sync e2e is slow (~2-3min); rerun without -short")
	}

	// Generous timeout — staging round-trips + headsync periods
	// (~30s) for two spaces means the joiner can take a while to
	// converge, especially the first time the network sees these
	// trees. Anything below ~3min has reproduced as flaky.
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	// One identity used by both devices.
	provider := newFixedSeedProvider(t)

	// Device A: fresh DB, do all the writing, then stay open.
	dataDirA := t.TempDir()
	cfgA := config.Config{
		Storage: config.Storage{DataDir: dataDirA, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	sdkA, err := anysyncsdk.Open(ctx, cfgA, provider)
	require.NoError(t, err, "device A: Open")
	t.Cleanup(func() { _ = sdkA.Close() })

	// Build two spaces, each with one user-type and two objects.
	type spaceFixture struct {
		Id        string
		Name      string
		TypeId    string
		PropId    string
		ObjectIds []string
	}
	wantSpaces := make([]spaceFixture, 0, 2)

	for _, label := range []string{"Library", "Notebook"} {
		sp, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: label})
		if err != nil {
			if isNoNetworkErr(err) {
				t.Skipf("network unreachable on space create: %v", err)
			}
			t.Fatalf("device A: Spaces().Create(%s): %v", label, err)
		}

		typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{
			Name: label + "Type",
		})
		require.NoError(t, err)

		propId, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
			Name: "Title",
			XKey: "title",
			Kind: space.PropertyKindString,
		})
		require.NoError(t, err)

		fix := spaceFixture{
			Id: sp.Id(), Name: label, TypeId: typeId, PropId: propId,
		}

		for _, title := range []string{label + "-One", label + "-Two"} {
			objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
				Types: []string{typeId},
			})
			require.NoError(t, err)
			_, err = sp.Properties().SetBase(ctx, objId, typeId, map[string]any{
				propId: title,
			})
			require.NoError(t, err)
			fix.ObjectIds = append(fix.ObjectIds, objId)
		}

		wantSpaces = append(wantSpaces, fix)
		t.Logf("device A: created space %s id=%s typeId=%s propId=%s objects=%v",
			label, fix.Id, fix.TypeId, fix.PropId, fix.ObjectIds)
	}

	// A small local sanity pass on device A — make sure what we wrote
	// is actually readable here. If this fails the test below has no
	// chance, and the failure is local-only (no network needed).
	for _, fix := range wantSpaces {
		sp, err := sdkA.Spaces().Get(ctx, fix.Id)
		require.NoError(t, err, "device A: Spaces().Get(%s)", fix.Id)
		docs, err := sp.QueryObjects().All(ctx)
		require.NoError(t, err, "device A: QueryObjects(%s)", fix.Id)
		// type row + 2 objects = 3
		require.GreaterOrEqual(t, len(docs), 3,
			"device A should see its own type + objects locally; got %d docs in %s", len(docs), fix.Id)
	}

	// Brief pause so device A's PutSyncTree broadcasts durably land
	// on the local node before B's SpacePull asks for them. Stream
	// sync is reactive, but the node still needs a beat to commit
	// the received header to its own storage; without this, B's
	// first SpacePull races A's push and gets ErrSpaceMissing.
	time.Sleep(500 * time.Millisecond)

	// Device B: fresh DB, same keys.
	dataDirB := t.TempDir()
	cfgB := config.Config{
		Storage: config.Storage{DataDir: dataDirB, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	sdkB, err := anysyncsdk.Open(ctx, cfgB, provider)
	require.NoError(t, err, "device B: Open")
	t.Cleanup(func() { _ = sdkB.Close() })

	require.Equal(t, sdkA.Account().Id(), sdkB.Account().Id(),
		"shared seed should derive the same account id on both devices")

	// Step 1: tech-space rehydrates from coordinator → both spaces
	// must show up in List on device B. No public Subscribe API on
	// the tech-space, so we tail List() at a tight cadence — it's a
	// local read against an in-process store, the cost is negligible.
	wantSpaceIds := []string{wantSpaces[0].Id, wantSpaces[1].Id}
	sort.Strings(wantSpaceIds)

	var lastList []space.SpaceInfo
	if !waitFor(ctx, 90*time.Second, 50*time.Millisecond, func() bool {
		list, err := sdkB.Spaces().List(ctx)
		if err != nil {
			return false
		}
		lastList = list
		got := spaceIdsOnly(list)
		sort.Strings(got)
		return stringSliceContainsAll(got, wantSpaceIds)
	}) {
		t.Fatalf("device B never saw both spaces in tech-space; want %v, last=%v",
			wantSpaceIds, spaceIdsOnly(lastList))
	}

	// Step 2: per-space — types and objects must converge. Event-
	// driven via SubscribeProperties: the per-space `objects`
	// firehose fires on every tree's "objects" dataset write
	// (eventbus.ObjectsDataset), which covers BOTH type creates
	// (typesAPI.Create writes any.types=["__type__"] to objects)
	// AND instance creates / SetBase. We register the firehose
	// before re-checking and recheck on every event arrival until
	// the local snapshot satisfies the fixture.
	for _, fix := range wantSpaces {
		fix := fix
		t.Run("space="+fix.Name, func(t *testing.T) {
			// Get may transiently fail with "space is missing" if the
			// space-index entry has propagated through the tech-space
			// but the space's own tree hasn't finished loading via the
			// commonspace cache. Local check, retry tightly.
			var sp space.Space
			require.True(t, waitFor(ctx, 30*time.Second, 50*time.Millisecond, func() bool {
				sp, err = sdkB.Spaces().Get(ctx, fix.Id)
				return err == nil
			}), "device B: Spaces().Get(%s) never succeeded: %v", fix.Id, err)

			propsSub, err := sp.SubscribeProperties(ctx)
			require.NoError(t, err, "device B: SubscribeProperties(%s)", fix.Id)
			defer propsSub.Close()

			var lastTypeIds []string
			var lastObjectIds []string
			snapshotConverged := func() bool {
				typeIds := userTypeIds(sp, ctx)
				lastTypeIds = typeIds
				if !containsString(typeIds, fix.TypeId) {
					return false
				}
				docs, err := sp.QueryObjects().All(ctx)
				if err != nil {
					return false
				}
				ids := make([]string, 0, len(docs))
				for _, d := range docs {
					ids = append(ids, d.GetString("id"))
				}
				lastObjectIds = ids
				for _, want := range fix.ObjectIds {
					if !containsString(ids, want) {
						return false
					}
				}
				// Property values must also be present — SetBase
				// is a separate "objects" write from the create,
				// so seeing the row id isn't enough.
				for i, objId := range fix.ObjectIds {
					wantTitle := fix.Name + []string{"-One", "-Two"}[i]
					rec, err := sp.Properties().Get(ctx, objId, space.PropertyReadOpts{})
					if err != nil || rec == nil {
						return false
					}
					if rec.GetString(fix.TypeId, fix.PropId) != wantTitle {
						return false
					}
				}
				return true
			}

			waitCtx, cancelWait := context.WithTimeout(ctx, 3*time.Minute)
			defer cancelWait()

			for !snapshotConverged() {
				if _, err := propsSub.Mailbox().WaitOne(waitCtx); err != nil {
					t.Fatalf("device B: space %s never converged\n  want type=%s objects=%v\n  got types=%v objects=%v\n  wait err: %v",
						fix.Name, fix.TypeId, fix.ObjectIds, lastTypeIds, lastObjectIds, err)
				}
			}

			// Re-read property values for the explicit assertion
			// signal — the convergence gate above already verified
			// equality, so these only fail on a regression in the
			// gate itself.
			for i, objId := range fix.ObjectIds {
				wantTitle := fix.Name + []string{"-One", "-Two"}[i]
				rec, err := sp.Properties().Get(ctx, objId, space.PropertyReadOpts{})
				require.NoError(t, err, "device B: Properties().Get(%s)", objId)
				require.NotNil(t, rec, "device B: nil property record for %s", objId)
				assert.Equal(t, wantTitle, rec.GetString(fix.TypeId, fix.PropId),
					"device B: property value mismatch on %s", objId)
			}
		})
	}
}

// waitFor polls fn at the given interval until it returns true or the
// per-call deadline elapses (whichever is sooner than ctx). Returns
// true on success.
func waitFor(ctx context.Context, deadline, interval time.Duration, fn func() bool) bool {
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if ctx.Err() != nil {
			return false
		}
		if fn() {
			return true
		}
		select {
		case <-time.After(interval):
		case <-ctx.Done():
			return false
		}
	}
	return false
}

// userTypeIds returns the ids of types on sp that are NOT marked
// BuiltIn. The SDK ships a small set of synthetic built-ins (Any,
// Markdown, Chat, Nav) which would otherwise mask whether the user
// type from the source device made it across.
func userTypeIds(sp space.Space, ctx context.Context) []string {
	list, err := sp.Types().List(ctx)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(list))
	for _, ti := range list {
		if ti.BuiltIn {
			continue
		}
		out = append(out, ti.Id)
	}
	return out
}

func spaceIdsOnly(infos []space.SpaceInfo) []string {
	out := make([]string, 0, len(infos))
	for _, s := range infos {
		out = append(out, s.Id)
	}
	return out
}

func stringSliceContainsAll(haystack, needles []string) bool {
	for _, n := range needles {
		if !containsString(haystack, n) {
			return false
		}
	}
	return true
}

func containsString(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// loadAnySyncNetwork picks the best available network config for the
// e2e test. Order:
//
//  1. ANYSYNC_NETWORK_YAML — explicit override path. Set this on CI
//     (or locally) to point at a controlled fixture.
//  2. /tmp/anysync-dev/sdk-network.yml — the path the local
//     any-sync-tools/any-sync-network bootstrapper writes when
//     --output is left at default. Catches the common "I started a
//     local network for debugging" case automatically.
//  3. ../test-etc/staging.yml — public staging. Slow + flaky against
//     parts of the cluster (some addresses no longer resolve), but
//     works for a smoke run.
//
// Returns (yaml bytes, path used, err). err is non-nil only when
// nothing readable was found; callers should t.Skip on that.
func loadAnySyncNetwork() ([]byte, string, error) {
	candidates := []string{
		os.Getenv("ANYSYNC_NETWORK_YAML"),
		filepath.Join("..", "test-etc", "staging.yml"),
	}
	for _, p := range candidates {
		if p == "" {
			continue
		}
		yaml, err := os.ReadFile(p)
		if err == nil {
			return yaml, p, nil
		}
	}
	return nil, "", os.ErrNotExist
}
