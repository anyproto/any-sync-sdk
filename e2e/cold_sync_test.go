package e2e

import (
	"context"
	"os"
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
	t.Parallel()
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
			_, err = sp.Properties().Set(ctx, objId, typeId, map[string]any{
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

	// Make sure device A's data is durably on the node before B pulls.
	// Stream sync is reactive, but B's SpacePull can otherwise race A's
	// push and get ErrSpaceMissing. Kicking A's tech space + each space
	// head-syncs them with the node now, so B sees a fully-populated
	// node instead of waiting on A's periodic push.
	_ = sdkA.Spaces().SyncSpaceList(ctx)
	for _, fix := range wantSpaces {
		if sp, err := sdkA.Spaces().Get(ctx, fix.Id); err == nil {
			_ = sp.SyncHeads(ctx)
		}
	}

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
	if !waitFor(ctx, 90*time.Second, 250*time.Millisecond, func() bool {
		// Kick an immediate tech-space diff round instead of waiting
		// for the periodic headsync timer — this is what makes cold
		// sync converge in a beat rather than seconds.
		_ = sdkB.Spaces().SyncSpaceList(ctx)
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
	// driven via QueryObjects().Subscribe: the per-space `objects`
	// dataset writes fire for BOTH type creates (typesAPI.Create
	// writes any.types=["__type__"] to objects) AND instance creates
	// / Properties().Set. We register before re-checking and recheck on every
	// event arrival until the local snapshot satisfies the fixture.
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

			subRes, err := sp.QueryObjects().Subscribe(ctx, space.QueryOpts{})
			require.NoError(t, err, "device B: QueryObjects().Subscribe(%s)", fix.Id)
			defer subRes.Sub.Close()

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
				// Property values must also be present — Properties().Set
				// is a separate "objects" write from the create,
				// so seeing the row id isn't enough.
				for i, objId := range fix.ObjectIds {
					wantTitle := fix.Name + []string{"-One", "-Two"}[i]
					rec, err := sp.Properties().Get(ctx, objId)
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
				// Force an immediate per-space diff round so trees pull
				// now instead of on the periodic timer, then wake on the
				// next subscribe event or a short fallback tick and
				// re-check. The kick is what de-flakes / speeds this up;
				// the subscription is still exercised as the wake signal.
				_ = sp.SyncHeads(waitCtx)
				tickCtx, cancelTick := context.WithTimeout(waitCtx, 500*time.Millisecond)
				_, _ = subRes.Sub.Events().WaitOne(tickCtx)
				cancelTick()
				if waitCtx.Err() != nil {
					t.Fatalf("device B: space %s never converged\n  want type=%s objects=%v\n  got types=%v objects=%v\n  wait err: %v",
						fix.Name, fix.TypeId, fix.ObjectIds, lastTypeIds, lastObjectIds, waitCtx.Err())
				}
			}

			// Re-read property values for the explicit assertion
			// signal — the convergence gate above already verified
			// equality, so these only fail on a regression in the
			// gate itself.
			for i, objId := range fix.ObjectIds {
				wantTitle := fix.Name + []string{"-One", "-Two"}[i]
				rec, err := sp.Properties().Get(ctx, objId)
				require.NoError(t, err, "device B: Properties().Get(%s)", objId)
				require.NotNil(t, rec, "device B: nil property record for %s", objId)
				assert.Equal(t, wantTitle, rec.GetString(fix.TypeId, fix.PropId),
					"device B: property value mismatch on %s", objId)
			}
		})
	}
}

// getSpaceEventually retries Spaces().Get until the load succeeds. A
// Get racing a concurrent load of the same space can surface a
// transient "context canceled" verdict from a coalesced network call
// inside NewSpace — ocache retries only loads aborted by the load ctx
// itself, not inner cancellations (the same transient the indexer's
// spawnWorker retries with backoff).
func getSpaceEventually(ctx context.Context, t *testing.T, sdk *anysyncsdk.SDK, spaceId string) space.Space {
	t.Helper()
	var (
		sp   space.Space
		last error
	)
	if !waitFor(ctx, 30*time.Second, 500*time.Millisecond, func() bool {
		sp, last = sdk.Spaces().Get(ctx, spaceId)
		return last == nil
	}) {
		t.Fatalf("space %s never loaded: %v", spaceId, last)
	}
	return sp
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

// stagingNetworkPath is the single any-sync network config every e2e
// test uses. Hard-coded (no env-var indirection) so the whole test
// process targets one network deterministically — mixing networks
// across tests in the same run was a real source of cross-test
// interference. Kept inside the repo (and gitignored) so tests run
// without leaning on a path outside the module; refresh by copying
// from your local source of truth. Edit this constant or replace
// the file to point at a different network.
var stagingNetworkPath = "staging.yml"

// loadAnySyncNetwork reads the staging network YAML. Returns
// (yaml bytes, path used, err). The path is returned in both the
// success and failure case so callers can use it in t.Skipf messages.
func loadAnySyncNetwork() ([]byte, string, error) {
	yaml, err := os.ReadFile(stagingNetworkPath)
	return yaml, stagingNetworkPath, err
}
