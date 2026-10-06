package e2e

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_JoinerGetsDefinitionsFirst: a joiner of a large space holds
// every type and collection object before most of the ordinary
// objects, so an object's changes apply on arrival instead of parking
// until its definitions land. A sync round probes the roots of its
// missing trees for their changeType and fetches types and collections
// first; a bundle declares its type on a root of the plain object
// changeType, which the round fetches after the spaceIndex that lists
// it.
//
// Skips when no any-sync network is reachable.
func TestE2E_JoinerGetsDefinitionsFirst(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("join e2e is slow; rerun without -short")
	}

	const (
		userTypes   = 4
		collections = 3
		objects     = 400
		// maxObjectsAtDefs bounds the objects already local when the
		// last definition becomes local. Definitions first measure the
		// sampling's own lag: a handful, a few dozen on a loaded
		// machine. In diff order the last of the 8 definitions lands
		// with about 90% of the objects local.
		maxObjectsAtDefs = objects / 3
	)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	// LAN p2p is off: the owner, in this process, would push the space
	// to the joiner over it in its own order, outside the joiner's
	// rounds.
	p2pOff := false
	openSDK := func(name string) *anysyncsdk.SDK {
		t.Helper()
		cfg := config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
			P2P:     config.P2P{Enabled: &p2pOff},
		}
		sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
		require.NoError(t, err, "%s: Open", name)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk
	}

	alice := openSDK("alice")
	bob := openSDK("bob")
	require.NoError(t, bob.Account().UpdateMetadata(ctx, space.AccountMetadata{Name: "Bob"}))

	sp, err := alice.Spaces().Create(ctx, space.CreateRequest{Name: "AliceCatalog"})
	require.NoError(t, err, "alice: Spaces().Create")

	// def is a type or a collection and the property ids its members
	// write: title and rank on a type, note on a collection.
	type def struct {
		id    string
		props []string
	}
	strProp := func(name, xkey string) space.PropertyDraft {
		return space.PropertyDraft{Name: name, XKey: xkey, Kind: space.PropertyKindString}
	}
	rankProp := space.PropertyDraft{Name: "Rank", XKey: "rank", Kind: space.PropertyKindNumber}

	var types, colls []def
	for i := 0; i < userTypes; i++ {
		id, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: fmt.Sprintf("Kind%d", i)})
		require.NoError(t, err)
		title, err := sp.Types().AddProperty(ctx, id, strProp("Title", "title"))
		require.NoError(t, err)
		rank, err := sp.Types().AddProperty(ctx, id, rankProp)
		require.NoError(t, err)
		types = append(types, def{id: id, props: []string{title, rank}})
	}
	// The bundle's root is an object tree; only the spaceIndex's
	// bundle list names it as a definition.
	bundle, _, err := sp.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "recipes/v1", Name: "Recipe", XKey: "recipe",
		Properties: []space.PropertyDraft{strProp("Title", "title"), rankProp},
	})
	require.NoError(t, err, "alice: Bundles().Ensure")
	bundleProps, err := sp.Types().Properties(ctx, bundle.RootId)
	require.NoError(t, err)
	byKey := map[string]string{}
	for _, p := range bundleProps {
		byKey[p.XKey] = p.Id
	}
	require.NotEmpty(t, byKey["title"])
	require.NotEmpty(t, byKey["rank"])
	types = append(types, def{id: bundle.RootId, props: []string{byKey["title"], byKey["rank"]}})
	for i := 0; i < collections; i++ {
		id, err := sp.Collections().Create(ctx, space.CollectionCreateParams{Name: fmt.Sprintf("Shelf%d", i)})
		require.NoError(t, err)
		note, err := sp.Collections().AddProperty(ctx, id, strProp("Note", "note"))
		require.NoError(t, err)
		colls = append(colls, def{id: id, props: []string{note}})
	}
	var defIds []string
	for _, d := range slices.Concat(types, colls) {
		defIds = append(defIds, d.id)
	}

	// Object i has type i%5 and, three times in four, collection i%4
	// with a note: every create names one or two definitions.
	collOf := func(i int) int {
		if c := i % (len(colls) + 1); c < len(colls) {
			return c
		}
		return -1
	}
	ids := make([]string, objects)
	for i := range ids {
		tp := types[i%len(types)]
		opts := space.CreateObjectOpts{
			Type: tp.id,
			InitialProperties: map[string]map[string]any{
				tp.id: {tp.props[0]: fmt.Sprintf("item %d", i), tp.props[1]: float64(i)},
			},
		}
		if c := collOf(i); c >= 0 {
			opts.Collections = []string{colls[c].id}
			opts.InitialProperties[colls[c].id] = map[string]any{colls[c].props[0]: fmt.Sprintf("note %d", i)}
		}
		ids[i], err = sp.Objects().Create(ctx, opts)
		require.NoError(t, err, "alice: create object %d", i)
	}

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

	_, err = bob.Spaces().Join(ctx, space.JoinRequest{Invite: token})
	require.ErrorIs(t, err, spaceimpl.ErrJoinPending, "bob: Join")
	req := awaitJoinRequest(t, ctx, sp, bob.Account().Id())

	// Bob's first round must find the whole space on the network, or
	// the order under test covers only a part of it.
	if !waitFor(ctx, 2*time.Minute, 500*time.Millisecond, func() bool {
		_ = sp.SyncHeads(ctx)
		return alice.Spaces().Status(sp.Id()).State == space.SyncStateSynced
	}) {
		t.Fatalf("alice's space never reached synced")
	}

	require.NoError(t, sp.ACL().AcceptRequest(ctx, req.RecordId, space.PermissionWriter), "alice: AcceptRequest")
	accepted := time.Now()

	var bobSpace space.Space
	if !waitFor(ctx, 2*time.Minute, 5*time.Millisecond, func() bool {
		bobSpace, err = bob.Spaces().Get(ctx, sp.Id())
		return err == nil
	}) {
		t.Fatalf("bob: Get after accept: %v", err)
	}

	// A tree counts as local once Objects().Get answers for it — with
	// its row, or with {id} while its changes are parked.
	isLocal := func(id string) bool {
		_, err := bobSpace.Objects().Get(ctx, id)
		return err == nil
	}
	// Parked changes are rows of the space's detached collection; a
	// replay deletes its row, so the ids seen are collected per sample.
	detachedName := sp.Id() + "_" + spaceobjects.DetachedCollection
	var detached anystore.Collection
	everParked := map[string]struct{}{}
	peakParked := 0
	sampleParked := func() int {
		if detached == nil {
			c, err := bob.Store().OpenCollection(ctx, detachedName)
			if errors.Is(err, anystore.ErrCollectionNotFound) {
				return 0
			}
			require.NoError(t, err)
			// The handle is the SDK's own; it is never closed here.
			detached = c
		}
		n, err := detached.Count(ctx)
		require.NoError(t, err)
		if n == 0 {
			return 0
		}
		peakParked = max(peakParked, n)
		iter, err := detached.Find(nil).Iter(ctx)
		require.NoError(t, err)
		for iter.Next() {
			doc, err := iter.Doc()
			require.NoError(t, err)
			everParked[doc.Value().GetString("id")] = struct{}{}
		}
		require.NoError(t, iter.Close())
		return n
	}

	// Each sample checks the definitions before the objects, so the
	// object count recorded when the last definition is seen is an
	// upper bound on the objects local at that moment.
	localDefs := map[string]int{}
	localObjs := map[string]struct{}{}
	objectsAtDefs := -1
	var defsAfter time.Duration
	if !waitFor(ctx, 3*time.Minute, 5*time.Millisecond, func() bool {
		var arrived []string
		for _, id := range defIds {
			if _, ok := localDefs[id]; !ok && isLocal(id) {
				arrived = append(arrived, id)
			}
		}
		for _, id := range ids {
			if _, ok := localObjs[id]; !ok && isLocal(id) {
				localObjs[id] = struct{}{}
			}
		}
		for _, id := range arrived {
			localDefs[id] = len(localObjs)
		}
		if objectsAtDefs < 0 && len(localDefs) == len(defIds) {
			objectsAtDefs = len(localObjs)
			defsAfter = time.Since(accepted)
		}
		sampleParked()
		return len(localDefs) == len(defIds) && len(localObjs) == objects
	}) {
		t.Fatalf("bob: %d of %d definitions and %d of %d objects synced",
			len(localDefs), len(defIds), len(localObjs), objects)
	}
	allAfter := time.Since(accepted)

	// Every object carries its values and membership, and nothing
	// stays parked.
	mismatch := func(i int) error {
		row, err := bobSpace.Objects().Get(ctx, ids[i])
		if err != nil {
			return err
		}
		tp := types[i%len(types)]
		if got := row.GetString("any", "type"); got != tp.id {
			return fmt.Errorf("type %q, want %q", got, tp.id)
		}
		if got, want := row.GetString(tp.id, tp.props[0]), fmt.Sprintf("item %d", i); got != want {
			return fmt.Errorf("title %q, want %q", got, want)
		}
		if got := row.GetFloat64(tp.id, tp.props[1]); got != float64(i) {
			return fmt.Errorf("rank %v, want %d", got, i)
		}
		var want []string
		if c := collOf(i); c >= 0 {
			want = []string{colls[c].id}
			if got, wantNote := row.GetString(colls[c].id, colls[c].props[0]), fmt.Sprintf("note %d", i); got != wantNote {
				return fmt.Errorf("note %q, want %q", got, wantNote)
			}
		}
		if got := collArrayOf(row, "any", "collections"); !slices.Equal(got, want) {
			return fmt.Errorf("collections %v, want %v", got, want)
		}
		return nil
	}
	var firstBad error
	if !waitFor(ctx, time.Minute, 100*time.Millisecond, func() bool {
		firstBad = nil
		for i := range ids {
			if err := mismatch(i); err != nil {
				firstBad = fmt.Errorf("object %d (%s): %w", i, ids[i], err)
				break
			}
		}
		return sampleParked() == 0 && firstBad == nil
	}) {
		t.Fatalf("bob never converged: %v; %d changes still parked", firstBad, sampleParked())
	}

	var positions []int
	for _, id := range defIds {
		positions = append(positions, localDefs[id])
	}
	t.Logf("bob: definitions local after %s, all %d objects after %s, converged after %s",
		defsAfter.Round(time.Millisecond), objects, allAfter.Round(time.Millisecond), time.Since(accepted).Round(time.Millisecond))
	t.Logf("bob: objects local as each definition arrived (types, bundle type, collections): %v", positions)
	t.Logf("bob: %d of %d objects local when the last definition arrived", objectsAtDefs, objects)
	t.Logf("bob: %d changes parked through the sync, at most %d at once", len(everParked), peakParked)

	require.LessOrEqual(t, objectsAtDefs, maxObjectsAtDefs,
		"types and collections must arrive ahead of the bulk of the objects")
	require.Zero(t, len(everParked),
		"objects synced after their definitions must apply without parking")
}
