package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_Meta_ReachesTheOtherDevice: the creation bag a create carries
// lands on the row of every peer — it rides the bootstrap change, and
// the inbound apply admits it on the object's first change — and a
// filter on a key inside it answers there too.
func TestE2E_Meta_ReachesTheOtherDevice(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("two-device e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	providerA := newFixedSeedProvider(t)
	providerB := sameAccountFreshDevice(t, providerA)
	sdkA := collDevice(t, ctx, yaml, providerA, "device A")

	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "Meta"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("device A: Spaces().Create: %v", err)
	}
	typeId, err := spA.Types().Create(ctx, space.TypeCreateParams{Name: "Doc"})
	require.NoError(t, err)
	note, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)
	task, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId, Meta: map[string]any{"createdIn": note}})
	require.NoError(t, err)

	sdkB := collDevice(t, ctx, yaml, providerB, "device B")
	var spB space.Space
	require.Eventually(t, func() bool {
		list, lerr := sdkB.Spaces().List(ctx)
		if lerr != nil {
			return false
		}
		for _, info := range list {
			if info.Id == spA.Id() {
				spB, lerr = sdkB.Spaces().Get(ctx, spA.Id())
				return lerr == nil
			}
		}
		return false
	}, 3*time.Minute, 3*time.Second, "device B: space never appeared")

	require.Eventually(t, func() bool {
		row, gerr := spB.Objects().Get(ctx, task)
		return gerr == nil && row.GetString("meta", "createdIn") == note
	}, 3*time.Minute, 2*time.Second, "device B: the task row never carried meta.createdIn")
	assert.Equal(t, []string{task}, collRowIds(collQueryAll(t, ctx, spB, map[string]any{"meta.createdIn": note})))
	noteRow, err := spB.Objects().Get(ctx, note)
	require.NoError(t, err)
	assert.Nil(t, noteRow.Get("meta"))
}
