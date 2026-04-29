package anysyncx

import (
	"context"
	"encoding/hex"
	"math"
	"testing"

	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHeadCache_GetSetDelete(t *testing.T) {
	c := newHeadCache()
	_, ok := c.Get("space-A")
	assert.False(t, ok)

	c.Set("space-A", "deadbeef")
	got, ok := c.Get("space-A")
	require.True(t, ok)
	assert.Equal(t, "deadbeef", got)

	c.Delete("space-A")
	_, ok = c.Get("space-A")
	assert.False(t, ok)
}

func TestHeadCache_ObserverUpdates(t *testing.T) {
	c := newHeadCache()
	obs := c.observerFor("space-A")
	obs.OnHashChange("o1", "n1")
	got, ok := c.Get("space-A")
	require.True(t, ok)
	assert.Equal(t, "n1", got)
	obs.OnHashChange("n1", "n2")
	got, _ = c.Get("space-A")
	assert.Equal(t, "n2", got)
}

// TestHeadSync_FastPathFromCache exercises the spaceSyncHandler's
// HeadSync fast path in isolation — no network, no spaces loaded.
// Confirms a full-range V3 request with a cached hash is answered
// without going through getSpace.
func TestHeadSync_FastPathFromCache(t *testing.T) {
	h := newSpaceSyncHandler()
	h.headCache = newHeadCache()

	const spaceId = "space-cached"
	hashHex := hex.EncodeToString([]byte("0123456789abcdef0123"))
	h.headCache.Set(spaceId, hashHex)

	resp, err := h.HeadSync(context.Background(), &spacesyncproto.HeadSyncRequest{
		SpaceId:  spaceId,
		DiffType: spacesyncproto.DiffType_V3,
		Ranges:   []*spacesyncproto.HeadSyncRange{{From: 0, To: math.MaxUint64}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Results, 1)
	assert.Equal(t, spacesyncproto.DiffType_V3, resp.DiffType)
	wantHash, _ := hex.DecodeString(hashHex)
	assert.Equal(t, wantHash, resp.Results[0].Hash)
	assert.EqualValues(t, 1, resp.Results[0].Count)
}

// V2 (legacy) requests must NOT be answered from the cache — fall
// through to the deep path.
func TestHeadSync_FallsThroughOnV2(t *testing.T) {
	h := newSpaceSyncHandler()
	h.headCache = newHeadCache()
	h.headCache.Set("space-X", "deadbeef")

	_, err := h.HeadSync(context.Background(), &spacesyncproto.HeadSyncRequest{
		SpaceId:  "space-X",
		DiffType: spacesyncproto.DiffType_V2,
		Ranges:   []*spacesyncproto.HeadSyncRange{{From: 0, To: math.MaxUint64}},
	})
	require.Error(t, err, "V2 must fall through to getSpace")
}

// Sub-range request must NOT short-circuit; the cached hash covers
// the whole space and can't answer a sub-range probe.
func TestHeadSync_FallsThroughOnSubRange(t *testing.T) {
	h := newSpaceSyncHandler()
	h.headCache = newHeadCache()
	h.headCache.Set("space-X", "deadbeef")

	_, err := h.HeadSync(context.Background(), &spacesyncproto.HeadSyncRequest{
		SpaceId:  "space-X",
		DiffType: spacesyncproto.DiffType_V3,
		Ranges:   []*spacesyncproto.HeadSyncRange{{From: 0, To: 1000}},
	})
	require.Error(t, err)
}

// Elements=true asks for the actual head-id list, not just the hash —
// must NOT short-circuit.
func TestHeadSync_FallsThroughOnElementsRequest(t *testing.T) {
	h := newSpaceSyncHandler()
	h.headCache = newHeadCache()
	h.headCache.Set("space-Y", "deadbeef")

	_, err := h.HeadSync(context.Background(), &spacesyncproto.HeadSyncRequest{
		SpaceId:  "space-Y",
		DiffType: spacesyncproto.DiffType_V3,
		Ranges:   []*spacesyncproto.HeadSyncRange{{From: 0, To: math.MaxUint64, Elements: true}},
	})
	require.Error(t, err)
}
