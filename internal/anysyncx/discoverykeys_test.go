package anysyncx

import (
	"context"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestDiscoveryKeySource builds a source over an empty storage root:
// every derivation fails with ErrSpaceStorageMissing, which is exactly
// the request-time state of a fresh join (space not pulled yet).
func newTestDiscoveryKeySource(t *testing.T) *discoveryKeySource {
	keys, err := accountdata.NewRandom()
	require.NoError(t, err)
	return newDiscoveryKeySource(newStorageProvider(t.TempDir()), keys)
}

func TestDiscoveryKeysNegativeCache(t *testing.T) {
	d := newTestDiscoveryKeySource(t)
	ctx := context.Background()
	const spaceId = "space.id"

	assert.Empty(t, d.DiscoveryKeys(ctx, []string{spaceId}))
	d.mu.Lock()
	stampedAt, stamped := d.failedAt[spaceId]
	d.mu.Unlock()
	require.True(t, stamped)

	// Within negativeRetryAfter the failure is served from cache: the
	// stamp must not move.
	assert.Empty(t, d.DiscoveryKeys(ctx, []string{spaceId}))
	d.mu.Lock()
	assert.Equal(t, stampedAt, d.failedAt[spaceId])
	d.mu.Unlock()

	// An expired stamp re-attempts derivation (and re-stamps on failure).
	d.mu.Lock()
	d.failedAt[spaceId] = time.Now().Add(-negativeRetryAfter - time.Second)
	d.mu.Unlock()
	assert.Empty(t, d.DiscoveryKeys(ctx, []string{spaceId}))
	d.mu.Lock()
	assert.True(t, d.failedAt[spaceId].After(stampedAt))
	d.mu.Unlock()
}

func TestDiscoveryKeysResetNegative(t *testing.T) {
	d := newTestDiscoveryKeySource(t)
	ctx := context.Background()
	const spaceId = "space.id"

	assert.Empty(t, d.DiscoveryKeys(ctx, []string{spaceId}))
	d.mu.Lock()
	before := d.failedAt[spaceId]
	d.mu.Unlock()
	require.False(t, before.IsZero())

	// ResetNegative drops the entry, so the next lookup re-attempts
	// derivation immediately instead of waiting out negativeRetryAfter —
	// observable as a fresh (later) failure stamp.
	d.ResetNegative()
	d.mu.Lock()
	assert.Empty(t, d.failedAt)
	d.mu.Unlock()

	assert.Empty(t, d.DiscoveryKeys(ctx, []string{spaceId}))
	d.mu.Lock()
	after, stamped := d.failedAt[spaceId]
	d.mu.Unlock()
	require.True(t, stamped)
	assert.False(t, after.Before(before))
}
