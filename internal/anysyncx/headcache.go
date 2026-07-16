package anysyncx

import (
	"sync"

	"github.com/anyproto/any-sync/commonspace/headsync/statestorage"
)

// HeadCache keeps the current per-space space-hash in memory so the
// HeadSync RPC can answer the common full-range probe without
// loading the underlying commonspace.Space.
//
// Only the new (DiffType_V3) hash is tracked — V2/V1 are legacy. A
// V2 request that reaches us falls through to the deep-diff path
// where any-sync's headsync subsystem still computes the right
// answer; we just don't accelerate it.
//
// The cache is populated by a statestorage.Observer wired during
// space load, and survives space TTL eviction. The hash is opaque
// hex from any-sync — we never compute or compare it ourselves.
type HeadCache struct {
	mu sync.RWMutex
	m  map[string]string
}

func newHeadCache() *HeadCache { return &HeadCache{m: make(map[string]string)} }

// Get returns the cached hash for spaceId. The bool is false when
// the cache has nothing — callers fall back to the deep diff.
func (c *HeadCache) Get(spaceId string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.m[spaceId]
	return v, ok
}

// Set replaces the cached hash for spaceId.
func (c *HeadCache) Set(spaceId, hash string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[spaceId] = hash
}

// Delete drops the entry — used when a space is deleted locally.
func (c *HeadCache) Delete(spaceId string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, spaceId)
}

// observerFor returns a statestorage.Observer that updates the cache
// whenever any-sync writes a new hash for spaceId.
func (c *HeadCache) observerFor(spaceId string) statestorage.Observer {
	return &cacheObserver{cache: c, spaceId: spaceId}
}

type cacheObserver struct {
	cache   *HeadCache
	spaceId string
}

// OnHashChange satisfies statestorage.Observer. Fires from inside
// any-sync's SpaceStorage on a successful SetHash — same write tx
// the head update committed under, so the cache moves in lockstep
// with persistence.
func (o *cacheObserver) OnHashChange(newHash string) {
	o.cache.Set(o.spaceId, newHash)
}
