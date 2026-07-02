package fetch

import (
	"context"
	"sync"

	"github.com/anyproto/any-sync-sdk/internal/files/store"
)

// baseURLKeyPrefix namespaces the persisted base per network — a
// config pointing at another network must not reuse a stale base.
const baseURLKeyPrefix = "publicReadBaseUrl/"

// NewBaseURL builds the standard BaseURL provider: a config override
// wins outright (private deployments, tests); otherwise the value is
// read from the store KV (persisted at first resolve, so URL
// construction works offline from then on) and fetched once via info
// (the broker Info RPC) when never seen. An empty answer from info is
// valid ("no public read") and is cached in memory only — the next
// process retries, so a network that turns public read on is picked
// up on restart without a TTL scheme.
func NewBaseURL(st *store.Store, networkId, override string, info func(ctx context.Context) (string, error)) BaseURL {
	var (
		mu     sync.Mutex
		cached string
		loaded bool
	)
	key := baseURLKeyPrefix + networkId
	return func(ctx context.Context) (string, error) {
		if override != "" {
			return override, nil
		}
		mu.Lock()
		defer mu.Unlock()
		if loaded {
			return cached, nil
		}
		if v, ok, err := st.GetKV(ctx, key); err != nil {
			return "", err
		} else if ok {
			cached, loaded = v, true
			return cached, nil
		}
		v, err := info(ctx)
		if err != nil {
			return "", err // not cached: resolve again next call
		}
		if v != "" {
			if err = st.SetKV(ctx, key, v); err != nil {
				return "", err
			}
		}
		cached, loaded = v, true
		return cached, nil
	}
}
