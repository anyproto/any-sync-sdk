package spaceimpl

import (
	"context"
	"sync"
)

// watcherRegistry tracks every active members poller in the
// service. We can't keep them on spaceImpl alone — the SDK creates a
// fresh spaceImpl per Get / Create / Derive call, so a Subscribe
// on one handle would never be visible to a Close on another. The
// registry sits on the Service (one per SDK) and is drained on
// SDK.Close.
type watcherRegistry struct {
	mu sync.Mutex
	wm map[*memberWatcher]struct{}
}

// register adds w to the registry. Safe to call multiple times — it's
// a set.
func (r *watcherRegistry) register(w *memberWatcher) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.wm == nil {
		r.wm = make(map[*memberWatcher]struct{})
	}
	r.wm[w] = struct{}{}
}

// unregister removes w from the registry without stopping it.
// Called by membersAPI when the last subscriber departs.
func (r *watcherRegistry) unregister(w *memberWatcher) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.wm, w)
}

// stopAll signals every registered watcher to stop and waits for
// each goroutine to finish. Idempotent — re-running is a no-op.
func (r *watcherRegistry) stopAll() {
	r.mu.Lock()
	ws := make([]*memberWatcher, 0, len(r.wm))
	for w := range r.wm {
		ws = append(ws, w)
	}
	r.wm = nil
	r.mu.Unlock()
	for _, w := range ws {
		w.stop()
	}
}

// kickProfiles asks every registered watcher to refetch
// identityRepo profiles immediately rather than waiting for the
// next slow tick. Used after Account.UpdateMetadata so the caller's
// own profile change is visible across loaded spaces without a
// 60-second delay.
func (r *watcherRegistry) kickProfiles(ctx context.Context) {
	r.mu.Lock()
	ws := make([]*memberWatcher, 0, len(r.wm))
	for w := range r.wm {
		ws = append(ws, w)
	}
	r.mu.Unlock()
	for _, w := range ws {
		go w.fetchProfilesOnce(ctx)
	}
}
