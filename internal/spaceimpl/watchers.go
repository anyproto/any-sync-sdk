package spaceimpl

import (
	"sync"
)

// stopper is the minimal contract a watcher registers with the
// per-Service watcherRegistry: a blocking stop() that signals shutdown
// and waits for the watcher goroutine(s) to drain. Both memberWatcher
// and spaceIndexWatcher satisfy it.
type stopper interface {
	stop()
}

// spaceScoped is the optional contract a watcher implements so the
// registry can stop just the watchers belonging to one space — used by
// the space-offload path. Watchers that don't implement it are left
// alone by stopForSpace and only drained by stopAll on SDK shutdown.
type spaceScoped interface {
	stopper
	spaceID() string
}

// watcherRegistry tracks every active per-space watcher in the
// service. We can't keep them on spaceImpl alone — the SDK creates a
// fresh spaceImpl per Get / Create / Derive call, so a Subscribe
// on one handle would never be visible to a Close on another. The
// registry sits on the Service (one per SDK) and is drained on
// SDK.Close.
type watcherRegistry struct {
	mu sync.Mutex
	wm map[stopper]struct{}
}

// register adds w to the registry. Safe to call multiple times — it's
// a set.
func (r *watcherRegistry) register(w stopper) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.wm == nil {
		r.wm = make(map[stopper]struct{})
	}
	r.wm[w] = struct{}{}
}

// unregister removes w from the registry without stopping it.
// Called by membersAPI when the last subscriber departs.
func (r *watcherRegistry) unregister(w stopper) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.wm, w)
}

// stopAll signals every registered watcher to stop and waits for
// each goroutine to finish. Idempotent — re-running is a no-op.
func (r *watcherRegistry) stopAll() {
	r.mu.Lock()
	ws := make([]stopper, 0, len(r.wm))
	for w := range r.wm {
		ws = append(ws, w)
	}
	r.wm = nil
	r.mu.Unlock()
	for _, w := range ws {
		w.stop()
	}
}

// stopForSpace stops and unregisters every space-scoped watcher whose
// spaceID matches, leaving other spaces' watchers untouched. Used by
// the offload path to drain a single space's pollers. Blocks until each
// matched watcher's goroutine drains.
func (r *watcherRegistry) stopForSpace(spaceId string) {
	r.mu.Lock()
	var matched []stopper
	for w := range r.wm {
		if sw, ok := w.(spaceScoped); ok && sw.spaceID() == spaceId {
			matched = append(matched, w)
			delete(r.wm, w)
		}
	}
	r.mu.Unlock()
	for _, w := range matched {
		w.stop()
	}
}

// kickProfiles queues an identityRepo refetch on every registered
// members watcher rather than waiting for the next slow tick. Used after Account.UpdateMetadata so the caller's
// own profile change is visible across loaded spaces without a
// 60-second delay. Watchers that don't track profiles (e.g. the
// spaceIndex watcher) are skipped via the type assertion.
func (r *watcherRegistry) kickProfiles() {
	r.mu.Lock()
	ws := make([]*memberWatcher, 0, len(r.wm))
	for w := range r.wm {
		if mw, ok := w.(*memberWatcher); ok {
			ws = append(ws, mw)
		}
	}
	r.mu.Unlock()
	for _, w := range ws {
		w.requestProfiles(nil)
	}
}
