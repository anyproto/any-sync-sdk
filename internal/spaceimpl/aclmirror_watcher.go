// Per-space ACL mirror: ACL-derived state → tech-space row. Mirrors
// the pattern of anytype-heart's SetAclInfo → space-view details
// write. Two row fields today:
//
//   - push keys — clients that must decrypt
//     push payloads while the SDK process is down (mobile
//     notification extensions) read the derived key material off the
//     space row and cache it natively;
//   - own role — this account's ACL permission, so
//     List/Subscribe surface SpaceInfo.OwnRole without loading every
//     space.
//
// Trigger model: one mirror attempt at wiring time (space load — the
// freshest ACL state, covering anything missed while unloaded) plus a
// kick per applied ACL record via the aclKickMux (local ops AND
// records pushed from peers), so a read-key rotation lands on the row
// promptly. A failed pass self-retries on a delay (bounded — see
// scheduleRetry) because on a quiet space no further ACL record will
// re-kick it. No periodic poll beyond that: keys only change when an
// ACL record applies, and applying requires the loaded space this
// watcher is wired to.

package spaceimpl

import (
	"context"
	"sync"
	"time"

	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/pushclient"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

var aclMirrorLog = logger.NewNamed("sdk.aclmirror")

const (
	// aclMirrorTimeout bounds one mirror pass. Everything it
	// touches is local (loaded space's ACL under RLock, tech-space
	// store write); the generous bound only guards teardown races —
	// and stop() cancels the watcher ctx, so shutdown never waits it
	// out.
	aclMirrorTimeout = 30 * time.Second
	// aclMirrorRetryDelay / aclMirrorRetryMax bound the failed-pass retry
	// loop. Retries stop after the cap so an expected-persistent
	// failure (pending joiner without read access, space storage not
	// materialized yet) doesn't reload the space forever; any external
	// ACL kick — including the accept record that resolves the joiner
	// case — resets the budget.
	aclMirrorRetryDelay = 30 * time.Second
	aclMirrorRetryMax   = 5
)

type aclMirrorWatcher struct {
	s  *Service
	id string

	// ctx is the watcher lifetime — mirror passes derive their
	// timeout from it, and stop() cancels it so an in-flight pass
	// aborts instead of stalling shutdown.
	ctx    context.Context
	cancel context.CancelFunc

	// kickCh coalesces mirror triggers (buffered 1) so the syncacl
	// write path never blocks on the mirror.
	kickCh   chan struct{}
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	// retryMu guards the failed-pass retry state: one pending timer at
	// a time, attempts counted against aclMirrorRetryMax and reset by
	// any external ACL kick.
	retryMu    sync.Mutex
	retryTimer *time.Timer
	attempts   int
}

func newAclMirrorWatcher(s *Service, spaceId string) *aclMirrorWatcher {
	w := &aclMirrorWatcher{
		s:      s,
		id:     spaceId,
		kickCh: make(chan struct{}, 1),
		stopCh: make(chan struct{}),
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	// Prime one pass so the row is mirrored at wiring time without
	// waiting for an ACL record.
	w.kickCh <- struct{}{}
	w.wg.Add(1)
	go w.loop()
	return w
}

// UpdateAcl satisfies headupdater.AclUpdater (via the aclKickMux).
// Non-blocking by construction. An external kick means fresh ACL
// state, so it also resets the failed-pass retry budget.
func (w *aclMirrorWatcher) UpdateAcl(list.AclList) {
	w.retryMu.Lock()
	w.attempts = 0
	w.retryMu.Unlock()
	w.kick()
}

func (w *aclMirrorWatcher) kick() {
	select {
	case w.kickCh <- struct{}{}:
	default:
	}
}

func (w *aclMirrorWatcher) spaceID() string { return w.id }

func (w *aclMirrorWatcher) stop() {
	w.stopOnce.Do(func() {
		w.cancel()
		close(w.stopCh)
		w.retryMu.Lock()
		if w.retryTimer != nil {
			w.retryTimer.Stop()
			w.retryTimer = nil
		}
		w.retryMu.Unlock()
	})
	w.wg.Wait()
}

func (w *aclMirrorWatcher) loop() {
	defer w.wg.Done()
	for {
		select {
		case <-w.stopCh:
			return
		case <-w.kickCh:
			if !w.mirror() {
				w.scheduleRetry()
			}
		}
	}
}

// scheduleRetry arms one delayed re-kick after a failed mirror pass —
// on a quiet space the wiring-time kick is the only trigger, so
// "the next ACL kick retries" alone would leave a transient failure
// (store busy at boot, row not written yet) sticking until restart.
// Bounded by aclMirrorRetryMax so an expected-persistent failure
// (pending joiner) doesn't spin; UpdateAcl resets the budget.
func (w *aclMirrorWatcher) scheduleRetry() {
	w.retryMu.Lock()
	defer w.retryMu.Unlock()
	if w.retryTimer != nil || w.attempts >= aclMirrorRetryMax {
		return
	}
	w.attempts++
	w.retryTimer = time.AfterFunc(aclMirrorRetryDelay, func() {
		w.retryMu.Lock()
		w.retryTimer = nil
		w.retryMu.Unlock()
		w.kick()
	})
}

// mirror derives the ACL-sourced row state — own role and push keys —
// and writes each onto the tech-space row when it differs from what's
// there. Returns false when the pass should be retried. The two
// halves succeed independently: a keyless reader or pending joiner
// still has a definite (possibly None) ACL permission entry, so the
// role lands even while PushKeys keeps erroring until the accept
// record (itself an ACL kick) resolves it. Derive failures are either
// transient (row not written yet) or expected (the pending-joiner
// case above); those log at debug only, write errors at warn.
func (w *aclMirrorWatcher) mirror() bool {
	ctx, cancel := context.WithTimeout(w.ctx, aclMirrorTimeout)
	defer cancel()

	// Row-exists check first — techspace writes are strict (non-upsert)
	// modifies that silently no-op on absent ids. The row snapshot also
	// backs the no-op guards below, so subscribers of the spaces
	// dataset don't see a spurious row event per ACL record.
	cur, ok := w.s.tsp.Get(ctx, w.id)
	if !ok {
		aclMirrorLog.Debug("mirror row absent", zap.String("spaceId", w.id))
		return false
	}

	done := w.mirrorOwnRole(ctx, cur)
	if !w.mirrorPushKeys(ctx, cur) {
		done = false
	}
	if !w.mirrorGuestRevoked(ctx, cur) {
		done = false
	}
	return done
}

// mirrorGuestRevoked maintains the guestRevoked localStatus on a
// guest-mode row: stamped when the shared guest identity (this space's
// state.Identity()) has lost its ACL permission — the owner revoked
// public access — and healed back to active if a fresh ACL shows it
// active again. Skipped while the initial load is still in flight
// (guestLoading): an incomplete ACL must not read as a revocation.
// Non-guest rows always report done.
func (w *aclMirrorWatcher) mirrorGuestRevoked(ctx context.Context, cur techspace.SpaceIndexRecord) bool {
	if cur.GuestKey == "" || cur.LocalStatus == guestLoadingLocalStatus || cur.IsDeleted() {
		return true
	}
	role, err := w.s.ownRole(ctx, w.id)
	if err != nil {
		aclMirrorLog.Debug("mirror guest revoked failed", zap.String("spaceId", w.id), zap.Error(err))
		return false
	}
	var want string
	switch {
	case role == space.PermissionNone && cur.LocalStatus == techspace.StatusActive:
		// Only a row this device has fully loaded (localStatus active)
		// may flip to revoked: on a second device the row syncs in with
		// no localStatus while the ACL may not be pulled yet, and an
		// absent identity must not read as a revocation there — the
		// guest loader flips the row active after a complete pull, and
		// detection starts from that point.
		want = guestRevokedLocalStatus
	case role == space.PermissionGuest && cur.LocalStatus == guestRevokedLocalStatus:
		want = techspace.StatusActive
	default:
		return true
	}
	if _, err := w.s.tsp.SetLocalStatus(ctx, w.id, want); err != nil {
		aclMirrorLog.Warn("mirror guest revoked write failed", zap.String("spaceId", w.id), zap.Error(err))
		return false
	}
	return true
}

// mirrorOwnRole writes this account's current ACL permission onto the
// row when it differs (the SpaceInfo.OwnRole source).
func (w *aclMirrorWatcher) mirrorOwnRole(ctx context.Context, cur techspace.SpaceIndexRecord) bool {
	role, err := w.s.ownRole(ctx, w.id)
	if err != nil {
		aclMirrorLog.Debug("mirror own role failed", zap.String("spaceId", w.id), zap.Error(err))
		return false
	}
	if cur.OwnRole == role {
		return true
	}
	if _, err := w.s.tsp.SetOwnRole(ctx, w.id, role); err != nil {
		aclMirrorLog.Warn("mirror own role write failed", zap.String("spaceId", w.id), zap.Error(err))
		return false
	}
	return true
}

// mirrorPushKeys writes the derived push-key material onto the row
// when it differs.
func (w *aclMirrorWatcher) mirrorPushKeys(ctx context.Context, cur techspace.SpaceIndexRecord) bool {
	spaceKey, encKey, err := w.s.PushKeys(ctx, w.id)
	if err != nil {
		aclMirrorLog.Debug("mirror derive failed", zap.String("spaceId", w.id), zap.Error(err))
		return false
	}
	want := space.PushKeys{}
	if want.SpaceKey, err = pushclient.EncodeSpaceKey(spaceKey); err != nil {
		aclMirrorLog.Debug("mirror encode space key failed", zap.String("spaceId", w.id), zap.Error(err))
		return false
	}
	if want.EncKey, err = pushclient.EncodeEncKey(encKey); err != nil {
		aclMirrorLog.Debug("mirror encode enc key failed", zap.String("spaceId", w.id), zap.Error(err))
		return false
	}
	if want.EncKeyId, err = pushclient.EncKeyId(encKey); err != nil {
		aclMirrorLog.Debug("mirror enc key id failed", zap.String("spaceId", w.id), zap.Error(err))
		return false
	}
	if cur.PushKeys != nil && *cur.PushKeys == want {
		return true
	}
	if _, err := w.s.tsp.SetPushKeys(ctx, w.id, want); err != nil {
		aclMirrorLog.Warn("mirror write failed", zap.String("spaceId", w.id), zap.Error(err))
		return false
	}
	return true
}
