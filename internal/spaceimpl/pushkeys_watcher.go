// Per-space push-key mirror: ACL state → tech-space row (SYN-47
// receiver side). Clients that must decrypt push payloads while the
// SDK process is down (mobile notification extensions) read the
// derived key material off the space row and cache it natively; this
// watcher is what keeps that row field fresh. Mirrors the pattern of
// anytype-heart's SetAclInfo → space-view details write.
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
	"github.com/anyproto/any-sync-sdk/space"
)

var pushKeyLog = logger.NewNamed("sdk.pushkeys")

const (
	// pushKeyMirrorTimeout bounds one mirror pass. Everything it
	// touches is local (loaded space's ACL under RLock, tech-space
	// store write); the generous bound only guards teardown races —
	// and stop() cancels the watcher ctx, so shutdown never waits it
	// out.
	pushKeyMirrorTimeout = 30 * time.Second
	// pushKeyRetryDelay / pushKeyRetryMax bound the failed-pass retry
	// loop. Retries stop after the cap so an expected-persistent
	// failure (pending joiner without read access, space storage not
	// materialized yet) doesn't reload the space forever; any external
	// ACL kick — including the accept record that resolves the joiner
	// case — resets the budget.
	pushKeyRetryDelay = 30 * time.Second
	pushKeyRetryMax   = 5
)

type pushKeyWatcher struct {
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
	// a time, attempts counted against pushKeyRetryMax and reset by
	// any external ACL kick.
	retryMu    sync.Mutex
	retryTimer *time.Timer
	attempts   int
}

func newPushKeyWatcher(s *Service, spaceId string) *pushKeyWatcher {
	w := &pushKeyWatcher{
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
func (w *pushKeyWatcher) UpdateAcl(list.AclList) {
	w.retryMu.Lock()
	w.attempts = 0
	w.retryMu.Unlock()
	w.kick()
}

func (w *pushKeyWatcher) kick() {
	select {
	case w.kickCh <- struct{}{}:
	default:
	}
}

func (w *pushKeyWatcher) spaceID() string { return w.id }

func (w *pushKeyWatcher) stop() {
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

func (w *pushKeyWatcher) loop() {
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
// Bounded by pushKeyRetryMax so an expected-persistent failure
// (pending joiner) doesn't spin; UpdateAcl resets the budget.
func (w *pushKeyWatcher) scheduleRetry() {
	w.retryMu.Lock()
	defer w.retryMu.Unlock()
	if w.retryTimer != nil || w.attempts >= pushKeyRetryMax {
		return
	}
	w.attempts++
	w.retryTimer = time.AfterFunc(pushKeyRetryDelay, func() {
		w.retryMu.Lock()
		w.retryTimer = nil
		w.retryMu.Unlock()
		w.kick()
	})
}

// mirror derives the current push keys and writes them onto the
// tech-space row when they differ from what's there. Returns false
// when the pass should be retried. Failure modes are either transient
// (row not written yet) or expected (keyless reader/pending joiner —
// PushKeys errors until the accept lands, which itself is an ACL
// record and therefore a kick); both are logged at debug only, the
// write error at warn.
func (w *pushKeyWatcher) mirror() bool {
	ctx, cancel := context.WithTimeout(w.ctx, pushKeyMirrorTimeout)
	defer cancel()

	spaceKey, encKey, err := w.s.PushKeys(ctx, w.id)
	if err != nil {
		pushKeyLog.Debug("mirror derive failed", zap.String("spaceId", w.id), zap.Error(err))
		return false
	}
	want := space.PushKeys{}
	if want.SpaceKey, err = pushclient.EncodeSpaceKey(spaceKey); err != nil {
		pushKeyLog.Debug("mirror encode space key failed", zap.String("spaceId", w.id), zap.Error(err))
		return false
	}
	if want.EncKey, err = pushclient.EncodeEncKey(encKey); err != nil {
		pushKeyLog.Debug("mirror encode enc key failed", zap.String("spaceId", w.id), zap.Error(err))
		return false
	}
	if want.EncKeyId, err = pushclient.EncKeyId(encKey); err != nil {
		pushKeyLog.Debug("mirror enc key id failed", zap.String("spaceId", w.id), zap.Error(err))
		return false
	}

	// Row-exists check first — techspace writes are strict (non-upsert)
	// modifies that silently no-op on absent ids — doubling as the
	// no-op guard so subscribers of the spaces dataset don't see a
	// spurious row event per ACL record.
	cur, ok := w.s.tsp.Get(ctx, w.id)
	if !ok {
		pushKeyLog.Debug("mirror row absent", zap.String("spaceId", w.id))
		return false
	}
	if cur.PushKeys != nil && *cur.PushKeys == want {
		return true
	}
	if _, err := w.s.tsp.SetPushKeys(ctx, w.id, want); err != nil {
		pushKeyLog.Warn("mirror write failed", zap.String("spaceId", w.id), zap.Error(err))
		return false
	}
	return true
}
