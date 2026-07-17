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
// promptly. No periodic poll: keys only change when an ACL record
// applies, and applying requires the loaded space this watcher is
// wired to.

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

// pushKeyMirrorTimeout bounds one mirror pass. Everything it touches
// is local (loaded space's ACL under RLock, tech-space store write);
// the generous bound only guards SDK shutdown races.
const pushKeyMirrorTimeout = 30 * time.Second

type pushKeyWatcher struct {
	s  *Service
	id string

	// kickCh coalesces UpdateAcl signals (buffered 1) so the syncacl
	// write path never blocks on the mirror.
	kickCh   chan struct{}
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func newPushKeyWatcher(s *Service, spaceId string) *pushKeyWatcher {
	w := &pushKeyWatcher{
		s:      s,
		id:     spaceId,
		kickCh: make(chan struct{}, 1),
		stopCh: make(chan struct{}),
	}
	// Prime one pass so the row is mirrored at wiring time without
	// waiting for an ACL record.
	w.kickCh <- struct{}{}
	w.wg.Add(1)
	go w.loop()
	return w
}

// UpdateAcl satisfies headupdater.AclUpdater (via the aclKickMux).
// Non-blocking by construction.
func (w *pushKeyWatcher) UpdateAcl(list.AclList) {
	select {
	case w.kickCh <- struct{}{}:
	default:
	}
}

func (w *pushKeyWatcher) spaceID() string { return w.id }

func (w *pushKeyWatcher) stop() {
	w.stopOnce.Do(func() { close(w.stopCh) })
	w.wg.Wait()
}

func (w *pushKeyWatcher) loop() {
	defer w.wg.Done()
	for {
		select {
		case <-w.stopCh:
			return
		case <-w.kickCh:
			w.mirror()
		}
	}
}

// mirror derives the current push keys and writes them onto the
// tech-space row when they differ from what's there. Best-effort by
// design: every failure mode is either transient (row not written
// yet — the next kick retries) or expected (keyless reader/pending
// joiner — PushKeys errors until the accept lands, which itself is an
// ACL record and therefore a kick).
func (w *pushKeyWatcher) mirror() {
	ctx, cancel := context.WithTimeout(context.Background(), pushKeyMirrorTimeout)
	defer cancel()

	spaceKey, encKey, err := w.s.PushKeys(ctx, w.id)
	if err != nil {
		return
	}
	want := space.PushKeys{}
	if want.SpaceKey, err = pushclient.EncodeSpaceKey(spaceKey); err != nil {
		return
	}
	if want.EncKey, err = pushclient.EncodeEncKey(encKey); err != nil {
		return
	}
	if want.EncKeyId, err = pushclient.EncKeyId(encKey); err != nil {
		return
	}

	// Row-exists check first — techspace writes are strict (non-upsert)
	// modifies that silently no-op on absent ids — doubling as the
	// no-op guard so subscribers of the spaces dataset don't see a
	// spurious row event per ACL record.
	cur, ok := w.s.tsp.Get(ctx, w.id)
	if !ok {
		return
	}
	if cur.PushKeys != nil && *cur.PushKeys == want {
		return
	}
	if _, err := w.s.tsp.SetPushKeys(ctx, w.id, want); err != nil {
		pushKeyLog.Warn("mirror write failed", zap.String("spaceId", w.id), zap.Error(err))
	}
}
