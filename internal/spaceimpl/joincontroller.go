package spaceimpl

import (
	"context"
	"time"

	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/acl/aclwaiter"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/techspace"
)

var joinLog = logger.NewNamed("sdk.spacejoin")

// joinReconcileInterval is how often the controller rescans the
// tech-space index for joining rows that need a waiter, absent an
// explicit kick. A local Join kicks the loop immediately; the tick is a
// safety net that also covers boot resumption of a join started in a
// previous session. A var (not const) so tests can shorten it.
var joinReconcileInterval = 180 * time.Second

// SetJoinReconcileIntervalForTest overrides the reconcile poll interval
// and returns a func that restores the previous value. Test seam only —
// the interval is read once when the loop starts, so call before opening
// the SDK.
func SetJoinReconcileIntervalForTest(d time.Duration) (restore func()) {
	prev := joinReconcileInterval
	joinReconcileInterval = d
	return func() { joinReconcileInterval = prev }
}

// startJoinController launches the background join loop, bound to its own
// cancellable context. Close cancels it and drains joinWG.
func (s *Service) startJoinController() {
	ctx, cancel := context.WithCancel(context.Background())
	s.joinCancel = cancel
	s.joinWG.Add(1)
	go s.joinLoop(ctx)
}

// kickJoinController wakes the loop for an immediate pass. Non-blocking:
// the kick channel is buffered 1, so coalesced kicks collapse.
func (s *Service) kickJoinController() {
	select {
	case s.joinKick <- struct{}{}:
	default:
	}
}

// ResumePendingJoins triggers an immediate scan for joining rows left
// pending from a previous session, starting an ACL waiter for each.
// Called once by sdk.Open after the tech space is open: New runs (and
// starts the controller) before tsp.Open, so the controller's own
// initial pass races the open and finds an empty list. This kick after
// open makes boot resumption prompt instead of waiting for the tick.
func (s *Service) ResumePendingJoins() { s.kickJoinController() }

// joinLoop drives joiner-side post-acceptance loading until ctx is
// cancelled. It runs one pass shortly after start (boot resumption of a
// join left pending last session) and then on every tick or kick.
func (s *Service) joinLoop(ctx context.Context) {
	defer s.joinWG.Done()
	t := time.NewTicker(joinReconcileInterval)
	defer t.Stop()
	s.reconcileJoins(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.reconcileJoins(ctx)
		case <-s.joinKick:
			s.reconcileJoins(ctx)
		}
	}
}

// reconcileJoins owns the waiter lifecycle. It starts an ACL waiter for
// each joining row that lacks one, and stops the waiter for any space
// that has left the joining state (accepted→active or declined→deleted,
// or its load completed). It also resumes accepted-invite loads
// (localStatus="inviteLoading" — pull-until-available, no ACL waiter;
// the account is already a member). Single-threaded: only joinLoop
// calls it.
func (s *Service) reconcileJoins(ctx context.Context) {
	joining := make(map[string]techspace.SpaceIndexRecord)
	for _, r := range s.tsp.List(ctx) {
		if r.LocalStatus == joiningLocalStatus {
			joining[r.Id] = r
		}
		if r.LocalStatus == inviteLoadingLocalStatus || r.LocalStatus == guestLoadingLocalStatus {
			// Same pull-until-available load for both: no ACL waiter —
			// the account (or the shared guest identity) is already an
			// ACL member; loadAcceptedInvite's invite-specific branches
			// never trigger on a guest row.
			s.startPendingLoad(ctx, r.Id)
		} else if r.GuestKey != "" && !r.IsDeleted() &&
			(r.LocalStatus == "" || r.LocalStatus == techspace.StatusActive) &&
			!s.app.SpaceExists(r.Id) {
			// Guest row without local storage and without a loading
			// marker: a JoinGuest that crashed between the row write and
			// the marker, or a row synced in from another device. The row
			// itself is the durable intent — resume the pull regardless
			// of the device-local marker. Revoked/deleted rows stay out.
			s.startPendingLoad(ctx, r.Id)
		}
	}

	// Stop waiters whose row is no longer joining.
	s.mu.Lock()
	var stale []string
	for spaceId := range s.joinWaiters {
		if _, ok := joining[spaceId]; !ok {
			stale = append(stale, spaceId)
		}
	}
	s.mu.Unlock()
	for _, spaceId := range stale {
		s.stopJoinWaiter(spaceId)
	}

	// Start a waiter for each joining row that lacks one.
	for spaceId, rec := range joining {
		s.mu.Lock()
		_, running := s.joinWaiters[spaceId]
		s.mu.Unlock()
		if running {
			continue
		}
		s.startJoinWaiter(ctx, rec)
	}
}

// startPendingLoad spawns (at most one per spaceId) background load for
// an accepted direct-add invite left unloaded — AcceptInvite crashed or
// went offline mid-pull, or the accept happened moments ago and its
// synchronous attempt failed.
func (s *Service) startPendingLoad(ctx context.Context, spaceId string) {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return
	}
	if _, running := s.pendingLoads[spaceId]; running {
		s.mu.Unlock()
		return
	}
	s.pendingLoads[spaceId] = struct{}{}
	s.mu.Unlock()
	s.joinWG.Add(1)
	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.pendingLoads, spaceId)
			s.mu.Unlock()
		}()
		s.loadAcceptedInvite(ctx, spaceId)
	}()
}

// loadAcceptedInvite drives an accepted direct-add invite to loaded.
// Unlike loadJoinedSpace it re-reads the row every attempt, because the
// row can move under a long-running retry:
//
//   - deleted (locally or synced) — the user's cleanup path for an
//     accept that can never complete (e.g. a spoofed spaceId, or the
//     owner removed us before we accepted): stop, never resurrect.
//   - declined on another device (synced) — the decline won; clear this
//     device's loading marker and stop.
//   - still invitePending — the accept was interrupted between the
//     loading marker and the synced flip; finish the flip here so the
//     accept becomes account-wide durable.
//
// Bound to the join controller's context: on shutdown it returns with
// the row still "inviteLoading" and the next session's boot pass
// resumes it.
func (s *Service) loadAcceptedInvite(ctx context.Context, spaceId string) {
	defer s.joinWG.Done()
	backoff := time.Second
	const maxBackoff = 20 * time.Second
	for {
		rec, ok := s.tsp.Get(ctx, spaceId)
		if !ok || rec.IsDeleted() {
			return
		}
		if rec.RemoteStatus == techspace.InviteDeclinedRemoteStatus {
			if _, err := s.tsp.SetLocalStatus(ctx, spaceId, ""); err != nil {
				joinLog.Warn("clear loading marker on declined invite",
					zap.String("spaceId", spaceId), zap.Error(err))
			}
			return
		}
		if rec.RemoteStatus == techspace.InvitePendingRemoteStatus {
			if _, err := s.tsp.SetRemoteStatus(ctx, spaceId, techspace.StatusActive); err != nil {
				joinLog.Warn("finish interrupted accept",
					zap.String("spaceId", spaceId), zap.Error(err))
			}
		}
		// load, not Get: the row keeps its loading localStatus until the
		// load succeeds, and the pending guard must not see it.
		if _, err := s.load(ctx, spaceId); err == nil {
			if _, err := s.tsp.SetLocalStatus(ctx, spaceId, techspace.StatusActive); err == nil {
				s.kickJoinController()
				return
			} else {
				joinLog.Warn("flip accepted invite active", zap.String("spaceId", spaceId), zap.Error(err))
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff = backoff * 3 / 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// startJoinWaiter builds and runs an ACL waiter for one joining space.
// onFinish (acceptance) spawns the space load; onReject (decline) marks
// the row deleted. Both are quick and durable — the heavy load runs in a
// tracked goroutine so the waiter's poll loop is never blocked.
func (s *Service) startJoinWaiter(ctx context.Context, rec techspace.SpaceIndexRecord) {
	spaceId := rec.Id
	onFinish := func(list.AclList) error {
		joinLog.Info("join accepted", zap.String("spaceId", spaceId))
		s.joinWG.Add(1)
		go s.loadJoinedSpace(ctx, spaceId)
		return nil
	}
	onReject := func(list.AclList) error {
		joinLog.Info("join declined", zap.String("spaceId", spaceId))
		if _, err := s.tsp.SetLocalStatus(ctx, spaceId, techspace.StatusDeleted); err != nil {
			return err
		}
		s.kickJoinController()
		return nil
	}
	w, err := s.app.NewAclWaiter(spaceId, rec.AclHeadId, onFinish, onReject)
	if err != nil {
		joinLog.Warn("build acl waiter", zap.String("spaceId", spaceId), zap.Error(err))
		return
	}
	s.mu.Lock()
	if _, dup := s.joinWaiters[spaceId]; dup {
		s.mu.Unlock()
		_ = w.Close(ctx)
		return
	}
	s.joinWaiters[spaceId] = w
	s.mu.Unlock()
	if err := w.Run(ctx); err != nil {
		joinLog.Warn("run acl waiter", zap.String("spaceId", spaceId), zap.Error(err))
		s.stopJoinWaiter(spaceId)
	}
}

// loadJoinedSpace pulls and wires an accepted space, then flips the row
// to active. Retries with backoff because ACL acceptance can land
// slightly before the space storage is pullable. Bound to the join
// controller's context: on shutdown it returns with the row still
// "joining", and the next session's boot pass resumes it.
func (s *Service) loadJoinedSpace(ctx context.Context, spaceId string) {
	defer s.joinWG.Done()
	backoff := time.Second
	const maxBackoff = 20 * time.Second
	for {
		// load, not Get: the row stays "joining" until the load succeeds,
		// and the pending guard must not see it.
		if _, err := s.load(ctx, spaceId); err == nil {
			if _, err := s.tsp.SetLocalStatus(ctx, spaceId, techspace.StatusActive); err == nil {
				s.kickJoinController()
				return
			} else {
				joinLog.Warn("flip joined space active", zap.String("spaceId", spaceId), zap.Error(err))
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff = backoff * 3 / 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// stopJoinWaiter closes and removes the waiter for spaceId, if any.
func (s *Service) stopJoinWaiter(spaceId string) {
	s.mu.Lock()
	w := s.joinWaiters[spaceId]
	delete(s.joinWaiters, spaceId)
	s.mu.Unlock()
	if w != nil {
		_ = w.Close(context.Background())
	}
}

// stopJoinWaiters closes every live waiter. Called from Close after the
// loop has been cancelled and drained.
func (s *Service) stopJoinWaiters() {
	s.mu.Lock()
	waiters := s.joinWaiters
	s.joinWaiters = make(map[string]aclwaiter.AclWaiter)
	s.mu.Unlock()
	for _, w := range waiters {
		_ = w.Close(context.Background())
	}
}
