package spaceimpl

import (
	"context"
	"time"

	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/acl/aclwaiter"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

var joinLog = logger.NewNamed("sdk.spacejoin")

// joinReconcileInterval is how often the controller rescans the
// tech-space index for joining rows that need a waiter, absent an
// explicit kick. A local Join kicks the loop immediately; the tick is a
// safety net that also covers boot resumption of a join started in a
// previous session. A var (not const) so tests can shorten it.
var joinReconcileInterval = 180 * time.Second

// joinProbeMinInterval bounds how often a kick-driven pass may probe the
// chain for one row it could not settle yet. Index changes kick the
// controller freely and a probe is a network round; the tick pass is
// never throttled.
const joinProbeMinInterval = 30 * time.Second

// SetJoinReconcileIntervalForTest overrides the reconcile poll interval
// and returns a func that restores the previous value. Test seam only —
// the interval is read once when the loop starts, so call before opening
// the SDK.
func SetJoinReconcileIntervalForTest(d time.Duration) (restore func()) {
	prev := joinReconcileInterval
	joinReconcileInterval = d
	return func() { joinReconcileInterval = prev }
}

// joinWaiter is a live ACL waiter plus whether it was built without a
// head. A head-less waiter observes acceptance only — the any-sync
// waiter reports a decline solely when the request is gone AND the head
// it was given exists on the chain — so the tick pass keeps trying to
// resolve a head for it.
type joinWaiter struct {
	w        aclwaiter.AclWaiter
	headless bool
}

// joinResolution is what one chain snapshot says about this account's
// join on a space: granted (a member — load, nothing to wait for), a
// pending request (head is a chain head at-or-after it, so a waiter
// built on it can detect a decline), or neither (the request is gone,
// or never landed). err is an unreadable chain (offline).
type joinResolution struct {
	granted bool
	head    string
	err     error
}

// resolveJoin snapshots the space's ACL through the joining client — no
// local storage, nothing materialized — and classifies this account's
// standing on it.
func (s *Service) resolveJoin(ctx context.Context, spaceId string) joinResolution {
	acl, err := s.app.AclSnapshot(ctx, spaceId)
	if err != nil {
		return joinResolution{err: err}
	}
	st := acl.AclState()
	if !st.Permissions(st.Identity()).NoPermissions() {
		return joinResolution{granted: true}
	}
	if _, err := st.JoinRecord(st.Identity(), false); err == nil {
		return joinResolution{head: acl.Head().Id}
	}
	return joinResolution{}
}

// startJoinController launches the background join loop, bound to its own
// cancellable context. Close cancels it and drains joinWG.
func (s *Service) startJoinController() {
	ctx, cancel := context.WithCancel(context.Background())
	s.joinCtx, s.joinCancel = ctx, cancel
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

// ResumePendingJoins triggers an immediate scan for joining rows that
// lack a waiter — joins left pending from a previous session, and rows
// synced in from the account's other devices. Called by sdk.Open after
// the tech space is open (New runs, and starts the controller, before
// tsp.Open, so the controller's own initial pass races the open and
// finds an empty list) and on every tech-space index change, so a join
// requested on another device gets its waiter here promptly instead of
// on the next tick.
func (s *Service) ResumePendingJoins() { s.kickJoinController() }

// joinLoop drives joiner-side post-acceptance loading until ctx is
// cancelled. It runs one full pass shortly after start (boot resumption
// of a join left pending last session), a full pass on every tick, and
// the cheap pass on every kick.
func (s *Service) joinLoop(ctx context.Context) {
	defer s.joinWG.Done()
	t := time.NewTicker(joinReconcileInterval)
	defer t.Stop()
	s.reconcileJoins(ctx, true)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.reconcileJoins(ctx, true)
		case <-s.joinKick:
			s.reconcileJoins(ctx, false)
		}
	}
}

// reconcileJoins owns the waiter lifecycle. It starts an ACL waiter for
// each joining row that lacks one, and stops the waiter for any space
// that has left the joining state (accepted→active or ended, on this or
// another device, or its load completed). It also resumes accepted-
// invite and guest loads (localStatus="inviteLoading"/"guestLoading" —
// pull-until-available, no ACL waiter; the account is already a member).
//
// A full pass (boot, tick) additionally re-resolves a head for waiters
// running without one and probes ended joins that still hold local
// storage; a kick pass skips both and throttles per-row chain probes, so
// a burst of index changes cannot turn into a burst of network rounds.
// Single-threaded: only joinLoop calls it.
func (s *Service) reconcileJoins(ctx context.Context, full bool) {
	joining := make(map[string]techspace.SpaceIndexRecord)
	for _, r := range s.tsp.List(ctx) {
		switch {
		case mapStatus(r.Type, r.LocalStatus, r.RemoteStatus) == space.StatusJoining:
			joining[r.Id] = r
		case full && r.JoinEnded() && s.app.SpaceExists(r.Id):
			// Storage under an ended join is the accept-vs-cancel race:
			// one device loaded the accepted space while another device's
			// withdrawal won the row. Membership is the truth — when the
			// ACL grants it, the space loads and the row flips active.
			// Storage-gated so this never probes every declined join in
			// the account's history.
			s.probeEndedJoin(ctx, r.Id)
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

	// Stop waiters whose row is no longer joining; forget the probe
	// throttle of rows that left the state.
	s.mu.Lock()
	var stale []string
	for spaceId := range s.joinWaiters {
		if _, ok := joining[spaceId]; !ok {
			stale = append(stale, spaceId)
		}
	}
	for spaceId := range s.joinProbes {
		if _, ok := joining[spaceId]; !ok {
			delete(s.joinProbes, spaceId)
		}
	}
	s.mu.Unlock()
	for _, spaceId := range stale {
		s.stopJoinWaiter(spaceId)
	}

	// Start a waiter for each joining row that lacks one. A running
	// head-less waiter is left alone on a kick pass; a full pass tries
	// once more to give it a head, restarting it only when one is found
	// (or membership is, in which case the load replaces it).
	for spaceId, rec := range joining {
		s.mu.Lock()
		jw, running := s.joinWaiters[spaceId]
		s.mu.Unlock()
		if running {
			if !full || !jw.headless {
				continue
			}
			if rec.AclHeadId == "" {
				res := s.resolveJoin(ctx, spaceId)
				if res.err != nil || (!res.granted && res.head == "") {
					continue
				}
				s.stopJoinWaiter(spaceId)
				if res.granted {
					s.startJoinLoad(spaceId)
					continue
				}
				rec.AclHeadId = res.head
				s.storeJoinHead(ctx, spaceId, res.head)
			} else {
				s.stopJoinWaiter(spaceId)
			}
		}
		s.startJoinWaiter(ctx, rec, full)
	}
}

// joinProbeDue reports whether a chain probe for spaceId may run on a
// kick pass, recording the attempt; a full pass always may.
func (s *Service) joinProbeDue(spaceId string, full bool) bool {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if last, ok := s.joinProbes[spaceId]; ok && !full && now.Sub(last) < joinProbeMinInterval {
		return false
	}
	s.joinProbes[spaceId] = now
	return true
}

// storeJoinHead records a resolved chain head on the row (device-local).
// Best-effort: a failed write leaves the head empty and the next full
// pass resolves it again.
func (s *Service) storeJoinHead(ctx context.Context, spaceId, head string) {
	if _, err := s.tsp.SetAclHeadId(ctx, spaceId, head); err != nil {
		joinLog.Warn("record resolved acl head", zap.String("spaceId", spaceId), zap.Error(err))
	}
}

// probeEndedJoin runs the membership probe for an ended-join row that
// still holds local storage (see reconcileJoins): granted means the
// accept happened and the load flips the row active; anything else
// leaves the row ended.
func (s *Service) probeEndedJoin(ctx context.Context, spaceId string) {
	if !s.joinProbeDue(spaceId, true) {
		return
	}
	res := s.resolveJoin(ctx, spaceId)
	if res.granted {
		joinLog.Info("ended join holds storage and the ACL grants membership; loading",
			zap.String("spaceId", spaceId))
		s.startJoinLoad(spaceId)
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

// startJoinLoad spawns (at most one per spaceId) the background load of
// a space whose join the ACL has granted, on the controller's context —
// callable from any goroutine (the waiter's callback, Join, CancelJoin,
// the reconcile pass). No-op while closing or before the controller
// started.
func (s *Service) startJoinLoad(spaceId string) {
	s.mu.Lock()
	ctx := s.joinCtx
	if s.closing || ctx == nil {
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
		s.loadJoinedSpace(ctx, spaceId)
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
		if !ok {
			return
		}
		if rec.GuestKey != "" && rec.LocalStatus == guestLoadingLocalStatus &&
			rec.RemoteStatus == techspace.GuestDeletedRemoteStatus {
			// Interrupted guest re-join: JoinGuest crashed between the
			// loading marker and the synced un-delete — the marker proves
			// the re-add intent, so finish the flip here.
			if _, err := s.tsp.SetRemoteStatus(ctx, spaceId, techspace.StatusActive); err != nil {
				joinLog.Warn("finish interrupted guest re-join",
					zap.String("spaceId", spaceId), zap.Error(err))
			}
		} else if rec.IsDeleted() {
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
// the row ended — the synced marker CancelJoin also writes and Join
// revives. Both are quick and durable — the heavy load runs in a
// tracked goroutine so the waiter's poll loop is never blocked.
//
// A row with no stored head — this device never posted the request; it
// learned of the join from the synced row — first resolves one from the
// chain: membership already granted skips the waiter for the load, a
// pending request yields a head the waiter can detect a decline with,
// neither means there is nothing to wait for (the requesting device
// syncs its verdict, or a local CancelJoin / Join settles the row) so no
// waiter starts. An unreadable chain (offline) starts a head-less waiter
// — acceptance-only — that the next full pass tries to upgrade.
func (s *Service) startJoinWaiter(ctx context.Context, rec techspace.SpaceIndexRecord, full bool) {
	spaceId := rec.Id
	head := rec.AclHeadId
	headless := false
	if head == "" {
		if !s.joinProbeDue(spaceId, full) {
			return
		}
		res := s.resolveJoin(ctx, spaceId)
		switch {
		case res.err != nil:
			joinLog.Debug("resolve join head", zap.String("spaceId", spaceId), zap.Error(res.err))
			headless = true
		case res.granted:
			s.startJoinLoad(spaceId)
			return
		case res.head != "":
			head = res.head
			s.storeJoinHead(ctx, spaceId, head)
		default:
			joinLog.Debug("joining row without a request on the chain", zap.String("spaceId", spaceId))
			return
		}
	}
	onFinish := func(list.AclList) error {
		joinLog.Info("join accepted", zap.String("spaceId", spaceId))
		s.startJoinLoad(spaceId)
		return nil
	}
	onReject := func(list.AclList) error {
		joinLog.Info("join declined", zap.String("spaceId", spaceId))
		if err := s.markJoinEnded(ctx, spaceId); err != nil {
			return err
		}
		s.kickJoinController()
		return nil
	}
	w, err := s.app.NewAclWaiter(spaceId, head, onFinish, onReject)
	if err != nil {
		joinLog.Warn("build acl waiter", zap.String("spaceId", spaceId), zap.Error(err))
		return
	}
	// Run before the map insert, both under the lock: Close is a no-op on
	// a waiter that has not run, so a stopJoinWaiter (CancelJoin runs
	// one from the caller's goroutine) slipping between insert and Run
	// would delete the entry, "close" nothing, and leave the waiter
	// running untracked past Service.Close. Run only spawns the loop,
	// so the lock is held for no longer than the insert.
	s.mu.Lock()
	if _, dup := s.joinWaiters[spaceId]; dup {
		s.mu.Unlock()
		return
	}
	if err := w.Run(ctx); err != nil {
		s.mu.Unlock()
		joinLog.Warn("run acl waiter", zap.String("spaceId", spaceId), zap.Error(err))
		_ = w.Close(ctx)
		return
	}
	s.joinWaiters[spaceId] = &joinWaiter{w: w, headless: headless}
	s.mu.Unlock()
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
			if err := s.flipJoinActive(ctx, spaceId); err == nil {
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

// flipJoinActive records membership on a join row after its space has
// loaded here: the SYNCED active (the account's other devices converge
// on it and load), then this device's local active. Written only after
// a successful load, so the pending guard never releases a space that is
// not local yet. A synced tombstone that landed meanwhile is left alone
// — the delete wins; the eager-loader reclaims the storage on the next
// boot.
func (s *Service) flipJoinActive(ctx context.Context, spaceId string) error {
	rec, ok := s.tsp.Get(ctx, spaceId)
	if !ok {
		return nil
	}
	if rec.IsDeleted() && !rec.JoinEnded() {
		joinLog.Info("joined space was deleted meanwhile; leaving the tombstone", zap.String("spaceId", spaceId))
		return nil
	}
	if rec.RemoteStatus != techspace.StatusActive {
		if _, err := s.tsp.SetRemoteStatus(ctx, spaceId, techspace.StatusActive); err != nil {
			return err
		}
	}
	if rec.LocalStatus != techspace.StatusActive {
		if _, err := s.tsp.SetLocalStatus(ctx, spaceId, techspace.StatusActive); err != nil {
			return err
		}
	}
	return nil
}

// healJoinMembership flips a join row to active when the live ACL of the
// LOADED space already grants membership and the row still says
// otherwise: its synced verdict lost a race with the acceptance (a
// withdrawal from another device landing over the accept), or a legacy
// device-local marker never flipped. Callers have verified membership on
// the ACL; this only writes the row. Best-effort — the next Info() /
// watcher tick retries.
func (s *Service) healJoinMembership(ctx context.Context, spaceId string) {
	rec, ok := s.tsp.Get(ctx, spaceId)
	if !ok {
		return
	}
	if mapStatus(rec.Type, rec.LocalStatus, rec.RemoteStatus) != space.StatusJoining && !rec.JoinEnded() {
		return
	}
	if err := s.flipJoinActive(ctx, spaceId); err != nil {
		joinLog.Debug("heal join membership", zap.String("spaceId", spaceId), zap.Error(err))
		return
	}
	s.kickJoinController()
}

// stopJoinWaiter closes and removes the waiter for spaceId, if any.
func (s *Service) stopJoinWaiter(spaceId string) {
	s.mu.Lock()
	jw := s.joinWaiters[spaceId]
	delete(s.joinWaiters, spaceId)
	s.mu.Unlock()
	if jw != nil {
		_ = jw.w.Close(context.Background())
	}
}

// stopJoinWaiters closes every live waiter. Called from Close after the
// loop has been cancelled and drained.
func (s *Service) stopJoinWaiters() {
	s.mu.Lock()
	waiters := s.joinWaiters
	s.joinWaiters = make(map[string]*joinWaiter)
	s.mu.Unlock()
	for _, jw := range waiters {
		_ = jw.w.Close(context.Background())
	}
}
