package spaceimpl

import (
	"context"
	"time"

	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/techspace"
)

var delLog = logger.NewNamed("sdk.spacedeletion")

// deletionReconcileInterval is how often the reconciler polls the
// coordinator absent an explicit kick. Matches anytype-heart's deletion
// controller cadence — space deletion is not latency-sensitive, and a
// local Delete kicks the loop immediately anyway.
const deletionReconcileInterval = 180 * time.Second

// startDeletionReconciler launches the background reconcile loop. Bound
// to its own cancellable context; Close cancels it and drains delWG.
func (s *Service) startDeletionReconciler() {
	ctx, cancel := context.WithCancel(context.Background())
	s.delCancel = cancel
	s.delWG.Add(1)
	go s.deletionLoop(ctx)
}

// kickDeletionReconciler wakes the loop for an immediate pass. Non-
// blocking: the kick channel is buffered 1, so coalesced kicks collapse.
func (s *Service) kickDeletionReconciler() {
	select {
	case s.delKick <- struct{}{}:
	default:
	}
}

// deletionLoop reconciles space deletions with the coordinator until
// ctx is cancelled. It runs one pass shortly after start (to retry
// deletes marked offline in a previous session) and then on every tick
// or kick.
func (s *Service) deletionLoop(ctx context.Context) {
	defer s.delWG.Done()
	t := time.NewTicker(deletionReconcileInterval)
	defer t.Stop()
	// Initial pass so a space the user deleted while offline last
	// session gets driven to the coordinator as soon as we're online.
	s.reconcileDeletions(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.reconcileDeletions(ctx)
		case <-s.delKick:
			s.reconcileDeletions(ctx)
		}
	}
}

// reconcileDeletions runs one reconcile pass. Offline-tolerant: if the
// coordinator is unreachable the StatusCheckMany call errors and the
// pass is skipped, so the next tick retries — no local state changes.
func (s *Service) reconcileDeletions(ctx context.Context) {
	rows := s.tsp.List(ctx)
	if len(rows) == 0 {
		return
	}
	ids := make([]string, len(rows))
	for i, r := range rows {
		ids[i] = r.Id
	}
	statuses, err := s.app.SpaceStatuses(ctx, ids)
	if err != nil {
		// Offline / coordinator down — try again next tick.
		delLog.Debug("status check failed", zap.Error(err))
		return
	}
	if len(statuses) != len(rows) {
		delLog.Warn("status count mismatch",
			zap.Int("rows", len(rows)), zap.Int("statuses", len(statuses)))
		return
	}
	for i, r := range rows {
		s.reconcileOne(ctx, r, statuses[i])
	}
}

// reconcileOne advances one space toward its terminal deletion state.
func (s *Service) reconcileOne(ctx context.Context, row techspace.SpaceIndexRecord, st *coordinatorproto.SpaceStatusPayload) {
	if st == nil {
		return
	}
	locallyDeleted := row.RemoteStatus == techspace.StatusDeleted

	switch {
	case locallyDeleted &&
		st.Status == coordinatorproto.SpaceStatus_SpaceStatusCreated &&
		st.Permissions == coordinatorproto.SpacePermissions_SpacePermissionsOwner:
		// We deleted locally but the coordinator still has the space
		// active and we own it — send the signed delete. Idempotent: the
		// coordinator moves it to PendingDeletion and later passes no-op.
		if err := s.app.SpaceDelete(ctx, row.Id); err != nil {
			delLog.Warn("coordinator delete failed", zap.String("spaceId", row.Id), zap.Error(err))
			return
		}
		delLog.Info("space delete sent to coordinator", zap.String("spaceId", row.Id))

	case !locallyDeleted && remotelyGone(st.Status):
		// The coordinator reports the space gone (deleted on another
		// device, or the owner deleted a space we joined) but we haven't
		// marked it locally. Set the synced tombstone — which also emits
		// the Subscribe `Removed` event consumers wipe their indexes on —
		// then offload local state.
		if _, err := s.tsp.SetRemoteStatus(ctx, row.Id, techspace.StatusDeleted); err != nil {
			delLog.Warn("mark inbound-deleted failed", zap.String("spaceId", row.Id), zap.Error(err))
			return
		}
		delLog.Info("offloading remotely-deleted space", zap.String("spaceId", row.Id))
		s.OffloadSpace(ctx, row.Id)
	}
}

// remotelyGone reports whether a coordinator status means the space no
// longer exists for us: pending/started/finished deletion, or absent.
func remotelyGone(status coordinatorproto.SpaceStatus) bool {
	switch status {
	case coordinatorproto.SpaceStatus_SpaceStatusPendingDeletion,
		coordinatorproto.SpaceStatus_SpaceStatusDeletionStarted,
		coordinatorproto.SpaceStatus_SpaceStatusDeleted,
		coordinatorproto.SpaceStatus_SpaceStatusNotExists:
		return true
	default:
		return false
	}
}
