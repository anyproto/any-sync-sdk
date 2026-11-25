package spaceimpl

import (
	"context"
	"errors"

	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	anynet "github.com/anyproto/any-sync/net"
	"go.uber.org/zap"
)

// A member removed from a child space by an Admin or the Owner of its
// parent keeps the current read key: the remover holds none to change.
// The ACL is "rotation pending" until an Admin or the Owner of the
// child writes a read key change. The ACL mirror watcher does that on
// every device of such a member, at space load and on each applied ACL
// record. Nothing is persisted: the pending state is ACL state. A
// change that found no network is sent again when a tree node next
// answers a head-sync round of the space.

// completeRotation ends a pending rotation of the space when this
// device can. It returns false when the record could not be published
// and the pass is to be retried. It never loads the space: the mirror
// half of the pass does, and retries a space that does not load.
func (w *aclMirrorWatcher) completeRotation() bool {
	ctx, cancel := context.WithTimeout(w.ctx, aclMirrorTimeout)
	defer cancel()
	handle, ok := w.s.app.PickSpace(ctx, w.id)
	if !ok {
		return true
	}
	acl := handle.Inner().Acl()
	if acl == nil {
		return true
	}
	if err := rotateIfPending(ctx, acl, handle.Inner().AclClient().AddRecord); err != nil {
		// Offline, or another device rotated first: the next pass finds
		// the rotation still pending, or done. A stopped watcher has no
		// next pass.
		aclMirrorLog.Debug("rotation not published", zap.String("spaceId", w.id), zap.Error(err))
		if errors.Is(err, anynet.ErrUnableToConnect) {
			w.awaitsNetwork.Store(true)
		}
		return w.ctx.Err() != nil
	}
	return true
}

// nodeRound is told that a tree node answered a head-sync round of
// spaceId: the network is in reach. A rotation the space's watcher
// could not publish for lack of it is tried again now.
func (s *Service) nodeRound(spaceId string) {
	s.mu.Lock()
	w := s.aclMirrorWatchers[spaceId]
	s.mu.Unlock()
	if w != nil && w.awaitsNetwork.CompareAndSwap(true, false) {
		w.kick()
	}
}

// rotateIfPending writes the read key change that ends a pending
// rotation of acl, when this account is an Admin or the Owner and this
// device holds the read key. publish posts the record to the network
// and applies it.
//
// The pending state is checked and the record built under one lock, on
// the same head. A device that already applied another device's
// rotation builds nothing; one that has not yet builds on the old head,
// which the network refuses, and finds the rotation done on its next
// pass.
func rotateIfPending(ctx context.Context, acl list.AclList, publish func(context.Context, *consensusproto.RawRecord) error) error {
	acl.RLock()
	owed := rotationOwed(acl.AclState())
	acl.RUnlock()
	if !owed {
		return nil
	}
	change, err := newReadKeyChange()
	if err != nil {
		return err
	}
	acl.Lock()
	if !rotationOwed(acl.AclState()) {
		acl.Unlock()
		return nil
	}
	rec, err := acl.RecordBuilder().BuildReadKeyChange(change)
	acl.Unlock()
	if err != nil {
		// No read key on this device: a device that holds it rotates.
		return nil
	}
	return publish(ctx, rec)
}

// rotationOwed reports a pending rotation this account may end. The
// caller holds the ACL lock.
func rotationOwed(state *list.AclState) bool {
	return state.RotationPending() && state.Permissions(state.Identity()).CanManageAccounts()
}
