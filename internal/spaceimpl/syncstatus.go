package spaceimpl

import (
	"github.com/anyproto/any-sync-sdk/internal/syncstatus"
	"github.com/anyproto/any-sync-sdk/space"
)

// syncStatusAPI implements space.SyncStatusAPI by routing through the
// per-account syncstatus.Service held on anysyncx.App.
//
// The tracker for this space is created lazily on first access — most
// reads happen before any hook has fired, which is fine: Service.For
// returns a fresh empty Tracker and the snapshot path reports
// State=Unknown until something lands.
type syncStatusAPI struct {
	svc     *syncstatus.Service
	spaceId string
}

func newSyncStatusAPI(svc *syncstatus.Service, spaceId string) *syncStatusAPI {
	return &syncStatusAPI{svc: svc, spaceId: spaceId}
}

func (s *syncStatusAPI) Space() space.SpaceSyncStatus {
	return s.svc.Status(s.spaceId)
}

func (s *syncStatusAPI) Object(objectId string) space.ObjectSyncStatus {
	// For(spaceId) is cheap and idempotent; using it (rather than the
	// no-create variant) means an Object() call before any hook has
	// fired still returns a sensible Unknown for the object, and
	// downstream SubscribeObject calls land on the same tracker.
	return s.svc.For(s.spaceId).Object(objectId)
}

func (s *syncStatusAPI) SubscribeObject(objectId string, cb func(space.ObjectSyncStatus)) func() {
	return s.svc.For(s.spaceId).SubscribeObject(objectId, cb)
}
