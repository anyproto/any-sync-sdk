package spaceimpl

import (
	"context"
	"errors"

	anystore "github.com/anyproto/any-store/v2"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/payloads"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/space"
)

// purgeFileLeftovers clears what the files subsystem keeps for a purged
// object. Runs from the store's purge hook: after the purge transaction
// committed, before the object's collections drop, so the rows are
// still readable. Two cases:
//
//   - a purged payloads object: the `fidx/<spaceId>/<fileId>` index
//     entries of its rows (nothing else removes them — FindRow only
//     answers ErrNotFound through a stale entry);
//   - a purged owner: its payloads object of either shape, when present.
//     The signed shape is cascade-deleted by any-sync and cleans itself
//     up through its own purge; the derived shape is unparented and
//     outlives the owner, so its index entries and live subscriptions
//     (Files().Query(owner).Subscribe) end here — the owner's files are
//     gone with it (see PayloadsAPI.ownerDeleted).
//
// Reads collections by name; never loads an object.
func (s *Service) purgeFileLeftovers(ctx context.Context, spaceId string, st *spaceobjects.Store, objectId string) {
	if s.dropFileIndex(ctx, spaceId, objectId) {
		// A payloads object owns no files of its own.
		return
	}
	for _, ownerDerived := range []bool{false, true} {
		payloadsId, err := st.DeriveId(ctx, payloadsDeriveOpts(objectId, ownerDerived))
		if err != nil {
			continue
		}
		e, err := st.TreeEntry(ctx, payloadsId)
		if err != nil || !e.Present {
			continue
		}
		st.SubEngine().CloseObject(payloadsId, space.ErrObjectDeleted)
		s.dropFileIndex(ctx, spaceId, payloadsId)
	}
}

// dropFileIndex deletes the index entries of every row in objectId's
// payloads collection. Reports whether objectId has that collection —
// a missing one means objectId is not a payloads object with rows.
func (s *Service) dropFileIndex(ctx context.Context, spaceId, objectId string) bool {
	kv := s.filesStore()
	coll, err := s.db.OpenCollection(ctx, objectId+"_"+payloads.Dataset)
	if err != nil {
		if !errors.Is(err, anystore.ErrCollectionNotFound) {
			filesLog.Warn("purge: open payloads rows", zap.String("objectId", objectId), zap.Error(err))
		}
		return false
	}
	if kv == nil {
		return true
	}
	iter, err := coll.Find(nil).Iter(ctx)
	if err != nil {
		filesLog.Warn("purge: scan payloads rows", zap.String("objectId", objectId), zap.Error(err))
		return true
	}
	var keys []string
	for iter.Next() {
		doc, err := iter.Doc()
		if err != nil {
			continue
		}
		if id := doc.Value().GetString("id"); id != "" {
			keys = append(keys, fileIndexKey(spaceId, id))
		}
	}
	_ = iter.Close()
	if err := kv.DeleteKVs(ctx, keys); err != nil {
		filesLog.Warn("purge: clear file index", zap.String("objectId", objectId), zap.Error(err))
	}
	return true
}
