package spaceimpl

import (
	"context"
	"errors"

	anystore "github.com/anyproto/any-store/v2"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/files/status"
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
//     answers ErrNotFound through a stale entry) and their pending
//     file jobs, which would otherwise retry the lookup forever;
//   - a purged owner: live subscriptions on its payloads object of
//     either shape end (Files().Query(owner).Subscribe) — the owner's
//     files are gone with it (see PayloadsAPI.ownerDeleted). The signed
//     shape is cascade-deleted by any-sync and clears its own entries
//     and jobs through its own purge; the derived shape is unparented
//     and outlives the owner with its rows, whose entries stay valid
//     (an entry lives as long as its row, see FindRow).
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
	}
}

// dropFileIndex deletes the index entries and pending jobs of every
// row in objectId's payloads collection. Reports whether objectId has
// that collection — a missing one means objectId is not a payloads
// object with rows.
func (s *Service) dropFileIndex(ctx context.Context, spaceId, objectId string) bool {
	coll, err := s.db.OpenCollection(ctx, objectId+"_"+payloads.Dataset)
	if err != nil {
		if !errors.Is(err, anystore.ErrCollectionNotFound) {
			filesLog.Warn("purge: open payloads rows", zap.String("objectId", objectId), zap.Error(err))
		}
		return false
	}
	iter, err := coll.Find(nil).Iter(ctx)
	if err != nil {
		filesLog.Warn("purge: scan payloads rows", zap.String("objectId", objectId), zap.Error(err))
		return true
	}
	var fileIds []string
	for iter.Next() {
		doc, err := iter.Doc()
		if err != nil {
			continue
		}
		if id := doc.Value().GetString("id"); id != "" {
			fileIds = append(fileIds, id)
		}
	}
	scanErr := iter.Err()
	_ = iter.Close()
	if scanErr != nil {
		// A partial list is cleared; the rest heals on lookup.
		filesLog.Warn("purge: scan payloads rows", zap.String("objectId", objectId), zap.Error(scanErr))
	}
	if kv := s.filesStore(); kv != nil {
		keys := make([]string, len(fileIds))
		for i, id := range fileIds {
			keys[i] = fileIndexKey(spaceId, id)
		}
		if err := kv.DeleteKVs(ctx, keys); err != nil {
			filesLog.Warn("purge: clear file index", zap.String("objectId", objectId), zap.Error(err))
		}
	}
	if q := s.fqueue; q != nil {
		for _, id := range fileIds {
			for _, kind := range []string{status.KindDurable, status.KindPin} {
				if err := q.Remove(ctx, kind, spaceId, id); err != nil {
					filesLog.Warn("purge: remove file job", zap.String("fileId", id), zap.String("kind", kind), zap.Error(err))
				}
			}
		}
	}
	return true
}
