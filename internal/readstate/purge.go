package readstate

import (
	"context"
	"errors"
	"fmt"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/query"
)

// purgeSpaceChunk bounds the objects one PurgeSpace transaction clears.
var purgeSpaceChunk = 5000

// purgeCollections opens the two read-state collections by name without
// creating them. A collection that was never created (nothing tracked
// on this DB yet) is nil.
func purgeCollections(ctx context.Context, db anystore.DB) (unread, state anystore.Collection, err error) {
	open := func(name string) (anystore.Collection, error) {
		coll, err := db.OpenCollection(ctx, name)
		if errors.Is(err, anystore.ErrCollectionNotFound) {
			return nil, nil
		}
		return coll, err
	}
	if unread, err = open(UnreadCollectionName); err != nil {
		return nil, nil, err
	}
	if state, err = open(StateCollectionName); err != nil {
		return nil, nil, err
	}
	return unread, state, nil
}

func objectUnread(objectId string) query.Filter {
	return query.Key{Path: []string{fObject}, Filter: query.NewComp(query.CompOpEq, objectId)}
}

// purgeObjectIn removes objectId's unread rows and state row through
// the given handles, in the caller's ctx.
func purgeObjectIn(ctx context.Context, unread, state anystore.Collection, objectId string) error {
	if unread != nil {
		if _, err := unread.Find(objectUnread(objectId)).Delete(ctx); err != nil {
			return fmt.Errorf("readstate: purge unread %s: %w", objectId, err)
		}
	}
	if state != nil {
		if err := state.DeleteId(ctx, objectId); err != nil && !errors.Is(err, anystore.ErrDocNotFound) {
			return fmt.Errorf("readstate: purge state %s: %w", objectId, err)
		}
	}
	return nil
}

// PurgeObject removes objectId's read state — its unread rows and its
// state row — in the caller's ctx, so an object purge clears it in the
// same transaction as the object's row. Collections that were never
// created and rows that are already gone are not errors. Without this
// a purged object's row would outlive it: `sd` stays set, so a later
// materialization of the same id would skip first-sight seeding and
// keep the stale frontier.
func PurgeObject(ctx context.Context, db anystore.DB, objectId string) error {
	unread, state, err := purgeCollections(ctx, db)
	if err != nil {
		return err
	}
	return purgeObjectIn(ctx, unread, state, objectId)
}

// PurgeSpace removes every read-state row of spaceId: the state rows
// are read through the (space, stateSeq) index in chunks, and each
// chunk's objects lose their unread rows (object index) and state rows
// in one write transaction, so the sweep holds the writer for at most
// purgeSpaceChunk objects at a time and committed chunks survive a
// failure. The state rows enumerate the whole space — TrackChange
// persists an unread row and its object's state row together. Returns
// the number of objects purged and the first error; the rest is left
// for the next attempt. Space offload and the boot orphan sweep call
// it: a removed space's rows would otherwise stand in for the all-read
// seed if the space re-materializes.
func PurgeSpace(ctx context.Context, db anystore.DB, spaceId string) (purged int, err error) {
	unread, state, err := purgeCollections(ctx, db)
	if err != nil || state == nil {
		return 0, err
	}
	filter := query.Key{Path: []string{fSpace}, Filter: query.NewComp(query.CompOpEq, spaceId)}
	for {
		ids, err := spaceChunkIds(ctx, state, filter)
		if err != nil {
			return purged, err
		}
		if len(ids) == 0 {
			return purged, nil
		}
		if err := purgeChunk(ctx, db, unread, state, ids); err != nil {
			return purged, err
		}
		purged += len(ids)
	}
}

// spaceChunkIds reads the object ids of the next state-row chunk.
func spaceChunkIds(ctx context.Context, state anystore.Collection, filter query.Filter) ([]string, error) {
	iter, err := state.Find(filter).Limit(uint(purgeSpaceChunk)).Iter(ctx)
	if err != nil {
		return nil, fmt.Errorf("readstate: purge space scan: %w", err)
	}
	defer iter.Close()
	var ids []string
	for iter.Next() {
		doc, err := iter.Doc()
		if err != nil {
			return nil, err
		}
		if id := doc.Value().GetString("id"); id != "" {
			ids = append(ids, id)
		}
	}
	return ids, iter.Err()
}

// purgeChunk clears one chunk of objects in a single write transaction.
func purgeChunk(ctx context.Context, db anystore.DB, unread, state anystore.Collection, ids []string) error {
	tx, err := db.WriteTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, id := range ids {
		if err := purgeObjectIn(tx.Context(), unread, state, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}
