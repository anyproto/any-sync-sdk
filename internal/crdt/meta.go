package crdt

import (
	"context"
	"errors"
	"fmt"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

// MetaCollectionName is the shared collection for per-object metadata.
// Not prefixed by objectId — one collection holds metadata for all objects
// in the DB, keyed by objectId.
const MetaCollectionName = "_meta"

const (
	metaAddSeqKey          = "q"
	metaHandlerVersionsKey = "hv"
	// metaSpaceIdKey scopes a per-object row to its space. The SDK DB is
	// shared across spaces by default and AddSeq is per-space, so the
	// change-index query filters on this field. Absent on the
	// `space:<id>` watermark rows and on object rows written before
	// space-scoping (those stay invisible to the query until their next
	// change backfills the field — the accepted "index from now on"
	// behaviour).
	metaSpaceIdKey = "sp"

	// spaceMetaKeyPrefix namespaces space-scoped rows inside the same
	// _meta collection. Colon is not a valid char in any-sync's
	// content-addressable object IDs, so "space:<id>" rows can't
	// collide with per-object rows keyed by objectId.
	spaceMetaKeyPrefix = "space:"
)

// ensureMetaIndexes ensures the indexes the _meta collection needs.
// Idempotent — safe to call on every open. The (sp, q) compound index
// backs QueryChangedObjects' "objects in this space with AddSeq > N,
// ordered by AddSeq" scan.
func ensureMetaIndexes(ctx context.Context, coll anystore.Collection) error {
	return coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name:   "idx__meta_sp_q",
		Fields: []string{metaSpaceIdKey, metaAddSeqKey},
		Sparse: true,
	})
}

// SpaceMetaKey returns the _meta document id for a space's row.
func SpaceMetaKey(spaceId string) string { return spaceMetaKeyPrefix + spaceId }

// LoadMeta reads the per-object metadata from the _meta collection.
// Returns zero values if the document doesn't exist yet.
func LoadMeta(ctx context.Context, coll anystore.Collection, objectId string) (maxAddSeq uint64, handlerVersions map[string]int, err error) {
	doc, findErr := coll.FindId(ctx, objectId)
	if findErr != nil {
		if errors.Is(findErr, anystore.ErrDocNotFound) {
			return 0, nil, nil
		}
		return 0, nil, findErr
	}
	v := doc.Value()
	maxAddSeq = uint64(v.GetInt(metaAddSeqKey))
	if hv := v.Get(metaHandlerVersionsKey); hv != nil && hv.Type() == anyenc.TypeObject {
		handlerVersions = make(map[string]int)
		obj, _ := hv.Object()
		obj.Visit(func(k []byte, vv *anyenc.Value) {
			if vv.Type() == anyenc.TypeNumber {
				handlerVersions[string(k)] = vv.GetInt()
			}
		})
	}
	return maxAddSeq, handlerVersions, nil
}

// PersistMeta writes per-object metadata to the _meta collection. Call
// inside the same WriteTx as the record mutations for atomicity.
//
// spaceId scopes the row for the change-index query; pass "" to leave it
// unset (unit tests, raw mode without a space). An unset row is
// invisible to QueryChangedObjects, which is the accepted lazy-backfill
// behaviour.
func PersistMeta(ctx context.Context, coll anystore.Collection, objectId string, maxAddSeq uint64, handlerVersions map[string]int, spaceId string) error {
	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		v.Set(metaAddSeqKey, a.NewNumberInt(int(maxAddSeq)))
		if spaceId != "" {
			v.Set(metaSpaceIdKey, a.NewString(spaceId))
		}
		if len(handlerVersions) > 0 {
			hv := a.NewObject()
			for name, ver := range handlerVersions {
				hv.Set(name, a.NewNumberInt(ver))
			}
			v.Set(metaHandlerVersionsKey, hv)
		}
		return v, true, nil
	})
	_, err := coll.UpsertId(ctx, objectId, mod)
	return err
}

// ObjectSeq pairs an object id with its persisted max AddSeq. Returned
// by QueryChangedObjects for the consumer-side change-index feed.
type ObjectSeq struct {
	ObjectId string
	AddSeq   uint64
}

// QueryChangedObjects returns the objects in spaceId whose persisted max
// AddSeq exceeds `since`, ordered by AddSeq ascending so the caller can
// page by passing the last returned AddSeq as the next `since`. limit<=0
// means no cap. Backs the change-index "what changed" pull path.
//
// The spaceId filter naturally excludes the `space:<id>` watermark rows
// (no `sp` field) and per-object rows written before space-scoping.
func QueryChangedObjects(ctx context.Context, coll anystore.Collection, spaceId string, since uint64, limit int) ([]ObjectSeq, error) {
	filter, err := query.ParseCondition(map[string]any{
		metaSpaceIdKey: spaceId,
		metaAddSeqKey:  map[string]any{"$gt": int(since)},
	})
	if err != nil {
		return nil, fmt.Errorf("crdt: build changed-objects filter: %w", err)
	}
	sort, err := query.ParseSort(metaAddSeqKey)
	if err != nil {
		return nil, fmt.Errorf("crdt: build changed-objects sort: %w", err)
	}
	q := coll.Find(filter).Sort(sort)
	if limit > 0 {
		q = q.Limit(uint(limit))
	}
	iter, err := q.Iter(ctx)
	if err != nil {
		return nil, fmt.Errorf("crdt: changed-objects iter: %w", err)
	}
	defer iter.Close()
	var out []ObjectSeq
	for iter.Next() {
		doc, derr := iter.Doc()
		if derr != nil {
			return nil, derr
		}
		v := doc.Value()
		out = append(out, ObjectSeq{
			ObjectId: v.GetString(IdField),
			AddSeq:   uint64(v.GetInt(metaAddSeqKey)),
		})
	}
	return out, nil
}

// MaxObjectAddSeq returns the highest persisted per-object AddSeq in
// spaceId — the current upper bound a change-index cursor can reach.
// Returns 0 when the space has no scoped object rows yet.
func MaxObjectAddSeq(ctx context.Context, coll anystore.Collection, spaceId string) (uint64, error) {
	filter, err := query.ParseCondition(map[string]any{metaSpaceIdKey: spaceId})
	if err != nil {
		return 0, fmt.Errorf("crdt: build max-addseq filter: %w", err)
	}
	sort, err := query.ParseSort("-" + metaAddSeqKey)
	if err != nil {
		return 0, fmt.Errorf("crdt: build max-addseq sort: %w", err)
	}
	iter, err := coll.Find(filter).Sort(sort).Limit(1).Iter(ctx)
	if err != nil {
		return 0, fmt.Errorf("crdt: max-addseq iter: %w", err)
	}
	defer iter.Close()
	if iter.Next() {
		doc, derr := iter.Doc()
		if derr != nil {
			return 0, derr
		}
		return uint64(doc.Value().GetInt(metaAddSeqKey)), nil
	}
	return 0, nil
}

// HandlerVersions returns a map of dataset→version from the Controller's
// registered handlers.
func (c *Controller) HandlerVersions() map[string]int {
	hv := make(map[string]int, len(c.versions))
	for name, ver := range c.versions {
		hv[name] = ver
	}
	return hv
}

// PersistMeta writes the Controller's maxAddSeq and handler versions to
// the _meta collection. The caller should pass a context carrying the same
// WriteTx as the record mutations for atomicity.
func (c *Controller) PersistMeta(ctx context.Context, metaColl anystore.Collection) error {
	return PersistMeta(ctx, metaColl, c.objectId, c.maxAddSeq, c.HandlerVersions(), c.spaceId)
}

// LoadAndSeedMeta reads metadata from the _meta collection, seeds the
// Controller's maxAddSeq, and returns the stored handler versions so the
// caller can compare them with current versions for re-indexing decisions.
func (c *Controller) LoadAndSeedMeta(ctx context.Context, metaColl anystore.Collection) (handlerVersions map[string]int, err error) {
	maxAddSeq, hv, err := LoadMeta(ctx, metaColl, c.objectId)
	if err != nil {
		return nil, err
	}
	c.maxAddSeq = maxAddSeq
	return hv, nil
}

// LoadSpaceMaxAddSeq reads the persisted space-level head-store
// watermark — the lower bound of "we've already replayed any-sync
// trees up to this LastAddSeq for this space". Returns 0 when no
// row exists yet (first boot or never caught up).
func LoadSpaceMaxAddSeq(ctx context.Context, coll anystore.Collection, spaceId string) (uint64, error) {
	doc, err := coll.FindId(ctx, SpaceMetaKey(spaceId))
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return uint64(doc.Value().GetInt(metaAddSeqKey)), nil
}

// PersistSpaceMaxAddSeq writes the space-level head-store watermark.
// Written once at the end of a successful catch-up pass — per-change
// progress is already captured by the per-object _meta rows, so this
// value is a coarse-grained "no work needed at startup" hint, not a
// per-change atomic counter.
func PersistSpaceMaxAddSeq(ctx context.Context, coll anystore.Collection, spaceId string, maxAddSeq uint64) error {
	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		v.Set(metaAddSeqKey, a.NewNumberInt(int(maxAddSeq)))
		return v, true, nil
	})
	_, err := coll.UpsertId(ctx, SpaceMetaKey(spaceId), mod)
	return err
}
