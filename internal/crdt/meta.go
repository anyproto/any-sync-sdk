package crdt

import (
	"context"
	"errors"

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

	// spaceMetaKeyPrefix namespaces space-scoped rows inside the same
	// _meta collection. Colon is not a valid char in any-sync's
	// content-addressable object IDs, so "space:<id>" rows can't
	// collide with per-object rows keyed by objectId.
	spaceMetaKeyPrefix = "space:"
)

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
func PersistMeta(ctx context.Context, coll anystore.Collection, objectId string, maxAddSeq uint64, handlerVersions map[string]int) error {
	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		v.Set(metaAddSeqKey, a.NewNumberInt(int(maxAddSeq)))
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

// HandlerVersions returns a map of dataset→version from the Controller's
// registered handlers.
func (c *Controller) HandlerVersions() map[string]int {
	hv := make(map[string]int, len(c.handlers))
	for name, h := range c.handlers {
		hv[name] = h.Version()
	}
	return hv
}

// PersistMeta writes the Controller's maxAddSeq and handler versions to
// the _meta collection. The caller should pass a context carrying the same
// WriteTx as the record mutations for atomicity.
func (c *Controller) PersistMeta(ctx context.Context, metaColl anystore.Collection) error {
	return PersistMeta(ctx, metaColl, c.objectId, c.maxAddSeq, c.HandlerVersions())
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
