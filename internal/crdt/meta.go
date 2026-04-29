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
)

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
