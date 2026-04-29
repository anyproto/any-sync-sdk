package types

import (
	"context"
	"errors"
	"fmt"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// LiveRegistry is the real Registry: it answers kind / shortId
// lookups by reading the per-type-object collections on the SDK DB.
//
// No in-memory caching in v1 — every lookup hits any-store. Cheap
// for the gate (single FindId by id) and avoids invalidation
// bookkeeping. Add caching with an event-driven invalidation when
// the gate's hot path benchmarks demand it.
//
// Naming convention: collections are `<typeId>/properties` (defs)
// and `<typeId>/shortIds` (sibling). Both are written by
// typetype.PropertyHandler at apply time.
type LiveRegistry struct {
	db anystore.DB
}

// NewLiveRegistry returns a Registry bound to the given SDK DB.
// Pass nil to disable lookups (every method returns "not found",
// equivalent to StubRegistry{}).
func NewLiveRegistry(db anystore.DB) *LiveRegistry { return &LiveRegistry{db: db} }

// LookupKind reads the property-definition record for (typeId,
// propId) from the type's `properties` dataset and returns its
// declared kind. Returns (KindUnknown, false) when the type, the
// property, or the kind field is absent.
func (r *LiveRegistry) LookupKind(typeId, propId string) (schema.Kind, bool) {
	if r == nil || r.db == nil {
		return schema.KindUnknown, false
	}
	ctx := context.Background()
	coll, err := r.openCollection(ctx, typeId, "properties")
	if err != nil {
		return schema.KindUnknown, false
	}
	doc, err := coll.FindId(ctx, propId)
	if err != nil {
		return schema.KindUnknown, false
	}
	v := doc.Value()
	if v == nil {
		return schema.KindUnknown, false
	}
	label := v.GetString("kind")
	if label == "" {
		return schema.KindUnknown, false
	}
	return schema.ParseKind(label)
}

// KnownShortId reports whether the type's shortIds dataset has a
// row with id = shortId. The shortId row itself is added or removed
// by typetype.PropertyHandler on every "important" change to the
// type's properties dataset.
func (r *LiveRegistry) KnownShortId(ctx context.Context, typeId, shortId string) (bool, error) {
	if r == nil || r.db == nil {
		return false, nil
	}
	coll, err := r.openCollection(ctx, typeId, "shortIds")
	if err != nil {
		return false, err
	}
	if _, err := coll.FindId(ctx, shortId); err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// LatestShortId returns the lexicographically-greatest `_ver.id` from
// the type's shortIds dataset. The lex-monotonic VersionId allocator
// guarantees this is the most-recently-applied important change.
//
// Returns ("", nil) when the type has had no important changes yet
// (e.g. a type with no properties). Callers stamp DataVersion as
// empty in that case.
func (r *LiveRegistry) LatestShortId(ctx context.Context, typeId string) (string, error) {
	if r == nil || r.db == nil {
		return "", nil
	}
	coll, err := r.openCollection(ctx, typeId, "shortIds")
	if err != nil {
		return "", err
	}
	iter, err := coll.Find(nil).Sort("-_ver.id").Limit(1).Iter(ctx)
	if err != nil {
		return "", fmt.Errorf("types: latest shortId iter: %w", err)
	}
	defer iter.Close()
	if !iter.Next() {
		return "", iter.Err()
	}
	doc, err := iter.Doc()
	if err != nil {
		return "", err
	}
	v := doc.Value()
	if v == nil || v.Type() != anyenc.TypeObject {
		return "", nil
	}
	return v.GetString("id"), nil
}

// openCollection opens (and lazily creates) the named per-type-
// object collection. Returns the same handle on subsequent calls.
func (r *LiveRegistry) openCollection(ctx context.Context, typeId, dataset string) (anystore.Collection, error) {
	return r.db.Collection(ctx, typeId+"/"+dataset)
}

// Compile-time check that LiveRegistry satisfies Registry.
var _ Registry = (*LiveRegistry)(nil)
