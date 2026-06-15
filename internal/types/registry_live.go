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

	// static is the schema overlay for types whose definitions don't
	// live in a per-type-object `properties` collection: the built-in
	// `any` / `spaceIndex` tables and every registered external type's
	// declared Properties. Keyed typeId → propId → PropInfo. Read-only
	// after construction. A typeId present here is resolved ENTIRELY
	// from the overlay (no defs-collection fallback); user types are
	// absent here and fall through to the collection read.
	static map[string]map[string]PropInfo
}

// NewLiveRegistry returns a Registry bound to the given SDK DB and a
// static schema overlay (built-ins + registered external types).
// Pass a nil db to disable collection lookups (overlay-only, e.g.
// equivalent to StubRegistry{} for user types); pass a nil overlay
// for none.
func NewLiveRegistry(db anystore.DB, static map[string]map[string]PropInfo) *LiveRegistry {
	return &LiveRegistry{db: db, static: static}
}

// LookupKind resolves the declared kind of (typeId, propId): from the
// static overlay when typeId is a built-in / registered type,
// otherwise from the type's `properties` defs collection. Returns
// (KindUnknown, false) when the type, the property, or the kind is
// absent.
func (r *LiveRegistry) LookupKind(typeId, propId string) (schema.Kind, bool) {
	if r == nil {
		return schema.KindUnknown, false
	}
	if props, ok := r.static[typeId]; ok {
		p, ok := props[propId]
		if !ok {
			return schema.KindUnknown, false
		}
		return p.Kind, true
	}
	if r.db == nil {
		return schema.KindUnknown, false
	}
	ctx := context.Background()
	coll, err := r.openCollection(ctx, typeId, "properties")
	if err != nil {
		// ErrCollectionNotFound means no writer has touched this
		// type's `properties` dataset yet — treat as "unknown".
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

// TypeKnown reports whether a schema source exists for typeId: the
// static overlay (built-in / registered) or a materialised
// `<typeId>_properties` defs collection (a user type with at least
// one important change). Used by the writer-side validator to tell
// "type not here yet" apart from "type known, property unknown".
func (r *LiveRegistry) TypeKnown(typeId string) bool {
	if r == nil {
		return false
	}
	if _, ok := r.static[typeId]; ok {
		return true
	}
	if r.db == nil {
		return false
	}
	_, err := r.openCollection(context.Background(), typeId, "properties")
	return err == nil
}

// PropsOf returns the declared properties of typeId — from the static
// overlay, or by scanning the defs collection for a user type. ok is
// false only when neither source resolves (unknown type). An empty,
// existing type resolves as (nil, true).
func (r *LiveRegistry) PropsOf(typeId string) ([]PropInfo, bool) {
	if r == nil {
		return nil, false
	}
	if props, ok := r.static[typeId]; ok {
		out := make([]PropInfo, 0, len(props))
		for _, p := range props {
			out = append(out, p)
		}
		return out, true
	}
	if r.db == nil {
		return nil, false
	}
	ctx := context.Background()
	coll, err := r.openCollection(ctx, typeId, "properties")
	if err != nil {
		return nil, false
	}
	iter, err := coll.Find(nil).Iter(ctx)
	if err != nil {
		return nil, false
	}
	defer iter.Close()
	var out []PropInfo
	for iter.Next() {
		doc, derr := iter.Doc()
		if derr != nil {
			return nil, false
		}
		v := doc.Value()
		if v == nil {
			continue
		}
		kind, ok := schema.ParseKind(v.GetString("kind"))
		if !ok {
			continue
		}
		// Absent / unparsable scope reads as the zero value, which
		// EffectiveScope normalizes to ScopeSynced — pre-scope
		// definitions keep their historical behavior.
		scope, _ := schema.ParseScope(v.GetString("scope"))
		out = append(out, PropInfo{
			Id:    v.GetString("id"),
			Name:  v.GetString("name"),
			Kind:  kind,
			Scope: scope,
		})
	}
	if iter.Err() != nil {
		return nil, false
	}
	return out, true
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
		if errors.Is(err, anystore.ErrCollectionNotFound) {
			return false, nil
		}
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
		if errors.Is(err, anystore.ErrCollectionNotFound) {
			return "", nil
		}
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

// openCollection opens the named per-type-object collection without
// creating it. Returns anystore.ErrCollectionNotFound when no writer
// has materialised the dataset yet — callers treat that as "no rows".
// Using OpenCollection (not Collection) is what keeps reads from
// leaking empty collections for built-in or unwritten types (e.g.
// the virtual `any` type that contributes no real datasets).
func (r *LiveRegistry) openCollection(ctx context.Context, typeId, dataset string) (anystore.Collection, error) {
	return r.db.OpenCollection(ctx, typeId+"_"+dataset)
}

// Compile-time check that LiveRegistry satisfies Registry.
var _ Registry = (*LiveRegistry)(nil)
