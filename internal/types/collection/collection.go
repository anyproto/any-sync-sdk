// Package collectiontype is the built-in `collection` meta-type — the
// shape of collection objects. A collection is a type without parts:
// property definitions only, what an object is filed under rather than
// what it is. An object has one type (`any.type`) and any number of
// collections (`any.collections`).
//
// Directory is internal/types/collection/; the package is declared
// `collectiontype` to sit next to `typetype`.
//
// A collection object owns the same `properties` and `shortIds`
// datasets a type object does (typetype.PropertyHandler serves both);
// it never writes the `datasets` dataset.
package collectiontype

import (
	"github.com/anyproto/any-sync-sdk/internal/schema"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
)

// WellKnownDeriveSeed mints the same `collection` meta-type object id
// on every peer.
const WellKnownDeriveSeed = "builtin:collection"

// MetaMarker is the reserved value every collection object carries in
// `any.type`; TypeId is the meta-collection's id — the namespace its
// collection-only values live under (`record.collection.xkey`) and the
// id surfaced through Space.Collections(). Both reserved, like the
// meta-type's pair (typetype.MetaTypeMarker / typetype.TypeId).
const (
	MetaMarker = "__collection__"
	TypeId     = "collection"
)

// Display metadata for the `collection` meta-type object.
const (
	Name        = "Collection"
	Description = "A collection — defines properties for the objects filed under it"
)

// Properties lists the meta-collection's hardcoded property
// definitions: the type-side subset that describes a definition rather
// than how its objects render — the handle, the listing flag and the
// consumer flag bag. Same field ids as the meta-type's, so the two
// namespaces read alike.
var Properties = []typetype.BuiltInProperty{
	{Id: typetype.FieldXKeyProp, Name: "XKey", Kind: schema.KindString, Scope: schema.ScopeSynced,
		Description: "Programmatic handle of the collection; consumers keep it unique per space."},
	{Id: typetype.FieldHiddenProp, Name: "Hidden", Kind: schema.KindBoolean, Scope: schema.ScopeSynced,
		Description: "Keeps the collection out of default listings and pickers.",
		XFormat:     map[string]any{"type": "checkbox"}},
	{Id: typetype.FieldMetaProp, Name: "Meta", Kind: schema.KindObject, Scope: schema.ScopeSynced,
		Description: "Consumer flags, one scalar per key."},
}
