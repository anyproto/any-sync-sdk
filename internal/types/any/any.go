// Package anytype is the built-in `any` type — the universal shape
// every object in a space implements: name, description, icon
// (synced, CRDT-mutable), plus id, author, spaceId, createdAt,
// modifiedAt, modifiedBy (derived, read-only, stamped from any-sync
// context).
//
// Directory is internal/types/any/; the package is declared `anytype`
// because `any` is a predeclared identifier and shadowing it inside
// every file of this package is noisier than the import alias.
//
// `any` ships as a derived object in every space. It contributes no
// user datasets and does not register a crdt.Handler of its own — its
// properties flow through properties.SystemPropertiesHandler like any
// other synced-scope property writes. This package exists to expose
// the well-known id and the hardcoded property definitions that the
// types registry surfaces uniformly with user-defined types.
package anytype

import "github.com/anyproto/any-sync-sdk/internal/schema"

// WellKnownDeriveSeed is the derivation input used to mint the same
// `any` type object id on every peer. Every space has exactly one
// `any` type object, always derived from this seed.
const WellKnownDeriveSeed = "builtin:any"

// TypeId is the public type id used to address `any`-typed properties
// on instance objects. v1 uses the literal "any" as both the
// namespace key in property records (`record.any.name`) and the
// TypeInfo.Id surfaced through Space.Types() — it's reserved
// (user-derived ids are content-addressable, never produce this
// string), so there's no collision risk.
const TypeId = "any"

// Display metadata for the `any` type object.
const (
	Name        = "Any"
	Description = "Universal properties shared by every object"
)

// Membership fields on the objects row: `any.type` holds the object's
// one type (a scalar LWW register — a definition object carries its
// marker there instead), `any.collections` the set of collections it
// belongs to.
const (
	FieldType        = "type"
	FieldCollections = "collections"
)

// BuiltInProperty is one hardcoded property definition for the `any`
// type. Uses human-readable ids (e.g. "name") that never collide with
// generated user propIds (11-char base58). Scope uses the unified
// schema.Scope taxonomy — the same vocabulary user property
// definitions declare (docs/data-structure.md §"Property Types").
type BuiltInProperty struct {
	Id    string
	Name  string
	Kind  schema.Kind
	Scope schema.Scope
	// Description and XFormat are the descriptive slice: a display
	// description and the opaque descriptor bag (docs/data-structure.md § The
	// `x-format` descriptor). Surfaced by Types().Properties and
	// dataset discovery like a user definition's; never interpreted
	// or enforced by the SDK.
	Description string
	XFormat     map[string]any
}

// Properties lists the `any` type's hardcoded property definitions in
// canonical display order. Consumed by the types registry on space
// init to seed the derived `any` type object.
//
// Descriptors follow the consumer vocabulary (the `any` server's
// docs/27-descriptors.md): a slug only where one names the value —
// display text and instants. Identities, ids, the icon and the two
// system arrays are system values and carry a description alone.
var Properties = []BuiltInProperty{
	{Id: "id", Name: "Id", Kind: schema.KindString, Scope: schema.ScopeDerived,
		Description: "Object id; derived, never written."},
	{Id: "author", Name: "Author", Kind: schema.KindString, Scope: schema.ScopeDerived,
		Description: "Account identity that created the object; derived."},
	{Id: "spaceId", Name: "Space", Kind: schema.KindString, Scope: schema.ScopeDerived,
		Description: "Id of the space the object lives in; derived."},
	{Id: "createdAt", Name: "Created at", Kind: schema.KindDatetime, Scope: schema.ScopeDerived,
		Description: "Instant of the creating change (author's clock); derived.",
		XFormat:     map[string]any{"type": "datetime"}},
	{Id: "modifiedAt", Name: "Modified at", Kind: schema.KindDatetime, Scope: schema.ScopeDerived,
		Description: "Instant of the latest valid synced change (author's clock); derived.",
		XFormat:     map[string]any{"type": "datetime"}},
	// `modifiedBy` is the account identity that signed the change
	// `modifiedAt` points at — both are stamped by the same change and
	// converge together (properties.SystemPropertiesHandler).
	{Id: "modifiedBy", Name: "Modified by", Kind: schema.KindString, Scope: schema.ScopeDerived,
		Description: "Account identity that signed the change modifiedAt names; derived."},
	{Id: "name", Name: "Name", Kind: schema.KindString, Scope: schema.ScopeSynced,
		Description: "Display name.",
		XFormat:     map[string]any{"type": "text"}},
	{Id: "description", Name: "Description", Kind: schema.KindString, Scope: schema.ScopeSynced,
		Description: "Display description.",
		XFormat:     map[string]any{"type": "longtext"}},
	{Id: "icon", Name: "Icon", Kind: schema.KindString, Scope: schema.ScopeSynced,
		Description: "Display icon; the encoding is the client's."},
	// `type` is the ONE type the object has; `collections` the set of
	// collections it belongs to (docs/data-structure.md § Type and collections). Both
	// synced-only: membership is structural and shared — never
	// per-account or per-device.
	{Id: FieldType, Name: "Type", Kind: schema.KindString, Scope: schema.ScopeSynced,
		Description: "Id of the object's type; a definition object carries its marker here."},
	{Id: FieldCollections, Name: "Collections", Kind: schema.KindArray, Scope: schema.ScopeSynced,
		Description: "Ids of the collections the object belongs to."},
	// `tags` is a free-form list of user labels. Array of strings.
	// Synced: tags are shared object metadata, like name/description.
	{Id: "tags", Name: "Tags", Kind: schema.KindArray, Scope: schema.ScopeSynced,
		Description: "Free-form user labels."},
}
