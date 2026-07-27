// Package anytype is the built-in `any` type — the universal shape
// every object in a space implements: name, description, icon
// (synced, CRDT-mutable), plus id, author, spaceId, createdAt
// (derived, read-only, stamped from any-sync context).
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

// BuiltInProperty is one hardcoded property definition for the `any`
// type. Uses human-readable ids (e.g. "name") that never collide with
// generated user propIds (11-char base58). Scope uses the unified
// schema.Scope taxonomy — the same vocabulary user property
// definitions declare (docs/06-data-structure.md §"Property Types").
type BuiltInProperty struct {
	Id    string
	Name  string
	Kind  schema.Kind
	Scope schema.Scope
}

// Properties lists the `any` type's hardcoded property definitions in
// canonical display order. Consumed by the types registry on space
// init to seed the derived `any` type object.
var Properties = []BuiltInProperty{
	{Id: "id", Name: "Id", Kind: schema.KindString, Scope: schema.ScopeDerived},
	{Id: "author", Name: "Author", Kind: schema.KindString, Scope: schema.ScopeDerived},
	{Id: "spaceId", Name: "Space", Kind: schema.KindString, Scope: schema.ScopeDerived},
	{Id: "createdAt", Name: "Created at", Kind: schema.KindNumber, Scope: schema.ScopeDerived},
	{Id: "modifiedAt", Name: "Modified at", Kind: schema.KindNumber, Scope: schema.ScopeDerived},
	{Id: "name", Name: "Name", Kind: schema.KindString, Scope: schema.ScopeSynced},
	{Id: "description", Name: "Description", Kind: schema.KindString, Scope: schema.ScopeSynced},
	{Id: "icon", Name: "Icon", Kind: schema.KindString, Scope: schema.ScopeSynced},
	// `xkey` is the optional caller-side programmatic key. Set on type objects
	// at Create (TypeCreateParams.XKey → any.xkey); harmless/unset on others.
	{Id: "xkey", Name: "XKey", Kind: schema.KindString, Scope: schema.ScopeSynced},
	// `types` is the list of type ids this object implements (docs 06
	// §"Property ids" / §"types list"). Array of strings. Deliberately
	// synced-only: type membership is structural and shared — never
	// per-account or per-device.
	{Id: "types", Name: "Types", Kind: schema.KindArray, Scope: schema.ScopeSynced},
}
