// Package anytype is the built-in `any` type — the universal shape
// every object in a space implements: name, description, icon (base-
// scope, CRDT-mutable), plus id, author, spaceId, createdAt (auto,
// read-only, derived from any-sync context).
//
// Directory is internal/types/any/; the package is declared `anytype`
// because `any` is a predeclared identifier and shadowing it inside
// every file of this package is noisier than the import alias.
//
// `any` ships as a derived object in every space. It contributes no
// user datasets and does not register a crdt.Handler of its own — its
// properties flow through internal/properties.baseProperty like any
// other base-scope property writes. This package exists to expose the
// well-known id and the hardcoded property definitions that the types
// registry surfaces uniformly with user-defined types.
package anytype

import "github.com/anyproto/any-sync-sdk/internal/schema"

// WellKnownDeriveSeed is the derivation input used to mint the same
// `any` type object id on every peer. Every space has exactly one
// `any` type object, always derived from this seed.
const WellKnownDeriveSeed = "builtin:any"

// Display metadata for the `any` type object.
const (
	Name        = "Any"
	Description = "Universal properties shared by every object"
)

// Scope distinguishes auto (read-only, derived) from base-scope
// (CRDT-mutable) properties. Maps onto the property-type matrix in
// docs/06-data-structure.md §"Property Types".
type Scope uint8

const (
	ScopeAuto Scope = iota + 1
	ScopeBase
)

// BuiltInProperty is one hardcoded property definition for the `any`
// type. Uses human-readable ids (e.g. "name") that never collide with
// generated user propIds (11-char base58).
type BuiltInProperty struct {
	Id    string
	Name  string
	Kind  schema.Kind
	Scope Scope
}

// Properties lists the `any` type's hardcoded property definitions in
// canonical display order. Consumed by the types registry on space
// init to seed the derived `any` type object.
var Properties = []BuiltInProperty{
	{Id: "id", Name: "Id", Kind: schema.KindString, Scope: ScopeAuto},
	{Id: "author", Name: "Author", Kind: schema.KindString, Scope: ScopeAuto},
	{Id: "spaceId", Name: "Space", Kind: schema.KindString, Scope: ScopeAuto},
	{Id: "createdAt", Name: "Created at", Kind: schema.KindNumber, Scope: ScopeAuto},
	{Id: "name", Name: "Name", Kind: schema.KindString, Scope: ScopeBase},
	{Id: "description", Name: "Description", Kind: schema.KindString, Scope: ScopeBase},
	{Id: "icon", Name: "Icon", Kind: schema.KindString, Scope: ScopeBase},
	// `types` is the list of type ids this object implements (docs 06
	// §"Property ids" / §"types list"). Array of strings.
	{Id: "types", Name: "Types", Kind: schema.KindArray, Scope: ScopeBase},
}
