// Package spaceindex is the built-in `spaceIndex` type — one
// derived object per space carrying the space's display metadata
// (name, description, icon, spaceType) as CRDT-mutable base-scope
// properties. Replicates to every member like any other in-space
// object; each device mirrors the converged state into its own
// account-private tech-space row.
//
// Pattern mirrors internal/types/any: hardcoded property defs, no
// custom handler — writes flow through internal/properties.
// SystemPropertiesHandler against the per-space `objects`
// collection just like `any.*` writes.
package spaceindex

import "github.com/anyproto/any-sync-sdk/internal/schema"

// WellKnownDeriveSeed is the derivation input used to mint the same
// `spaceIndex` object id on every peer of a space. Every space has
// exactly one spaceIndex object, always derived from this seed.
const WellKnownDeriveSeed = "builtin:spaceIndex"

// TypeId is the public type id used to namespace spaceIndex
// property paths (`record.spaceIndex.name` etc.). v1 uses the
// literal "spaceIndex" both as the namespace key in property
// records and as the TypeInfo.Id surfaced through Space.Types().
// User-derived type ids are content-addressable and cannot collide
// with this reserved string.
const TypeId = "spaceIndex"

// Display metadata for the built-in spaceIndex type.
const (
	Name        = "Space Index"
	Description = "Per-space metadata mirrored to every member's tech-space row"
)

// Field names — the property ids inside the `spaceIndex` namespace.
// Used by writers and the tech-space mirror to address each value.
const (
	FieldName        = "name"
	FieldDescription = "description"
	FieldIcon        = "icon"
	FieldSpaceType   = "spaceType"
)

// BuiltInProperty is one hardcoded property definition. Same shape
// as anytype.BuiltInProperty so the types registry surfaces both
// uniformly. Scope uses the unified schema.Scope taxonomy.
type BuiltInProperty struct {
	Id    string
	Name  string
	Kind  schema.Kind
	Scope schema.Scope
	// Description and XFormat are the descriptive slice: a display
	// description and the opaque descriptor bag (docs/06 § The
	// `x-format` descriptor). Surfaced by Types().Properties and
	// dataset discovery like a user definition's; never interpreted
	// or enforced by the SDK.
	Description string
	XFormat     map[string]any
}

// Properties lists the spaceIndex type's hardcoded property
// definitions in canonical display order. All four are synced
// strings — `spaceType` is intentionally writable through the CRDT
// (first writer wins in practice via the initial Create write).
var Properties = []BuiltInProperty{
	{Id: FieldName, Name: "Name", Kind: schema.KindString, Scope: schema.ScopeSynced,
		Description: "Space display name.", XFormat: map[string]any{"type": "text"}},
	{Id: FieldDescription, Name: "Description", Kind: schema.KindString, Scope: schema.ScopeSynced,
		Description: "Space display description.", XFormat: map[string]any{"type": "longtext"}},
	{Id: FieldIcon, Name: "Icon", Kind: schema.KindString, Scope: schema.ScopeSynced,
		Description: "Space icon CID."},
	{Id: FieldSpaceType, Name: "Space type", Kind: schema.KindString, Scope: schema.ScopeSynced,
		Description: "Application space type; pinned by the creating write."},
}
