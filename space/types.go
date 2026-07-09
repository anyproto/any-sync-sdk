package space

import (
	"context"
	"errors"

	"github.com/anyproto/any-sync-sdk/handler"
)

// ErrPinnedField is returned by PatchProperty when a Set/Unset path
// targets pinned state (key, kind, scope, items, properties, the whole
// `format` object, or format.type), or when a format.* value is not a
// string. Consumers (e.g. the `any` server) match it to surface a clean
// 400 rather than the handler's per-op drop.
var ErrPinnedField = errors.New("space: property field is pinned or immutable")

// ErrPropertyNoFormat is returned by PatchProperty when a Set/Unset path
// descends into `format.*` on a property that declared no format at
// creation (format.type is pinned-absent). A client error → 400.
var ErrPropertyNoFormat = errors.New("space: property has no format")

// ErrTypeRegistered is returned by AddProperty / RemoveProperty /
// PatchProperty when the target type is a registered built-in whose
// properties are statically declared and cannot be mutated at runtime.
// A client error → 4xx.
var ErrTypeRegistered = errors.New("space: type is registered — properties are statically declared")

// Scope is the unified write/sync class shared by property definitions
// and dataset schema fields — how a value is written, which version
// domain stamps its `_ver` entries, and how far it syncs. A property
// lives in exactly ONE scope, declared at creation and pinned for the
// propId's life (like Kind); there is no per-value override stack.
//
// Aliased from the handler package so the whole SDK uses one type and
// one label vocabulary (synced / derived / account / local).
type Scope = handler.Scope

const (
	// ScopeSynced: written through the object's own CRDT, synced to
	// everyone with space access. The default.
	ScopeSynced = handler.ScopeSynced
	// ScopeDerived: SDK-stamped (author / createdAt / …), read-only.
	// Reserved for built-ins — user definitions cannot declare it.
	ScopeDerived = handler.ScopeDerived
	// ScopeAccount: synced across this account's devices only, via the
	// private tech space; invisible to other space members.
	ScopeAccount = handler.ScopeAccount
	// ScopeLocal: this device only; never synced.
	ScopeLocal = handler.ScopeLocal
)

// ParseScope parses a scope's wire label ("synced" / "derived" /
// "local" / "account") — the inverse of Scope.String. Returns
// (0, false) on an unknown label.
func ParseScope(label string) (Scope, bool) { return handler.ParseScope(label) }

// TypesAPI manages type objects and property definitions inside a space.
//
// A type is an object with `type = type`. Each type defines properties
// (field-level schemas) stored on the type object itself. Instance
// objects reference types via their `any.types` list; property values
// for those instances live in the per-space `properties` system
// dataset, namespaced by typeId (see PropertiesAPI).
//
// Built-in types (`any`, `type`) are derived on demand — callers don't
// create them. List returns them alongside user-defined types.
type TypesAPI interface {
	List(ctx context.Context) ([]TypeInfo, error)
	Get(ctx context.Context, typeId string) (TypeInfo, error)

	// Create a new user-defined type in this space. Returns the new
	// type object's id.
	Create(ctx context.Context, params TypeCreateParams) (typeId string, err error)

	// Delete a type. Existing instance references in `any.types` stay
	// as-is (orphan), per docs §"read tolerance".
	Delete(ctx context.Context, typeId string) error

	// Properties returns the current property definitions for a type.
	// Includes definitions inherited from `any` / `type` built-ins.
	Properties(ctx context.Context, typeId string) ([]PropertyDef, error)

	// AddProperty mints a new property on the type. The returned
	// propId is base58(xxh3-64(changeId)) — immutable for the life of
	// the property.
	AddProperty(ctx context.Context, typeId string, draft PropertyDraft) (propId string, err error)

	// RemoveProperty drops a property definition. Existing value
	// records are NOT cleaned up; subsequent writes touching that
	// property are dropped op-by-op via the unknown-property rule.
	RemoveProperty(ctx context.Context, typeId, propId string) error

	// PatchProperty applies a generic per-path patch to a property
	// definition — the write half of #1 (rename) and #3/#5 (option
	// CRUD, colors, order). Set assigns values at dotted paths; Unset
	// removes them. Both merge per-path through the record's CRDT, so
	// concurrent edits to different paths converge.
	//
	// Mutable paths: name, description, x-key, x-kind, format.ui,
	// format.filter, format.options.<key>.{name,color,pos,meta.<k>},
	// format.meta.<k>. Pinned paths (key, kind, scope, items,
	// properties, the whole `format` object, format.type) are rejected
	// — define a new property to change them. Format-leaf paths require
	// a property that declared a format at creation.
	PatchProperty(ctx context.Context, typeId, propId string, patch PropertyPatch) error
}

// TypeInfo is a point-in-time snapshot of a type object.
type TypeInfo struct {
	Id          string
	Name        string
	Description string
	IconCID     string
	// XKey is the optional caller-side "programmatic" name set at
	// Create. Empty if unset.
	XKey string
	// BuiltIn marks `any` / `type` (immutable, always-present). User
	// types return false.
	BuiltIn bool
}

// TypeCreateParams is the input to TypesAPI.Create.
type TypeCreateParams struct {
	Name        string
	Description string
	IconCID     string

	// XKey is an optional stable, caller-side "programmatic" name for
	// the type (e.g. for generated client code mapping). Like Name and
	// Description it's client-set display metadata — not unique, not
	// enforced by the SDK.
	XKey string
}

// PropertyDef is the live shape of one property definition. All
// fields reflect the current (post-merge) state; renames and other
// CRDT-mutable changes are visible via List / Properties refresh.
type PropertyDef struct {
	Id          string // base58(xxh3-64(changeId)), immutable
	Name        string // display label, CRDT-mutable
	Description string // CRDT-mutable

	// XKey is an optional stable caller-side key (e.g. for generated
	// client code mapping). Not unique, not enforced — metadata only.
	XKey string

	// XKind is a free-form classification hint. Opaque to the SDK.
	XKind string

	// Meta is an opaque consumer-controlled flag map. The SDK stores
	// and returns it verbatim and never interprets it — e.g. the `any`
	// server's search indexer reads meta["index"] = "<scope>" to mark
	// a property as full-text/vector indexable. Nil when unset.
	// Deliberately not schema-bearing: CRDT-mutable via PatchProperty
	// (meta.<k> paths).
	Meta map[string]string

	Kind       PropertyKind  // first-write-wins; immutable
	Items      *PropertyDef  // for arrays
	Properties []PropertyDef // for objects
	Required   []string      // for objects — CRDT-mutable additions

	// Scope is the property's write/sync class (synced / account /
	// local; derived on built-ins). First-write-wins like Kind —
	// immutable for the propId's life. Definitions written before
	// scopes existed read back as ScopeSynced.
	Scope Scope

	// Format is the property's value-format annotation (links / date /
	// datetime / …). Nil for definitions that never declared one —
	// including everything written before formats existed. Format.Type
	// is pinned like Kind; UI and Filter are CRDT-mutable. A definition
	// carrying a format type this SDK version doesn't know reads back
	// as nil (read tolerance).
	Format *PropertyFormat
}

// PropertyFormat is the live format annotation on a PropertyDef.
//
// The SDK validates only the structure (Type is a known enum because it
// constrains Kind; UI and Filter are strings) — it never interprets UI
// values or Filter contents. Semantic validation (UI enums, filter
// syntax, value shapes) is a consumer concern, e.g. the `any` server.
type PropertyFormat struct {
	// Type declares the value convention — see FormatType. Pinned by
	// the first write, like Kind.
	Type FormatType
	// UI is a presentation hint (e.g. "select", "multiselect", "link",
	// "links"). Opaque to the SDK, like XKind. CRDT-mutable.
	UI string
	// Filter is a mongo-style condition over candidate objects, stored
	// as its JSON text. Opaque to the SDK. Empty means "no filter".
	// CRDT-mutable — concurrent edits replace each other as a unit.
	Filter string
	// Options is the enumerated option set for select / multiselect
	// formats, keyed by the option's stable key — which IS the value a
	// select/multiselect value stores. The key is immutable (changing it
	// orphans existing values); name/color/pos/meta are CRDT-mutable per
	// path. Nil when the format declares no options. See PropertyOption.
	Options map[string]PropertyOption
	// Meta is an opaque, CRDT-mutable format-level config bag (e.g. a
	// date display pattern, number precision). String leaves only,
	// stored verbatim. Distinct from PropertyDef.Meta (property-level
	// consumer flags). Nil when unset.
	Meta map[string]string
}

// PropertyOption is one enumerated choice on a select / multiselect
// format. Its map key in Format.Options is the stored value; the fields
// below are the CRDT-mutable display slice.
type PropertyOption struct {
	// Name is the display label. CRDT-mutable — renaming touches only
	// this leaf, never the values that reference the option key.
	Name string
	// Color is an opaque presentation string (palette name or hex).
	// CRDT-mutable.
	Color string
	// Pos is a lexid ordering key for display order. CRDT-mutable;
	// reorder is a single-leaf write.
	Pos string
	// Meta is an opaque per-option string bag (icon, description, …).
	// CRDT-mutable. Nil when unset.
	Meta map[string]string
}

// PropertyFormatDraft is the format input on a PropertyDraft.
type PropertyFormatDraft struct {
	Type FormatType
	UI   string
	// Filter accepts the JSON text of a condition (stored verbatim) or
	// any JSON-marshalable value (map/struct), which is serialized to
	// its JSON text. The SDK does not parse or validate the condition.
	Filter any
	// Options optionally declares select / multiselect choices at
	// creation. Options can also be added later via PatchProperty
	// (format.options.<key>.* paths). Nil for non-enumerated formats.
	Options map[string]PropertyOption
	// Meta optionally declares format-level config at creation. Nil when
	// unset.
	Meta map[string]string
}

// PropertyDraft is the input to TypesAPI.AddProperty. Kind, Items,
// Properties, and Scope are locked by the first write; the rest remain
// mutable.
type PropertyDraft struct {
	Name        string
	Description string
	XKey        string
	XKind       string
	Meta        map[string]string // opaque consumer flags — see PropertyDef.Meta
	Kind        PropertyKind
	Items       *PropertyDraft
	Properties  []PropertyDraft
	Required    []string

	// Scope is the property's write/sync class. Zero value means
	// ScopeSynced. ScopeDerived is reserved for built-ins and rejected.
	// Pinned by the first write — to change a property's scope, define
	// a new property (which mints a new propId).
	Scope Scope

	// Format optionally declares the value convention. Format.Type
	// constrains Kind (links/tags/multiselect ⇒ array of string;
	// date/datetime/select ⇒ string) and, when Kind is zero, defaults
	// it. Format.Type is pinned by the first write; UI, Filter, Options
	// and Meta stay mutable via PatchProperty. FormatTags is reserved
	// until the space-level tags table lands and is rejected.
	Format *PropertyFormatDraft
}

// PropertyPatch is a generic per-path patch to a property definition,
// the input to PatchProperty.
//
// Set maps a dotted field path to its new value (values are stored
// verbatim; format.* leaves must be strings — the CRDT handler enforces
// this). Unset lists dotted field paths to remove (subtree removals are
// allowed, e.g. "format.options.<key>" to delete a whole option). A path
// present in neither is left unchanged. At least one entry across Set /
// Unset is required.
//
// Pinned paths are rejected before any write (see PatchProperty).
type PropertyPatch struct {
	Set   map[string]any
	Unset []string
}

// PropertyKind mirrors the JSON-Schema-subset types supported on
// property definitions. See docs/types-properties-proposal.md.
//
// The zero value is intentionally unnamed — every property has a
// concrete kind, so a zero PropertyKind is a caller-side error
// (e.g. a PropertyDraft with Kind unset). The SDK validates this on
// the write path.
type PropertyKind uint8

const (
	PropertyKindString PropertyKind = iota + 1
	PropertyKindNumber
	PropertyKindBoolean
	PropertyKindNull
	PropertyKindArray
	PropertyKindObject
)

// FormatType declares a property's value convention beyond its
// structural Kind. Formats are annotations: the SDK checks only that
// the declared format is compatible with the Kind — it never validates
// values against the format (that's a consumer concern, e.g. the `any`
// server checks that a datetime value parses).
//
// Value conventions per format:
//   - FormatLinks:       array of "any://<objectId>" URI strings (see the
//     anyuri package)
//   - FormatDate:        "2006-01-02" date string
//   - FormatDatetime:    RFC 3339 datetime string
//   - FormatTags:        array of tag record ids referencing the space's
//     tag table — reserved, not accepted by AddProperty yet
//   - FormatSelect:      a single option key (string) chosen from the
//     property's Format.Options set
//   - FormatMultiselect: an array of option keys (strings) from
//     Format.Options
//
// FormatSelect / FormatMultiselect carry the enumerated option set in
// Format.Options (key → {name, color, pos, meta}). The SDK stores those
// options but does NOT validate that a value is a member of the set —
// membership, like every other value semantic, is a consumer concern.
//
// The zero value means "no format declared".
type FormatType uint8

const (
	FormatLinks FormatType = iota + 1
	FormatDate
	FormatDatetime
	FormatTags
	FormatSelect
	FormatMultiselect
)

// String returns the on-wire label ("links", "date", "datetime",
// "tags", "select", "multiselect"), or "" for the zero/unknown value.
func (f FormatType) String() string {
	switch f {
	case FormatLinks:
		return "links"
	case FormatDate:
		return "date"
	case FormatDatetime:
		return "datetime"
	case FormatTags:
		return "tags"
	case FormatSelect:
		return "select"
	case FormatMultiselect:
		return "multiselect"
	}
	return ""
}

// ParseFormatType decodes the on-wire format label. Returns false on an
// unknown label.
func ParseFormatType(s string) (FormatType, bool) {
	switch s {
	case "links":
		return FormatLinks, true
	case "date":
		return FormatDate, true
	case "datetime":
		return FormatDatetime, true
	case "tags":
		return FormatTags, true
	case "select":
		return FormatSelect, true
	case "multiselect":
		return FormatMultiselect, true
	}
	return 0, false
}
