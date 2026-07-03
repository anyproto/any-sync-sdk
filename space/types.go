package space

import (
	"context"

	"github.com/anyproto/any-sync-sdk/handler"
)

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

	// UpdatePropertyMeta mutates the CRDT-mutable fields (Name,
	// Description, XKey, XKind, Format.UI, Format.Filter). Type-shape
	// fields (Kind, Items, Properties, Scope, Format.Type) are
	// immutable per first-write-wins on the property record.
	UpdatePropertyMeta(ctx context.Context, typeId, propId string, update PropertyMetaUpdate) error
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
	// Deliberately not schema-bearing: mutable once UpdatePropertyMeta
	// lands.
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
}

// PropertyFormatDraft is the format input on a PropertyDraft.
type PropertyFormatDraft struct {
	Type FormatType
	UI   string
	// Filter accepts the JSON text of a condition (stored verbatim) or
	// any JSON-marshalable value (map/struct), which is serialized to
	// its JSON text. The SDK does not parse or validate the condition.
	Filter any
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
	// constrains Kind (links/tags ⇒ array of string; date/datetime ⇒
	// string) and, when Kind is zero, defaults it. Format.Type is
	// pinned by the first write; UI and Filter stay mutable via
	// UpdatePropertyMeta. FormatTags is reserved until the space-level
	// tags table lands and is rejected.
	Format *PropertyFormatDraft
}

// PropertyMetaUpdate carries optional updates to the CRDT-mutable meta
// fields. Nil pointer means "leave unchanged"; a non-nil pointer to an
// empty string means "unset".
type PropertyMetaUpdate struct {
	Name        *string
	Description *string
	XKey        *string
	XKind       *string

	// FormatUI / FormatFilter update the mutable format leaves. Only
	// valid on a property that declared a format at creation —
	// format.type itself is pinned and cannot be added, changed, or
	// removed here.
	FormatUI     *string
	FormatFilter *string
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
//   - FormatLinks:    array of "any://<objectId>" URI strings (see the
//     anyuri package)
//   - FormatDate:     "2006-01-02" date string
//   - FormatDatetime: RFC 3339 datetime string
//   - FormatTags:     array of tag record ids referencing the space's
//     tag table — reserved, not accepted by AddProperty yet
//
// The zero value means "no format declared".
type FormatType uint8

const (
	FormatLinks FormatType = iota + 1
	FormatDate
	FormatDatetime
	FormatTags
)

// String returns the on-wire label ("links", "date", "datetime",
// "tags"), or "" for the zero/unknown value.
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
	}
	return 0, false
}
