package space

import (
	"context"
	"errors"

	"github.com/anyproto/any-sync-sdk/handler"
)

// ErrPinnedField is returned by PatchProperty / PatchDataset /
// PatchDatasetField when a Set/Unset path targets pinned state (key,
// kind, scope, items, properties; a dataset head's behavioral fields).
// Consumers (e.g. the `any` server) match it to surface a clean 400
// rather than the handler's per-op drop.
var ErrPinnedField = errors.New("space: property field is pinned or immutable")

// ErrInvalidFieldValue is returned by PatchDataset when a MUTABLE
// leaf's value is malformed (e.g. a `search.text` mapping with empty
// or duplicate keys, or an empty spelling where Unset is the clear
// path) — distinct from ErrPinnedField, which reports an immutable
// PATH. Consumers map it to a validation-class client error.
var ErrInvalidFieldValue = errors.New("space: invalid dataset field value")

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
	// definition. Set assigns values at dotted paths; Unset removes
	// them. Both merge per-path through the record's CRDT, so
	// concurrent edits to different paths converge.
	//
	// Mutable paths: name, description, x-key, meta.<k>, x-format and
	// every path under it (any JSON value — the SDK stores the
	// descriptor opaquely; the consumer owns its vocabulary and its
	// leaf-only patch rule). Pinned paths (key, kind, scope, items,
	// properties) are rejected — define a new property to change them.
	PatchProperty(ctx context.Context, typeId, propId string, patch PropertyPatch) error

	// Datasets returns the type's runtime dataset definitions (the
	// compiled view — orphan/invalid records folded out).
	Datasets(ctx context.Context, typeId string) ([]DatasetDef, error)

	// AddDataset defines a new dataset on the type at runtime. The
	// definition syncs like any space data; peers register the dataset
	// (enforced by the SDK's generic schema handler) as it applies.
	// Returns the definition's stable id. Behavioral parts of the
	// declaration (name, id rule, delete gate, field kinds/flags) are
	// pinned — remove and re-add to change them; display parts patch
	// via PatchDataset.
	AddDataset(ctx context.Context, typeId string, draft DatasetDraft) (datasetDefId string, err error)

	// AddDatasetField appends a field to an existing dataset definition
	// (additive evolution). Returns the field definition's id. Additive
	// fields cannot be Required — validation always runs against the
	// current schema, so a required field added later would reject the
	// dataset's own history on fresh devices. Declare required fields
	// at AddDataset.
	AddDatasetField(ctx context.Context, typeId, datasetDefId string, draft DatasetFieldDraft) (fieldDefId string, err error)

	// RemoveDataset drops a dataset definition. Existing record data is
	// NOT cleaned up (the RemoveProperty stance); subsequent writes to
	// the dataset drop once peers apply the removal.
	RemoveDataset(ctx context.Context, typeId, datasetDefId string) error

	// RemoveDatasetField drops one field definition. Existing values
	// stay stored; subsequent writes to the field are rejected as
	// undeclared (non-dynamic datasets).
	RemoveDatasetField(ctx context.Context, typeId, fieldDefId string) error

	// PatchDataset edits a definition's mutable leaves: displayName,
	// description, name (field records' display label), search.title,
	// search.text (a field key string or a non-empty array of unique
	// keys; single-element arrays canonicalize to the bare string on
	// the wire, and clearing the mapping is Unset's job), search.scope.
	// Pinned paths are rejected up-front (ErrPinnedField); malformed
	// values on mutable search leaves return ErrInvalidFieldValue.
	PatchDataset(ctx context.Context, typeId, defId string, patch DatasetDefPatch) error

	// PatchDatasetField edits one field definition's mutable leaves:
	// name, description, x-format and every path under it. The
	// behavioral parts (key, kind, shape, scope, required, mutableBy,
	// stamp) are pinned (ErrPinnedField). Unknown fieldDefId →
	// ErrNotFound.
	PatchDatasetField(ctx context.Context, typeId, fieldDefId string, patch DatasetDefPatch) error
}

// Behavioral dataset-schema vocabulary, aliased from the handler
// package (one vocabulary for compiled-in and runtime declarations).
type (
	Mutability   = handler.Mutability
	Stamp        = handler.Stamp
	IdRule       = handler.IdRule
	DeletePolicy = handler.DeletePolicy
	SearchFields = handler.SearchFields
)

const (
	MutableNever    = handler.MutableNever
	MutableByAuthor = handler.MutableByAuthor
	MutableByAnyone = handler.MutableByAnyone

	StampNone       = handler.StampNone
	StampCreator    = handler.StampCreator
	StampCreateTime = handler.StampCreateTime
	StampModifyTime = handler.StampModifyTime

	IdAuto = handler.IdAuto
	IdUser = handler.IdUser

	DeleteByAnyone = handler.DeleteByAnyone
	DeleteByAuthor = handler.DeleteByAuthor
)

// DatasetDraft is the input to TypesAPI.AddDataset.
type DatasetDraft struct {
	// Name is the dataset's collection name — pinned for the life of
	// the definition. No "_" prefix, dots, slashes or colons; built-in
	// names are reserved.
	Name        string
	DisplayName string
	Description string

	// Dynamic keeps a free-form keyspace next to the declared fields.
	Dynamic bool

	// IdRule / IdPattern / IdMaxLen: record-id production. Zero rule =
	// auto-derived ids; IdUser accepts caller ids (also the upsert
	// idempotency key) constrained by pattern/length.
	IdRule    IdRule
	IdPattern string
	IdMaxLen  int

	// DeleteBy gates record deletes. DeleteByAuthor requires a
	// StampCreator field among Fields.
	DeleteBy DeletePolicy

	// SkipHistory keeps the dataset out of the version-history index.
	SkipHistory bool

	// Search is the optional search-extraction annotation (x-search).
	Search *SearchFields

	// Fields are the initial field definitions.
	Fields []DatasetFieldDraft
}

// DatasetFieldDraft is one field definition — input to AddDataset /
// AddDatasetField.
type DatasetFieldDraft struct {
	// Key is the on-record field name — pinned.
	Key         string
	Name        string
	Description string

	// Kind is the value kind. Required unless Stamp implies one
	// (creator ⇒ string, createTime/modifyTime ⇒ datetime).
	Kind PropertyKind
	// Shape optionally refines array/object values (items/properties).
	Shape *handler.FieldShape

	// Scope: zero = synced. Derived is implied by Stamp and rejected
	// otherwise.
	Scope Scope
	// Required: must be present on create. Incompatible with Stamp.
	Required bool
	// MutableBy: post-create write rule. Zero = write-once.
	MutableBy Mutability
	// Stamp: apply-time derived value (handler-written).
	Stamp Stamp

	// XFormat is the field's opaque descriptor (semantic slug, icon,
	// options, …) — the same bag a property definition carries. Stored
	// verbatim, mutable via PatchDatasetField, surfaced by Datasets()
	// and as the `x-format` keyword in discovery. Nil when unset.
	XFormat map[string]any
}

// DatasetDef is the compiled view of one runtime dataset definition.
type DatasetDef struct {
	Id          string // head record id, immutable
	Name        string
	DisplayName string
	Description string
	Dynamic     bool
	IdRule      IdRule
	IdPattern   string
	IdMaxLen    int
	DeleteBy    DeletePolicy
	SkipHistory bool
	Search      *SearchFields
	Fields      []DatasetFieldDef

	// Invalid marks a definition whose folded declaration fails
	// validation (InvalidReason says why). Invalid definitions never
	// register or accept data but stay listed so they can be repaired
	// (AddDatasetField) or removed.
	Invalid       bool
	InvalidReason string
}

// DatasetFieldDef is the compiled view of one dataset field.
type DatasetFieldDef struct {
	// Id is the field definition record's id — the identity
	// RemoveDatasetField / PatchDatasetField target.
	Id          string
	Key         string
	Name        string
	Description string
	Kind        PropertyKind
	// Shape is the full declared value shape (kind plus items /
	// properties); Kind is its top-level kind.
	Shape     *handler.FieldShape
	Scope     Scope
	Required  bool
	MutableBy Mutability
	Stamp     Stamp
	// XFormat is the opaque descriptor declared on the field, nil when
	// unset. See DatasetFieldDraft.XFormat.
	XFormat map[string]any
}

// DatasetDefPatch is the input to PatchDataset — same per-path model
// as PropertyPatch, over the dataset-def mutable leaves.
type DatasetDefPatch struct {
	Set   map[string]any
	Unset []string
}

// TypeInfo is a point-in-time snapshot of a type object.
type TypeInfo struct {
	Id          string
	Name        string
	Description string
	IconCID     string
	// XKey is the optional caller-side "programmatic" name set at
	// Create, stored at `type.xkey` on the type object. Empty if
	// unset.
	XKey string
	// BuiltIn marks the synthetic types — `any`, `spaceIndex`,
	// `type` and every caller-registered type (immutable,
	// always-present). User types return false.
	BuiltIn bool
}

// TypeCreateParams is the input to TypesAPI.Create.
type TypeCreateParams struct {
	Name        string
	Description string
	IconCID     string

	// XKey is an optional stable, caller-side "programmatic" name for
	// the type (e.g. for generated client code mapping). Client-set,
	// not unique, not enforced by the SDK. Unlike Name/Description it
	// is stored in the meta-type's own namespace (`type.xkey`), so
	// only rows carrying the type marker can hold one.
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

	// XFormat is the property's opaque descriptor — semantic slug,
	// icon, ordering key, option set, relation targets, per-format
	// config — as the consumer wrote it. The SDK stores it verbatim,
	// never interprets it, and lets every path under it mutate
	// (PatchProperty). Nil for definitions that carry none. Decoded
	// from the record with plain Go values: nested objects as
	// map[string]any, arrays as []any, numbers as float64.
	XFormat map[string]any
}

// PropertyDraft is the input to TypesAPI.AddProperty. Kind, Items,
// Properties, and Scope are locked by the first write; the rest remain
// mutable.
type PropertyDraft struct {
	Name        string
	Description string
	XKey        string
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

	// XFormat optionally declares the descriptor at creation, written
	// as one whole object (see PropertyDef.XFormat). Values convert as
	// Op.Value describes; nothing inside is validated — the consumer
	// owns the vocabulary.
	XFormat map[string]any
}

// PropertyPatch is a generic per-path patch to a property definition,
// the input to PatchProperty.
//
// Set maps a dotted field path to its new value (values convert as
// Op.Value describes and are not otherwise validated). Unset lists
// dotted field paths to remove (subtree removals are allowed, e.g.
// "x-format.options.<key>" to delete a whole option). A path present in
// neither is left unchanged. At least one entry across Set / Unset is
// required.
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
	// PropertyKindDatetime is an instant, stored as any-store's native
	// TypeDateTime (unix millis, orderable, index-keyable, `{"$date": …}`
	// in JSON). The kind the `date` / `datetime` formats imply.
	PropertyKindDatetime
)
