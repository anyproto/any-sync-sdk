package space

import (
	"context"
	"errors"

	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/schema"
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

// ErrTypeRegistered is returned by the runtime mutators (AddProperty /
// RemoveProperty / PatchProperty / Patch and the part methods) when the
// target type or collection is a registered built-in whose declarations
// are static and cannot be mutated at runtime. A client error → 4xx.
var ErrTypeRegistered = errors.New("space: definition is registered — its declarations are static")

// ErrNotAType is returned by the type-only surface (Parts / AddPart /
// AddDataset / Patch / … on TypesAPI) when the id names a collection
// object; ErrNotACollection by CollectionsAPI.Patch when the id names
// a type object. The shared property-definition methods accept either.
// Client errors → 4xx.
var (
	ErrNotAType       = errors.New("space: the id names a collection, not a type")
	ErrNotACollection = errors.New("space: the id names a type, not a collection")
)

// ErrWrongSlot is returned by the membership writes (SetType /
// AttachCollection / Objects().Create / Derive / Bundles().Ensure) and
// by a raw Modify on the objects row when a known collection id is set
// as `any.type` or a known type id is added to `any.collections`. An
// id this device cannot resolve passes. A client error → 4xx.
var ErrWrongSlot = errors.New("space: a type goes in any.type, a collection in any.collections")

// ErrMetaType is returned by the same writes when `any.type` is set to
// a meta id: `any`, `type`, `collection` or `spaceIndex`. Types().List
// lists them, but none is a type an object carries. Create, Derive and
// Bundles().Ensure refuse before minting anything. A client error →
// 4xx.
var ErrMetaType = errors.New("space: any, type, collection and spaceIndex are not an object's type")

// ErrModuleOwned is returned by the field-level dataset methods
// (AddDatasetField / RemoveDatasetField / PatchDatasetField) when the
// dataset is served by a module: the module owns the schema, so a
// declaration carries no fields. A client error → 4xx.
var ErrModuleOwned = errors.New("space: dataset schema is owned by its module")

// ErrModuleReserved is returned by AddPart / AddDataset and by
// BundlesAPI.Ensure when a dataset draft names a module registered with
// handler.Module.Reserved: only the consumer's own installs (the
// SystemInstall ensure option) may declare it. A client error → 4xx.
var ErrModuleReserved = errors.New("space: module is reserved for the consumer's own installs")

// ErrDatasetNotDeclared is returned by the write surface (Modify /
// ModifyMany / Delete / Upsert) when the target object carries no type
// whose parts declare the dataset — a namespaced collection's one
// owner, or any owner of a module's canonical collection. No type is
// attached on write; the caller attaches one first. A client error →
// 4xx.
var ErrDatasetNotDeclared = errors.New("space: dataset is declared by none of the object's types")

// ErrRecordIdOfAnotherObject is returned by a write to a shared
// dataset whose record id does not belong to the target object: it
// carries another object's prefix, or contains "/" without being one of
// this object's ids. A record id written to a shared dataset is the
// plain id or `<objectId>/<recordId>` for the batch's own object. A
// client error → 4xx.
var ErrRecordIdOfAnotherObject = errors.New("space: record id does not belong to the object")

// ErrDatasetNotShared is returned by a read across objects
// (QueryDataset / AggregateDataset) of a dataset that is not declared
// Shared, or that the space does not hold. A client error → 4xx.
var ErrDatasetNotShared = errors.New("space: dataset is not a shared dataset")

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

// PropertyDefsAPI is the property-definition surface shared by types
// and collections: one implementation, addressed by the owning
// definition's id (a type id or a collection id) through either
// Space.Types() or Space.Collections(). Definitions live on the owner
// object; values live on the objects row under `<ownerId>.<propId>`.
type PropertyDefsAPI interface {
	// Properties returns the current property definitions of the
	// owner. Built-ins (`any`, `type`, `collection`, `spaceIndex`) and
	// registered definitions answer their static tables.
	Properties(ctx context.Context, ownerId string) ([]PropertyDef, error)

	// AddProperty mints a new property on the owner. The returned
	// propId is base58(xxh3-64(changeId)) — immutable for the life of
	// the property. Registered owners refuse (ErrTypeRegistered).
	AddProperty(ctx context.Context, ownerId string, draft PropertyDraft) (propId string, err error)

	// RemoveProperty drops a property definition. Existing value
	// records are NOT cleaned up; subsequent writes touching that
	// property are dropped op-by-op via the unknown-property rule.
	RemoveProperty(ctx context.Context, ownerId, propId string) error

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
	PatchProperty(ctx context.Context, ownerId, propId string, patch PropertyPatch) error
}

// TypesAPI manages type objects inside a space.
//
// A type is what an object IS: property definitions, parts (the
// datasets it renders) and a layout. An object has exactly one type,
// named in `any.type`; a type object itself carries the reserved
// marker `__type__` there (it has no type of its own). Property
// values for an object live on the per-space objects row under
// `<typeId>.<propId>` (see PropertiesAPI). What an object is filed
// UNDER is a collection — see CollectionsAPI.
//
// Built-in types (`any`, `type`, `collection`, `spaceIndex`) are
// derived on demand — callers don't create them. List returns them,
// and every registered type, alongside user-defined types.
type TypesAPI interface {
	PropertyDefsAPI

	List(ctx context.Context) ([]TypeInfo, error)
	Get(ctx context.Context, typeId string) (TypeInfo, error)

	// Create a new user-defined type in this space. Returns the new
	// type object's id.
	Create(ctx context.Context, params TypeCreateParams) (typeId string, err error)

	// Delete a type. Objects naming it in `any.type` keep the
	// reference (orphan), per docs §"read tolerance".
	Delete(ctx context.Context, typeId string) error

	// Patch edits a user type's display and rendering metadata: name,
	// description, icon, layout, hidden, meta. Absent fields keep
	// their value. Registered built-ins refuse (ErrTypeRegistered); a
	// collection id refuses (ErrNotAType).
	Patch(ctx context.Context, typeId string, patch TypePatch) error

	// Parts returns the type's parts with their datasets (the compiled
	// view — duplicate keys folded, orphan records dropped, invalid
	// datasets flagged).
	Parts(ctx context.Context, typeId string) ([]PartDef, error)

	// AddPart declares a part with its initial datasets in one change.
	// Returns the part's stable id. The key is pinned; the display
	// slice patches via PatchPart; datasets evolve via AddDataset /
	// RemoveDataset on the part.
	AddPart(ctx context.Context, typeId string, draft PartDraft) (partId string, err error)

	// PatchPart edits a part's mutable leaves: name, icon, pos, hidden,
	// ui (written whole), uses. The key is pinned (ErrPinnedField).
	PatchPart(ctx context.Context, typeId, partId string, patch DatasetDefPatch) error

	// RemovePart tombstones a part and every dataset declared under it.
	// Record data is NOT cleaned up (the RemoveProperty stance);
	// subsequent writes to the datasets drop once peers apply the
	// removal, and removing a module's canonical dataset only withdraws
	// this type's ownership of that collection.
	RemovePart(ctx context.Context, typeId, partId string) error

	// Datasets returns the type's dataset definitions across every part
	// (the compiled view — orphan/invalid records folded out).
	Datasets(ctx context.Context, typeId string) ([]DatasetDef, error)

	// AddDataset declares a dataset on an existing part. The definition
	// syncs like any space data; peers register the dataset (the
	// generic schema handler for records, the module's handler
	// otherwise) as it applies. Returns the definition's stable id.
	// Behavioral parts of the declaration (key, module, id rule,
	// delete gate, field kinds/flags) are pinned — remove and
	// re-add to change them; display parts patch via PatchDataset.
	AddDataset(ctx context.Context, typeId, partId string, draft DatasetDraft) (datasetDefId string, err error)

	// AddDatasetField appends a field to an existing records dataset
	// (additive evolution). Returns the field definition's id. Rows may
	// predate the field, so it cannot be Required, and on a Dynamic
	// dataset it is synced, unstamped and mutable by anyone, its shape
	// enforced on this device's writes only (DatasetFieldDef.Additive);
	// a draft asking for more there is refused. Declare such fields at
	// AddDataset. A module-served dataset refuses (ErrModuleOwned).
	AddDatasetField(ctx context.Context, typeId, datasetDefId string, draft DatasetFieldDraft) (fieldDefId string, err error)

	// RemoveDataset drops a dataset definition. Existing record data is
	// NOT cleaned up (the RemoveProperty stance); subsequent writes to
	// the dataset drop once peers apply the removal.
	RemoveDataset(ctx context.Context, typeId, datasetDefId string) error

	// RemoveDatasetField drops one field definition. Existing values
	// stay stored; subsequent writes to the field are rejected as
	// undeclared (non-dynamic datasets).
	RemoveDatasetField(ctx context.Context, typeId, fieldDefId string) error

	// AddDatasetIndex declares a secondary index on an existing records
	// dataset; one may be added to a dataset that holds records.
	// Returns the index definition's id. The definition syncs, so every
	// device keeps the same set. A per-object dataset builds the index
	// when an object's collection is next opened; a shared dataset
	// builds it once per space in the background — every write to the
	// account's data waits for that build. SubscribeIndexBuilds reports
	// a build; an index of a shape the collection already holds builds
	// nothing and reports nothing. A module-served dataset refuses
	// (ErrModuleOwned).
	AddDatasetIndex(ctx context.Context, typeId, datasetDefId string, draft IndexDraft) (indexDefId string, err error)

	// RemoveDatasetIndex drops one index definition, with every
	// concurrent declaration of its key. A shared dataset's collection
	// loses the index once its device applies the removal, a per-object
	// one at its next open; an index another definition of the same
	// shape still names stays.
	RemoveDatasetIndex(ctx context.Context, typeId, indexDefId string) error

	// SubscribeIndexBuilds registers cb for the builds of shared
	// datasets' declared indexes in this space. A build in flight is
	// reported to cb at once as Started. cb runs on the build worker —
	// keep it small or hand off, and do not subscribe from it. The
	// returned cancel is idempotent.
	SubscribeIndexBuilds(cb func(IndexBuild)) (cancel func())

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

// TypePatch is the input to TypesAPI.Patch. Nil pointers keep the
// current value; an empty string clears a text field. Layout replaces
// the whole layout object when non-nil; ClearLayout removes it.
type TypePatch struct {
	Name        *string
	Description *string
	IconCID     *string
	Layout      map[string]any
	ClearLayout bool
	Hidden      *bool
	// Meta patches the flag bag per key: a scalar value sets the key,
	// a nil value unsets it. Keys not named are untouched, so writers
	// on different devices touching different keys merge.
	Meta map[string]any
}

// PartDraft is the input to TypesAPI.AddPart: the part's key and
// display slice plus its initial datasets.
type PartDraft struct {
	// Key is the part's slug, unique within the type — pinned.
	Key string
	// Name / Icon / Pos are the display slice; clients sort parts by
	// Pos. Hidden parts are not shown by default but stay revealable.
	Name   string
	Icon   string
	Pos    string
	Hidden bool
	// UI is the widget descriptor — a slug plus an opaque config in the
	// x-format shape ({type, config}). Written whole; nil = the first
	// dataset's module default.
	UI map[string]any
	// Uses names other datasets OF THIS TYPE the part renders without
	// owning them (keys).
	Uses []string
	// Datasets are the part's initial dataset declarations.
	Datasets []DatasetDraft
}

// PartDef is the compiled view of one part.
type PartDef struct {
	Id       string // part record id, immutable
	Key      string
	Name     string
	Icon     string
	Pos      string
	Hidden   bool
	UI       map[string]any
	Uses     []string
	Datasets []DatasetDef
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

// RecordsModule is the built-in generic module: a schema-enforced
// dataset with no canonical collection, always namespaced.
const RecordsModule = "records"

// DatasetDraft is the input to TypesAPI.AddDataset (and PartDraft's
// Datasets).
type DatasetDraft struct {
	// Key is the dataset's slug inside its type — pinned — and decides
	// the collection. A key equal to the module's canonical collection
	// name is that collection, the one every type declaring it
	// addresses, so retyping an object keeps its records. Any other
	// key is the collection `<typeId>_<key>`, this type's own. Empty
	// defaults to the module's canonical name; a module without one,
	// records among them, requires a key.
	Key string
	// Module is the serving module — "records" (the default when
	// empty), or a registered module such as "editor" / "chat".
	Module string
	// Shared stores the dataset's records from every object in one
	// collection per space, so they are read across objects as well as
	// per object. A record keeps belonging to the object it was
	// written on, and its id on reads is `<objectId>/<recordId>`.
	// Records datasets only; pinned.
	Shared bool

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

	// Indexes are the initial declared indexes. Records datasets only.
	Indexes []IndexDraft
	// Fields are the initial field definitions. Records datasets only —
	// a module owns its schema and refuses fields.
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
	Id string // head record id, immutable
	// Key is the slug inside the type; Collection the name reads and
	// writes address (the module's canonical collection when the key
	// names it, `<typeId>_<key>` otherwise) — server-computed, never
	// client-set.
	Key        string
	Collection string
	Module     string
	// Shared: the records of every object live in one collection per
	// space (DatasetDraft.Shared).
	Shared bool
	// PartId is the owning part's id.
	PartId      string
	DisplayName string
	Description string
	Dynamic     bool
	IdRule      IdRule
	IdPattern   string
	IdMaxLen    int
	DeleteBy    DeletePolicy
	SkipHistory bool
	Search      *SearchFields
	// Fields are the declared fields in creation order; fields declared
	// in one change come in key order.
	Fields []DatasetFieldDef
	// Indexes are the declared indexes in creation order, invalid ones
	// included.
	Indexes []IndexDef

	// Invalid marks a definition whose folded declaration fails
	// validation (InvalidReason says why). Invalid definitions never
	// register or accept data but stay listed so they can be repaired
	// (AddDatasetField) or removed.
	Invalid       bool
	InvalidReason string
}

// Index limits and the protocol paths an index may name next to
// declared fields.
const (
	// MaxIndexFields bounds the paths of one index.
	MaxIndexFields = schema.MaxIndexFields
	// MaxDatasetIndexes bounds the indexes of one dataset.
	MaxDatasetIndexes = schema.MaxDatasetIndexes
	// IndexPathCreated orders records by creation within one object.
	IndexPathCreated = schema.IndexPathCreated
	// IndexPathObject is the object of a shared dataset's record.
	IndexPathObject = schema.IndexPathObject
)

// IndexDraft declares a secondary index of a records dataset — input to
// AddDataset / AddDatasetIndex. A filter or sort on a leading run of
// its fields is a range read. Every part is pinned: change an index by
// removing it and adding another.
type IndexDraft struct {
	// Key is the index's slug, unique within the dataset.
	Key string
	// Fields are the indexed paths in order, at most MaxIndexFields; a
	// "-" prefix keeps that path descending. Each names a declared
	// field of kind string, number, boolean or datetime, or
	// IndexPathCreated; a shared dataset also takes IndexPathObject.
	// An indexed field's key is letters, digits and "_", starts with a
	// letter and is at most 48 bytes.
	Fields []string
	// Sparse leaves a record out of the index unless it carries every
	// indexed field.
	Sparse bool
}

// IndexDef is the compiled view of one declared index.
type IndexDef struct {
	// Id is the definition record's id — what RemoveDatasetIndex takes.
	Id     string
	Key    string
	Fields []string
	Sparse bool
	// Invalid marks an index no collection builds (InvalidReason says
	// why): a field it names is not a declared scalar field, or the
	// dataset already holds MaxDatasetIndexes. It stays listed so it
	// can be removed.
	Invalid       bool
	InvalidReason string
}

// IndexBuildPhase is the state an IndexBuild reports.
type IndexBuildPhase int

const (
	IndexBuildStarted IndexBuildPhase = iota
	IndexBuildDone
	IndexBuildFailed
)

// IndexBuild reports the build of a shared dataset's missing declared
// indexes on this device: Started before the collection is scanned,
// then Done or Failed. Every write waits while a build runs.
type IndexBuild struct {
	// Dataset is the shared dataset's storage collection name.
	Dataset string
	Phase   IndexBuildPhase
	// Err is set on IndexBuildFailed.
	Err error
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
	// Additive marks a field not declared with its dataset
	// (AddDatasetField, or only some of, or differing, concurrent
	// declarations). Never Required; on a Dynamic dataset synced,
	// unstamped and mutable by anyone, its shape enforced on this
	// device's writes only. See handler.Field.Additive.
	Additive bool
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
	// Layout is how the type's header and parts compose — a slug plus
	// config in the x-format shape ({type, config}), opaque to the
	// SDK. Lives in the meta-type's namespace (`type.layout`).
	Layout map[string]any
	// Hidden keeps the type out of default listings and pickers: a
	// client shows it only on request. A bundle root asks for it
	// (EnsureBundleRequest.Hidden) when it exists to host its bundle's
	// records rather than to be attached elsewhere; a registered type
	// declares it (handler.Type.Hidden). `type.hidden`.
	Hidden bool
	// Meta is the open bag of consumer flags on the type — one scalar
	// (string, bool, number) per single-level key, written per key so
	// concurrent writers merge. Opaque to the SDK; consumers read the
	// keys they own (an indexer's `index`, a client's tags).
	// `type.meta`.
	Meta map[string]any
	// BuiltIn marks the synthetic types — `any`, `spaceIndex`, `type`,
	// `collection` and every caller-registered type (immutable,
	// always-present). User types return false.
	BuiltIn bool
}

// CollectionInfo is a point-in-time snapshot of a collection object —
// the type-side subset that describes a definition rather than how
// its objects render: no layout, no parts. `collection.xkey` /
// `hidden` / `meta` hold XKey / Hidden / Meta.
type CollectionInfo struct {
	Id          string
	Name        string
	Description string
	IconCID     string
	XKey        string
	Hidden      bool
	Meta        map[string]any
	// BuiltIn marks the synthetic `collection` meta-type and every
	// caller-registered collection. User collections return false.
	BuiltIn bool
}

// CollectionCreateParams is the input to CollectionsAPI.Create.
type CollectionCreateParams struct {
	Name        string
	Description string
	IconCID     string
	XKey        string
	Hidden      bool
	Meta        map[string]any
}

// CollectionPatch is the input to CollectionsAPI.Patch. Nil pointers
// keep the current value; an empty string clears a text field; Meta
// patches per key, a nil value unsets.
type CollectionPatch struct {
	Name        *string
	Description *string
	IconCID     *string
	Hidden      *bool
	Meta        map[string]any
}

// CollectionsAPI manages collection objects inside a space.
//
// A collection is what an object is filed UNDER: property definitions
// and nothing else — no parts, no layout. An object belongs to any
// number of collections, listed in `any.collections`; a collection
// object itself carries the reserved marker `__collection__` in
// `any.type`. Values for an object live on the objects row under
// `<collectionId>.<propId>`, exactly like a type's. A query on
// `any.collections` returns members only — a collection object never
// lists its own id.
//
// The property-definition methods are the same surface TypesAPI
// exposes (PropertyDefsAPI) and accept a type id as well.
type CollectionsAPI interface {
	PropertyDefsAPI

	// List returns the meta `collection` built-in, every registered
	// collection and every user collection of this space. Hidden ones
	// are included; the consumer filters.
	List(ctx context.Context) ([]CollectionInfo, error)
	// Get returns one collection; ErrNotACollection for a type id,
	// ErrNotFound for anything else.
	Get(ctx context.Context, collectionId string) (CollectionInfo, error)

	// Create a new user-defined collection. Returns the new object's
	// id.
	Create(ctx context.Context, params CollectionCreateParams) (collectionId string, err error)

	// Delete a collection. Objects listing it keep the reference
	// (orphan), per docs §"read tolerance".
	Delete(ctx context.Context, collectionId string) error

	// Patch edits a user collection's display and listing metadata.
	// Registered built-ins refuse (ErrTypeRegistered); a type id
	// refuses (ErrNotACollection).
	Patch(ctx context.Context, collectionId string, patch CollectionPatch) error
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

	// Layout seeds the rendering metadata — see TypeInfo. Mutable
	// through Patch.
	Layout map[string]any

	// Hidden and Meta seed the listing flag and the consumer flag bag —
	// see TypeInfo. Both mutable through Patch.
	Hidden bool
	Meta   map[string]any
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
	// (PatchProperty); a whole-bag write must be an object. Nil for
	// definitions that carry none. Decoded from the record with plain
	// Go values: nested objects as map[string]any, arrays as []any,
	// numbers as float64, instants as time.Time, binaries as []byte,
	// object ids as hex strings, float vectors as []float64. Every
	// view is a fresh copy — the caller's to mutate.
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
