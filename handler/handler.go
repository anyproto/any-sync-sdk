// Package handler exposes the CRDT handler interface so callers can
// declare additional types whose instances carry custom datasets.
//
// Handlers are owned by types — there is no standalone handler
// registration. The built-in catalog already follows this rule:
//
//   - the `any` type owns the `objects` dataset (system properties)
//   - the `type` meta-type owns the `properties` (definitions) and
//     `shortIds` datasets
//
// External callers extend the catalog by passing a slice of Type
// values via config.Config.Types. Each Type binds a typeId to one or
// more dataset Registrations; the SDK wires every type's handlers
// onto every per-object Controller alongside the built-ins.
//
// Reserved dataset names ("objects", "properties", "shortIds") cannot
// be reused — sdk.Open returns an error if an external Registration
// collides with a built-in.
package handler

import (
	"errors"

	anystore "github.com/anyproto/any-store/v2"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// Handler owns one dataset. Lifecycle hooks fire inside any-store's
// Modify callback so handlers can read pre-state from ctx.Before and
// emit derived or sibling writes via sink without an extra DB
// round-trip.
//
// Per-op error returns from BeforeModify drop just the offending op;
// other ops in the same RecordChange still apply. BeforeCreate /
// BeforeDelete are per-record, and an error there drops the whole
// RecordChange.
type Handler = crdt.Handler

// ChangeCtx is the per-callback context handed to a Handler. Carries
// the originating Change (read-only metadata: VersionId, ChangeId,
// Timestamp, Creator) and the record's pre-op state. Pointer-passed;
// do not retain past the callback.
type ChangeCtx = crdt.ChangeCtx

// Sink collects same-record derived ops and cross-dataset sibling
// writes emitted during a single RecordChange's Modify callback.
type Sink = crdt.Sink

// Change is the apply-time CRDT change envelope. Read-only inside a
// handler callback.
type Change = crdt.Change

// RecordChange groups ops applied to one record id within a Change.
type RecordChange = crdt.RecordChange

// Op is one mongo-style apply-time modifier with a parsed path.
type Op = crdt.Op

// OpType is the modifier kind.
type OpType = crdt.OpType

const (
	OpSet      = crdt.OpSet
	OpUnset    = crdt.OpUnset
	OpAddToSet = crdt.OpAddToSet
	OpPull     = crdt.OpPull
	OpInc      = crdt.OpInc
	OpIncGated = crdt.OpIncGated
	OpDelete   = crdt.OpDelete
)

// DefaultHandler is a no-op Handler embeddable as a base for handlers
// that only need to override a subset of lifecycle hooks. Dataset name,
// version, and indexes live on the Dataset registration, not here.
type DefaultHandler = crdt.DefaultHandler

// ErrValidation is the sentinel wrapped by handler returns when an
// op is rejected. Programmatic discrimination uses errors.Is.
var ErrValidation = crdt.ErrValidation

// Per-reason property-validation sentinels. A rejected property write
// satisfies errors.Is(err, ErrValidation) and, additionally, exactly one
// of these — letting callers classify the specific cause with errors.Is
// instead of matching on the message text. Returned by the write-time
// pre-flight and recorded for apply-time per-op drops.
var (
	ErrValidationInvalidPath        = properties.ErrInvalidPath
	ErrValidationTypeNotImplemented = properties.ErrTypeNotImplemented
	ErrValidationTypeUnknown        = properties.ErrTypeUnknown
	ErrValidationUnknownProperty    = properties.ErrUnknownProperty
	ErrValidationKindMismatch       = properties.ErrKindMismatch
	// ErrValidationReservedCarrier — the write attaches a user type
	// declaring a reserved module (Module.Reserved) to a row other
	// than the type's own root. The consumer's install root is the
	// only carrier; a client cannot mint another instance of a
	// reserved module by attaching the type.
	ErrValidationReservedCarrier = properties.ErrReservedCarrier
)

// ValidationReason is the machine-readable cause of a property-write
// rejection. Returned by ClassifyValidation so callers can switch on a
// single typed value instead of chaining errors.Is against each
// sentinel. The string values are stable and match the discriminant
// carried on the wire-agnostic rejection.
type ValidationReason string

const (
	ReasonInvalidPath        ValidationReason = properties.ReasonInvalidPath
	ReasonTypeNotImplemented ValidationReason = properties.ReasonTypeNotImplemented
	ReasonTypeUnknown        ValidationReason = properties.ReasonTypeUnknown
	ReasonUnknownProperty    ValidationReason = properties.ReasonUnknownProperty
	ReasonKindMismatch       ValidationReason = properties.ReasonKindMismatch
	ReasonReservedCarrier    ValidationReason = properties.ReasonReservedCarrier
)

// ClassifyValidation maps a property-validation rejection to its cause
// in one call. It returns (reason, true) for any error produced by the
// property write-time pre-flight or apply-time per-op validator
// (anywhere in the error's Unwrap chain), and ("", false) for anything
// else — including nil. Classification is structural (errors.As on the
// rejection value), never a match on the message text.
//
//	if r, ok := handler.ClassifyValidation(err); ok {
//	    switch r {
//	    case handler.ReasonKindMismatch: ...
//	    case handler.ReasonUnknownProperty: ...
//	    }
//	}
//
// errors.Is(err, handler.ErrValidation) remains the umbrella check, and
// the per-reason ErrValidation* sentinels still work with errors.Is.
func ClassifyValidation(err error) (ValidationReason, bool) {
	var ve *properties.ValidationError
	if errors.As(err, &ve) {
		return ValidationReason(ve.Reason), true
	}
	return "", false
}

// ErrUnknownDataset signals a change targeting a dataset with no
// registered handler.
var ErrUnknownDataset = crdt.ErrUnknownDataset

// Dataset is a self-contained declaration of one dataset a type owns:
// its name, the on-wire DataVersion peers gate against, the Handler
// implementing its apply-time behavior, and any indexes to ensure on
// its collection. A type may own several (see Type.Datasets); names
// must be unique across the whole catalog.
type Dataset struct {
	// Name is the dataset's collection name. Required, unique across
	// the catalog; reserved names ("objects", "properties", "shortIds")
	// are rejected.
	Name string

	// DataVersion is stamped on every change emitted on this dataset and
	// is what peers gate against. Required (non-empty). Convention:
	// "<name>-v<n>" — but any opaque string unique to a (handler logic,
	// dataset) pair works. Bump the suffix when changing validation in a
	// way that must reject older writers.
	DataVersion string

	// HandlerVersion is the LOCAL version of this dataset's handler
	// logic. Bump it when a change to that logic makes rows already
	// materialized on disk wrong — a derived field that now holds a
	// different shape, a stamp computed differently. The next load of
	// each object wipes its materialized rows and replays its tree
	// through the current handlers. Zero means 1.
	//
	// This is not DataVersion. DataVersion gates PEERS: bumping it parks
	// this dataset's changes on every peer still running older code.
	// HandlerVersion never leaves the device and gates nothing — it only
	// decides whether local state has to be rebuilt.
	HandlerVersion int

	// Handler implements the dataset's lifecycle (Init / Before*).
	// Optional when Schema declares fields: a nil Handler gets the SDK's
	// generic schema handler, which enforces the declaration (required,
	// mutability, stamps, id rules, delete gates) with no bespoke code.
	// Bespoke handlers remain for cross-field rules.
	Handler Handler

	// Indexes are ensured on the dataset's collection the first time it
	// is opened. Optional. Idempotent across restarts.
	Indexes []anystore.IndexInfo

	// Schema declares this dataset's fields and their classes (synced /
	// derived / local) — the source of truth the apply path enforces and
	// consumers discover via Space.Datasets() / Service.Datasets().
	//
	// Optional for backward compatibility: a zero Schema (no Fields, not
	// Dynamic) is treated as Dynamic — a free-form synced keyspace, the
	// pre-schema behavior. Declare Fields to get apply-time enforcement
	// (undeclared fields rejected, derived fields handler-only) and a
	// meaningful discovery document; set Dynamic to keep a free-form
	// keyspace explicit.
	Schema Schema

	// ReadTracking opts the dataset into read/unread tracking: the
	// Classify callback tags each applied change, the SDK maintains
	// the per-object unread set / frontier / counters, and marking is
	// forward-only (Space read-state API). Optional; nil = untracked.
	// Fields named in RecordFlags must be declared local-scope in
	// Schema. See docs/read-tracking-proposal.md.
	ReadTracking *ReadTracking

	// SkipHistory keeps this dataset out of the version-history index:
	// no index rows are written and the dataset is invisible in
	// Space.History() listings. For chatty machine-written datasets
	// (presence-like state) whose permanent index would leak disk for
	// history nobody asks for. DAG changes still retain everything —
	// flipping the flag later just requires an index backfill. See
	// docs/version-history-proposal.md §4.4.
	SkipHistory bool

	// DisableFilteredReplay opts the dataset out of the record-scope
	// history fast path (History().RecordAt), forcing the full-object
	// slow path. Set it when the dataset's Handler reads OTHER records
	// during apply — the fast path is sound only for record-local
	// hooks. See docs/version-history-proposal.md §4.2.
	DisableFilteredReplay bool
}

// Read-tracking registration types, re-exported from the CRDT layer.
type (
	ReadTracking       = crdt.ReadTracking
	ReadClassification = crdt.ReadClassification
	ReadClassifier     = crdt.ReadClassifier
	ReadSeedMode       = crdt.ReadSeedMode
)

const (
	// ReadSeedAtFirstSight marks everything present at the object's
	// first tracked load as read; only later changes count as unread.
	ReadSeedAtFirstSight = crdt.ReadSeedAtFirstSight
	// ReadSeedAllUnread starts with the whole tracked history unread.
	ReadSeedAllUnread = crdt.ReadSeedAllUnread
)

// Schema is a dataset's field-schema declaration: the set of declared
// Fields plus whether the keyspace is Dynamic (free-form keys allowed,
// defaulting to synced). Attach it to Dataset.Schema. Re-exported from
// the SDK's internal schema layer so callers declare schemas without a
// separate import.
type Schema = schema.Dataset

// Field is one declared dataset field — Id and optional display Name,
// the value Shape (nil = unconstrained), and the field's Scope class.
type Field = schema.Field

// FieldShape is a recursive JSON-Schema-subset value shape: a Kind plus
// optional Items (arrays) / Properties (objects). Build leaf shapes with
// Leaf; nil means an unconstrained value.
type FieldShape = schema.Schema

// Scope is the unified write/sync class shared by dataset fields and
// property definitions: how a value is written, which version domain
// stamps it, and how far it syncs. Use the Scope* constants.
type Scope = schema.Scope

const (
	// ScopeSynced: user/DAG-written, change-versioned, synced to
	// everyone with space access (the default for undeclared dynamic
	// fields and for property definitions that don't declare a scope).
	ScopeSynced = schema.ScopeSynced
	// ScopeDerived: handler-computed from the change, read-only to
	// writers, converges across peers (e.g. creator / createdAt).
	ScopeDerived = schema.ScopeDerived
	// ScopeLocal: device-local, never synced (e.g. a per-device status).
	ScopeLocal = schema.ScopeLocal
	// ScopeAccount: synced across the same account's devices only, via
	// the private tech space; invisible to other space members (e.g. a
	// per-account read/unread flag).
	ScopeAccount = schema.ScopeAccount
)

// ParseScope parses a scope's wire label ("synced" / "derived" /
// "local" / "account") — the inverse of Scope.String. Returns
// (0, false) on an unknown label. Re-exported so HTTP layers share
// the schema.Scope label vocabulary.
func ParseScope(label string) (Scope, bool) { return schema.ParseScope(label) }

// Behavioral schema vocabulary, re-exported from the SDK's schema layer.
// A dataset whose declaration uses these and leaves Dataset.Handler nil
// gets the SDK's generic schema handler: required-on-create, write-once /
// author-gated mutability, apply-time stamps, id rules, and delete gates
// enforced without bespoke handler code.
type (
	// Mutability is a field's post-create write rule (zero = write-once).
	Mutability = schema.Mutability
	// Stamp marks a field derived from the change at apply time.
	Stamp = schema.Stamp
	// IdRule declares how record ids are produced (zero = auto-derived).
	IdRule = schema.IdRule
	// DeletePolicy is the dataset-level record-delete gate.
	DeletePolicy = schema.DeletePolicy
	// SearchFields is the dataset's search-extraction annotation.
	SearchFields = schema.SearchFields
)

const (
	MutableNever    = schema.MutableNever
	MutableByAuthor = schema.MutableByAuthor
	MutableByAnyone = schema.MutableByAnyone

	StampNone       = schema.StampNone
	StampCreator    = schema.StampCreator
	StampCreateTime = schema.StampCreateTime
	StampModifyTime = schema.StampModifyTime

	IdAuto = schema.IdAuto
	IdUser = schema.IdUser

	DeleteByAnyone = schema.DeleteByAnyone
	DeleteByAuthor = schema.DeleteByAuthor
)

// Leaf builds an unconstrained scalar value shape for a PropertyKind —
// convenience for declaring simple Field shapes.
func Leaf(k PropertyKind) *FieldShape { return schema.Leaf(schema.Kind(k)) }

// Type binds a typeId to the dataset handlers it owns. Built-in
// types follow the same shape: `any` owns the `objects` dataset;
// `type` owns `properties` and `shortIds`. Callers extend the
// catalog by supplying additional Types via config.Config.Types.
//
// A Type is a uniform declaration of what it owns: zero or more
// Datasets and/or zero or more Properties. Any combination is valid —
// dataset-only (e.g. an editor body tree), property-only (values in the
// shared `objects` namespace), both, or neither (a pure declaration an
// object names as its `any.type`). Only a non-empty Id is required.
//
// Display metadata (Name / Description / IconCID) is surfaced via
// space.Types().List() and Get() alongside user-created types, so
// callers can iterate the full catalog uniformly.
type Type struct {
	// Id is the type identifier this catalog entry registers under.
	// For built-in well-known types this is the reserved id ("any",
	// "type"); for caller-defined types it should be a stable
	// identifier the caller controls. Must be non-empty.
	Id string

	// Name is the display label surfaced through the public Types
	// API. Optional — when empty, the API returns the Id as the
	// label.
	Name string

	// Description is the long-form display text. Optional.
	Description string

	// IconCID is an optional icon reference (CID into the file
	// service). Opaque to the SDK.
	IconCID string

	// Datasets is the self-contained dataset registrations this type
	// owns (zero or more). Each Dataset.Name must be unique across the
	// whole catalog (built-ins + every external Type); the SDK rejects
	// collisions at sdk.Open.
	Datasets []Dataset

	// Properties declares this type's property definitions for the
	// per-space `objects` namespace keyed by Type.Id. The SDK
	// validates writes to `{typeId}.{propId}` against these: the
	// propId must be declared here and the value's kind must match.
	//
	// Optional. A type that only owns separate datasets (e.g. an
	// editor body tree) and contributes no values to the `objects`
	// record leaves this empty. Built-in `any` / `spaceIndex` carry
	// the equivalent tables internally; external types declare theirs
	// here so the SDK can resolve and enforce their schema.
	Properties []PropertyDecl

	// Parts are the type's display units, declared statically — the
	// shape a user type declares at runtime through
	// space.TypesAPI.AddPart. Each part names one or more datasets:
	// entries of Datasets by Name, or module datasets (a shared
	// canonical collection, or a namespaced `<typeId>_<key>` instance)
	// the SDK instantiates exactly as it does for a runtime declaration
	// — the write gate, discovery ownership and the module namespace on
	// the objects row all follow. With Parts set, every entry of
	// Datasets must be named by exactly one part (sdk.Open rejects the
	// rest). Without Parts, a type with Datasets gets one implicit part
	// per dataset, keyed by the dataset name. Read back through
	// space.TypesAPI.Parts / Datasets; never mutable at runtime.
	Parts []Part

	// Hidden keeps the type out of default listings and pickers
	// (space.TypeInfo.Hidden): a client shows it only on request. For a
	// capability type an object opts into rather than a class a user
	// picks.
	Hidden bool
}

// Collection is a registered collection: a definition objects are
// filed under, with property definitions and nothing else — no
// datasets, no parts, no layout. The compiled-in twin of a collection
// a user creates through space.CollectionsAPI.Create; surfaced by
// Collections().List / Get with BuiltIn set, immutable at runtime.
// Callers register them via config.Config.Collections.
type Collection struct {
	// Id is the collection identifier — a stable id the caller
	// controls. Must be non-empty and distinct from every registered
	// type and reserved id.
	Id string

	// Name / Description / IconCID are the display slice; Name falls
	// back to Id when empty.
	Name        string
	Description string
	IconCID     string

	// Properties declares the collection's property definitions for
	// the per-space `objects` namespace keyed by Id — validated on
	// write exactly like a registered type's.
	Properties []PropertyDecl

	// Hidden keeps the collection out of default listings and pickers
	// (space.CollectionInfo.Hidden).
	Hidden bool
}

// Part is one display unit of a registered type: the display slice a
// client renders plus the datasets the part owns. Mirrors
// space.PartDraft; Key is the part's slug, unique within the type.
type Part struct {
	Key    string
	Name   string
	Icon   string
	Pos    string
	Hidden bool
	// UI is the widget descriptor — {type, config} in the x-format
	// shape, opaque to the SDK.
	UI map[string]any
	// Uses names other datasets of this type (their key) the part
	// renders without owning them.
	Uses []string
	// Datasets are the part's datasets — at least one.
	Datasets []PartDataset
}

// PartDataset names one dataset a static part owns — the static twin
// of space.DatasetDraft, which a runtime part declares. Exactly one of
// the two forms (sdk.Open rejects the rest):
//
//   - Name: one of the owning Type's Datasets, by name. The dataset's
//     collection is its Name; its module reads as `records` when it
//     runs on the generic schema handler (nil Handler), none when
//     bespoke. Shared and Key stay empty.
//   - Module: a dataset of a registered module, declared as a runtime
//     part would — Shared for the module's canonical collection (Key
//     defaults to the canonical name), a namespaced `<typeId>_<Key>`
//     instance otherwise (the module needs a DataVersion for that).
//     The `records` module has no place here: a static records dataset
//     is a Type.Datasets entry with a Schema.
//
// Keys are one namespace per type: a module Key must not equal a
// static dataset's Name.
type PartDataset struct {
	Name   string
	Module string
	Shared bool
	Key    string
}

// ModuleInstance identifies one collection a module serves: the type
// declaring it, the dataset key inside that type, the collection name
// the instance reads and writes, and whether it is the module's shared
// canonical collection — TypeId and Key are empty for that one, since
// every type declaring a shared dataset of the module participates in
// it.
type ModuleInstance struct {
	TypeId     string
	Key        string
	Collection string
	Shared     bool
}

// Module is a compiled-in dataset behaviour a type declares at runtime
// inside one of its parts: the block editor, the chat message stream.
// Where a Type binds fixed dataset names, a Module is a factory the SDK
// instantiates per collection — once for the shared Canonical
// collection and once per namespaced `<typeId>_<key>` instance a type
// declares — so derivation, authorisation, read tracking and indexes
// behave identically on every instance. Callers register modules via
// config.Config.Modules; `records` (the generic schema-enforced
// dataset) is built in.
type Module struct {
	// Name is the slug types name in their dataset declarations
	// ("editor", "chat"). Required; "records" is reserved.
	Name string

	// Canonical is the shared collection name ("editor_blocks"): a type
	// declaring `shared: true` for this module participates in it.
	// Empty means the module has no shared collection and every
	// instance is namespaced.
	Canonical string

	// SharedOnly refuses namespaced instances, so an object carries at
	// most one collection of the module — the invariant behind a single
	// read frontier and a single push group per object. Requires
	// Canonical.
	SharedOnly bool

	// Reserved keeps the module out of runtime declarations: a part or
	// dataset draft naming it — through TypesAPI.AddPart / AddDataset or
	// a bundle's Parts — is refused with space.ErrModuleReserved, unless
	// the Ensure call carries the space.SystemInstall option (the
	// consumer's own catalog install). Registered types may still
	// declare it statically, and a declaration that reached the DAG
	// stays valid on apply (a peer that admitted it was the consumer's
	// own install). Requires SharedOnly. The install root is also the
	// module's only carrier: a local write attaching a user type that
	// declares a reserved module to any other row is refused
	// (ErrValidationReservedCarrier) — no SystemInstall escape, the
	// consumer's install attaches the type through its root alone. A
	// registered type's static declaration stays attachable.
	Reserved bool

	// DataVersion is stamped on changes to the Canonical collection —
	// the same opaque string a Dataset carries. Namespaced instances
	// stamp the declaring type's schema state instead.
	DataVersion string

	// HandlerVersion is the LOCAL logic version of the module's handler,
	// applied to every instance: bumping it replays every collection
	// the module serves. Zero means 1. See Dataset.HandlerVersion.
	HandlerVersion int

	// Properties declares the module's namespace on the objects row —
	// values stored under `<module name>.<propId>` (a chat's unread
	// counters, its notify mode). A row may carry the namespace when one
	// of its types declares a dataset of this module; the read-tracking
	// service writes the counters there.
	Properties []PropertyDecl

	// New builds the registration for one instance: Handler (nil for
	// the generic schema handler), Schema, Indexes, ReadTracking,
	// SkipHistory, DisableFilteredReplay. Name and DataVersion on the
	// returned value are ignored — the SDK fills them from the instance.
	// The handler must be safe to share across controllers: the SDK
	// builds one per collection per catalog snapshot and reuses it,
	// exactly as it shares the generic schema handler.
	New func(ModuleInstance) Dataset
}

// PropertyKind mirrors the JSON-Schema-subset value kinds the SDK
// validates against. The zero value is invalid — every declared
// property has a concrete kind. Values track space.PropertyKind 1:1
// but live here so callers declaring types via config.Config.Types
// don't need to import the space package.
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
	// in JSON) rather than as a string or an epoch number — the shape
	// any-store's date operators compute on.
	PropertyKindDatetime
)

// PropertyDecl is one property definition declared by an external
// Type. Id is the on-record field key under `{typeId}`; Kind is the
// value kind enforced on write; Name is an optional display label
// surfaced through space.Types().Properties(). Scope is the property's
// write/sync class — zero value means ScopeSynced; ScopeDerived is
// reserved for SDK built-ins and rejected at registration. Description
// and XFormat are the descriptive slice — a display description and the
// optional opaque descriptor (semantic slug, icon, options, …) —
// surfaced through space.Types().Properties() as declared; the SDK
// never interprets them and never validates values against them.
type PropertyDecl struct {
	Id          string
	Name        string
	Description string
	Kind        PropertyKind
	Scope       Scope
	XFormat     map[string]any
}
