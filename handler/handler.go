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

	// Handler implements the dataset's lifecycle (Init / Before*).
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
}

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
// shared `objects` namespace, e.g. a nav type), both, or neither (a
// pure declaration / tag in any.types). Only a non-empty Id is required.
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
)

// PropertyDecl is one property definition declared by an external
// Type. Id is the on-record field key under `{typeId}`; Kind is the
// value kind enforced on write; Name is an optional display label
// surfaced through space.Types().Properties(). Scope is the property's
// write/sync class — zero value means ScopeSynced; ScopeDerived is
// reserved for SDK built-ins and rejected at registration.
type PropertyDecl struct {
	Id    string
	Name  string
	Kind  PropertyKind
	Scope Scope
}
