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
	"github.com/anyproto/any-sync-sdk/internal/crdt"
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
// that only need to override a subset of lifecycle hooks.
type DefaultHandler = crdt.DefaultHandler

// IndexedHandler is an optional interface a Handler may implement to
// declare any-store indexes for its dataset's collection. The SDK
// calls EnsureIndex on each entry the first time the collection is
// opened (per process). EnsureIndex is idempotent — restarts re-run
// it harmlessly. Pair-import any-store for the IndexInfo type:
//
//	import anystore "github.com/anyproto/any-store/v2"
type IndexedHandler = crdt.IndexedHandler

// ErrValidation is the sentinel wrapped by handler returns when an
// op is rejected. Programmatic discrimination uses errors.Is.
var ErrValidation = crdt.ErrValidation

// ErrUnknownDataset signals a change targeting a dataset with no
// registered handler.
var ErrUnknownDataset = crdt.ErrUnknownDataset

// Registration bundles a Handler with its on-the-wire DataVersion
// stamp. The stamp travels on every change emitted on this dataset
// and is what peers gate against. Bump the stamp suffix when changing
// validation in a way that must reject older writers — peers running
// older SDK builds will park changes carrying an unknown stamp rather
// than misapply them.
type Registration struct {
	// Handler implements the dataset's apply-time validation and
	// (optionally) projection / derivation behavior.
	Handler Handler

	// DataVersion is the string stamped on every change emitted on
	// this handler's dataset. Required (non-empty). Convention:
	// "<datasetName>-v<n>" — but any opaque string works as long as
	// it is unique to a (handler logic, dataset) pair.
	DataVersion string
}

// Type binds a typeId to the dataset handlers it owns. Built-in
// types follow the same shape: `any` owns the `objects` dataset;
// `type` owns `properties` and `shortIds`. Callers extend the
// catalog by supplying additional Types via config.Config.Types.
//
// A Type with zero handlers is rejected — types exist to own
// handlers, so an empty Handlers slice is meaningless.
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

	// Handlers is the dataset registrations this type owns. Every
	// handler's Dataset() name must be unique across the whole
	// catalog (built-ins + every external Type's handlers); the
	// SDK rejects collisions at sdk.Open.
	Handlers []Registration
}
