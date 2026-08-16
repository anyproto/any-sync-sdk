package space

import "context"

// SetMetadataRequest is the input to Space.SetMetadata. Pointer
// semantics: nil = leave-unchanged; non-nil empty string = set-empty.
type SetMetadataRequest struct {
	Name        *string
	Description *string
	IconCID     *string
}

// Space is the caller-facing interface to one space. Obtained from
// Service.Create / Join / Derive / Get. Middleware holds Space handles
// for the lifetime of use; the SDK manages underlying ocache loading
// internally.
//
// No Close method — callers do not manage space lifecycle. sdk.Close
// tears everything down.
type Space interface {
	// Id returns the space identifier (any-sync CID-based).
	Id() string

	// Info returns a point-in-time snapshot of space metadata.
	Info() SpaceInfo

	// Objects is the object lifecycle API (Create / Derive / Delete).
	Objects() ObjectService

	// ACL exposes the full any-sync ACL feature set: invite, accept,
	// decline, change permissions, ownership transfer, self-remove.
	ACL() ACL

	// Members returns the members-collection view.
	Members() MembersAPI

	// Types manages type objects and property definitions.
	Types() TypesAPI

	// Properties reads computed property values and performs
	// base/account/device scope writes.
	Properties() PropertiesAPI

	// SyncStatus exposes per-space/object/peer status.
	SyncStatus() SyncStatusAPI

	// Changes exposes the change-index surface: a live feed and a
	// "changed since cursor N" query over the objects in this space,
	// for consumer-side incremental indexers (full-text / vector
	// search). See ChangeIndexAPI.
	Changes() ChangeIndexAPI

	// History exposes version history: list an object's changes (who
	// changed what, when), view objects/records at past versions, and
	// diff versions. See HistoryAPI and
	// docs/version-history-proposal.md.
	History() HistoryAPI

	// Payloads is the read-only view over the space's file payloads
	// index — the cleartext row fields only, readable without any
	// space key. See PayloadsView.
	Payloads() PayloadsView

	// Files is the file surface: attach content to objects, with the
	// storage tiers (inline / content-addressed + node backup) hidden.
	// See Files.
	Files() Files

	// PubSub is ephemeral space-scoped pub/sub — fire-and-forget,
	// at-most-once messages between the space's online members, never
	// persisted. See PubSubAPI.
	PubSub() PubSubAPI

	// ReadState tracks read/unread changes for datasets registered
	// with handler.Dataset.ReadTracking. Methods return
	// ErrReadTrackingDisabled when nothing in the space opted in.
	ReadState() ReadStateAPI

	// Debug returns the diagnostic surface for this space. See
	// DebugAPI — not a stable interface, intended for tooling and
	// inspection.
	Debug() DebugAPI

	// Query builds a read query against (objectId, dataset). Used for
	// per-object datasets that exist on the object's own controller —
	// e.g. a type object's `properties` (definitions) dataset.
	Query(objectId, dataset string) Query

	// QueryObjects builds a read query against the per-space `objects`
	// collection — one row per regular object in the space, holding
	// computed property values. Use this for cross-object queries
	// like "find every Movie with Title containing X".
	QueryObjects() Query

	// Aggregate builds a MongoDB-style aggregation pipeline against
	// (objectId, dataset) — the aggregation sibling of Query. The
	// pipeline is accepted in the same forms Query.Filter takes a
	// condition: a JSON string, *fastjson.Value, *anyenc.Value,
	// marshaled-anyenc []byte, or any JSON-marshalable Go value.
	// Snapshot-only. See Agg.
	Aggregate(objectId, dataset string, pipeline any) Agg

	// AggregateObjects builds an aggregation pipeline against the
	// per-space `objects` collection — the aggregation sibling of
	// QueryObjects. See Agg.
	AggregateObjects(pipeline any) Agg

	// Datasets returns the JSON-Schema description of every dataset in
	// this space — field names, value shapes, and per-field class
	// (synced / derived / local) via the `x-scope` keyword. For
	// discovery; per-type object property schemas are also available
	// through Types().
	Datasets() []DatasetSchema

	// Modify applies a write batch. Returns the VersionId, ChangeId,
	// and resolved per-record ids (auto-derived ids surface here for
	// callers who submitted records with empty Id — propId / shortId
	// convention).
	Modify(ctx context.Context, batch ModifyBatch) (ModifyResult, error)

	// ModifyMany applies multiple write batches in one logical
	// submission, with pre-validation as the atomicity boundary:
	// every batch is structurally validated up-front, and if ANY
	// validation fails NONE are written to any-sync. Otherwise
	// each batch produces its own DAG change in input order.
	//
	// All batches MUST target the same ObjectId — cross-object
	// atomicity is not supported (each object's tree signs its own
	// changes). Datasets may differ across batches; this is the
	// supported way to land properties + a user dataset together
	// in a single client submission.
	//
	// Per-op handler rejections at apply time still surface as
	// ModifyResult.Rejections, same as Modify — the apply layer is
	// per-op by design (convergent across peers regardless of
	// arrival order).
	//
	// The returned slice is aligned to input order. Validation
	// failure returns an empty slice and the joined error.
	ModifyMany(ctx context.Context, batches []ModifyBatch) ([]ModifyResult, error)

	// Delete produces sticky tombstones for the listed record ids.
	// Returned RecordIds mirror the input order.
	Delete(ctx context.Context, batch DeleteBatch) (ModifyResult, error)

	// SetMetadata mutates this space's display metadata (name,
	// description, icon) by writing to the per-space `spaceIndex`
	// derived object. The write is CRDT-replicated to every member;
	// each device's indexer hook mirrors the converged state into its
	// own tech-space row.
	//
	// Pointer-to-string semantics: a nil pointer means "leave
	// unchanged"; a non-nil pointer to an empty string means "set to
	// empty". This lets callers patch a single field without
	// clobbering the others.
	//
	// SpaceType is intentionally not settable — it's pinned by the
	// initial Create write on the spaceIndex object.
	//
	// v1 has no caller-side permission gate; non-writers are rejected
	// downstream by the ACL at apply time on peers.
	SetMetadata(ctx context.Context, req SetMetadataRequest) error

	// SpaceIndexObjectId returns the deterministic id of the in-space
	// `spaceIndex` derived object. Stable across peers and across
	// SDK reboots — same id on every member's device. Useful for
	// wrappers that want to attach a Query.Subscribe stream on the
	// spaceIndex's `objects` dataset for live UI updates.
	SpaceIndexObjectId() string

	// SyncHeads forces an immediate head-sync (diff) round on this
	// space against its responsible nodes, instead of waiting for the
	// periodic timer. Blocks until the round completes. Use it to
	// converge on demand (e.g. tests, or a manual "sync now"); normal
	// operation does not need it — periodic and reactive sync keep the
	// space up to date on their own.
	SyncHeads(ctx context.Context) error

	// TreeHeads returns the space's current sync frontier: the head
	// change ids of every live (non-deleted) tree known locally — one
	// entry per tree, materialized trees and heads-only stubs alike
	// (under selective sync every tree head-syncs even when its
	// content is not pulled), including system trees such as settings.
	// A causal-attestation primitive for embedders: a peer holding
	// every head of another peer's entry has seen at least that peer's
	// change set for the tree. Used by the filenode-v2 broker's GC
	// gate (CheckRefs).
	TreeHeads(ctx context.Context) ([]TreeHeads, error)
}

// TreeHeads is one tree's current heads as reported by
// Space.TreeHeads: the frontier element for that tree. Heads are
// change ids.
type TreeHeads struct {
	TreeId string
	Heads  []string
}
