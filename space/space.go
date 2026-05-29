package space

import "context"

// SetMetadataRequest is the input to Space.SetMetadata. Pointer
// semantics: nil = leave-unchanged; non-nil empty string = set-empty.
// Mirrors PropertyMetaUpdate's per-field patching shape.
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

}
