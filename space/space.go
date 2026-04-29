package space

import "context"

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

	// Subscribe delivers simplified user events for a set of
	// (objectId, dataset) pairs. The subscription runs until Close is
	// called on the returned handle or sdk.Close runs.
	Subscribe(ctx context.Context, targets []SubscribeTarget, opts SubscribeOpts) (Subscription, error)
}

// SubscribeTarget pins a subscription to one object. Datasets filter
// further to a subset of the object's datasets; empty Datasets means
// all.
type SubscribeTarget struct {
	ObjectId string
	Datasets []string
}
