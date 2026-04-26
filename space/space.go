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

	// Query builds a read query against (objectId, dataset).
	Query(objectId, dataset string) Query

	// Modify applies a write batch. Returns the VersionId assigned by
	// any-sync to the accepted change.
	Modify(ctx context.Context, batch ModifyBatch) (VersionId, error)

	// Delete produces sticky tombstones for the listed record ids.
	Delete(ctx context.Context, batch DeleteBatch) (VersionId, error)

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
