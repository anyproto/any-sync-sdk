package space

import "context"

// Service is the space-level entrypoint exposed by the top-level SDK.
// It owns lifecycle (Create / Join / Derive / Delete) and the space
// list; individual space operations live on Space.
type Service interface {
	// Create a new regular space owned by the authenticated account.
	Create(ctx context.Context, req CreateRequest) (Space, error)

	// Join a space via an invite. Depending on the invite key mode the
	// space is either immediately active or pending approval.
	Join(ctx context.Context, req JoinRequest) (Space, error)

	// Derive a deterministic space from the account keys. Used for
	// the tech space (never returned here) and future derived spaces.
	Derive(ctx context.Context, req DeriveRequest) (Space, error)

	// DeriveId returns the deterministic spaceId for a DeriveRequest
	// without creating or loading the space. Same id as Derive(...).Id()
	// for the same request. Lets a consumer recompute a known derived
	// space's id (from its own seed) to recognize or filter it
	// client-side.
	DeriveId(ctx context.Context, req DeriveRequest) (string, error)

	// OneToOne reaches out to — or explicitly accepts / un-declines — the
	// derived 1-1 space shared with otherIdentity. Materializes and
	// activates it locally (implicit self-approval). Same id regardless of
	// key order — both peers land on the same space. Idempotent; overrides
	// a prior local decline.
	OneToOne(ctx context.Context, otherIdentity string) (Space, error)

	// AcceptOneToOne approves an incoming pending 1-1 by space id (as
	// surfaced in List with Status == StatusOneToOnePending): materializes
	// and activates it. The peer identity is read off the row, so the
	// caller needn't re-derive it. Equivalent to OneToOne(peer).
	AcceptOneToOne(ctx context.Context, spaceId string) (Space, error)

	// DeclineOneToOne rejects an incoming 1-1. Writes a synced sticky
	// marker so the request is suppressed on all the account's devices and
	// never auto-resurfaces; a later explicit OneToOne(peer) overrides it.
	DeclineOneToOne(ctx context.Context, spaceId string) error

	// RegisterIncoming records an incoming 1-1 request learned out-of-band
	// (no coordinator) as a pending row for the user to approve, without
	// materializing storage. displayHint is an optional name/icon snapshot
	// for the UI. No-op if a row for the derived space already exists.
	RegisterIncoming(ctx context.Context, peerIdentity string, displayHint AccountMetadata) error

	// Get returns an already-joined space by id. Fails if the space is
	// unknown locally.
	Get(ctx context.Context, spaceId string) (Space, error)

	// Track registers a foreign spaceId in the local space index without
	// joining it, so a later Get can open it — any-sync bootstraps the
	// space from its responsible nodes when local storage is missing.
	// The caller does not become a member and holds no keys: synced
	// content stays sealed. Idempotent; a no-op when the id is already
	// indexed (including own/joined spaces — Track never downgrades a
	// membership row).
	//
	// The broker path (headless + selective sync): Track the spaceId,
	// Get it, read the payloads index via Space.Payloads.
	Track(ctx context.Context, spaceId string) error

	// Evict closes a space without deleting anything: per-space watchers
	// stop, the in-memory store and the any-sync space are released. All
	// disk state stays — a later Get reopens the space from local
	// storage. Idempotent; evicting a space that isn't open is a no-op.
	//
	// This is close-on-demand for embedders that hold many spaces (the
	// filenode-v2 broker); Delete is the destructive sibling.
	Evict(ctx context.Context, spaceId string) error

	// List returns a point-in-time snapshot of all known spaces.
	// Mirror of the tech space's space index.
	List(ctx context.Context) ([]SpaceInfo, error)

	// SyncSpaceList forces an immediate head-sync round on the tech
	// space so spaces added or removed on other devices land in the
	// local index, instead of waiting for the periodic timer. Call it
	// before List to converge the space list on demand. Blocks until
	// the round completes.
	SyncSpaceList(ctx context.Context) error

	// Delete tears down a space locally. For regular spaces this also
	// flags the space as deleted on the network; for 1-1 spaces it is
	// local-only (the space is always re-derivable). The record stays
	// in List with Status = StatusDeleted.
	Delete(ctx context.Context, spaceId string) error

	// Subscribe delivers space-list changes (added / updated / removed).
	// Returns a cancel function.
	Subscribe(cb func(SpaceListEvent)) (cancel func())

	// SpaceIndexObjectId returns the id of the tech-space index object —
	// the handle for generic Query/Subscribe over the system datasets
	// (spaces, profile). Future system objects expose their own ids.
	SpaceIndexObjectId() string

	// Query builds a generic read query over a system object's dataset
	// (e.g. SpaceIndexObjectId() + "spaces"), with the same chainable
	// Filter / Sort / Limit / Snapshot / Subscribe surface as
	// Space.Query. The bespoke List / Subscribe methods are convenience
	// wrappers over this.
	Query(objectId, dataset string) Query

	// Datasets returns the JSON-Schema description of the tech-space
	// system datasets (spaces, profile) — field names, value shapes, and
	// per-field class (synced / derived / local) via `x-scope`. For
	// discovery, mirroring Space.Datasets.
	Datasets() []DatasetSchema

	// Status returns a snapshot of one space's rolled-up sync state.
	// Cheap; safe to call on every render tick. Spaces unknown to
	// the SDK return SpaceSyncStatus{SpaceId: spaceId,
	// State: SyncStateUnknown}.
	Status(spaceId string) SpaceSyncStatus

	// SubscribeStatus delivers SpaceSyncStatus events whenever any
	// known space's rollup transitions. Account-wide — one cb sees
	// every space. cb runs synchronously on the dispatcher
	// goroutine; keep work small or hand off.
	SubscribeStatus(cb func(SpaceSyncStatus)) (cancel func())
}

// CreateRequest is the input to Service.Create.
type CreateRequest struct {
	Name        string
	Description string
	IconCID     string
	// SpaceType is stamped into the space header at create time and
	// gated by the any-sync-coordinator. Must be one of the public
	// constants (SpaceTypeRegular, SpaceTypeChat, SpaceTypeOneToOne)
	// or empty — empty defaults to SpaceTypeRegular. Anything else
	// is rejected by Create with a clear error; passing an invalid
	// type would otherwise produce a space the coordinator refuses
	// to sync.
	SpaceType string
}

// JoinRequest is the input to Service.Join.
type JoinRequest struct {
	// Invite is the invite string produced by ACL.CreateInvite on the
	// host side. The SDK parses it internally; opaque to the caller.
	Invite string

	// Metadata is the joining account's metadata that will be attached
	// to the ACL join record (display name, icon, etc.).
	Metadata AccountMetadata
}

// DeriveRequest is the input to Service.Derive.
type DeriveRequest struct {
	// Seed is hashed into the derivation. Zero seed = account-root
	// derivation (tech space).
	Seed []byte

	// SpaceType is an app-level tag surfaced as SpaceInfo.SpaceType for
	// client-side filtering. It is NOT the on-wire header type (that
	// stays anytype.space and is coordinator-gated) and not stamped into
	// the header. Empty defaults to SpaceTypeRegular.
	SpaceType string
}

// SpaceListEvent is delivered to Service.Subscribe callbacks.
type SpaceListEvent struct {
	Added   []SpaceInfo
	Updated []SpaceInfo
	Removed []string
}

// AccountMetadata is the owner/member metadata attached to ACL join
// records (identityRepo-backed on the wire).
type AccountMetadata struct {
	Name        string
	Description string
	IconCID     string
}
