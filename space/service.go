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

	// OneToOne returns the derived 1-1 space shared with otherIdentity,
	// creating it locally if it does not yet exist. Same id regardless
	// of key order — both peers land on the same space.
	OneToOne(ctx context.Context, otherIdentity string) (Space, error)

	// Get returns an already-joined space by id. Fails if the space is
	// unknown locally.
	Get(ctx context.Context, spaceId string) (Space, error)

	// List returns a point-in-time snapshot of all known spaces.
	// Mirror of the tech space's space index.
	List(ctx context.Context) ([]SpaceInfo, error)

	// Delete tears down a space locally. For regular spaces this also
	// flags the space as deleted on the network; for 1-1 spaces it is
	// local-only (the space is always re-derivable). The record stays
	// in List with Status = StatusDeleted.
	Delete(ctx context.Context, spaceId string) error

	// Subscribe delivers space-list changes (added / updated / removed).
	// Returns a cancel function.
	Subscribe(cb func(SpaceListEvent)) (cancel func())

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
