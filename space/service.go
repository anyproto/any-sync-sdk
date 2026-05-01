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
