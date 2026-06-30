package space

import "context"

// IdentitiesAPI is the account-global directory of every account identity
// this account has encountered — across spaces, 1-1s, and inbox invites.
// It is a device-local, persistent cache: profiles are resolved from
// identityRepo and the spaceIds set tracks where each identity was seen.
// (The decryption keys behind it sync across the account's devices; the
// profiles themselves are re-derived per device.)
type IdentitiesAPI interface {
	// List returns every known identity.
	List(ctx context.Context) ([]IdentityInfo, error)
	// Get returns a single identity, ok=false when never encountered.
	Get(ctx context.Context, identity string) (info IdentityInfo, ok bool, err error)
	// Subscribe streams add/update/remove batches for the directory.
	// The returned func cancels the subscription.
	Subscribe(cb func(IdentityListEvent)) (cancel func())
}

// IdentityInfo is a point-in-time view of one directory entry.
type IdentityInfo struct {
	// Identity is the account address (the row key).
	Identity string
	// Name / Description / IconCID are the last profile resolved from
	// identityRepo; empty until resolved (or if we lack the key).
	Name        string
	Description string
	IconCID     string
	// SpaceIds is the set of spaces where we've currently seen this
	// identity (pruned when we leave/offload a space).
	SpaceIds []string
}

// IdentityListEvent is a batch of directory changes, mirroring
// SpaceListEvent.
type IdentityListEvent struct {
	Added   []IdentityInfo
	Updated []IdentityInfo
	Removed []string
}
