package space

import "context"

// ACL is the owner/admin-side ACL surface: invite lifecycle, join
// approvals, permission changes, ownership transfer, member removal.
// Mirrors any-sync's AclSpaceClient one-to-one with SDK-native types
// (identity strings instead of crypto.PubKey, Permission enum instead
// of list.AclPermissions).
//
// Permissions: callers must hold the appropriate permission for each
// op (most ops are admin/owner-only). Failures surface as the
// underlying any-sync error.
//
// Scope: only RequestToJoin invites are supported. AnyoneCanJoin is
// deferred until any-sync ships v2 of that invite type.
type ACL interface {
	// CreateInvite mints a new RequestToJoin invite, revoking any
	// prior invite on this space (mirrors AclSpaceClient.ReplaceInvite
	// semantics — only one active invite at a time). The returned
	// Invite is share-friendly (base58-encoded via Invite.Encode()
	// equivalents on space.EncodeInvite).
	CreateInvite(ctx context.Context) (Invite, error)

	// RevokeInvite revokes a single invite by record id. The record id
	// is what AclState lists in InviteIds.
	RevokeInvite(ctx context.Context, inviteRecordId string) error

	// RevokeAllInvites tears down every active invite in one batch.
	RevokeAllInvites(ctx context.Context) error

	// AcceptRequest approves a pending join request and grants the
	// joiner the given permission. requestRecordId is the id from
	// MembersAPI.JoinRequests. Owner / admin only.
	AcceptRequest(ctx context.Context, requestRecordId string, perm Permission) error

	// DeclineRequest rejects a pending join request from identity.
	// Owner / admin only.
	DeclineRequest(ctx context.Context, identity string) error

	// ChangePermissions updates the permissions of one or more existing
	// members in a single batch. Owner / admin only; cannot demote the
	// owner.
	ChangePermissions(ctx context.Context, changes []PermissionChange) error

	// RemoveAccounts kicks one or more members out, rotating the read
	// key so removed members can no longer decrypt new content. Owner
	// / admin only.
	RemoveAccounts(ctx context.Context, identities []string) error

	// AddAccounts adds members directly without an invite/request
	// round-trip — the whole batch lands in ONE ACL record. Useful
	// wherever the caller already holds the joiners' identities.
	//
	// When the coordinator inbox transport is available, each added
	// account is also notified durably (queued + retried across
	// restarts): the space surfaces on their devices as
	// StatusInvitePending for them to AcceptInvite / DeclineInvite.
	// Without the transport (headless deployments) the ACL write still
	// happens but no notification is sent.
	AddAccounts(ctx context.Context, accounts []MemberAdd) error

	// OwnershipChange transfers ownership to newOwner; the old owner's
	// permission becomes oldOwnerPerm (typically PermissionAdmin).
	OwnershipChange(ctx context.Context, newOwner string, oldOwnerPerm Permission) error

	// RequestSelfRemove asks to be removed from this space. Self-service
	// — works for any non-owner, non-guest member.
	RequestSelfRemove(ctx context.Context) error

	// CancelJoinRequest withdraws a join request that the caller
	// previously made and that has not yet been accepted/declined.
	CancelJoinRequest(ctx context.Context) error

	// StopSharing drops every non-owner member, revokes every invite,
	// and rotates the read key in one batch. Owner only.
	StopSharing(ctx context.Context) error

	// CreateGuestKey enables public read-only access: it mints a shared
	// guest identity, adds it to the ACL with PermissionGuest, and
	// returns it as an InviteKindGuest invite for Service.JoinGuest.
	// One active guest key per space; the private key is persisted on
	// the owner's tech-space row, so repeated calls return the same
	// invite while the guest identity is still active in the ACL
	// (idempotent — the key is not recoverable from the ACL itself).
	// Owner only: custody lives in the owner's tech space, so admins
	// can neither fetch nor reissue it.
	CreateGuestKey(ctx context.Context) (Invite, error)

	// RevokeGuestKey removes the guest identity from the ACL and
	// rotates the read key, cutting every guest off from new content
	// (already-synced local copies stay readable on their devices).
	// Clears the stored key; a later CreateGuestKey mints a fresh
	// identity, so old invites die permanently. No-op error when no
	// guest key is active.
	RevokeGuestKey(ctx context.Context) error
}

// PermissionChange is one entry in a batch ChangePermissions call.
type PermissionChange struct {
	Identity   string
	Permission Permission
}

// MemberAdd is one entry in a batch AddAccounts call. Metadata is
// optional and gets attached to the join record (same shape as the
// metadata a joiner attaches via Service.Join).
type MemberAdd struct {
	Identity   string
	Permission Permission
	Metadata   AccountMetadata
}
