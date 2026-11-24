package space

import (
	"context"
	"errors"

	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
)

// ErrNoActiveGuestKey is returned by RevokeGuestKey when the space has
// no guest identity to revoke.
var ErrNoActiveGuestKey = errors.New("no active guest key")

// ErrBadIdentity is returned by identity-taking methods (ACL ops,
// OneToOne, RegisterIncoming) when the identity string is empty or not
// a decodable account address.
var ErrBadIdentity = errors.New("bad identity")

// Re-exported any-sync ACL and coordinator sentinels, so consumers
// classify ACL failures with errors.Is against this package instead
// of importing any-sync internals.
var (
	// ErrDuplicateInvite is returned by CreateInvite when an active
	// invite of the same type already exists.
	ErrDuplicateInvite = list.ErrDuplicateInvites

	// ErrInsufficientPermissions is returned by ACL ops the caller's
	// permission does not allow.
	ErrInsufficientPermissions = list.ErrInsufficientPermissions

	// ErrAclRecordNotFound is returned by ACL ops addressing a record
	// (or pending request) the ACL does not hold.
	ErrAclRecordNotFound = list.ErrNoSuchRecord

	// ErrAccountNotFound is returned by ACL ops, and by
	// Service.RemoveChildMembers and Service.ChangeChildOwner, for an
	// identity the space's ACL holds no account of.
	ErrAccountNotFound = list.ErrNoSuchAccount

	// ErrInviteNotFound is returned by Join when the space's ACL holds
	// no such invite (revoked or never minted), and by
	// RevokeInvite for an unknown invite record.
	ErrInviteNotFound = list.ErrNoSuchInvite

	// ErrCoordinatorForbidden is the coordinator refusing an operation
	// for this account. The Owner's CreateInvite, CreateGuestKey and
	// AddAccounts first make the space shareable, which only the Owner
	// may do: they return it when the coordinator holds another account
	// as the owner. An Admin's calls skip that step.
	ErrCoordinatorForbidden = coordinatorproto.ErrForbidden

	// ErrCoordinatorLimitReached is the coordinator refusing an
	// operation because the space owner's plan reached a limit, whoever
	// the caller is: shared spaces on make-shareable (CreateInvite,
	// CreateGuestKey, AddAccounts), or members on an ACL record that
	// raises the reader or writer count past the plan's cap
	// (AcceptRequest, ChangePermissions, AddAccounts, CreateGuestKey).
	// A plan allowing at most one reader and one writer refuses every
	// ACL record, removals and the joiner's Join and CancelJoin
	// included.
	ErrCoordinatorLimitReached = coordinatorproto.ErrSpaceLimitReached
)

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
//
// The Owner's CreateInvite, AddAccounts and CreateGuestKey need the
// space on the network. For a space the coordinator does not know yet
// they wait for its first push, and return ErrSpaceUnknown when it has
// not landed in time. A child space the coordinator refused to sign
// (SpaceInfo.Refusal) is not waited for: CreateInvite and AddAccounts
// return the error of that refusal, one of ErrParentSpaceNotAvailable,
// ErrParentSpaceInvalid, ErrParentPermissionDenied and
// ErrChildSpacesLimitReached.
//
// An organization and its spaces have no guest role: ErrNoGuestRole.
type ACL interface {
	// CreateInvite mints a RequestToJoin invite. A space holds one at a
	// time: ErrDuplicateInvite while one is active, until it is revoked
	// (RevokeInvite, RevokeAllInvites). An active invite of another
	// type, which this SDK does not mint, is revoked by the same
	// record. The returned Invite is share-friendly (base58 via
	// space.EncodeInvite).
	CreateInvite(ctx context.Context) (Invite, error)

	// RevokeInvite revokes a single invite by record id. The record id
	// is what AclState lists in InviteIds.
	RevokeInvite(ctx context.Context, inviteRecordId string) error

	// RevokeAllInvites tears down every active invite in one batch.
	RevokeAllInvites(ctx context.Context) error

	// AcceptRequest approves a pending join request and grants the
	// joiner the given permission. requestRecordId is the id from
	// MembersAPI.JoinRequests. Owner / admin only. In a child space
	// ErrNotParentMember for a joiner who is no longer a member of the
	// parent, and ErrParentSpaceNotAvailable when the coordinator
	// cannot read the parent's ACL. ErrNoGuestRole for PermissionGuest
	// in an organization or its spaces.
	AcceptRequest(ctx context.Context, requestRecordId string, perm Permission) error

	// DeclineRequest rejects a pending join request from identity.
	// Owner / admin only.
	DeclineRequest(ctx context.Context, identity string) error

	// ChangePermissions updates the permissions of one or more existing
	// members in a single batch. Owner / admin only; cannot demote the
	// owner. ErrNoGuestRole for a change to PermissionGuest in an
	// organization or its spaces.
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
	//
	// In a child space every added account is a member of the parent:
	// ErrNotParentMember otherwise, ErrParentSpaceNotAvailable when the
	// coordinator cannot read the parent's ACL. ErrNoGuestRole for
	// PermissionGuest in an organization or its spaces. Nobody of a
	// refused batch is added.
	AddAccounts(ctx context.Context, accounts []MemberAdd) error

	// OwnershipChange transfers ownership to newOwner; the old owner's
	// permission becomes oldOwnerPerm (typically PermissionAdmin). In a
	// child space the new owner is a member of the parent:
	// ErrNotParentMember otherwise, ErrParentSpaceNotAvailable when the
	// coordinator cannot read the parent's ACL.
	OwnershipChange(ctx context.Context, newOwner string, oldOwnerPerm Permission) error

	// RequestSelfRemove asks to be removed from this space. Self-service
	// — works for any non-owner, non-guest member.
	RequestSelfRemove(ctx context.Context) error

	// CancelJoinRequest withdraws the caller's pending request on a
	// LOADED space. A pending join is never loaded (Service.Get refuses
	// it), so withdrawing a join request goes through the account-level
	// Service.CancelJoin; this handle only reaches a request the loaded
	// space can hold, i.e. a pending self-remove.
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
	//
	// ErrNoGuestRole in an organization or its spaces.
	CreateGuestKey(ctx context.Context) (Invite, error)

	// RotationPending reports that members were removed from this
	// space without a read key change and still hold the current read
	// key. An Admin or the Owner of the parent space removes members of
	// a child this way (Service.RemoveChildMembers): they are members
	// no longer, and read what is written until the key changes.
	//
	// No call ends it. A device of an Admin or the Owner of this space
	// that holds the read key changes the key by itself, while the
	// space is loaded and the network reachable; RotationPending reads
	// false once that record is applied here.
	RotationPending(ctx context.Context) (bool, error)

	// RevokeGuestKey removes the guest identity from the ACL and
	// rotates the read key, cutting every guest off from new content
	// (already-synced local copies stay readable on their devices).
	// Clears the stored key; a later CreateGuestKey mints a fresh
	// identity, so old invites die permanently. No-op error when no
	// guest key is active. ErrNoGuestRole in an organization or its
	// spaces.
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
