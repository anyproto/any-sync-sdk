package syncsdk

import "github.com/anyproto/any-sync-sdk/keys"

// Permission represents the access level of a space member.
// Values match the any-sync AclUserPermissions proto enum directly.
type Permission int

const (
	PermissionOwner  Permission = 1
	PermissionAdmin  Permission = 2
	PermissionWriter Permission = 3
	PermissionReader Permission = 4
)

// MemberStatus represents the current state of a space member.
type MemberStatus int

const (
	MemberStatusActive   MemberStatus = 1
	MemberStatusJoining  MemberStatus = 2
	MemberStatusRemoving MemberStatus = 3
)

// Member describes a participant in a shared space.
type Member struct {
	Identity    keys.PublicKey
	Permissions Permission
	Status      MemberStatus
}

// InviteOption configures how a space invite is generated.
type InviteOption func(*inviteOptions)

type inviteOptions struct {
	permission       Permission
	approvalRequired bool
}

// WithInvitePermission sets the permission level granted to the invitee.
// Default is PermissionWriter.
func WithInvitePermission(p Permission) InviteOption {
	return func(o *inviteOptions) { o.permission = p }
}

// WithApprovalRequired creates a RequestToJoin invite that requires
// the space owner to accept the join request.
func WithApprovalRequired() InviteOption {
	return func(o *inviteOptions) { o.approvalRequired = true }
}

// ResolvedInviteOptions holds the resolved values of InviteOption functions.
type ResolvedInviteOptions struct {
	Permission       Permission
	ApprovalRequired bool
}

// ResolveInviteOptions applies the given InviteOption functions and returns the resolved values.
func ResolveInviteOptions(opts []InviteOption) ResolvedInviteOptions {
	o := inviteOptions{
		permission: PermissionWriter,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return ResolvedInviteOptions{
		Permission:       o.permission,
		ApprovalRequired: o.approvalRequired,
	}
}
