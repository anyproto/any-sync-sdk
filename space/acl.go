package space

// ACL is the owner/admin-side ACL surface: invite lifecycle, join
// approvals, permission changes, ownership transfer, member removal.
// Mirrors any-sync's AclClient one-to-one (see docs/03-space.md).
//
// Placeholder — methods groomed in the space/ACL pass.
type ACL interface {
	// TODO: CreateInvite, ReplaceInvite, RevokeInvite,
	// AcceptRequest, DeclineRequest, RemoveAccounts,
	// ChangePermissions, OwnershipChange, SelfRemove
}
