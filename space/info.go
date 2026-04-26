package space

import "time"

// SpaceInfo is a point-in-time snapshot of space metadata. Returned by
// Service.List and Space.Info; does not auto-update — subscribe via
// Service.Subscribe for live changes.
type SpaceInfo struct {
	Id          string
	Type        string // space type (regular, 1-1, tech — not returned for tech)
	Name        string
	Description string
	IconCID     string
	Status      Status
	OwnRole     Permission
	CreatedAt   time.Time
}

// Status is the combined local+remote state of a space.
type Status uint8

const (
	StatusUnknown Status = iota
	StatusActive
	StatusJoining    // request-to-join pending approval
	StatusLeaving    // local delete in flight
	StatusDeleted    // marked as deleted locally
	StatusRemoteDead // network says space no longer exists
)

// Permission mirrors any-sync's ACL permission ladder.
type Permission uint8

const (
	PermissionNone Permission = iota
	PermissionReader
	PermissionGuest
	PermissionWriter
	PermissionAdmin
	PermissionOwner
)
