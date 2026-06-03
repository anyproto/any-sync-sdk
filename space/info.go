package space

import "time"

// On-the-wire SpaceType strings stamped into the space header at
// derive/create time. The any-sync-coordinator gates inbound space
// changes against this value (see any-sync-coordinator
// spacestatus/changeverifier.go) — only the strings below plus an
// empty value are accepted; anything else is rejected with
// "unknown space type: <value>" and headsync fails.
//
// These mirror anytype-heart's spacedomain.SpaceType* constants so a
// space created by the SDK is interoperable with any-sync clients
// running anytype-heart.
const (
	// SpaceTypeRegular is the default for newly created spaces. Used
	// when CreateRequest.SpaceType is empty.
	SpaceTypeRegular = "anytype.space"

	// SpaceTypeChat is for chat spaces (one shared chat per space).
	SpaceTypeChat = "anytype.chatspace"

	// SpaceTypeOneToOne is for derived 1-1 spaces shared between two
	// identities.
	SpaceTypeOneToOne = "anytype.onetoone"
)

// SpaceInfo is a point-in-time snapshot of space metadata. Returned by
// Service.List and Space.Info; does not auto-update — subscribe via
// Service.Subscribe for live changes.
type SpaceInfo struct {
	Id   string
	Type string // on-wire header type (anytype.space, anytype.chatspace, anytype.onetoone — never the tech type)
	// SpaceType is the app-level tag set via DeriveRequest.SpaceType,
	// read from the in-space spaceIndex. Independent of the header Type;
	// use it for client-side classification/filtering. Empty/regular
	// spaces carry SpaceTypeRegular.
	SpaceType string
	// Author is the space owner's account identity, resolved from the
	// ACL. Best-effort: empty when the ACL is not loadable.
	Author      string
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
