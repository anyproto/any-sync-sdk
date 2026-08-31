package space

import "time"

// On-the-wire SpaceType strings stamped into the space header at
// derive/create time. The any-sync-coordinator gates inbound space
// changes against an allow-list (see any-sync-coordinator
// spacestatus/changeverifier.go); anything outside it is rejected
// with "unknown space type: <value>" and headsync fails. The type is
// content-addressed into the immutable header, so a rejected value
// bricks the space permanently.
//
// The SDK emits only the any.* family (the coordinator requires
// fileproto v2 in headers carrying it). anytype.* spaces belong to
// anytype-heart clients; the SDK can join/track them but never mints
// them.
const (
	// SpaceTypeAny is the type of every created or derived space.
	// Used when CreateRequest.SpaceType is empty (the only accepted
	// value).
	SpaceTypeAny = "any.space"

	// SpaceTypeOneToOne is for derived 1-1 spaces shared between two
	// identities. The type is content-addressed into the symmetric
	// derived id, so both peers must use the same value — 1-1s pair
	// only within a product, and the any.* variant makes an
	// any↔anytype 1-1 structurally impossible.
	SpaceTypeOneToOne = "any.onetoone"
)

// SpaceInfo is a point-in-time snapshot of space metadata. Returned by
// Service.List and Space.Info; does not auto-update — subscribe via
// Service.Subscribe for live changes.
type SpaceInfo struct {
	Id   string
	Type string // on-wire header type (any.space, any.onetoone, …; the tech type only on the tech handle)
	// SpaceType is the app-level tag set via DeriveRequest.SpaceType,
	// read from the in-space spaceIndex. Independent of the header Type;
	// use it for client-side classification/filtering. Untagged spaces
	// carry SpaceTypeAny.
	SpaceType string
	// Author is the space owner's account identity, resolved from the
	// ACL. Best-effort: empty when the ACL is not loadable.
	//
	// For a 1-1 (SpaceTypeOneToOne) space the ACL owner is a synthetic
	// shared key nobody holds, so Author instead carries the OTHER
	// participant's account identity — the friend this 1-1 is with.
	// Available even while the space is only pending (not materialized),
	// so clients can identify and resolve the friend's profile from the
	// space list directly.
	Author      string
	Name        string
	Description string
	IconCID     string
	Status      Status
	// OwnRole is this account's ACL permission in the space, mirrored
	// from ACL state onto the tech-space row by the per-space ACL
	// mirror (same trigger model as PushKeys: one pass at space load
	// plus a kick per applied ACL record). PermissionNone until the
	// mirror first runs — notably on rows whose space was never loaded
	// by this device — so treat "none" on an active space as
	// "unknown yet", not a verdict; Space.Members().Me stays the
	// authoritative per-space read.
	//
	// For a 1-1 (SpaceTypeOneToOne) space the ACL owner is the
	// synthetic shared key, so both participants mirror the role the
	// ACL actually grants them — never PermissionOwner.
	OwnRole   Permission
	CreatedAt time.Time
	// Settings is the account-private, client-owned per-space settings
	// object: free-form keys with scalar values (string / float64 /
	// bool — numbers decode as float64, JSON semantics). Written per
	// key via Service.SetSettings; synced across the account's devices
	// through the tech space, never visible to other space members.
	// Nil when never written.
	Settings map[string]any
	// PushKeys is the space's push-notification key material, mirrored
	// from ACL state so clients can cache it and decrypt push payloads
	// while the SDK process is down (see docs/02-tech-space.md
	// § "Space Index"). Nil until the per-space mirror has run —
	// notably on a joiner whose access is still pending (no read key
	// yet) and on rows whose space was never loaded by this device.
	PushKeys *PushKeys
	// Derived marks a space created by the account's own
	// Service.Derive — Delete refuses it (see ErrIsDerivedSpace).
	// Always false on created / joined / tracked / 1-1 spaces.
	Derived bool
}

// PushKeys is the per-space key material a push RECEIVER needs,
// byte-compatible with anytype-heart's spacePushNotificationKey /
// spacePushNotificationEncryptionKey space-view details.
//
// EncKey rotates with the ACL read key. Clients must treat their
// cache as append-only per space ({EncKeyId → EncKey}): a payload
// encrypted before a rotation still arrives carrying the old KeyId,
// and a KeyId that was never cached (fresh install, rotation missed
// while offline) means "render a generic notification".
//
// EncKey is a one-way SLIP-21 derivation from the read key — holding
// it decrypts push payloads only, never space data.
type PushKeys struct {
	// SpaceKey is the base64 (std) of the protobuf-marshalled ed25519
	// private key that identifies the space on the push server
	// (pushapi Topic.SpaceKey is its public half). Derived from the
	// ACL's first metadata key — fixed for the space's life.
	SpaceKey string
	// EncKey is the base64 (std) of the raw AES payload key derived
	// from the CURRENT ACL read key.
	EncKey string
	// EncKeyId is hex(sha256(raw EncKey bytes)) — the value stamped
	// into pushapi.Message.KeyId, i.e. the cache key a receiver looks
	// up on an incoming push.
	EncKeyId string
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
	// StatusOneToOnePending is an incoming 1-1 (direct) space awaiting
	// local approval. The space is not materialized or synced until
	// accepted — Accept it via Service.AcceptOneToOne / OneToOne, or
	// reject it via Service.DeclineOneToOne. Device-local: discovery is
	// per-device, so the prompt is approved/declined per device until a
	// decline (which is synced account-wide).
	StatusOneToOnePending
	// StatusOneToOneDeclined is a 1-1 space the user declined. Synced and
	// sticky across the account's devices: the request never re-surfaces
	// from the discovery layer. An explicit OneToOne(peer) overrides it.
	StatusOneToOneDeclined
	// StatusInvitePending is a regular space another account added us to
	// directly (ACL AddAccounts). We are already a full ACL member;
	// approval is a local materialization gate — nothing is downloaded
	// until accepted. Synced account-wide (any device can act on it).
	// Accept via Service.AcceptInvite, reject via Service.DeclineInvite.
	StatusInvitePending
	// StatusInviteDeclined is a direct-add invite the user declined.
	// Synced, sticky, non-terminal: a later AcceptInvite overrides it. No
	// ACL write happens on decline — the account remains an ACL member.
	StatusInviteDeclined
	// StatusGuestRevoked is a guest-key space whose shared guest identity
	// was removed from the ACL (the owner revoked public access). The
	// local copy stays readable; new content no longer arrives (the read
	// key rotated away). Device-local and non-terminal: it self-heals
	// back to Active if a fresh ACL shows the guest identity active
	// again. Remove the space with Service.Delete when no longer wanted.
	StatusGuestRevoked
)

// String returns the canonical wire label for the status — "unknown" /
// "active" / "joining" / "leaving" / "deleted" / "remote_dead" /
// "one_to_one_pending" / "one_to_one_declined" / "invite_pending" /
// "invite_declined" / "guest_revoked". Unknown values stringify as
// "unknown".
func (s Status) String() string {
	switch s {
	case StatusActive:
		return "active"
	case StatusJoining:
		return "joining"
	case StatusLeaving:
		return "leaving"
	case StatusDeleted:
		return "deleted"
	case StatusRemoteDead:
		return "remote_dead"
	case StatusOneToOnePending:
		return "one_to_one_pending"
	case StatusOneToOneDeclined:
		return "one_to_one_declined"
	case StatusInvitePending:
		return "invite_pending"
	case StatusInviteDeclined:
		return "invite_declined"
	case StatusGuestRevoked:
		return "guest_revoked"
	default:
		return "unknown"
	}
}

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

// String returns the canonical wire label for the permission —
// "none" / "reader" / "guest" / "writer" / "admin" / "owner". The
// inverse of ParsePermission; unknown values stringify as "none".
func (p Permission) String() string {
	switch p {
	case PermissionReader:
		return "reader"
	case PermissionGuest:
		return "guest"
	case PermissionWriter:
		return "writer"
	case PermissionAdmin:
		return "admin"
	case PermissionOwner:
		return "owner"
	default:
		return "none"
	}
}

// ParsePermission maps a canonical wire label back onto the enum.
// Unknown labels (including "") parse as PermissionNone — absent and
// no-access are the same answer for every caller.
func ParsePermission(s string) Permission {
	p, _ := ParsePermissionStrict(s)
	return p
}

// ParsePermissionStrict is ParsePermission with an explicit ok: false
// for any label that is not a canonical permission ("none" included as
// valid). For callers validating external input, where an unknown
// label must be an error rather than silently no-access.
func ParsePermissionStrict(s string) (Permission, bool) {
	switch s {
	case "none":
		return PermissionNone, true
	case "reader":
		return PermissionReader, true
	case "guest":
		return PermissionGuest, true
	case "writer":
		return PermissionWriter, true
	case "admin":
		return PermissionAdmin, true
	case "owner":
		return PermissionOwner, true
	default:
		return PermissionNone, false
	}
}
