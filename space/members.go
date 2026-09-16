package space

import (
	"context"

	"github.com/anyproto/any-sync/util/crypto"
)

// MembersAPI is the read-side facade over a space's ACL state. Reads
// are point-in-time snapshots derived from the locally replicated ACL
// list — fast, in-memory.
//
// Live updates are delivered via Subscribe — a firehose of MemberEvent
// messages covering add / remove / change for both confirmed members
// and pending join requests. The implementation polls the underlying
// AclList head id at a small interval (~250 ms); subscribers see new
// state within one tick of arrival.
type MembersAPI interface {
	// List returns every account currently visible in the ACL: active
	// members, removed-member tombstones (Status=Removed), and pending
	// join requests (Status=Joining). Order is not stable across calls.
	List(ctx context.Context) ([]Member, error)

	// Get returns a single member entry by identity, or ErrNotFound.
	// Pending join requests are reachable here too.
	Get(ctx context.Context, identity string) (Member, error)

	// Me returns the caller's own member entry. Convenient shortcut
	// for surfacing the caller's role in the UI.
	Me(ctx context.Context) (Member, error)

	// JoinRequests returns the subset of List() with Status=Joining —
	// kept as a convenience for admin UIs that approve / decline.
	JoinRequests(ctx context.Context) ([]JoinRequestInfo, error)

	// Invites returns the active invites for this space, with their
	// record ids (use these as inputs to ACL.RevokeInvite).
	Invites(ctx context.Context) ([]InviteInfo, error)

	// Subscribe registers a firehose listener. cb runs synchronously
	// from the watcher goroutine — keep work small or hand off to your
	// own goroutine. The returned cancel function detaches the
	// subscriber; it's safe to call multiple times.
	//
	// Events cover: a new member appearing (active or pending),
	// permission/status flips, and members disappearing (pending
	// requests dropped on accept/decline/cancel; full members keep
	// a Status=Removed tombstone via a Changed event).
	Subscribe(cb func(MemberEvent)) (cancel func())

	// Query returns a chainable query builder over the materialized
	// members system collection. Supports the same Filter / Sort /
	// Limit / Offset shape as Space.Query / Space.QueryObjects.
	//
	// The collection is kept in sync with the underlying ACL by the
	// per-space watcher (poll interval ~250 ms). Reads here may lag
	// in-memory state (List / Get / Me) by at most one tick; for
	// strictly current state use those direct methods.
	//
	// Schema fields (all strings unless noted):
	//   id          — identity (== Member.Identity)
	//   permission  — "owner" / "admin" / "writer" / "reader" / "guest" / "none"
	//   status      — "active" / "joining" / "removed" / "removing" /
	//                 "declined" / "canceled" / "unknown"
	//   name        — decoded display name
	//   description — decoded description
	//   icon        — IconCID
	//   requestId   — present only for status=joining (recordId for accept)
	Query() Query
}

// Member is one row in the members view. Status partitions the rows:
//
//   - MemberStatusActive — confirmed member with read/write access per
//     Permission.
//   - MemberStatusJoining — has an outstanding join request awaiting
//     owner approval. RequestRecordId is set; Permission is None.
//   - MemberStatusRemoved — was a member, removed by the owner. Kept
//     as a tombstone so UIs can render "Alice left this space".
//   - other transient states mirror any-sync's AclStatus.
//
// Name / Description / IconCID are decoded from the metadata blob the
// joiner attached at request time. identityRepo-backed enrichment
// (resolving names/icons published by the account itself elsewhere)
// is a follow-up.
type Member struct {
	Identity    string
	Permission  Permission
	Status      MemberStatus
	Name        string
	Description string
	IconCID     string

	// RequestRecordId is set only when Status == MemberStatusJoining.
	// Pass it to ACL.AcceptRequest / DeclineRequest.
	RequestRecordId string
}

// MemberStatus is the membership lifecycle stage. Mirrors any-sync's
// list.AclStatus, plus MemberStatusJoining for pending join requests
// (which any-sync tracks in a separate map but the SDK surfaces as
// part of the same members view — see docs/space.md § "Members as
// a collection").
type MemberStatus uint8

const (
	MemberStatusUnknown MemberStatus = iota
	MemberStatusJoining
	MemberStatusActive
	MemberStatusRemoved
	MemberStatusDeclined
	MemberStatusRemoving
	MemberStatusCanceled
)

// String returns the canonical wire label for the status — "unknown" /
// "joining" / "active" / "removed" / "declined" / "removing" /
// "canceled". Also the on-disk representation in the members
// collection (see MembersAPI.Query). Unknown values stringify as
// "unknown".
func (s MemberStatus) String() string {
	switch s {
	case MemberStatusJoining:
		return "joining"
	case MemberStatusActive:
		return "active"
	case MemberStatusRemoved:
		return "removed"
	case MemberStatusDeclined:
		return "declined"
	case MemberStatusRemoving:
		return "removing"
	case MemberStatusCanceled:
		return "canceled"
	default:
		return "unknown"
	}
}

// MemberEvent is one delivery on a Subscribe firehose. Kind tells the
// subscriber what changed; Member carries the post-event state;
// Previous carries the pre-event state (nil for Added, set otherwise).
type MemberEvent struct {
	Kind     MemberEventKind
	Member   Member
	Previous *Member
}

// MemberEventKind is the discriminator on MemberEvent.
type MemberEventKind uint8

const (
	// MemberEventAdded — the identity wasn't in the previous snapshot.
	// Fires for new active members and new pending join requests.
	MemberEventAdded MemberEventKind = iota + 1

	// MemberEventChanged — same identity, different fields. Fires for
	// permission upgrades/demotions, status flips (e.g. accept moves
	// joining → active), or metadata updates.
	MemberEventChanged

	// MemberEventRemoved — identity gone from the snapshot. Fires for
	// dropped pending requests (decline / cancel) and on full-member
	// removal where the tombstone is also dropped (rare — usually a
	// Changed event with Status=Removed lands instead).
	MemberEventRemoved
)

// JoinRequestInfo is one pending join request, kept as a convenience
// projection over Members.List() for admin UIs.
type JoinRequestInfo struct {
	// RecordId is the ACL record id — pass it to ACL.AcceptRequest.
	RecordId string
	// Identity is the joiner's account id.
	Identity string
	// Name / Description / IconCID are decoded from the metadata blob
	// the joiner attached at request time.
	Name        string
	Description string
	IconCID     string
}

// InviteInfo is one active invite. RecordId is what ACL.RevokeInvite
// expects.
type InviteInfo struct {
	RecordId string
	// Permission applies only to AnyoneCanJoin invites — for the
	// RequestToJoin path it is set at accept time. Always
	// PermissionNone here in v1.
	Permission Permission
	// Key is the invite private key when THIS account minted the
	// invite: recovered from the account's synced issued-key custody
	// (the ACL record carries only the public key), so it is present
	// on every device of the minting account and nil everywhere else —
	// other members', even admins', devices never held it. Also nil
	// for invites minted before custody shipped (re-mint once to make
	// them recoverable) and for custody gone stale (invite replaced /
	// revoked elsewhere). Non-nil Key re-encodes to the original share
	// token via EncodeInvite(Invite{SpaceId, InviteKey: Key}).
	Key crypto.PrivKey
}
