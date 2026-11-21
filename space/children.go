package space

import (
	"errors"

	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
)

// The coordinator's refusals around parent and child spaces, re-exported
// so consumers classify them with errors.Is against this package.
var (
	// ErrParentSpaceNotAvailable: the parent space does not exist on
	// the network or is deleted.
	ErrParentSpaceNotAvailable = coordinatorproto.ErrParentSpaceNotAvailable

	// ErrParentSpaceInvalid: the space cannot be a parent. A parent is
	// a regular top-level space of this SDK's header type: not the tech
	// space, a 1-1 space, a derived space or a child. CreateChild
	// returns it for a parent the index already shows as one of those.
	ErrParentSpaceInvalid = coordinatorproto.ErrParentSpaceInvalid

	// ErrParentPermissionDenied: the account's role in the parent space
	// does not allow the call.
	ErrParentPermissionDenied = coordinatorproto.ErrParentPermissionDenied

	// ErrChildSpacesLimitReached: the parent's Owner has as many child
	// spaces as the network allows that account.
	ErrChildSpacesLimitReached = coordinatorproto.ErrChildSpacesLimitReached
)

// ErrNotChildSpace is returned by the calls that act on a child space
// when the index shows the space has no parent.
var ErrNotChildSpace = errors.New("space: not a child space")

// ErrSpaceNotStored is returned by RetryPush on a device that holds no
// copy of the space: only a device that holds a space can push it.
var ErrSpaceNotStored = errors.New("space: not stored on this device")

// ErrOffline is returned by the calls that need an answer from the
// network when no coordinator or node could be reached. Nothing was
// changed.
var ErrOffline = errors.New("space: network not reachable")

// CreateChildRequest is the input to Service.CreateChild.
type CreateChildRequest struct {
	// ParentSpaceId is the space the child is created under. The
	// account is a Writer, an Admin or the Owner of it.
	ParentSpaceId string

	Name        string
	Description string
	IconCID     string
}

// Refusal names the check a child space failed when the coordinator
// refused to sign it at its push.
type Refusal uint8

const (
	// RefusalNone: the coordinator did not refuse the space.
	RefusalNone Refusal = iota
	// RefusalParentNotAvailable: the parent does not exist on the
	// network or is deleted (ErrParentSpaceNotAvailable).
	RefusalParentNotAvailable
	// RefusalParentInvalid: the parent cannot be a parent
	// (ErrParentSpaceInvalid). Final: retrying cannot change it.
	RefusalParentInvalid
	// RefusalParentPermissionDenied: the account is not a Writer, an
	// Admin or the Owner of the parent (ErrParentPermissionDenied).
	RefusalParentPermissionDenied
	// RefusalChildLimitReached: the parent's Owner is at its child
	// spaces limit (ErrChildSpacesLimitReached).
	RefusalChildLimitReached
)

// String returns the canonical wire label for the refusal — "none" /
// "parent_not_available" / "parent_invalid" /
// "parent_permission_denied" / "child_limit_reached". Unknown values
// stringify as "none".
func (r Refusal) String() string {
	switch r {
	case RefusalParentNotAvailable:
		return "parent_not_available"
	case RefusalParentInvalid:
		return "parent_invalid"
	case RefusalParentPermissionDenied:
		return "parent_permission_denied"
	case RefusalChildLimitReached:
		return "child_limit_reached"
	default:
		return "none"
	}
}

// ParseRefusal maps a canonical wire label back onto the enum. Unknown
// labels (including "") parse as RefusalNone.
func ParseRefusal(s string) Refusal {
	for r := RefusalParentNotAvailable; r <= RefusalChildLimitReached; r++ {
		if r.String() == s {
			return r
		}
	}
	return RefusalNone
}
