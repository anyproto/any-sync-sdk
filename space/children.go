package space

import (
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
)

// The coordinator's refusals around parent and child spaces, re-exported
// so consumers classify them with errors.Is against this package.
var (
	// ErrParentSpaceInvalid: the space cannot be a parent. A parent is
	// a regular top-level space of this SDK's header type: not the tech
	// space, a 1-1 space, a derived space or a child. CreateChild
	// returns it for a parent the index already shows as one of those.
	ErrParentSpaceInvalid = coordinatorproto.ErrParentSpaceInvalid
)

// CreateChildRequest is the input to Service.CreateChild.
type CreateChildRequest struct {
	// ParentSpaceId is the space the child is created under. The
	// account is a Writer, an Admin or the Owner of it.
	ParentSpaceId string

	Name        string
	Description string
	IconCID     string
}
