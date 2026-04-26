package space

import "context"

// Indexer is the seam through which the tech space plugs into space
// lifecycle events. internal/techspace implements it; sdk.Open wires
// it into the space.Service. space/ itself never imports techspace
// (that would reverse the layering).
type Indexer interface {
	// OnSpaceCreated runs after a new space is successfully created or
	// joined. The hook is expected to add a record to the space index
	// in the tech space. Errors are logged but do not abort the
	// creation — the space exists either way.
	OnSpaceCreated(ctx context.Context, spaceId string, meta SpaceInfo) error

	// OnSpaceDeleted runs after local deletion. The hook flips the
	// space index record to Status = StatusDeleted; the record is
	// never physically removed (see docs/02-tech-space.md).
	OnSpaceDeleted(ctx context.Context, spaceId string) error
}
