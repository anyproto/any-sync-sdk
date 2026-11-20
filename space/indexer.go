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
	// never physically removed (see docs/tech-space.md).
	OnSpaceDeleted(ctx context.Context, spaceId string) error

	// OnSpaceMetadataUpdated mirrors the converged state of the
	// per-space `spaceIndex` derived object into the tech-space row.
	// Fired by the per-space watcher every time the spaceIndex
	// object's property record applies (locally or pushed from a
	// peer). Idempotent overwrite of name/description/icon — `type`
	// stays pinned by the tech-space handler. See
	// docs/space-index-proposal.md for the convergence story.
	OnSpaceMetadataUpdated(ctx context.Context, spaceId string, meta SpaceInfo) error
}
