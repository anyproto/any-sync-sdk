// The space-index handler — the crdt.Handler for the tech space's
// list-of-spaces dataset. See doc.go for the package role.

package techspace

import "github.com/anyproto/any-sync-sdk/internal/crdt"

// SpaceIndexDeriveSeed mints the same space-index object id on every
// device for a given account. Tech space has exactly one space-index
// object, always derived from this seed, always owned by the
// account's own (owner-only) ACL.
const SpaceIndexDeriveSeed = "builtin:spaceIndex"

// SpaceIndexDataset is the name of the dataset on the space-index
// object that holds one record per space. Record id is the spaceId;
// fields are the caller-visible space metadata (type, name, icon,
// localStatus, remoteStatus, …) per docs/02-tech-space.md § "Space
// Index".
const SpaceIndexDataset = "spaces"

// HandlerVersion is the DataVersion string stamped on every change
// this handler emits. Tech-space datasets are account-private
// (owner-only ACL), so the only writer is the SDK itself; bump the
// suffix when the schema changes in a way that must reject stale
// writers.
const HandlerVersion = "spaceIndexHandler-v1"

// SpaceIndexHandler validates ops on the tech-space space-index
// dataset. This is the "special handler for tech space entries"
// called out in the handler inventory — it is not a general-purpose
// per-object handler (regular objects use
// properties.SystemPropertiesHandler), it guards a single derived
// object unique to the tech space.
//
// The concrete validator will enforce, per docs/02-tech-space.md
// § "Space Index" and § "Key Decisions (continued)":
//
//   - record id must be a well-formed spaceId (record == one space);
//   - `type` is first-write-wins (a space's type is immutable once
//     the index entry exists);
//   - `localStatus` / `remoteStatus` transitions follow a fixed
//     lattice — deleted is terminal (docs: "deleted spaces stay in
//     the index with status=deleted, never physically removed"), so
//     deletion writes a status update, not a `delete` op;
//   - only the owner can produce changes (already guaranteed by the
//     owner-only ACL at the any-sync layer; the handler is defence
//     in depth).
//
// Scaffolded as a no-op until the space-lifecycle work in
// space.Indexer is wired up.
type SpaceIndexHandler struct{}

func (SpaceIndexHandler) Dataset() string                              { return SpaceIndexDataset }
func (SpaceIndexHandler) Version() int                                 { return 1 }
func (SpaceIndexHandler) Validate(_ crdt.RecordChange, _ crdt.Op) error { return nil }
