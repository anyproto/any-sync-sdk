package space

import (
	"context"

	"github.com/anyproto/any-store/v2/anyenc"
)

// PropertiesAPI reads and writes the per-object property record in
// the space's `properties` system dataset.
//
// Read path: Get returns a single record, with variants collapsed by
// priority device > account > base to produce the computed root values.
// Opt into variant visibility via PropertyReadOpts.
//
// Write path: three scope-specific setters. Patches are keyed by
// either propId or x-key; the API resolves x-keys against the type's
// current property definitions. Unknown keys are rejected.
//
//   - SetBase    — routes through the target object's own CRDT. Syncs
//     to everyone with access. Returns the VersionId of the resulting
//     change.
//   - SetAccount — routes through the tech space's rewrite object.
//     Syncs to this account's other devices only. Returns the tech-space
//     VersionId.
//   - SetDevice  — local-only write. Not synced anywhere. No VersionId.
//
// AttachType / DetachType modify the object's `any.types` list. Both
// route through the target object's CRDT (base-scope metadata).
type PropertiesAPI interface {
	Get(ctx context.Context, objectId string, opts PropertyReadOpts) (*anyenc.Value, error)

	SetBase(ctx context.Context, objectId, typeId string, patch map[string]any) (ModifyResult, error)
	SetAccount(ctx context.Context, objectId, typeId string, patch map[string]any) (ModifyResult, error)
	SetDevice(ctx context.Context, objectId, typeId string, patch map[string]any) error

	AttachType(ctx context.Context, objectId, typeId string) (ModifyResult, error)
	DetachType(ctx context.Context, objectId, typeId string) (ModifyResult, error)
}

// PropertyReadOpts controls which reserved fields appear in the
// record returned by Get. The computed root values (collapsed by
// priority) are always present.
type PropertyReadOpts struct {
	// IncludeVariants returns the _device, _account, _base namespaces
	// alongside the computed root. Useful for "show me why this value
	// is the way it is" debugging or settings UI.
	IncludeVariants bool

	// IncludeMeta returns _ver, _traces, _deletedAt. Needed if the
	// caller wants per-field version info to reconcile optimistic
	// state.
	IncludeMeta bool
}
