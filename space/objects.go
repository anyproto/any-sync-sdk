package space

import "context"

// ObjectService is the object lifecycle surface on a space.
// Create/Derive/Delete only — reads, writes, and subscriptions happen
// at the space level keyed by objectId.
type ObjectService interface {
	// Create a fresh object. Returns the any-sync-assigned objectId.
	// Types attached here seed the object's any.types list at birth;
	// InitialProperties seeds the per-space properties record.
	Create(ctx context.Context, opts CreateObjectOpts) (objectId string, err error)

	// Derive a deterministic object from a seed. Re-runs idempotently;
	// second call with the same seed returns the same objectId.
	Derive(ctx context.Context, opts DeriveObjectOpts) (objectId string, err error)

	// Delete marks the object as deleted (any-sync settings tree) and
	// wipes local any-store state. See docs/04-object.md §"Deletion".
	Delete(ctx context.Context, objectId string) error
}

// CreateObjectOpts is the input to ObjectService.Create.
type CreateObjectOpts struct {
	// Types attaches type objects at birth. Each typeId is appended to
	// any.types.
	Types []string

	// InitialProperties seeds base-scope property values. Keyed by
	// typeId → propId → value.
	InitialProperties map[string]map[string]any
}

// DeriveObjectOpts is the input to ObjectService.Derive.
type DeriveObjectOpts struct {
	Seed []byte
	// Types to attach on first materialization. Ignored on subsequent
	// calls once the object exists.
	Types []string

	// ParentId derives the object as a child bound to this parent. The
	// parent id is hashed into the child's derived id, so a child is
	// re-derivable only with the same ParentId. Deleting the parent
	// cascade-deletes the child's tree and excludes it from cold sync.
	// Empty derives a top-level object.
	ParentId string
}
