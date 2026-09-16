package space

import (
	"context"
	"errors"

	"github.com/anyproto/any-store/v2/anyenc"
)

// ErrTypeRequired is returned by Objects().Create without a Type, by
// Derive when the object has no type and none is given, and by a raw
// write that clears `any.type`: every object has exactly one type.
// A client error → 4xx.
var ErrTypeRequired = errors.New("space: an object needs a type")

// ErrObjectDeleted is returned by ObjectService.Get for an object
// whose tree any-sync records as deleted — here or on a peer. Distinct
// from ErrNotFound: the id existed and is gone for good.
var ErrObjectDeleted = errors.New("space: object deleted")

// ErrObjectNotFound is returned by a per-object operation that has to
// open the object's tree — record reads, writes, history — when this
// device has no such tree: the id is unknown here, or the object was
// deleted. One sentinel for both because the caller can act on
// neither — the object is not addressable on this device.
//
// Subscribe reports it for a DELETED object but not for an unknown one:
// an id with no tree here yields an empty initial snapshot instead, so a
// subscription registered before the object lands still receives its
// events. Handle the sentinel on the subscribe path too.
//
// Distinct from ErrObjectDeleted, which ObjectService.Get raises for
// the narrower "this id existed and is gone for good"; a consumer that
// needs the distinction reads the row through Get.
var ErrObjectNotFound = errors.New("space: object not found")

// ObjectService is the object lifecycle surface on a space, plus the
// single-object row read. Record reads, writes and subscriptions
// happen at the space level keyed by objectId.
type ObjectService interface {
	// Get returns the object's row from the per-space objects
	// collection (membership and property values). An object whose tree
	// is present locally but that never wrote a row returns {id} only;
	// ErrNotFound when the id is unknown here, ErrObjectDeleted when
	// the object's tree is deleted — both read from the space's
	// any-sync storage.
	Get(ctx context.Context, objectId string) (*anyenc.Value, error)

	// Create a fresh object. Returns the any-sync-assigned objectId.
	// Type and Collections seed the object's membership at birth;
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
	// Type is the object's one type (`any.type`). Required: every
	// object has exactly one type (ErrTypeRequired otherwise).
	Type string
	// Collections are the collections the object is filed under at
	// birth (`any.collections`). Optional.
	Collections []string

	// InitialProperties seeds base-scope property values. Keyed by
	// owner (the type or a collection) → propId → value. An owner the
	// object does not have is rejected — name it in Type / Collections.
	InitialProperties map[string]map[string]any
}

// DeriveObjectOpts is the input to ObjectService.Derive.
type DeriveObjectOpts struct {
	Seed []byte
	// Type is set on first materialization; a type the object already
	// has is never replaced. Required unless the object already has one
	// (ErrTypeRequired). Collections it lacks are added on every call
	// ($addToSet, idempotent); optional.
	Type        string
	Collections []string

	// ParentId derives the object as a child bound to this parent. The
	// parent id is hashed into the child's derived id, so a child is
	// re-derivable only with the same ParentId. Deleting the parent
	// cascade-deletes the child's tree and excludes it from cold sync.
	// Empty derives a top-level object.
	ParentId string
}
