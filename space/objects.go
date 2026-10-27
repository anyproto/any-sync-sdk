package space

import (
	"context"
	"errors"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/types/template"
)

// ErrTypeRequired is returned by Objects().Create without a Type, by
// Derive when the object has no type and none is given, and by a raw
// write that clears `any.type`: every object has exactly one type.
// A client error → 4xx.
var ErrTypeRequired = errors.New("space: an object needs a type")

// ErrObjectDeleted is returned by ObjectService.Get for an object
// whose tree any-sync records as deleted — here or on a peer, and by
// Derive for a ParentId in that state. Distinct from ErrNotFound: the
// id existed and is gone for good.
var ErrObjectDeleted = errors.New("space: object deleted")

// ErrObjectNotFound is returned by a per-object operation that has to
// open the object's tree — record reads, writes, history — when this
// device has no such tree: the id is unknown here, or the object was
// deleted. One sentinel for both because the caller can act on
// neither — the object is not addressable on this device.
//
// Derive with a ParentId returns it for a parent not yet held here (see
// DeriveObjectOpts.ParentId); a retry after the parent syncs succeeds.
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
	// wipes local any-store state. See docs/object.md §"Deletion".
	Delete(ctx context.Context, objectId string) error
}

// CreateObjectOpts is the input to ObjectService.Create.
type CreateObjectOpts struct {
	// Type is the object's one type (`any.type`). Required: every
	// object has exactly one type (ErrTypeRequired otherwise). A meta
	// id is refused (ErrMetaType).
	Type string
	// Collections are the collections the object is filed under at
	// birth (`any.collections`). Optional.
	Collections []string

	// InitialProperties seeds base-scope property values. Keyed by
	// owner (the type or a collection) → propId → value. An owner the
	// object does not have is rejected — name it in Type / Collections.
	InitialProperties map[string]map[string]any

	// From names an object to copy. The new object takes the source's
	// type (Type is then optional and must equal it:
	// ErrFromTypeMismatch), its collections minus every registered
	// marker (a consumer's registered collections and the template
	// marker are states of the source, never content), its synced
	// `any.*` values, the declared synced values of every owner the new
	// object has, and the live records of every dataset the source's
	// type declares through its parts — a reserved or NoCopy module's
	// excepted — in changes of at most a thousand records after the
	// bootstrap.
	// Record ids are kept except where the generic schema handler
	// derives them (IdAuto); on a dataset that is not Dynamic only the
	// declared fields travel. Collections are added to the copied ones
	// and InitialProperties are written over the copied values (a
	// value is replaced, never cleared). A source filed under the
	// built-in TemplateCollectionId is a template: the row is stamped
	// `any.template` {id, version} with the source's `template.version`
	// (0 when unset); any other source keeps the stamp it has. An
	// unknown source, or one whose row has not synced here yet, is
	// ErrObjectNotFound; a deleted one ErrObjectDeleted; a definition's
	// own row ErrMetaType; a record the dataset's create rules refuse
	// today ErrFromRecordRefused, and the half copy is deleted.
	From string
}

// ErrFromTypeMismatch is returned by Objects().Create when Type names a
// type other than the one CreateObjectOpts.From carries: a copy keeps
// the source's type. A client error → 4xx.
var ErrFromTypeMismatch = errors.New("space: type differs from the copied object's type")

// ErrFromRecordRefused is returned by Objects().Create with From when a
// record of the source no longer passes its dataset's create rules, or
// a handler rejects it at apply: a required field unset since, a
// bespoke create check. The copy is abandoned and the new object
// deleted. The source, not the request, is what needs repair.
var ErrFromRecordRefused = errors.New("space: a record of the copied object is refused by its dataset")

// TemplateCollectionId is the built-in collection the SDK registers in
// every space as the template marker (hidden, builtIn, two properties:
// `title` and `version`). An object filed under it is a template of
// its type; CreateObjectOpts.From copies it.
const TemplateCollectionId = template.CollectionId

// DeriveObjectOpts is the input to ObjectService.Derive.
type DeriveObjectOpts struct {
	Seed []byte
	// Type is set on first materialization; a type the object already
	// has is never replaced. Required unless the object already has one
	// (ErrTypeRequired). A meta id is refused when it would be set
	// (ErrMetaType). Collections it lacks are added on every call
	// ($addToSet, idempotent); optional.
	Type        string
	Collections []string

	// ParentId derives the object as a child bound to this parent. The
	// parent id is hashed into the child's derived id, so a child is
	// re-derivable only with the same ParentId. Deleting the parent
	// cascade-deletes the child's tree and excludes it from cold sync.
	// Empty derives a top-level object.
	//
	// Creating the child needs the parent's tree on this device:
	// ErrObjectNotFound until it has synced, ErrObjectDeleted once it
	// is deleted. A child that is already here (synced ahead of its
	// parent) re-derives regardless.
	ParentId string
}
