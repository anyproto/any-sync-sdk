package space

import (
	"context"

	"github.com/anyproto/any-store/v2/anyenc"
)

// PropertiesAPI reads and writes the per-object property record in the
// space's shared `objects` collection (one row per object).
//
// Every property lives in exactly ONE scope, declared on its
// definition (see Scope / PropertyDraft.Scope). Values sit at their
// normal `{typeId}.{propId}` paths regardless of scope — there are no
// per-scope record fields and no read-time merging. What the scope
// decides is the WRITE ROUTE and the version domain:
//
//   - synced  — the object's own CRDT change; syncs to everyone with
//     access; VersionId is the object tree's orderId.
//   - account — a carrier record in the account's private tech space,
//     mirrored into this row on each of the account's devices;
//     VersionId is the tech tree's orderId. Invisible to other members.
//   - local   — written straight into this device's row, never synced;
//     VersionId is a locally-minted lexid.
//
// Read path: Get returns the row verbatim. In a shared space,
// account/local-scoped values reflect THIS account/device — queries
// and filters over them select per-account / per-device result sets.
//
// Write path: one auto-routing Set. The SDK resolves each patch key's
// declared scope and issues the write on that scope's route. A patch
// whose keys span MORE THAN ONE scope is rejected (routes commit
// independently and cannot be rolled back together — callers issue one
// call per scope instead). One call = one route = one VersionId domain
// in the returned ModifyResult.
//
// SetType writes the object's one type (`any.type`, a scalar LWW
// register — every object has exactly one, so there is no unset);
// AttachCollection / DetachCollection edit its collections set
// (`any.collections`). Membership is structural and shared, so all
// three always route through the object's own CRDT (synced).
type PropertiesAPI interface {
	Get(ctx context.Context, objectId string) (*anyenc.Value, error)

	// Set merges the patch into the object's property record. ownerId
	// is the type or collection whose properties the patch names; keys
	// are propIds and all must resolve to the SAME declared scope.
	// Unknown keys, kind mismatches, mixed-scope patches, and an owner
	// the object does not have are rejected before anything is
	// written.
	Set(ctx context.Context, objectId, ownerId string, patch map[string]any) (ModifyResult, error)

	// SetType replaces the object's type. The previous type's values
	// and dataset records become orphan data, read-tolerant; its
	// datasets refuse further writes. A known collection id is refused
	// (ErrWrongSlot).
	SetType(ctx context.Context, objectId, typeId string) (ModifyResult, error)

	// AttachCollection adds the object to a collection ($addToSet —
	// idempotent). A known type id is refused (ErrWrongSlot).
	AttachCollection(ctx context.Context, objectId, collectionId string) (ModifyResult, error)
	// DetachCollection removes the object from a collection ($pull).
	// Values in that namespace become orphan data, read-tolerant.
	DetachCollection(ctx context.Context, objectId, collectionId string) (ModifyResult, error)
}
