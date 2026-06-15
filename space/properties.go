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
// AttachType / DetachType modify the object's `any.types` list. Type
// membership is structural and shared, so both always route through
// the object's own CRDT (synced).
type PropertiesAPI interface {
	Get(ctx context.Context, objectId string) (*anyenc.Value, error)

	// Set merges the patch into the object's property record. Patch
	// keys are propIds; all keys must resolve to the SAME declared
	// scope. Unknown keys, kind mismatches, and mixed-scope patches
	// are rejected before anything is written.
	Set(ctx context.Context, objectId, typeId string, patch map[string]any) (ModifyResult, error)

	AttachType(ctx context.Context, objectId, typeId string) (ModifyResult, error)
	DetachType(ctx context.Context, objectId, typeId string) (ModifyResult, error)
}
