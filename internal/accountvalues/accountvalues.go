// Package accountvalues defines the tech-space carrier for
// account-scoped values and the pure diff that mirrors carrier state
// into target-space records. See docs/scoped-properties-proposal.md
// § "Account transport".
//
// Topology: the tech space hosts ONE derived carrier object per target
// space (bounded DAG, 1:1 lifecycle, one-tree GC on space leave). Its
// `account_values` dataset holds one record per (objectId, dataset,
// recordId) of the target space; record fields are the account-scoped
// paths VERBATIM (`{typeId}.{propId}` for the objects row), so the
// record's own `_ver` — tech-tree versionIds, including entries
// retained by $unset — is the version source the mirror replays.
package accountvalues

import (
	"cmp"
	"slices"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/anyenc/anyencutil"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// Dataset is the carrier dataset name on the per-space carrier object.
const Dataset = "account_values"

// HandlerVersion is the DataVersion stamped on carrier changes. The
// tech space is account-private (owner-only ACL) — the only writer is
// the SDK itself; bump if the record shape ever changes incompatibly.
const HandlerVersion = "accountValuesHandler-v1"

// DeriveSeedPrefix builds the deterministic derive seed for a target
// space's carrier object: every device of the account mints the same
// carrier object id for the same target space.
const DeriveSeedPrefix = "builtin:accountValues/"

// DeriveSeed returns the carrier object's derive seed for spaceId.
func DeriveSeed(spaceId string) string { return DeriveSeedPrefix + spaceId }

// Key builds a carrier record id for a target (objectId, dataset,
// recordId). Colon is not a valid character in any-sync's
// content-addressable ids, so the segments can't collide. The objects
// row is the degenerate case: Key(objId, "objects", objId).
func Key(objectId, dataset, recordId string) string {
	return objectId + ":" + dataset + ":" + recordId
}

// ParseKey splits a carrier record id. ok=false on a malformed key.
func ParseKey(key string) (objectId, dataset, recordId string, ok bool) {
	parts := strings.SplitN(key, ":", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

// KeyPrefixForObject returns the id range prefix selecting every
// carrier record of one target object (any dataset/recordId). Used by
// the object-delete GC.
func KeyPrefixForObject(objectId string) string { return objectId + ":" }

// ScopeResolver reports the declared scope of a (typeId, propId) pair
// in the TARGET space. known=false means the definition hasn't synced
// to this device yet — the diff skips such paths (never drops them:
// the carrier retains the value and the next re-mirror retries once
// the schema arrives).
type ScopeResolver func(typeId, propId string) (scope schema.Scope, known bool)

// Batch is one injected apply: every op in it carries the same
// carrier-tree versionId. The mirror issues one InjectedSet per Batch
// so per-path gating replays the carrier's converged order EXACTLY —
// stamping a single max version over many paths would over-claim.
type Batch struct {
	VersionId crdt.VersionId
	Ops       []crdt.Op
}

// Diff computes the injected applies that bring the target record's
// account-scoped paths in line with the carrier record. Pure function
// — both mirror paths (live carrier events and the on-load state
// re-mirror) share it, and it's table-testable.
//
//   - carrier value present  ⇒ $set at the path's carrier version;
//   - carrier value absent but the carrier `_ver` retains (or covers)
//     a version for a path the TARGET still holds ⇒ $unset at that
//     version — this is how unsets reach devices that were offline;
//   - unknown propId (resolver) ⇒ skip — Hole A, retried by the next
//     re-mirror once the definition syncs;
//   - declared scope ≠ account ⇒ skip (defensive — a buggy or stale
//     carrier entry must not touch synced/local paths);
//   - target version ≥ carrier version ⇒ no-op (already applied; the
//     injected gate would drop it anyway, skipping avoids event noise).
//
// Op payloads are deep-cloned onto arena so the result outlives the
// carrier record's buffer. Batches are ordered by ascending VersionId.
// Returns nil when the records are already in sync.
func Diff(arena *anyenc.Arena, carrier, target *anyenc.Value, resolve ScopeResolver) []Batch {
	if carrier == nil {
		return nil
	}
	byVersion := make(map[crdt.VersionId][]crdt.Op)

	// Pass 1 — carrier values: $set anything the target hasn't caught
	// up to.
	visitValuePaths(carrier, func(typeId, propId string, val *anyenc.Value) {
		if !accountPath(resolve, typeId, propId) {
			return
		}
		v := crdt.GetRecordVersion(carrier, typeId, propId)
		if v == "" {
			return // value without a version — malformed carrier row; leave it
		}
		if crdt.GetRecordVersion(target, typeId, propId) >= v {
			return
		}
		byVersion[v] = append(byVersion[v], crdt.Op{
			Type:    crdt.OpSet,
			Path:    []string{typeId, propId},
			Payload: cloneValue(arena, val),
		})
	})

	// Pass 2 — unsets: paths the TARGET holds that the carrier no
	// longer values but still claims (retained or covering `_ver`
	// entry). A path the carrier never claimed (version "") is left
	// alone — no authority to clear it.
	if target != nil {
		visitValuePaths(target, func(typeId, propId string, _ *anyenc.Value) {
			if carrier.Get(typeId, propId) != nil {
				return // valued — pass 1 owns it
			}
			if !accountPath(resolve, typeId, propId) {
				return
			}
			v := crdt.GetRecordVersion(carrier, typeId, propId)
			if v == "" {
				return
			}
			if crdt.GetRecordVersion(target, typeId, propId) >= v {
				return
			}
			byVersion[v] = append(byVersion[v], crdt.Op{
				Type: crdt.OpUnset,
				Path: []string{typeId, propId},
			})
		})
	}

	if len(byVersion) == 0 {
		return nil
	}
	out := make([]Batch, 0, len(byVersion))
	for v, ops := range byVersion {
		// Deterministic op order within a batch for testability.
		slices.SortFunc(ops, func(a, b crdt.Op) int {
			return cmp.Compare(pathKey(a.Path), pathKey(b.Path))
		})
		out = append(out, Batch{VersionId: v, Ops: ops})
	}
	slices.SortFunc(out, func(a, b Batch) int { return cmp.Compare(a.VersionId, b.VersionId) })
	return out
}

// accountPath reports whether (typeId, propId) is a KNOWN
// account-scoped property. Unknown or non-account ⇒ false (skip).
func accountPath(resolve ScopeResolver, typeId, propId string) bool {
	if resolve == nil {
		return false
	}
	sc, known := resolve(typeId, propId)
	return known && sc == schema.ScopeAccount
}

// visitValuePaths walks a record's two-segment value paths
// (`{typeId}.{propId}`), skipping reserved fields (`id`, `_`-prefixed)
// and non-object heads (root scalars like author/createdAt/spaceId).
func visitValuePaths(rec *anyenc.Value, fn func(typeId, propId string, val *anyenc.Value)) {
	if rec == nil || rec.Type() != anyenc.TypeObject {
		return
	}
	obj, _ := rec.Object()
	obj.Visit(func(head []byte, sub *anyenc.Value) {
		typeId := string(head)
		if typeId == crdt.IdField || strings.HasPrefix(typeId, "_") {
			return
		}
		if sub.Type() != anyenc.TypeObject {
			return
		}
		subObj, _ := sub.Object()
		subObj.Visit(func(prop []byte, val *anyenc.Value) {
			fn(typeId, string(prop), val)
		})
	})
}

func pathKey(p []string) string { return strings.Join(p, ".") }

// cloneValue deep-copies v onto arena (Diff results must outlive the
// carrier record's read buffer). Delegates to anyencutil.Copy so every
// anyenc type carries through — a hand-rolled switch turned an unlisted
// type (dateTime, objectID, vectorF32) into a nil payload, which the
// apply path drops silently, losing the account-scoped value on every
// device.
func cloneValue(arena *anyenc.Arena, v *anyenc.Value) *anyenc.Value {
	if v == nil {
		return nil
	}
	return anyencutil.Copy(arena, v)
}

// Sanity guard used by tests and the techspace registration: the
// dataset must stay Dynamic (carrier records carry free-form typeId
// heads).
func Schema() schema.Dataset { return schema.Dataset{Dynamic: true} }
