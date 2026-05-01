package crdt

import (
	"github.com/anyproto/any-store/v2/anyenc"
)

// OpType is one of the operation kinds defined in spec §5.
//
// There is no `insert` op. Records are auto-created by the first modify that
// targets a non-existent id (the record starts empty with `_ver = {}` and the
// op then runs as normal). This eliminates the dual-insert convergence
// problem and the "skip if exists" footgun. See the spec divergence memory
// note and docs/05a-crdt-spec.md §5 for rationale.
type OpType string

const (
	OpSet      OpType = "$set"
	OpUnset    OpType = "$unset"
	OpAddToSet OpType = "$addToSet"
	OpPull     OpType = "$pull"
	OpInc      OpType = "$inc"
	OpIncGated OpType = "$incGated"
	OpDelete   OpType = "delete"
)

// Op is one operation inside a RecordChange.
//
// Path is the dotted field path the op applies to. For $set and $unset, an
// empty Path activates the multi-field form: Payload is interpreted as an
// object whose keys are dot-separated field paths and whose values are
// applied as parallel $set/$unset ops sharing the change's versionId.
//
// Payload is op-specific (see spec §5):
//   - $set:       value to assign at Path, OR a multi-field object when Path is empty
//   - $unset:     unused for single-path; multi-field form uses an object whose values are ignored
//   - $addToSet:  the element to add to the set at Path
//   - $pull:      the element to remove from the set at Path
//   - $inc:       a number value, the delta
//   - $incGated:  same as $inc
//   - delete:     unused (nil); the record id comes from RecordChange.Id
type Op struct {
	Type    OpType
	Path    []string
	Payload *anyenc.Value
}

// RecordChange groups all ops applied to one record inside a single change.
//
// Upsert toggles the create-on-absent behavior for the modify ops in this
// batch:
//
//   - false (default, strict): if the target id has no record, every modify
//     in Ops is a silent no-op. Use this for "update if exists" semantics —
//     the safe default that prevents accidental record creation from typos
//     or stale ids.
//
//   - true: an empty record `{id, _ver:{}}` is created when the target id is
//     absent, and the ops then run normally. This is the explicit "create
//     or update" mode and is how callers create records (there is no
//     separate insert op). The first creating change is typically a
//     multi-field $set with Upsert=true.
//
// Tombstones are sticky in both modes: a delete'd record never resurrects
// regardless of the flag.
//
// `delete` ops ignore the flag — they always produce a tombstone (creating
// one on absent records too, so deletes that race ahead of creates still
// give "delete wins absolutely").
type RecordChange struct {
	Id     string
	Ops    []Op
	Upsert bool

	// Variant routes the ops into a top-level subdocument on the
	// record (e.g. "_base", "_account", "_device"). Empty string
	// means "no variant" — ops apply to the record root, the v1
	// behavior every existing dataset uses. When non-empty, the
	// apply loop ensures the subdocument exists, then runs ops
	// against it; `_ver` stamps land inside that subdoc, so per-
	// variant LWW comparisons stay structurally isolated.
	//
	// Tombstones (delete ops) and creation markers (`_ver.id`) live
	// at the record root regardless of variant — deletion is a
	// record-level event, not a variant-level one.
	//
	// See docs/06-data-structure.md § "Storage — Proposal 2".
	Variant string
}

// Change is one batch — exactly one any-sync DAG change. Every op in the batch
// shares the same VersionId and targets the same Dataset inside the same
// (SpaceId, ObjectId) tree.
//
// Three identifiers travel with each change, playing three different roles:
//
//   - VersionId (= any-sync orderId) — lexicographically sortable DAG order
//     used for gating. Scoped to one object tree. See VersionId docs.
//   - ChangeId — content-addressable, immutable DAG change hash. Used for
//     observability and as the seed for the "empty record id → ChangeId"
//     sugar on RecordChange. Distinct from VersionId.
//   - AddSeq — monotonic local delivery sequence assigned by any-sync's
//     spacestorage as changes are received. VersionId says "where in the DAG
//     did this happen"; AddSeq says "when did we receive it locally". The
//     Controller tracks the max AddSeq it has observed so the space layer
//     can efficiently ask any-sync "give me everything with seq > max" on
//     restore or reindex.
type Change struct {
	SpaceId   string
	ObjectId  string
	Dataset   string
	ChangeId  string
	VersionId VersionId
	AddSeq    uint64
	Records   []RecordChange
	// DataVersion pins the change to a specific schema/handler version. Its
	// meaning depends on the dataset:
	//
	//   - Data dataset on a user object: handler version (Phase 1: hardcoded
	//     in the SDK; later: a user-type-defined schema version).
	//   - `properties` dataset (per-space or per-type-object): shortId of the
	//     last "important" change to the governing type object.
	//
	// The string is opaque to the CRDT layer — only equality matters, not
	// ordering. Empty is invalid and causes ApplyChange to reject the change.
	// See docs/types-properties-proposal.md § "Change-level DataVersion".
	DataVersion string
	// Timestamp carried alongside the change for tombstones (delete writes a
	// `deletedAt` field). The CRDT layer treats it as advisory metadata; it is
	// not used for conflict resolution.
	Timestamp int64
	// ObjectAuthor is the StrKey-encoded identity (PubKey.Account()) of
	// the peer that created the OBJECT — i.e. the signer of the tree's
	// root change. Constant across every change in a tree; the apply
	// pipeline stamps it before calling Controller.ApplyChange. Used by
	// SystemPropertiesHandler to auto-stamp `author` at row root on
	// first record creation. Empty on hand-built changes (tests) where
	// no tree is wired.
	ObjectAuthor string
	// ObjectCreatedAt is the Unix-seconds timestamp of the tree's root
	// change — the moment the object was created. Constant across every
	// change in a tree. Used by SystemPropertiesHandler to auto-stamp
	// `createdAt` at row root on first record creation.
	ObjectCreatedAt int64
	// TraceIds are opaque correlation tokens attached by the originating
	// caller (e.g. a UI session id, an AI operation id). They travel with the
	// change through any-sync (stored in a dedicated, non-encrypted field on
	// the wire) so the whole space can be queried by trace. On the record
	// side, the CRDT stamps them into `_traces[versionId]` for any fields
	// this change actually landed, so per-record queries can map a field's
	// `_ver` entry back to the traces that produced it. An empty slice means
	// "no trace for this change" — any prior `_traces[versionId]` entry for
	// this versionId is cleared.
	TraceIds []string
}
