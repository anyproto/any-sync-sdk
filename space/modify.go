package space

import "github.com/anyproto/any-sync-sdk/internal/crdt"

// ModifyBatch is the caller-facing write batch: one or more record
// changes in a single dataset of a single object. Applied atomically
// and returns one VersionId for the whole batch.
type ModifyBatch struct {
	ObjectId string
	Dataset  string
	Records  []RecordModify

	// TraceIds are opaque caller-supplied correlation tokens that
	// travel through any-sync in a dedicated field so the whole space
	// can be queried by trace. Empty slice means no trace.
	TraceIds []string

	// Scope selects the write route for the whole batch. A write call
	// is single-scope — routes commit in different version domains and
	// there is no cross-route rollback (same rule as PropertiesAPI.Set).
	// Zero value = ScopeSynced: the object's own DAG change, synced to
	// every member. The default, and the only route ModifyMany and
	// Delete support.
	//
	// ScopeLocal is the device-local materialization route: no DAG
	// change, never syncs, VersionId minted by the local lexid
	// allocator; the write flows through Query/Subscribe like any
	// apply. Every op must target a field the dataset schema declares
	// ScopeLocal — ops on fields of any other scope are refused by the
	// apply layer's scope enforcement and surface in
	// ModifyResult.Rejections, like handler rejections on the synced
	// route. Records must already exist: explicit ids, no Upsert —
	// local fields annotate synced records, they don't create them
	// (a strict-mode miss surfaces as an ErrStrictSkipAbsent
	// rejection). TraceIds are rejected (they ride the any-sync
	// change). The shared `objects` dataset is rejected too: its
	// per-property scopes are enforced by the writer, so local
	// property values go through PropertiesAPI.Set.
	//
	// ScopeAccount and ScopeDerived are rejected: derived is
	// handler-only, and the account transport for dataset records is
	// not wired yet — the account mirror handles objects rows only
	// (docs/scoped-properties-proposal.md § Account transport).
	Scope Scope
}

// RecordModify groups ops applied to one record id.
//
// Id: when empty, the CRDT layer derives one from the change's
// ChangeId (base58(xxh3-64(ChangeId))); subsequent empty-id records
// in the same batch get a ":<index>" suffix. Empty id requires
// Upsert=true.
//
// Upsert: false (default) = strict update-if-exists — modifies on an
// absent record are silent no-ops. true = create-or-update; the
// record is auto-created if absent. Tombstones stay sticky in both
// modes.
type RecordModify struct {
	Id     string
	Upsert bool
	Ops    []Op
}

// Op is one mongo-style modifier.
//
// Path is a dotted field path ("a.b.c"). For $set and $unset, an
// empty Path activates the multi-field form: Value is interpreted as
// an object whose keys are paths applied in parallel under one
// VersionId.
type Op struct {
	Type OpType
	Path string
	// Value is the operand — scalar, slice, or map[string]any
	// depending on Type — converted as its JSON form. Extended-JSON
	// wrappers ({"$date": "<RFC 3339>"}, {"$binary": "<base64>"}) are
	// typed values, the same shape a Query.Filter literal uses; a
	// *fastjson.Value or *anyenc.Value passes through as-is. A
	// time.Time or []byte becomes the string encoding/json gives it.
	// The same rule governs every Go value that enters a record:
	// UpsertRecord.Fields, Properties.Set, CreateObjectOpts
	// .InitialProperties, PropertyPatch.Set, DatasetDefPatch.Set.
	Value any
}

// OpType is the set of supported modifiers for v1. No insert op —
// record creation is opt-in via RecordModify.Upsert.
type OpType string

const (
	OpSet      OpType = "$set"
	OpUnset    OpType = "$unset"
	OpAddToSet OpType = "$addToSet"
	OpPull     OpType = "$pull"
	OpInc      OpType = "$inc"
	OpIncGated OpType = "$incGated"
)

// DeleteBatch produces sticky tombstones for the listed record ids.
// Ignores RecordModify.Upsert semantics — tombstones always win and
// are always created, including for records that never existed
// locally (seeds a tombstone to preserve "delete wins absolutely").
type DeleteBatch struct {
	ObjectId  string
	Dataset   string
	RecordIds []string
	TraceIds  []string
}

// ModifyResult bundles the identifiers Space.Modify and Space.Delete
// return for any successful batch.
//
//   - VersionId is the peer-local lexid stamped on the batch's
//     records. Compare with CompareVersion against other VersionIds
//     this peer has observed.
//   - ChangeId is the any-sync DAG change id — content-addressable,
//     stable across peers. Use it for tracing / cross-peer
//     correlation.
//   - RecordIds is the per-record id list aligned to the input
//     RecordModify slice. For records the caller submitted with an
//     empty Id, the resolved value is `base58(xxh3-64(ChangeId))`
//     (with `:<index>` suffix for the second-and-later empty ids in
//     a batch). This is the propId / shortId convention; callers
//     creating types or properties read it from RecordIds[0].
//   - Rejections lists per-op handler rejections — ops that the
//     change carries but the handler refused (kind mismatch,
//     terminal status, immutable field, unknown property…). The
//     change still committed with a fresh VersionId, but those ops
//     did not land. HTTP layers can surface this as a partial
//     success or a hard error per their policy.
type ModifyResult struct {
	VersionId  VersionId
	ChangeId   string
	RecordIds  []string
	Rejections []OpRejection
}

// OpRejection describes one op the handler refused to apply. The
// containing change still committed; this op's effect did not.
//
//   - RecordIndex / RecordId identify the affected record in the
//     input batch.
//   - OpIndex is the index into RecordModify.Ops that was rejected;
//     -1 means the whole record was rejected (BeforeCreate /
//     BeforeDelete).
//   - Reason is the human-readable error string the handler
//     returned. Programmatic discrimination uses the SDK's typed
//     errors via errors.Is on the underlying error — exposed via
//     ReasonErr.
type OpRejection struct {
	RecordIndex int
	RecordId    string
	OpIndex     int
	Reason      string
	ReasonErr   error
}

// ErrRecordDeleted is the ReasonErr (match with errors.Is) of the
// whole-record rejection emitted when a modify or upsert lands on a
// tombstoned record. Record deletion is sticky (CRDT delete-wins):
// the write is absorbed, nothing is stored, and the id can never be
// reused — without this rejection the absorbed write would be
// indistinguishable from a successful create. Deleting an
// already-deleted record stays silent (idempotent).
var ErrRecordDeleted = crdt.ErrRecordDeleted
