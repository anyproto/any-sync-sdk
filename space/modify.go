package space

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
}

// RecordModify groups ops applied to one record id.
//
// Id: when empty, the CRDT layer derives one from the change's
// ChangeId (base58(xxhash64(ChangeId))); subsequent empty-id records
// in the same batch get a "/<index>" suffix. Empty id requires
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
	Type  OpType
	Path  string
	Value any // scalar, slice, or map[string]any depending on Type
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
