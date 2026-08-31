package space

import "errors"

// ErrUpsertRequiresUserIds: Upsert only serves datasets declared with
// IdUser — the caller-supplied record id is the idempotency key the
// diff runs on. Auto-derived-id datasets have no stable caller-side
// identity to upsert against.
var ErrUpsertRequiresUserIds = errors.New("space: upsert requires an id:user dataset")

// ErrImmutableFieldChanged marks an upsert record whose payload would
// change a write-once field on an existing record. The record is
// rejected explicitly (never silently skipped — that would hide caller
// data loss); the rest of the page proceeds.
var ErrImmutableFieldChanged = errors.New("space: upsert would change an immutable field")

// ErrUpsertNotAuthor marks an upsert record whose payload changes an
// author-mutable field on a record this account did not create. Checked
// client-side against the stored creator stamp so the rejection is
// deterministic instead of an apply-time surprise.
var ErrUpsertNotAuthor = errors.New("space: upsert would edit another author's field")

// UpsertRecord is one row of an UpsertBatch: the caller id plus the
// desired field values (top-level field → value; values follow
// Op.Value conventions).
type UpsertRecord struct {
	Id     string
	Fields map[string]any
}

// UpsertBatch is the input to Space.Upsert — generic schema-driven
// batch ingest: upsert by record id, diff only declared-mutable fields
// against stored values, skip identical records, one change per page.
//
// Semantics per record:
//   - absent  → created (one multi-field $set carrying all fields);
//   - present → only declared-mutable fields are diffed; changed ones
//     become single-path $set ops (per-field granularity for the
//     schema handler); identical records emit nothing;
//   - present with a differing write-once field → whole-record
//     rejection (ErrImmutableFieldChanged);
//   - stored tombstone → rejection (ErrRecordDeleted; ids never reuse).
//
// Not transactional against concurrent writers: the read-diff-write
// window resolves by per-path LWW like any other write. The intended
// deployment is a single ingest writer per dataset.
//
// Contract: a user-supplied record id must have a single writer. The
// id doubles as the idempotency key, and CONCURRENT creates of the
// same id by different members are outside the contract — creation
// verdicts (required fields, the creator stamp behind author gates)
// are taken by whichever create a replica applies first, so racing
// writers can observe different creators per replica. One ingest
// writer per dataset (or per id range) keeps this trivially true.
type UpsertBatch struct {
	ObjectId string
	Dataset  string
	Records  []UpsertRecord
	// PageSize caps records per emitted change. 0 = 500.
	PageSize int
	TraceIds []string
}

// UpsertRejection reports one rejected record: its batch index, id,
// and cause (errors.Is-matchable).
type UpsertRejection struct {
	Index int
	Id    string
	Err   error
}

// UpsertResult aggregates the batch outcome. Pages holds one
// ModifyResult per emitted change, in page order (pages with nothing
// to write are absent).
type UpsertResult struct {
	Pages      []ModifyResult
	Created    int
	Updated    int
	Skipped    int
	Rejections []UpsertRejection
}
