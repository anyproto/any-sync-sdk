package space

import "context"

// ObjectChange identifies an object that applied a change, paired with
// the per-space applySeq watermark that change advanced it to. It is
// the unit of both the live feed (Subscribe) and the catch-up query
// (ChangedSince).
//
// ApplySeq is the SDK's per-space, strictly local, monotonic apply
// counter. Unlike any-sync's AddSeq (the DAG delivery counter, which
// only DAG-borne changes have), applySeq advances on EVERY apply that
// mutates this space's records: synced changes, the tech-space account
// mirror's applies, and device-local writes — so a consumer cursoring
// on it never misses a non-DAG mutation. Treat it as an opaque
// ordering key within this space on this device: compare and persist
// it as a cursor, never ship it to another peer or treat it as a
// network clock. Records carry the matching per-record stamp as
// `_applySeq`.
//
// Continuity with the pre-applySeq feed (keyed on AddSeq): legacy
// per-object watermarks are backfilled applySeq := addSeq once per
// space, and the allocator seeds past the historical maximum, so a
// cursor persisted in AddSeq units stays valid on the applySeq axis.
//
// Deleted is true when the object was purged (object deletion): the entry
// carries a fresh ApplySeq strictly greater than the object's last content
// change, so it evicts in the same ordered stream as edits. Consuming
// Deleted is MANDATORY for eviction — a deleted object never re-appears as
// a content change.
type ObjectChange struct {
	ObjectId string
	ApplySeq uint64
	Deleted  bool
}

// ChangeIndexAPI is the surface a consumer-side indexer (full-text /
// vector search, etc.) drives to track what changed in a space and
// re-index incrementally. The SDK ships no index of its own and stores
// no cursor — the consumer owns both.
//
// Two paths that reconcile because they share applySeq ordering:
//
//   - Subscribe — best-effort live "this object is dirty" notifications.
//   - ChangedSince — durable catch-up: replay everything past a cursor.
//
// A consumer persists its last-seen ApplySeq, reacts to Subscribe for
// liveness, and on startup (or after a missed event) calls ChangedSince
// from its saved cursor to backfill. Changes to any of the object's
// datasets count, whatever route they arrived on.
//
// Object DELETION IS reported through this feed. A deleted object's
// projection is purged (the SDK keeps no `objects` tombstone; any-sync's
// head storage is the durable delete record), but the purge stamps the
// object's kept `_meta` row with a fresh ApplySeq, so it surfaces once as
// ObjectChange{Deleted:true} at an ApplySeq strictly greater than its last
// content change. The consumer reads change.Deleted and evicts. Once
// Deleted surfaces, any-sync guarantees no later content change follows, so
// per-object consumer state may be dropped.
//
// Rebuild: applySeq is peer-local and renumbers if the SDK store is rebuilt
// (e.g. its DB was wiped). Generation changes exactly then; a consumer whose
// stored Generation differs MUST reset its cursor to 0 and full-reindex from
// a live snapshot (QueryObjects), which re-establishes deletions by absence.
type ChangeIndexAPI interface {
	// MaxApplySeq returns the current upper bound of the cursor — the
	// highest per-object applySeq persisted in this space. 0 when
	// nothing has applied yet.
	MaxApplySeq(ctx context.Context) (uint64, error)

	// ChangedSince returns objects whose applySeq exceeds `since`,
	// ordered ascending, capped at limit (0 = no cap). Page by passing
	// the last returned ApplySeq as the next `since`.
	//
	// Objects already on disk before this SDK version started stamping
	// space scope appear only after their next change ("index from now
	// on" for pre-existing data).
	ChangedSince(ctx context.Context, since uint64, limit int) ([]ObjectChange, error)

	// Subscribe registers cb to fire once per applied change in this
	// space. cb runs synchronously on the apply path — keep it small or
	// hand work off to your own goroutine. The returned cancel is
	// idempotent.
	//
	// Best-effort: a dropped event (crash, slow cb) is recovered by
	// re-running ChangedSince from the consumer's persisted cursor. Do
	// not treat the callback as a durable queue.
	Subscribe(cb func(ObjectChange)) (cancel func())

	// Generation is a per-space epoch that changes iff the SDK store was
	// rebuilt (the applySeq axis renumbered). It is stable across normal
	// restarts. A consumer persists it alongside its cursor; when the
	// returned value differs from the stored one (or the stored cursor
	// exceeds MaxApplySeq, an older-backup restore), it must reset the
	// cursor to 0 and full-reindex from a live QueryObjects snapshot.
	Generation(ctx context.Context) (string, error)
}
