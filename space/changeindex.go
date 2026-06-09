package space

import "context"

// ObjectChange identifies an object that applied a change, paired with
// the per-space AddSeq watermark that change advanced it to. It is the
// unit of both the live feed (Subscribe) and the catch-up query
// (ChangedSince).
//
// AddSeq is any-sync's per-space, peer-local, monotonic delivery
// counter. Treat it as an opaque ordering key within this space on this
// device: compare and persist it as a cursor, but do not assume it
// matches another peer's value for the same change.
type ObjectChange struct {
	ObjectId string
	AddSeq   uint64
}

// ChangeIndexAPI is the surface a consumer-side indexer (full-text /
// vector search, etc.) drives to track what changed in a space and
// re-index incrementally. The SDK ships no index of its own and stores
// no cursor — the consumer owns both.
//
// Two paths that reconcile because they share AddSeq ordering:
//
//   - Subscribe — best-effort live "this object is dirty" notifications.
//   - ChangedSince — durable catch-up: replay everything past a cursor.
//
// A consumer persists its last-seen AddSeq, reacts to Subscribe for
// liveness, and on startup (or after a missed event) calls ChangedSince
// from its saved cursor to backfill. Pure base-content changes count:
// AddSeq advances on a change to any of the object's datasets, not just
// its property values.
type ChangeIndexAPI interface {
	// MaxAddSeq returns the current upper bound of the cursor — the
	// highest per-object AddSeq persisted in this space. 0 when nothing
	// has applied yet.
	MaxAddSeq(ctx context.Context) (uint64, error)

	// ChangedSince returns objects whose AddSeq exceeds `since`, ordered
	// ascending, capped at limit (0 = no cap). Page by passing the last
	// returned AddSeq as the next `since`.
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
}
