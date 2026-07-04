package space

import (
	"context"
	"errors"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// ObjectReadState is one element of the read-state feed: an object
// whose read state changed (new unread, a mark, a cross-device merge,
// a delete clearing entries), at the StateSeq that change advanced it
// to. The feed carries no per-change detail — the consumer re-pulls
// UnreadSnapshot for the object and diffs against what it holds.
type ObjectReadState struct {
	ObjectId string
	// StateSeq is the cursor axis — same per-space monotonic domain
	// as ObjectChange.ApplySeq. Persist the last seen value and pass
	// it back to ChangedSince.
	StateSeq uint64
}

// UnreadChange is one currently-unread change in an object's snapshot.
type UnreadChange struct {
	ObjectId string
	// Dataset the change applied to (a tracked dataset).
	Dataset string
	// ChangeId is the DAG change (cross-peer identity); VersionId is
	// its peer-local order key — for a record-creating change it
	// equals the record's `_ver.id`, so entries join to records with
	// no extra lookup.
	ChangeId  string
	VersionId crdt.VersionId
	AddSeq    uint64
	ApplySeq  uint64
	// RecordIds are the records the change touched; Tags the
	// classifier's labels ("message", "mention", ...).
	RecordIds []string
	Tags      []string
	// StateSeq is the feed watermark at which this entry became
	// unread.
	StateSeq uint64
}

// ReadStateAPI tracks read/unread changes for datasets that opted in
// via handler.Dataset.ReadTracking. Read state is private to the
// account (synced across its devices through the tech space, never
// visible to other space members) and marking is forward-only: a
// change, once read, never becomes unread again.
//
// Same consumption contract as ChangeIndexAPI: Subscribe is a
// best-effort liveness ping; ChangedSince from a persisted cursor is
// the durable catch-up returning dirty OBJECTS (state, not a log — it
// never grows and never prunes); UnreadSnapshot is both the per-object
// pull after a dirty mark and the full resync after a Generation
// change.
type ReadStateAPI interface {
	// Subscribe fires after a committed read-state change for an
	// object. cb runs synchronously on the notifying path — keep it
	// small or hand off.
	Subscribe(cb func(objectId string, stateSeq uint64)) (cancel func())

	// ChangedSince returns objects whose read state advanced past
	// since, ascending by StateSeq, capped at limit (0 = no cap) —
	// one element per object. Re-pull UnreadSnapshot per dirty object
	// and diff against your held set.
	ChangedSince(ctx context.Context, since uint64, limit int) ([]ObjectReadState, error)

	// UnreadSnapshot returns the object's full current unread set
	// (ascending by VersionId) and the object's current stateSeq —
	// the per-object pull after a ChangedSince hit, and the cold
	// start / resync entry point.
	UnreadSnapshot(ctx context.Context, objectId string) ([]UnreadChange, uint64, error)

	// UnreadCounts returns the object's per-tag unread counters.
	UnreadCounts(ctx context.Context, objectId string) (map[string]int, error)

	// MarkRead covers the given changes and their causal ancestry,
	// then publishes the account's read frontier to the account's
	// other devices.
	MarkRead(ctx context.Context, objectId string, changeIds []string) error

	// MarkReadUpTo covers every unread change with VersionId <= upTo —
	// "this and everything before" in this device's display order.
	// "" reads everything. Backs Read(message) / ReadAll() sugar.
	MarkReadUpTo(ctx context.Context, objectId string, upTo crdt.VersionId) error

	// Generation is the same per-space rebuild epoch as
	// ChangeIndexAPI.Generation — cursors reset when it changes.
	Generation(ctx context.Context) (string, error)
}

// ErrReadTrackingDisabled is returned by ReadStateAPI methods when no
// dataset in the space opted into read tracking.
var ErrReadTrackingDisabled = errors.New("space: read tracking not enabled")
