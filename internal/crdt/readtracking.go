package crdt

import "github.com/anyproto/any-store/v2/query"

// ReadClassification is a ReadClassifier's verdict for one applied
// record change. Track=false records no unread entry, but a non-empty
// Key still clears a previously-tracked same-key entry (a reaction
// toggle: react tracks with a key, un-react clears it).
type ReadClassification struct {
	Track bool
	// Tags label the unread entry and bucket the per-object counters
	// (chat: "message", "mention", "reaction"). Ignored when
	// Track=false.
	Tags []string
	// Key is the optional supersede key: a later classification with
	// the same key replaces (Track=true) or clears (Track=false) the
	// prior unread entry.
	Key string
	// Audience optionally restricts WHO counts this entry unread: the
	// entry tracks only on replicas where the target record (post-
	// apply) matches this typed any-store filter. On every other
	// account the change applies as untracked — a Key still
	// supersedes/clears. nil = everyone (except the change author, who
	// is always born read). Build identity-relative conditions from
	// ChangeCtx.SelfIdentity, e.g. "count a reaction only for the
	// reacted-to message's author":
	//
	//	query.Key{Path: []string{"creator"},
	//	    Filter: query.NewCompValue(query.CompOpEq, arena.NewString(ctx.SelfIdentity))}
	//
	// This is the sanctioned way for a verdict to depend on record
	// state the classifier cannot see (ChangeCtx.Before is nil there):
	// the engine resolves it with one in-tx point read of the record
	// after the change applied. A missing record tracks for nobody.
	// The filter must only consult fields immutable post-create
	// (derived-scope creation stamps qualify; freely-edited fields do
	// not) so the verdict is replay-deterministic. Ignored when
	// Track=false.
	Audience query.Filter
}

// ReadClassifier classifies one record change for read tracking. It
// runs on the apply path for every change on an opted-in dataset —
// keep it pure and cheap (no locks). It receives the change
// envelope: classification runs after the record loop, so
// ChangeCtx.Before is ALWAYS nil here (unlike the Before* hooks) —
// classify from the ops and the envelope (Upsert, Creator, paths),
// never from prior record state. When the verdict must depend on the
// stored record, two tools exist: return an Audience filter and the
// engine resolves it against the post-apply record (it gates the
// WHOLE entry — every tag), or point-read the post-apply record
// yourself via ChangeCtx.Get + ChangeCtx.RecordId (both populated on
// this path) when only PART of the verdict depends on record state —
// e.g. adding a "mention" tag next to an unconditional "message" tag
// by inspecting a handler-derived field. Verdicts are device-local,
// so combining SelfIdentity with post-apply state is sound;
// incremental replays reapply from the persisted watermark, so
// post-apply state at re-classification matches the original run
// (first restore is seed-skipped). Self-authored changes are born read
// regardless of the verdict; the classifier still runs for them so a
// supersede Key can clear entries (own un-react clears the unread
// reaction another device tracked).
type ReadClassifier func(ctx *ChangeCtx, rec *RecordChange) ReadClassification

// ReadSeedMode picks the initial frontier for an object that has no
// stored read state yet.
type ReadSeedMode int

const (
	// ReadSeedAtFirstSight (default) marks everything present at the
	// object's first tracked load as read — a fresh joiner starts
	// clean and only subsequent changes count as unread.
	ReadSeedAtFirstSight ReadSeedMode = iota
	// ReadSeedAllUnread starts with an empty frontier: the object's
	// whole tracked history is unread.
	ReadSeedAllUnread
)

// ReadTracking opts a dataset into read/unread tracking (see
// docs/read-tracking-proposal.md). Attached to the dataset's
// registration; nil = untracked.
type ReadTracking struct {
	// Classify is required: the per-change verdict (track / tags /
	// supersede key).
	Classify ReadClassifier

	// Seed picks the initial frontier for objects with no stored read
	// state.
	Seed ReadSeedMode

	// CounterFields materializes per-tag unread counters as
	// local-scope properties on the object's row in the shared
	// `objects` dataset (tag → property id), e.g.
	// "message" → "unreadCount". Optional.
	CounterFields map[string]string

	// RecordFlags materializes per-record unread booleans as
	// local-scope fields on this dataset's records (tag → field id),
	// e.g. "message" → "unread". A flag is true iff at least one
	// unread entry with that tag references the record. The fields
	// must be declared local-scope in the dataset Schema. Optional.
	RecordFlags map[string]string
}
