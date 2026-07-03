package crdt

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
}

// ReadClassifier classifies one record change for read tracking. It
// runs on the apply path for every change on an opted-in dataset —
// keep it pure and cheap (no I/O, no locks); it sees the same
// ChangeCtx the Before* hooks do. Self-authored changes are born read
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
