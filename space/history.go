package space

import (
	"context"
	"errors"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
)

// Version history (docs/version-history-proposal.md). The public
// version handle is a ChangeId — the content-hash CID of a DAG change,
// stable across peers and restarts. "State at version X" is the
// projection of exactly X's causal past, inclusive (§3.2): stable on
// every device, at the cost that a historical view may include
// concurrent changes the user hadn't seen at the time.
type Version = string

// History-surface errors.
var (
	// ErrHistoryTruncated — the causal past walks off the locally
	// available history (snapshot horizon or ACL gap). Version history
	// is best-effort-depth by contract, never a durability promise.
	ErrHistoryTruncated = errors.New("space: history: earlier changes not available on this device")
	// ErrViewTooLarge — the requested cut materializes more state than
	// a view may hold; narrow the scope (dataset / record).
	ErrViewTooLarge = errors.New("space: history: view too large — narrow the scope")
	// ErrVersionNotFound — the version (ChangeId) is not present in
	// this device's tree storage.
	ErrVersionNotFound = errors.New("space: history: version not found")
)

// HistoryAPI is the per-space version-history surface (proposal §7).
type HistoryAPI interface {
	// ListChanges lists an object's history, newest first (descending
	// causal order: parents never appear after children; timestamps
	// are author-supplied and display-only). Pagination via the opaque
	// cursor returned in ChangeList. The first call on an object whose
	// index is stale triggers a lazy backfill.
	ListChanges(ctx context.Context, objectId string, f HistoryFilter, limit int, cursor string) (ChangeList, error)

	// ViewAt materializes the object as of version — object or (via
	// filter in the returned view's Records calls) dataset scope. The
	// view holds an in-memory projection of the SYNCED scope only:
	// local/account-scope values have no history in the object's DAG
	// and are excluded rather than misleadingly shown current. The
	// caller must Close.
	ViewAt(ctx context.Context, objectId string, version Version) (HistoricalView, error)

	// RecordAt reconstructs one record as of version — the chat-scale
	// fast path (filtered replay ∩ causal past). Returns nil when the
	// record does not exist at that cut; a deleted record surfaces as
	// its tombstone (check the `_deletedAt` field).
	RecordAt(ctx context.Context, objectId, dataset, recordId string, version Version) (*anyenc.Value, error)

	// Diff computes the structural difference between two versions.
	// base == "" means version's parents — the per-change effect diff
	// ("what did this change actually land", ops gated out by newer
	// concurrent writes excluded). Scope narrows via the filter.
	Diff(ctx context.Context, objectId string, base, version Version, f DiffFilter) (DiffResult, error)
}

// HistoryFilter narrows ListChanges (proposal §7).
type HistoryFilter struct {
	Dataset  string        // only this dataset
	RecordId string        // only changes touching this record (requires Dataset)
	TraceId  string        // only changes carrying this trace id
	Author   string        // identity filter
	Coalesce *CoalesceOpts // group keystroke-grained changes; nil = raw
}

// CoalesceOpts tunes list-time grouping (proposal §7.1): consecutive
// same-author linear-chain changes within Window collapse into one
// entry whose handle is the group's newest ChangeId. Never across
// merges or (page-visible) branches.
type CoalesceOpts struct {
	// Window bounds the author-clock spread between adjacent group
	// members. <=0 = 5 minutes.
	Window time.Duration
}

// ChangeList is one ListChanges page.
type ChangeList struct {
	Changes []ChangeMeta
	// Cursor resumes the NEXT page; "" = history exhausted.
	Cursor string
}

// ChangeMeta describes one listed change (or coalesced group).
type ChangeMeta struct {
	Version   Version // ChangeId; for groups: the head (newest) member
	Author    string  // per-change signer identity
	Timestamp int64   // author clock, Unix seconds — display-only
	Dataset   string
	TraceIds  []string
	Touched   []TouchedRecord
	// Truncated marks the oldest listable entry when the history
	// horizon was hit. RESERVED: always false today — the SDK never
	// writes tree snapshots, so full history is always local. It
	// becomes meaningful with the future snapshot/GC contract
	// (proposal §9); until then only ViewAt/Diff can surface
	// ErrHistoryTruncated (ACL gaps).
	Truncated bool
	GroupSize int // 1 unless coalesced
}

// TouchedRecord names one record a change touched with its op kinds
// (e.g. "$set", "$inc", "$delete").
type TouchedRecord struct {
	Dataset  string
	RecordId string
	Ops      []string
}

// HistoricalView is a read-only projection of an object at a version.
// Backed by an in-memory scratch store; Close releases it. Not safe
// for use after Close.
type HistoricalView interface {
	// Version returns the cut handle the view was built at.
	Version() Version
	// Datasets lists dataset names this view can serve.
	Datasets() []string
	// Record returns one record (nil if absent at this version).
	// Deleted records surface as tombstones (`_deletedAt` set).
	Record(ctx context.Context, dataset, recordId string) (*anyenc.Value, error)
	// Records returns all live records of a dataset at this version.
	Records(ctx context.Context, dataset string) ([]*anyenc.Value, error)
	Close() error
}

// DiffFilter narrows a Diff (proposal §7).
type DiffFilter struct {
	Dataset   string
	RecordIds []string // requires Dataset
}

// DiffKind classifies a record-level difference.
type DiffKind string

const (
	DiffAdded   DiffKind = "added"
	DiffRemoved DiffKind = "removed" // physically absent (horizon/scope), not tombstoned
	DiffChanged DiffKind = "changed"
	DiffDeleted DiffKind = "deleted" // tombstoned at the newer version
)

// FieldDiff is one leaf-level difference; nil Before/After = absent on
// that side. Peer-local bookkeeping (`_ver`, `_traces`, `_applySeq`,
// `_addSeq`) never appears here.
type FieldDiff struct {
	Path   []string
	Before *anyenc.Value
	After  *anyenc.Value
}

// RecordDiff is one record's difference between two versions.
type RecordDiff struct {
	Id     string
	Kind   DiffKind
	Fields []FieldDiff
}

// DatasetDiff groups record diffs of one dataset.
type DatasetDiff struct {
	Dataset string
	Records []RecordDiff
}

// DiffResult is the object-level diff between two versions.
type DiffResult struct {
	Base     Version
	Version  Version
	Datasets []DatasetDiff
}
