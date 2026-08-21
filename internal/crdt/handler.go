package crdt

import (
	"context"
	"errors"
	"slices"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// ErrUnknownDataset is returned when a change targets a dataset that has no
// registered handler. Per spec §8.2 the SDK still persists such changes via
// any-sync; this in-memory state just skips them.
var ErrUnknownDataset = errors.New("crdt: no handler for dataset")

// ErrValidation is the sentinel returned (via errors.Join) when a handler
// rejects an operation. Per-op rejections drop just the offending op from
// the apply step; whole-change validation failures (path syntax) abort the
// Change entirely.
var ErrValidation = errors.New("crdt: validation rejected")

// ErrRecordDeleted is the rejection sentinel for a modify (including
// upsert) that landed on a tombstoned record. Delete-wins absorbs the
// write — the tombstone is sticky and none of the ops land — and
// without this rejection the absorbed write is indistinguishable from
// a successful create to a local caller (ModifyResult with recordIds
// and no rejections, nothing stored). The absorption itself is the
// convergence rule and stays; this sentinel only makes it visible.
// Deleting an already-deleted record stays silent (idempotent).
var ErrRecordDeleted = errors.New("crdt: record is deleted; the id cannot be reused")

// ChangeCtx is the per-callback context handed to a Handler. It carries the
// originating Change (read-only metadata: VersionId, ChangeId, Timestamp,
// Creator, ObjectAuthor, …) and the record's pre-op state.
//
// ObjectAuthor is the constant root-signer (object creator); Creator is the
// per-change signer (who wrote THIS change). For the root change they
// coincide; for shared spaces / multi-author objects they diverge — handlers
// that gate "only the author of this message can edit it" should read
// Creator, while handlers that stamp object-level provenance should read
// ObjectAuthor.
//
// Before evolves across ops in the same RecordChange: op[1]'s Before is
// op[0]'s after — but only counting ops that actually landed (rejected ops
// don't mutate state). Nil during BeforeCreate, since the record didn't
// exist before this Change.
//
// Pointer-passed; do not retain the pointer past the callback.
type ChangeCtx struct {
	Change *Change
	Before *anyenc.Value
	// SelfIdentity is this replica's account identity
	// (PubKey.Account() encoding). Populated ONLY on the read-tracking
	// classify path — read state is device-local, so identity-relative
	// verdicts (ReadClassification.Audience) are sound there. It stays
	// zero in the Before* handler hooks on purpose: handler validation
	// runs on every replica for the same change and must be
	// replica-independent, or CRDT replicas diverge.
	SelfIdentity string

	// Get resolves the CURRENT value of a record in one of this
	// object's datasets by explicit id — nil when the record is absent,
	// the id is empty, or the invoker wired no reader (hand-built test
	// contexts). Reads happen inside the apply WriteTx, so a record
	// created earlier in the same change is visible. Tombstones are
	// returned as-is (check _deletedAt); their content fields are wiped.
	//
	// Determinism contract for Before* hooks: the hook runs on every
	// replica for the same change and its output must not diverge, so a
	// handler may consult ONLY fields that are immutable post-create
	// (derived creation stamps such as creator/createdAt qualify;
	// freely-edited fields do not) and only on records that are causal
	// ancestors of the triggering change — the writer saw them, so
	// every replica applies them first. Known accepted edge: a record
	// deleted CONCURRENTLY with the triggering change loses its fields
	// on the tombstone, so a replica that applied the delete first
	// reads nil-equivalent state; derived output disagrees between
	// replicas on that race, which is tolerable only because derived
	// ops are local re-derivation, never synced payload.
	Get func(dataset, id string) *anyenc.Value

	// RecordId is the resolved id of the record being classified.
	// Populated ONLY on the read-tracking classify path (like
	// SelfIdentity) — a create with an auto-derived id carries an empty
	// RecordChange.Id, and a classifier that wants to point-read the
	// just-applied record via Get needs the real id. Zero in the
	// Before* handler hooks.
	RecordId string
}

// Sibling describes a write the handler wants applied to a different
// dataset on the same Controller, atomically with the triggering change.
// The Record's `_ver` entries are stamped by the apply loop with the
// triggering Change's VersionId — handlers must not set them.
type Sibling struct {
	Dataset string
	Record  RecordChange
}

// Sink collects same-record derived ops and cross-dataset sibling writes
// emitted during a single RecordChange's Modify callback. Pooled at the
// Controller, reset between RecordChanges, drained by the apply loop.
//
// Derived ops are folded into the same any-store Modify call (zero extra
// reads/writes); sibling writes execute as separate UpsertIds in the same
// transaction after the outer Modify returns.
type Sink struct {
	derived []Op
	sibling []Sibling
}

// Derive queues an op to apply on the same record, in the same Modify
// callback as the triggering op. Inherits the change's VersionId.
func (s *Sink) Derive(op Op) { s.derived = append(s.derived, op) }

// DeriveOnce queues op unless a derived op with the same Path is already
// queued. For per-change stamps (e.g. modifiedAt) emitted from the per-op
// BeforeModify hook, which may fire several times for one RecordChange —
// without the guard each op would queue a duplicate stamp, and every
// duplicate is projected onto the wire as a separate derived op.
func (s *Sink) DeriveOnce(op Op) {
	for i := range s.derived {
		if slices.Equal(s.derived[i].Path, op.Path) {
			return
		}
	}
	s.derived = append(s.derived, op)
}

// Project queues a write to a different dataset on the same Controller.
// Applied in the same WriteTx as the triggering change.
func (s *Sink) Project(dataset string, rec RecordChange) {
	s.sibling = append(s.sibling, Sibling{Dataset: dataset, Record: rec})
}

// reset clears both slices for pool reuse without releasing capacity.
func (s *Sink) reset() {
	for i := range s.derived {
		s.derived[i] = Op{}
	}
	s.derived = s.derived[:0]
	for i := range s.sibling {
		s.sibling[i] = Sibling{}
	}
	s.sibling = s.sibling[:0]
}

// Handler is the lifecycle behavior for one dataset. Hooks fire inside
// any-store's Modify callback so handlers can read pre-state from
// ctx.Before and emit derived or sibling writes via sink without an extra
// DB round-trip. Cross-record reads go through ctx.Get, subject to its
// determinism contract (immutable fields of causal ancestors only) —
// hooks run on every replica and must not diverge.
//
// Per-op error returns from BeforeModify drop just the offending op; other
// ops in the same RecordChange still apply. BeforeCreate / BeforeDelete are
// per-record, and an error there drops the whole RecordChange.
//
// A handler is pure behavior: its dataset name, wire DataVersion, handler
// version, and indexes are declared alongside it in a HandlerReg at
// registration (see NewController / HandlerReg), not via methods on the
// handler. All three Before* methods may be no-ops (see DefaultHandler).
type Handler interface {
	// Init runs once at registration (NewController / RegisterHandler).
	// Use it to acquire dependencies; nil for stateless handlers.
	Init(ctx context.Context) error

	BeforeCreate(ctx *ChangeCtx, rec *RecordChange, sink *Sink) error
	BeforeModify(ctx *ChangeCtx, rec *RecordChange, op *Op, sink *Sink) error
	BeforeDelete(ctx *ChangeCtx, rec *RecordChange, sink *Sink) error
}

// HandlerReg binds a dataset name to its handler behavior and the
// metadata the Controller needs: the handler Version (persisted in _meta
// for re-index decisions; defaults to 1) and any indexes to ensure on the
// dataset's collection. The wire DataVersion peers gate against is a
// write-time concern owned by the caller (it stamps Change.DataVersion),
// not the Controller — so it lives on the public handler.Dataset, not here.
type HandlerReg struct {
	Name    string
	Version int
	Handler Handler
	Indexes []anystore.IndexInfo
	// Schema is the dataset's required, JSON-Schema-compatible field
	// declaration. Each field carries a class (Scope: synced/derived/
	// local/account) the apply path enforces: derived fields are
	// handler-only, local fields never sync, and a dataset that isn't
	// Dynamic rejects undeclared fields. Free-form datasets (shortIds,
	// the per-type `objects` namespace) set Schema.Dynamic.
	Schema schema.Dataset

	// SchemaRev is an opaque fingerprint of the registered schema for
	// runtime-defined datasets. The store compares a resident
	// controller's rev against the current catalog rev to detect stale
	// registrations (a field added/removed after construction) and
	// evict lazily. Empty for static registrations.
	SchemaRev string

	// DynamicScopeByKey marks a Dynamic dataset whose UNDECLARED field
	// heads carry per-key scopes owned by the dataset's own layer (the
	// per-space `objects` dataset: scope lives on the property
	// definition, resolved by the writer's routing and the handler's
	// registry validation — the controller can't see second path
	// segments). For such datasets the head-level local/synced
	// direction check is skipped for undeclared heads; DECLARED fields
	// (e.g. the derived author/createdAt/spaceId) stay fully enforced.
	DynamicScopeByKey bool

	// ReadTracking opts the dataset into read/unread tracking; nil =
	// untracked. See readtracking.go.
	ReadTracking *ReadTracking

	// DisableFilteredReplay opts the dataset out of the record-filtered
	// history fast path (docs/version-history-proposal.md §4.2). The
	// fast path is sound only while apply hooks stay record-local —
	// a handler that reads OTHER records during apply must set this,
	// forcing per-record history through the full-object slow path.
	DisableFilteredReplay bool

	// SkipHistory keeps this dataset out of the persistent history
	// index (docs/version-history-proposal.md §4.4): no index rows are
	// written and the dataset is invisible in history listings. For
	// chatty machine-written datasets (presence-like state) whose
	// permanent index would leak disk for history nobody asks for.
	// DAG changes still retain everything — flipping the flag later
	// just requires a backfill.
	SkipHistory bool
}

// CreateStamper is an optional interface a Handler may implement to
// derive creation stamps — $setCreate offers (creator/author,
// createdAt) that must track the `_ver.id` creation marker. The
// modifier invokes it at exactly the sites that touch the marker: the
// creating branch, and every upsert modify on a live record —
// INDEPENDENT of per-op validation verdicts. Emitting from the per-op
// accept path instead diverges: a change can be accepted as a create
// on one peer and all-rejected as a modify on another (write-once and
// author-gated fields validate asymmetrically), so the marker would
// move without an offer and the stamps would split while `_ver.id`
// agrees. Tied to the marker, the stamps are exactly as convergent as
// the marker itself.
//
// Not consulted on Local/Injected materializations (handler-exclusive
// routes; their rows pick stamps up from the next synced upsert), on
// tombstones (delete-wins wipes fields), or on sibling writes (no
// handler runs there at all).
type CreateStamper interface {
	DeriveCreateStamps(ctx *ChangeCtx, sink *Sink)
}

// LocalPreValidator is an optional interface a Handler may implement
// to validate a LOCAL change before it enters the DAG. The Controller
// calls PreValidate from the local-write path (not on inbound apply);
// a non-nil error rejects the whole write, keeping a malformed change
// out of any-sync entirely. Used for strict writer-side schema
// validation that returns an agent-readable error, while inbound apply
// stays read-tolerant.
//
// `before` is the current value of the change's target record (nil
// when the change creates it). The Controller resolves it once and
// hands it to PreValidate, which inspects ch.Records itself. For the
// shared `objects` dataset every RecordChange collapses onto the
// object's single row, so one `before` suffices.
type LocalPreValidator interface {
	PreValidate(ch *Change, before *anyenc.Value) error
}

// RecordGetter resolves a record's CURRENT value by explicit id (nil
// when absent or the id is empty). Handed to LocalPreValidatorMulti so
// a batch validator can read per-record pre-state.
type RecordGetter func(id string) *anyenc.Value

// LocalPreValidatorMulti is the batch-friendly variant of
// LocalPreValidator: instead of one pre-resolved `before`, the handler
// receives a getter and resolves pre-state per record — required for
// changes carrying N explicit-id records (e.g. recording N networkSigns
// in one change). When a handler implements both interfaces the
// Controller prefers this one.
type LocalPreValidatorMulti interface {
	PreValidateMulti(ch *Change, get RecordGetter) error
}

// DefaultHandler is a no-op handler accepting every op. Useful as a base
// for tests and as a convenient embed; the dataset name / version /
// indexes live on the HandlerReg, not here.
type DefaultHandler struct{}

func (DefaultHandler) Init(_ context.Context) error { return nil }

func (DefaultHandler) BeforeCreate(_ *ChangeCtx, _ *RecordChange, _ *Sink) error { return nil }
func (DefaultHandler) BeforeModify(_ *ChangeCtx, _ *RecordChange, _ *Op, _ *Sink) error {
	return nil
}
func (DefaultHandler) BeforeDelete(_ *ChangeCtx, _ *RecordChange, _ *Sink) error { return nil }
