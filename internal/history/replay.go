package history

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/anyenc/anyencutil"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
)

// Engine A: on-demand causal replay (proposal §4.1). "View object O at
// version X" = build the history tree for X's causal past, replay it
// through a fresh Controller into an in-memory scratch store under one
// outer WriteTx, and hand the caller a read-only View.

var (
	// ErrViewTooLarge — the causal cut materializes more records than
	// the guardrail allows; the caller must narrow scope (dataset /
	// record) instead (proposal §9 "Memory").
	ErrViewTooLarge = errors.New("history: view too large — narrow the scope (dataset or record)")
	// ErrVersionNotFound — a requested cut head is not present in this
	// device's tree storage.
	ErrVersionNotFound = errors.New("history: version not found")
	// ErrHistoryTruncated — the causal past walks off the locally
	// available history (missing PrevId: snapshot horizon or ACL gap).
	// Best-effort-depth contract, proposal §9.
	ErrHistoryTruncated = errors.New("history: earlier changes not available on this device")
)

// DefaultMaxViewRecords bounds how many record-rows a single view may
// materialize before the replay aborts with ErrViewTooLarge. Documents
// stay far below this; million-record datasets are meant to go through
// the record-scope fast path.
const DefaultMaxViewRecords = 500_000

// ViewParams describes one causal-cut materialization.
type ViewParams struct {
	ObjectId string
	// Heads are the ChangeIds of the cut; the view is the projection of
	// exactly their causal past, inclusive (proposal §3.2). Usually one
	// ChangeId (the public Version); several for a PrevIds-based
	// per-change base cut. Must be non-empty.
	Heads []string
	// Dataset, when set, replays only changes of this dataset (one
	// change carries exactly one dataset, so this is a clean filter).
	Dataset string

	// Storage/Acl come from the live tree (tree.Storage(), tree.AclList());
	// full-epoch read-key derivation makes old changes decryptable.
	Storage objecttree.Storage
	Acl     list.AclList

	// Regs is the object's current handler set — the same registrations
	// its live Controller runs, so derived fields recompute identically.
	Regs []crdt.HandlerReg
	// SharedDatasets lists datasets that project into an ObjectId-keyed
	// shared collection on the live side (the per-space `objects`
	// dataset). The scratch store gives each its own collection so the
	// view stays self-contained.
	SharedDatasets []string

	// MaxRecords overrides DefaultMaxViewRecords when > 0.
	MaxRecords int
}

// View is a read-only historical projection. Not safe for concurrent
// use with Close. Caller must Close.
type View struct {
	ObjectId string
	// Version is the caller-facing cut handle: the single head ChangeId,
	// or the comma-joined heads for multi-head (PrevIds) cuts.
	Version Version

	db       anystore.DB
	ctrl     *crdt.Controller
	datasets []string
}

// BuildView materializes the causal cut described by p. The scratch
// projection contains synced-scope state only: local/account values
// never enter the DAG, so they cannot appear here (proposal §4.3
// contract) — and no _applySeq is stamped (no allocator on the scratch
// controller).
func BuildView(ctx context.Context, p ViewParams) (*View, error) {
	if p.ObjectId == "" {
		return nil, errors.New("history: BuildView: ObjectId required")
	}
	if len(p.Heads) == 0 {
		return nil, errors.New("history: BuildView: at least one cut head required")
	}
	if p.Storage == nil || p.Acl == nil {
		return nil, errors.New("history: BuildView: Storage and Acl required")
	}

	tree, err := objecttree.BuildNonVerifiableHistoryTree(objecttree.HistoryTreeParams{
		Storage:         p.Storage,
		AclList:         p.Acl,
		Heads:           p.Heads,
		IncludeBeforeId: true,
	})
	if err != nil {
		return nil, mapTreeErr(err)
	}

	db, err := anystore.Open(ctx, ":memory:", &anystore.Config{InMemory: true})
	if err != nil {
		return nil, fmt.Errorf("history: open scratch store: %w", err)
	}
	view, err := replayIntoScratch(ctx, db, tree, p)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return view, nil
}

func replayIntoScratch(ctx context.Context, db anystore.DB, tree objecttree.HistoryTree, p ViewParams) (*View, error) {
	shared := crdt.SharedCollections{}
	for _, name := range p.SharedDatasets {
		coll, err := db.Collection(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("history: open scratch shared collection %s: %w", name, err)
		}
		shared[name] = coll
	}
	// No applySeq allocator and no apply hook: scratch rows carry no
	// _applySeq and feed no read-tracking — replay artifacts stay out.
	ctrl, err := crdt.NewControllerWithShared(ctx, p.ObjectId, db, shared, p.Regs...)
	if err != nil {
		return nil, fmt.Errorf("history: scratch controller: %w", err)
	}

	known := make(map[string]struct{}, len(p.Regs))
	datasets := make([]string, 0, len(p.Regs))
	for _, reg := range p.Regs {
		known[reg.Name] = struct{}{}
		datasets = append(datasets, reg.Name)
	}
	sort.Strings(datasets)

	maxRecords := p.MaxRecords
	if maxRecords <= 0 {
		maxRecords = DefaultMaxViewRecords
	}

	codec := object.NewCodec() // per-replay: the codec arena is not concurrency-safe

	// Root-change metadata is constant across the tree; stamp it on
	// every decoded change so handler hooks (author/createdAt) derive
	// the same values as the live projection.
	var (
		objectAuthor    string
		objectCreatedAt int64
	)
	if root := tree.Root(); root != nil {
		objectCreatedAt = root.Timestamp
		if root.Identity != nil {
			objectAuthor = root.Identity.Account()
		}
	}

	tx, err := db.WriteTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("history: scratch tx: %w", err)
	}
	txCtx := tx.Context()

	rootId := tree.Id()
	records := 0
	var fatalErr error

	convert := func(ch *objecttree.Change, decrypted []byte) (any, error) {
		if ch.Id == rootId || len(decrypted) == 0 {
			return nil, nil
		}
		decoded, decodeErr := codec.Decode(decrypted)
		if decodeErr != nil {
			// Non-CRDT payload (settings change, foreign data type) —
			// skip, same tolerance as the live replay path.
			return nil, nil
		}
		return &decoded, nil
	}

	iter := func(ch *objecttree.Change) bool {
		decoded, ok := ch.Model.(*crdt.Change)
		if !ok || decoded == nil {
			return true
		}
		if p.Dataset != "" && decoded.Dataset != p.Dataset {
			return true
		}
		if _, ok := known[decoded.Dataset]; !ok {
			// Unregistered dataset: invisible in the projection, same
			// as the live schema gate's parked changes (proposal §9).
			return true
		}
		decoded.ObjectId = p.ObjectId
		decoded.ChangeId = ch.Id
		decoded.AddSeq = ch.AddSeq
		decoded.Timestamp = ch.Timestamp
		// Live OrderIds are preserved by the history tree, so _ver maps
		// come out identical to what the live projection held when only
		// these changes existed (proposal §4.1).
		decoded.VersionId = crdt.VersionId(ch.OrderId)
		decoded.PrevIds = ch.PreviousIds
		decoded.ObjectAuthor = objectAuthor
		decoded.ObjectCreatedAt = objectCreatedAt
		if ch.Identity != nil {
			decoded.Creator = ch.Identity.Account()
		}

		records += len(decoded.Records)
		if records > maxRecords {
			fatalErr = ErrViewTooLarge
			return false
		}

		// Replay order is correctness-irrelevant (order-tolerant CRDT);
		// per-change apply errors are infrastructure-level here (the
		// scratch store is ours), so abort rather than salvage.
		if applyErr := ctrl.ApplyChange(txCtx, *decoded); applyErr != nil {
			fatalErr = fmt.Errorf("history: apply %s: %w", ch.Id, applyErr)
			return false
		}
		return true
	}

	if err := tree.IterateRoot(convert, iter); err != nil {
		_ = tx.Rollback()
		return nil, mapTreeErr(err)
	}
	if fatalErr != nil {
		_ = tx.Rollback()
		return nil, fatalErr
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("history: commit scratch: %w", err)
	}

	if p.Dataset != "" {
		datasets = []string{p.Dataset}
	}
	return &View{
		ObjectId: p.ObjectId,
		Version:  strings.Join(p.Heads, ","),
		db:       db,
		ctrl:     ctrl,
		datasets: datasets,
	}, nil
}

// Datasets lists the dataset names this view can serve (the registered
// handler set, or the single replayed dataset for dataset-scoped views).
func (v *View) Datasets() []string {
	return append([]string(nil), v.datasets...)
}

// Record returns one record (nil if absent), tombstones included —
// callers that need liveness filtering check _deletedAt. _applySeq is
// never present (scratch controllers run without an allocator).
func (v *View) Record(ctx context.Context, dataset, recordId string) *anyenc.Value {
	return v.ctrl.Get(ctx, dataset, recordId)
}

// Records returns all live (non-tombstone) records of a dataset.
func (v *View) Records(ctx context.Context, dataset string) []*anyenc.Value {
	return v.ctrl.Records(ctx, dataset)
}

// AllRecords returns every record of a dataset INCLUDING tombstones —
// the diff engine needs them to classify deletions.
func (v *View) AllRecords(ctx context.Context, dataset string) ([]*anyenc.Value, error) {
	coll := v.ctrl.Collection(ctx, dataset)
	if coll == nil {
		return nil, nil
	}
	iter, err := coll.Find(nil).Iter(ctx)
	if err != nil {
		return nil, fmt.Errorf("history: iterate %s: %w", dataset, err)
	}
	defer iter.Close()
	var out []*anyenc.Value
	a := &anyenc.Arena{}
	for iter.Next() {
		doc, err := iter.Doc()
		if err != nil {
			return nil, err
		}
		out = append(out, anyencutil.Copy(a, doc.Value()))
	}
	return out, nil
}

// Close releases the scratch store. Idempotent.
func (v *View) Close() error {
	if v.db == nil {
		return nil
	}
	db := v.db
	v.db = nil
	return db.Close()
}

// DiffFilter narrows a view diff (proposal §7 DiffFilter).
type DiffFilter struct {
	Dataset   string
	RecordIds []string // requires Dataset
}

// DiffViews computes the structural diff between two materialized cuts.
// Both views must come from the same object (same handler set).
func DiffViews(ctx context.Context, base, version *View, f DiffFilter) (DiffResult, error) {
	if len(f.RecordIds) > 0 && f.Dataset == "" {
		return DiffResult{}, errors.New("history: DiffFilter.RecordIds requires Dataset")
	}

	res := DiffResult{Base: base.Version, Version: version.Version}

	if len(f.RecordIds) > 0 {
		dd := DatasetDiff{Dataset: f.Dataset}
		for _, id := range f.RecordIds {
			rd := DiffRecords(id, base.Record(ctx, f.Dataset, id), version.Record(ctx, f.Dataset, id))
			if rd != nil {
				dd.Records = append(dd.Records, *rd)
			}
		}
		if len(dd.Records) > 0 {
			res.Datasets = append(res.Datasets, dd)
		}
		return res, nil
	}

	datasets := version.datasets
	if f.Dataset != "" {
		datasets = []string{f.Dataset}
	}
	for _, ds := range datasets {
		baseRecs, err := base.AllRecords(ctx, ds)
		if err != nil {
			return DiffResult{}, err
		}
		versionRecs, err := version.AllRecords(ctx, ds)
		if err != nil {
			return DiffResult{}, err
		}
		dd := DiffRecordSets(ds, baseRecs, versionRecs)
		if len(dd.Records) > 0 {
			res.Datasets = append(res.Datasets, dd)
		}
	}
	return res, nil
}

// mapTreeErr converts any-sync tree-build failures into the history
// package's contract errors where recognizable.
func mapTreeErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, objecttree.ErrLoadBeforeRoot):
		return fmt.Errorf("%w: %v", ErrHistoryTruncated, err)
	default:
		return fmt.Errorf("history: build history tree: %w", err)
	}
}
