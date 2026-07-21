package history

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
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

// DefaultMaxViewRecords bounds how many DISTINCT record-rows a single
// view may materialize before the replay aborts with ErrViewTooLarge —
// a memory bound on the scratch projection, not a cap on replay work:
// a small document with a million-change edit history stays under it,
// while million-record datasets are meant to go through the
// record-scope fast path.
const DefaultMaxViewRecords = 500_000

// recordCounter enforces the distinct-record bound with a hashed
// bitset: one bit per seeded hash of the resolved (dataset, recordId)
// pair, so a record touched by many changes counts once. Sized at 16
// bits per expected entry, the bound is deliberately approximate — a
// collision undercounts one record, fine for a soft memory guardrail.
// The random seed keeps collisions non-craftable by a peer choosing
// record ids.
type recordCounter struct {
	seed  maphash.Seed
	bits  []uint64
	mask  uint64
	count int
	max   int
}

func newRecordCounter(max int) *recordCounter {
	size := uint64(1) << 20 // 128KB floor
	for size < uint64(max)*16 {
		size <<= 1
	}
	return &recordCounter{
		seed: maphash.MakeSeed(),
		bits: make([]uint64, size/64),
		mask: size - 1,
		max:  max,
	}
}

// add registers the change's resolved record ids; false once the
// distinct-record bound is exceeded. Malformed changes (unresolvable
// ids) contribute no rows — same stance as backfill, which skips them.
func (rc *recordCounter) add(ch *crdt.Change) bool {
	ids, err := crdt.ResolveRecordIds(*ch)
	if err != nil {
		return true
	}
	for _, id := range ids {
		var h maphash.Hash
		h.SetSeed(rc.seed)
		_, _ = h.WriteString(ch.Dataset)
		_ = h.WriteByte(0)
		_, _ = h.WriteString(id)
		pos := h.Sum64() & rc.mask
		word, bit := pos/64, uint64(1)<<(pos%64)
		if rc.bits[word]&bit != 0 {
			continue // seen (or collided — soft bound)
		}
		rc.bits[word] |= bit
		rc.count++
		if rc.count > rc.max {
			return false
		}
	}
	return true
}

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

	// Tree, when set, is a pre-built history tree at Heads and Storage/
	// Acl are ignored for tree building. Callers reading a LIVE tree's
	// storage should build under that tree's lock and pass the result
	// here: the build is the only storage-touching phase, the storage
	// parser is not thread-safe outside the lock, and holding the lock
	// also fences ocache eviction (TryClose→tree.Close closes the
	// shared storage) for the duration of the scan.
	Tree objecttree.HistoryTree

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
	tree := p.Tree
	if tree == nil {
		if p.Storage == nil || p.Acl == nil {
			return nil, errors.New("history: BuildView: Tree or Storage+Acl required")
		}
		var err error
		tree, err = objecttree.BuildNonVerifiableHistoryTree(objecttree.HistoryTreeParams{
			Storage:         p.Storage,
			AclList:         p.Acl,
			Heads:           p.Heads,
			IncludeBeforeId: true,
		})
		if err != nil {
			return nil, mapTreeErr(err)
		}
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

// scratchController opens the scratch shared collections and builds a
// fresh Controller for a replay. No applySeq allocator and no apply
// hook: scratch rows carry no _applySeq and feed no read-tracking —
// replay artifacts stay out.
func scratchController(ctx context.Context, db anystore.DB, p ViewParams) (*crdt.Controller, []string, error) {
	shared := crdt.SharedCollections{}
	for _, name := range p.SharedDatasets {
		coll, err := db.Collection(ctx, name)
		if err != nil {
			return nil, nil, fmt.Errorf("history: open scratch shared collection %s: %w", name, err)
		}
		shared[name] = coll
	}
	ctrl, err := crdt.NewControllerWithShared(ctx, p.ObjectId, db, shared, p.Regs...)
	if err != nil {
		return nil, nil, fmt.Errorf("history: scratch controller: %w", err)
	}
	datasets := make([]string, 0, len(p.Regs))
	for _, reg := range p.Regs {
		datasets = append(datasets, reg.Name)
	}
	sort.Strings(datasets)
	return ctrl, datasets, nil
}

// BuildEmptyView returns a view of the empty projection — the causal
// past of a first change's (absent) parents. Heads are ignored.
func BuildEmptyView(ctx context.Context, p ViewParams) (*View, error) {
	db, err := anystore.Open(ctx, ":memory:", &anystore.Config{InMemory: true})
	if err != nil {
		return nil, fmt.Errorf("history: open scratch store: %w", err)
	}
	ctrl, datasets, err := scratchController(ctx, db, p)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if p.Dataset != "" {
		datasets = []string{p.Dataset}
	}
	return &View{ObjectId: p.ObjectId, Version: "", db: db, ctrl: ctrl, datasets: datasets}, nil
}

func replayIntoScratch(ctx context.Context, db anystore.DB, tree objecttree.HistoryTree, p ViewParams) (*View, error) {
	ctrl, datasets, err := scratchController(ctx, db, p)
	if err != nil {
		return nil, err
	}

	known := make(map[string]struct{}, len(p.Regs))
	for _, reg := range p.Regs {
		known[reg.Name] = struct{}{}
	}

	maxRecords := p.MaxRecords
	if maxRecords <= 0 {
		maxRecords = DefaultMaxViewRecords
	}

	codec := object.NewCodec() // per-replay: the codec arena is not concurrency-safe
	objectAuthor, objectCreatedAt := rootMeta(tree)

	tx, err := db.WriteTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("history: scratch tx: %w", err)
	}
	txCtx := tx.Context()

	counter := newRecordCounter(maxRecords)
	var fatalErr error

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
		stampEnvelope(decoded, ch, p.ObjectId, objectAuthor, objectCreatedAt)

		if !counter.add(decoded) {
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

	if err := tree.IterateRoot(decodeConvert(codec, tree.Id()), iter); err != nil {
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
