package history

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
)

// Engine B: the persistent history index (proposal §4.4). Normalized
// metadata collections in the space DB, space-scoped with an `sp`
// field exactly like `_meta` (shared-topology safe). Warm-path rows
// append inside the same WriteTx as the projection apply (via the
// Controller's ApplyHook); the cold-restore path skips the index and
// marks the object stale for lazy backfill.

// Collection names follow the underscore convention.
const (
	HistoryCollection       = "_history"        // one row per change
	HistoryRecsCollection   = "_history_recs"   // one row per (change, record)
	HistoryTracesCollection = "_history_traces" // one row per (change, trace)
	HistoryMetaCollection   = "_history_meta"   // per-object index state (stale flag)
)

// DefaultBackfillBatch bounds changes per backfill transaction
// (proposal §4.4: ~1000 changes per tx).
const DefaultBackfillBatch = 1000

// NOTE: v1 runs stale-index backfills synchronously inside the first
// history query (spaceimpl ensureFresh). The proposal's async contract
// (an ErrHistoryIndexBuilding + progress callback) is deferred until
// backfill latency proves to matter — no dead error variable until
// then.

// Index is the per-space history index. Safe for use from apply hooks:
// any-store's single writer serializes concurrent object applies.
type Index struct {
	spaceId string
	db      anystore.DB
	changes anystore.Collection
	recs    anystore.Collection
	traces  anystore.Collection
	meta    anystore.Collection
	skip    map[string]struct{}
}

// OpenIndex opens (creating if needed) the history collections in the
// space DB and ensures their indexes. skipDatasets lists datasets
// registered with SkipHistory.
func OpenIndex(ctx context.Context, db anystore.DB, spaceId string, skipDatasets []string) (*Index, error) {
	ix := &Index{
		spaceId: spaceId,
		db:      db,
		skip:    make(map[string]struct{}, len(skipDatasets)),
	}
	for _, ds := range skipDatasets {
		ix.skip[ds] = struct{}{}
	}

	var err error
	if ix.changes, err = db.Collection(ctx, HistoryCollection); err != nil {
		return nil, fmt.Errorf("history: open %s: %w", HistoryCollection, err)
	}
	if err = ix.changes.EnsureIndex(ctx,
		anystore.IndexInfo{Name: "idx_sp_obj_o", Fields: []string{"sp", "obj", "o"}},
		anystore.IndexInfo{Name: "idx_sp_o", Fields: []string{"sp", "o"}},
	); err != nil {
		return nil, fmt.Errorf("history: ensure %s indexes: %w", HistoryCollection, err)
	}

	if ix.recs, err = db.Collection(ctx, HistoryRecsCollection); err != nil {
		return nil, fmt.Errorf("history: open %s: %w", HistoryRecsCollection, err)
	}
	if err = ix.recs.EnsureIndex(ctx,
		anystore.IndexInfo{Name: "idx_sp_obj_ds_rec_o", Fields: []string{"sp", "obj", "ds", "rec", "o"}},
	); err != nil {
		return nil, fmt.Errorf("history: ensure %s indexes: %w", HistoryRecsCollection, err)
	}

	if ix.traces, err = db.Collection(ctx, HistoryTracesCollection); err != nil {
		return nil, fmt.Errorf("history: open %s: %w", HistoryTracesCollection, err)
	}
	if err = ix.traces.EnsureIndex(ctx,
		anystore.IndexInfo{Name: "idx_sp_tr_o", Fields: []string{"sp", "tr", "o"}},
	); err != nil {
		return nil, fmt.Errorf("history: ensure %s indexes: %w", HistoryTracesCollection, err)
	}

	if ix.meta, err = db.Collection(ctx, HistoryMetaCollection); err != nil {
		return nil, fmt.Errorf("history: open %s: %w", HistoryMetaCollection, err)
	}
	return ix, nil
}

// SkipsDataset reports whether the dataset is opted out of history.
func (ix *Index) SkipsDataset(dataset string) bool {
	_, ok := ix.skip[dataset]
	return ok
}

// IndexChange writes the index rows for one applied change. Call from
// the Controller's ApplyHook with the apply's tx-carrying context so
// rows commit atomically with the projection (warm path). recordIds
// are the resolved per-record ids, index-aligned with ch.Records.
// No-op for local/injected changes (no DAG identity) and SkipHistory
// datasets. Upserts, so re-applies and backfill races stay idempotent.
func (ix *Index) IndexChange(txCtx context.Context, ch *crdt.Change, recordIds []string) error {
	if ch.ChangeId == "" || ch.Local || ch.Injected {
		return nil
	}
	if ix.SkipsDataset(ch.Dataset) {
		return nil
	}

	a := &anyenc.Arena{}

	row := a.NewObject()
	row.Set("id", a.NewString(ch.ChangeId))
	row.Set("sp", a.NewString(ix.spaceId))
	row.Set("obj", a.NewString(ch.ObjectId))
	row.Set("o", a.NewString(string(ch.VersionId)))
	row.Set("ds", a.NewString(ch.Dataset))
	row.Set("author", a.NewString(ch.Creator))
	row.Set("ts", a.NewNumberFloat64(float64(ch.Timestamp)))
	row.Set("n", a.NewNumberInt(len(recordIds)))
	if len(ch.TraceIds) > 0 {
		row.Set("traces", stringArray(a, ch.TraceIds))
	}
	if len(ch.PrevIds) > 0 {
		// PrevIds power list-time coalescing (§7.1) without a tree hit.
		row.Set("prev", stringArray(a, ch.PrevIds))
	}
	// Touched records inline: keeps ChangeMeta assembly one row wide.
	// The normalized _history_recs rows below are the record-filter
	// index; this duplication trades a few bytes per change for a
	// join-free list path.
	recsArr := a.NewArray()
	for i, rec := range recordIds {
		item := a.NewObject()
		item.Set("rec", a.NewString(rec))
		if i < len(ch.Records) {
			item.Set("k", stringArray(a, opKinds(ch.Records[i].Ops)))
		}
		recsArr.SetArrayItem(i, item)
	}
	row.Set("recs", recsArr)
	if err := ix.changes.UpsertOne(txCtx, row); err != nil {
		return fmt.Errorf("history: index change %s: %w", ch.ChangeId, err)
	}

	for _, rec := range recordIds {
		rrow := a.NewObject()
		rrow.Set("id", a.NewString(ch.ChangeId+"/"+rec))
		rrow.Set("sp", a.NewString(ix.spaceId))
		rrow.Set("obj", a.NewString(ch.ObjectId))
		rrow.Set("ds", a.NewString(ch.Dataset))
		rrow.Set("rec", a.NewString(rec))
		rrow.Set("o", a.NewString(string(ch.VersionId)))
		if err := ix.recs.UpsertOne(txCtx, rrow); err != nil {
			return fmt.Errorf("history: index change record %s/%s: %w", ch.ChangeId, rec, err)
		}
	}

	for _, tr := range ch.TraceIds {
		trow := a.NewObject()
		trow.Set("id", a.NewString(ch.ChangeId+"/"+tr))
		trow.Set("sp", a.NewString(ix.spaceId))
		trow.Set("tr", a.NewString(tr))
		trow.Set("o", a.NewString(string(ch.VersionId)))
		trow.Set("obj", a.NewString(ch.ObjectId))
		if err := ix.traces.UpsertOne(txCtx, trow); err != nil {
			return fmt.Errorf("history: index change trace %s/%s: %w", ch.ChangeId, tr, err)
		}
	}
	return nil
}

// MarkStale flags an object's index as incomplete (cold restore /
// re-index skipped the warm path). History queries must backfill
// before serving it.
func (ix *Index) MarkStale(ctx context.Context, objectId string) error {
	a := &anyenc.Arena{}
	row := a.NewObject()
	row.Set("id", a.NewString(objectId))
	row.Set("sp", a.NewString(ix.spaceId))
	row.Set("stale", a.NewTrue())
	return ix.meta.UpsertOne(ctx, row)
}

// IsStale reports whether the object's index needs a backfill.
func (ix *Index) IsStale(ctx context.Context, objectId string) (bool, error) {
	doc, err := ix.meta.FindId(ctx, objectId)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return false, nil
		}
		return false, err
	}
	return doc.Value().GetBool("stale"), nil
}

func (ix *Index) clearStale(ctx context.Context, objectId string) error {
	a := &anyenc.Arena{}
	row := a.NewObject()
	row.Set("id", a.NewString(objectId))
	row.Set("sp", a.NewString(ix.spaceId))
	row.Set("stale", a.NewFalse())
	return ix.meta.UpsertOne(ctx, row)
}

// Backfill (re)builds the object's index rows by walking the tree —
// bounded batches, one WriteTx per batch, stale flag cleared in the
// final one (proposal §4.4). The caller provides a tree that is safe
// to iterate (a history tree, or the live tree under its lock).
// Synchronous; the API layer decides whether to run it inline or
// return ErrHistoryIndexBuilding with a progress callback.
func (ix *Index) Backfill(ctx context.Context, tree objecttree.ReadableObjectTree, objectId string, batchSize int) error {
	if batchSize <= 0 {
		batchSize = DefaultBackfillBatch
	}
	codec := object.NewCodec()

	var (
		batch    []crdt.Change
		fatalErr error
	)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		tx, err := ix.db.WriteTx(ctx)
		if err != nil {
			return err
		}
		for i := range batch {
			recordIds, idErr := crdt.ResolveRecordIds(batch[i])
			if idErr != nil {
				continue // malformed change: not listable, keep going
			}
			if err := ix.IndexChange(tx.Context(), &batch[i], recordIds); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		batch = batch[:0]
		return tx.Commit()
	}

	objectAuthor, objectCreatedAt := rootMeta(tree)
	iter := func(ch *objecttree.Change) bool {
		decoded, ok := ch.Model.(*crdt.Change)
		if !ok || decoded == nil {
			return true
		}
		stampEnvelope(decoded, ch, objectId, objectAuthor, objectCreatedAt)
		// Op payloads alias the codec's parser buffer and are only
		// valid until the NEXT Decode — this batch outlives many
		// Decodes, so drop them now. Index rows read op types and
		// record ids only; a nil Payload can never be misread later.
		for i := range decoded.Records {
			for j := range decoded.Records[i].Ops {
				decoded.Records[i].Ops[j].Payload = nil
			}
		}
		batch = append(batch, *decoded)
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				fatalErr = err
				return false
			}
		}
		return true
	}

	if err := tree.IterateRoot(decodeConvert(codec, tree.Id()), iter); err != nil {
		return mapTreeErr(err)
	}
	if fatalErr != nil {
		return fatalErr
	}
	if err := flush(); err != nil {
		return err
	}
	return ix.clearStale(ctx, objectId)
}

// Filter narrows a ListChanges scan (proposal §7 HistoryFilter; the
// Coalesce option layers on top at the API level).
type Filter struct {
	ObjectId string // required unless TraceId is set (space-wide trace scan)
	Dataset  string // only this dataset
	RecordId string // only changes touching this record (requires Dataset)
	TraceId  string // only changes carrying this trace
	Author   string // identity filter
}

// TouchedRecord names one record a change touched, with its op kinds.
type TouchedRecord struct {
	Dataset  string
	RecordId string
	Ops      []string
}

// ChangeMeta is one history-list entry (proposal §7).
type ChangeMeta struct {
	Version   Version // ChangeId; for coalesced entries: head of group
	ObjectId  string
	Author    string
	Timestamp int64 // author clock — display-only
	Dataset   string
	TraceIds  []string
	Touched   []TouchedRecord
	// PrevIds power list-time coalescing; internal, not part of the
	// public surface.
	PrevIds []string
	OrderId string // internal pagination key; never an identity
	// Truncated is RESERVED (always false today): the SDK writes no
	// tree snapshots, so full history is always local. See the public
	// space.ChangeMeta.Truncated doc.
	Truncated bool
	GroupSize int // 1 unless coalesced
}

// DefaultListLimit / MaxListLimit bound ListChanges pages.
const (
	DefaultListLimit = 50
	MaxListLimit     = 1000
)

// ListChanges scans the index in descending OrderId (causal parents
// never appear after children; timestamps are display-only). cursor is
// the opaque value returned by the previous page ("" = start). Returns
// the page and the next cursor ("" = exhausted).
func (ix *Index) ListChanges(ctx context.Context, f Filter, limit int, cursor string) ([]ChangeMeta, string, error) {
	if f.ObjectId == "" && f.TraceId == "" {
		return nil, "", errors.New("history: ListChanges requires ObjectId (or TraceId for a space-wide trace scan)")
	}
	if f.RecordId != "" && f.Dataset == "" {
		return nil, "", errors.New("history: Filter.RecordId requires Dataset")
	}
	if limit <= 0 {
		limit = DefaultListLimit
	}
	if limit > MaxListLimit {
		limit = MaxListLimit
	}

	switch {
	case f.RecordId != "":
		return ix.listViaJoin(ctx, ix.recs, map[string]any{
			"sp": ix.spaceId, "obj": f.ObjectId, "ds": f.Dataset, "rec": f.RecordId,
		}, f, limit, cursor)
	case f.TraceId != "":
		cond := map[string]any{"sp": ix.spaceId, "tr": f.TraceId}
		if f.ObjectId != "" {
			cond["obj"] = f.ObjectId
		}
		return ix.listViaJoin(ctx, ix.traces, cond, f, limit, cursor)
	default:
		cond := map[string]any{"sp": ix.spaceId, "obj": f.ObjectId}
		if f.Dataset != "" {
			cond["ds"] = f.Dataset
		}
		if f.Author != "" {
			cond["author"] = f.Author
		}
		return ix.listDirect(ctx, cond, limit, cursor)
	}
}

func withCursor(cond map[string]any, cursor string) map[string]any {
	if cursor != "" {
		cond["o"] = map[string]any{"$lt": cursor}
	}
	return cond
}

func (ix *Index) listDirect(ctx context.Context, cond map[string]any, limit int, cursor string) ([]ChangeMeta, string, error) {
	iter, err := ix.changes.Find(withCursor(cond, cursor)).Sort("-o").Limit(uint(limit + 1)).Iter(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("history: list changes: %w", err)
	}
	defer iter.Close()

	var (
		out     []ChangeMeta
		hasMore bool
	)
	for iter.Next() {
		if len(out) >= limit {
			hasMore = true
			break
		}
		doc, derr := iter.Doc()
		if derr != nil {
			return nil, "", derr
		}
		out = append(out, changeMetaFromRow(doc.Value()))
	}
	return out, nextCursor(out, hasMore), nil
}

// listViaJoin pages a side collection (recs / traces) by o, then loads
// the matching _history rows by ChangeId. The side collections share
// the o axis, so the cursor contract is identical.
func (ix *Index) listViaJoin(ctx context.Context, side anystore.Collection, cond map[string]any, f Filter, limit int, cursor string) ([]ChangeMeta, string, error) {
	// Over-fetch when a post-join author filter will drop rows.
	fetch := limit
	if f.Author != "" {
		fetch = limit * 4
	}
	iter, err := side.Find(withCursor(cond, cursor)).Sort("-o").Limit(uint(fetch + 1)).Iter(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("history: list via %s: %w", side.Name(), err)
	}
	defer iter.Close()

	// One bookkeeping rule: lastO tracks the `o` of the last PROCESSED
	// side row, and the scan "has more" the moment we stop before the
	// iterator is naturally exhausted — either because the page filled
	// or because the fetch window (fetch rows + 1 sentinel) still had
	// rows left. The next cursor is then always lastO: resuming
	// strictly below the last processed row can neither skip nor
	// relist, and a window whose rows were all rejected by the author
	// filter continues instead of terminating early.
	var (
		out     []ChangeMeta
		lastO   string
		scanned int
		hasMore bool
	)
	for iter.Next() {
		if len(out) >= limit || scanned >= fetch {
			hasMore = true // unprocessed rows remain past this page/window
			break
		}
		scanned++
		doc, derr := iter.Doc()
		if derr != nil {
			return nil, "", derr
		}
		sideRow := doc.Value()
		lastO = string(sideRow.GetStringBytes("o"))
		changeId, _, ok := splitSideId(string(sideRow.GetStringBytes("id")))
		if !ok {
			continue
		}
		row, ferr := ix.changes.FindId(ctx, changeId)
		if ferr != nil {
			if errors.Is(ferr, anystore.ErrDocNotFound) {
				continue // side row without its change row: backfill in flight
			}
			return nil, "", ferr
		}
		meta := changeMetaFromRow(row.Value())
		if f.Author != "" && meta.Author != f.Author {
			continue
		}
		out = append(out, meta)
	}
	if !hasMore {
		return out, "", nil
	}
	return out, lastO, nil
}

func nextCursor(out []ChangeMeta, hasMore bool) string {
	if !hasMore || len(out) == 0 {
		return ""
	}
	return out[len(out)-1].OrderId
}

// TouchedChangeIds returns every ChangeId known to touch (dataset,
// recordId) on the object — the RecordAt fast-path pre-filter. Bounded
// by maxTouchedChangeIds; a record touched more often than that falls
// back to the decode-and-filter path (nil, nil).
func (ix *Index) TouchedChangeIds(ctx context.Context, objectId, dataset, recordId string) ([]string, error) {
	iter, err := ix.recs.Find(map[string]any{
		"sp": ix.spaceId, "obj": objectId, "ds": dataset, "rec": recordId,
	}).Iter(ctx)
	if err != nil {
		return nil, fmt.Errorf("history: touched change ids: %w", err)
	}
	defer iter.Close()
	var out []string
	for iter.Next() {
		doc, derr := iter.Doc()
		if derr != nil {
			return nil, derr
		}
		if changeId, _, ok := splitSideId(string(doc.Value().GetStringBytes("id"))); ok {
			out = append(out, changeId)
		}
		if len(out) > maxTouchedChangeIds {
			return nil, nil
		}
	}
	return out, nil
}

// maxTouchedChangeIds caps the fast-path pre-filter list; beyond it the
// id set itself is heavy enough that decode-and-filter is comparable.
const maxTouchedChangeIds = 100_000

// HasState reports whether the object is known to the index at all —
// a meta row (stale marker / completed backfill) or at least one
// change row. Objects predating the history feature have neither and
// need a backfill before their history can be served.
func (ix *Index) HasState(ctx context.Context, objectId string) (bool, error) {
	if _, err := ix.meta.FindId(ctx, objectId); err == nil {
		return true, nil
	} else if !errors.Is(err, anystore.ErrDocNotFound) {
		return false, err
	}
	iter, err := ix.changes.Find(map[string]any{"sp": ix.spaceId, "obj": objectId}).Limit(1).Iter(ctx)
	if err != nil {
		return false, err
	}
	defer iter.Close()
	return iter.Next(), nil
}

// splitSideId splits "changeId/suffix" (recs: recordId; traces:
// traceId). ChangeIds are CIDs and never contain '/', so the first
// separator wins.
func splitSideId(id string) (changeId, suffix string, ok bool) {
	return strings.Cut(id, "/")
}

func changeMetaFromRow(row *anyenc.Value) ChangeMeta {
	meta := ChangeMeta{
		Version:   string(row.GetStringBytes("id")),
		ObjectId:  string(row.GetStringBytes("obj")),
		Author:    string(row.GetStringBytes("author")),
		Timestamp: int64(row.GetFloat64("ts")),
		Dataset:   string(row.GetStringBytes("ds")),
		OrderId:   string(row.GetStringBytes("o")),
		GroupSize: 1,
	}
	for _, tv := range row.GetArray("traces") {
		meta.TraceIds = append(meta.TraceIds, string(tv.GetStringBytes()))
	}
	for _, pv := range row.GetArray("prev") {
		meta.PrevIds = append(meta.PrevIds, string(pv.GetStringBytes()))
	}
	for _, rv := range row.GetArray("recs") {
		tr := TouchedRecord{
			Dataset:  meta.Dataset,
			RecordId: string(rv.GetStringBytes("rec")),
		}
		for _, kv := range rv.GetArray("k") {
			tr.Ops = append(tr.Ops, string(kv.GetStringBytes()))
		}
		meta.Touched = append(meta.Touched, tr)
	}
	return meta
}

func stringArray(a *anyenc.Arena, items []string) *anyenc.Value {
	arr := a.NewArray()
	for i, s := range items {
		arr.SetArrayItem(i, a.NewString(s))
	}
	return arr
}

// opKinds returns the deduped, sorted op-type names of a record's ops.
func opKinds(ops []crdt.Op) []string {
	seen := make(map[string]struct{}, len(ops))
	var out []string
	for _, op := range ops {
		name := string(op.Type)
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}
