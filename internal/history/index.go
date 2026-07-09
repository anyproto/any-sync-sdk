package history

import (
	"context"
	"errors"
	"fmt"
	"sort"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
)

// Engine B: the persistent history index (proposal §4.4). Change and
// record rows live in PER-OBJECT collections (mirroring the
// `<objectId>_<dataset>` projection layout) keyed by OrderId, so:
//
//   - inserts are append-ordered (lexids are monotonic per tree) —
//     no random-CID page splits, and descending listings are reverse
//     primary-key scans with NO secondary indexes;
//   - an object's history is dropped by the same `<objectId>_*` purge
//     sweep that drops its dataset collections;
//   - rows carry no obj/sp fields (the collection name scopes them).
//
// Only the trace index stays space-level (its query is space-wide by
// requirement) plus the tiny per-object state collection.

// Collection naming. The double underscore keeps the namespace
// disjoint from `<objectId>_<dataset>` projections: dataset names with
// a "_" prefix are rejected at registration (ValidateExternalTypes).
const (
	// HistoryCollectionSuffix — `<objectId>__history`: one row per
	// change, PK = OrderId.
	HistoryCollectionSuffix = "__history"
	// HistoryRecsCollectionSuffix — `<objectId>__history_recs`: one row
	// per (change, record), PK = recordId + sep + OrderId, so "changes
	// touching record R in order" is a PK prefix range scan.
	HistoryRecsCollectionSuffix = "__history_recs"
	// HistoryTracesCollection — space-level: one row per (change,
	// trace), PK = spaceId + sep + traceId + sep + OrderId + sep +
	// objectId (OrderIds are only unique per tree, hence the object
	// tail).
	HistoryTracesCollection = "_history_traces"
	// HistoryMetaCollection — space-level per-object index state
	// (stale flag), id = objectId.
	HistoryMetaCollection = "_history_meta"
)

// keySep separates composite primary-key parts; keySepEnd is its
// exclusive upper bound for prefix range scans. Record and trace ids
// must not contain NUL — enforced nowhere today, but a violating id
// only mis-scopes its own history filter, nothing else.
const (
	keySep    = "\x00"
	keySepEnd = "\x01"
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
	traces  anystore.Collection
	meta    anystore.Collection
	skip    map[string]struct{}
}

// OpenIndex opens (creating if needed) the space-level history
// collections. Per-object collections are created lazily on first
// write. skipDatasets lists datasets registered with SkipHistory.
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
	if ix.traces, err = db.Collection(ctx, HistoryTracesCollection); err != nil {
		return nil, fmt.Errorf("history: open %s: %w", HistoryTracesCollection, err)
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

// historyColl / recsColl resolve an object's collections. create=false
// returns (nil, nil) when the collection doesn't exist — the object
// simply has no indexed history yet.
func (ix *Index) historyColl(ctx context.Context, objectId string, create bool) (anystore.Collection, error) {
	return ix.objColl(ctx, objectId+HistoryCollectionSuffix, create)
}

func (ix *Index) recsColl(ctx context.Context, objectId string, create bool) (anystore.Collection, error) {
	return ix.objColl(ctx, objectId+HistoryRecsCollectionSuffix, create)
}

func (ix *Index) objColl(ctx context.Context, name string, create bool) (anystore.Collection, error) {
	if create {
		coll, err := ix.db.Collection(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("history: open %s: %w", name, err)
		}
		return coll, nil
	}
	coll, err := ix.db.OpenCollection(ctx, name)
	if err != nil {
		if errors.Is(err, anystore.ErrCollectionNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("history: open %s: %w", name, err)
	}
	return coll, nil
}

// IndexChange writes the index rows for one applied change. Call from
// the Controller's ApplyHook with the apply's tx-carrying context so
// rows commit atomically with the projection (warm path). recordIds
// are the resolved per-record ids, index-aligned with ch.Records.
// No-op for local/injected changes (no DAG identity) and SkipHistory
// datasets. Upserts, so re-applies and backfill races stay idempotent.
func (ix *Index) IndexChange(txCtx context.Context, ch *crdt.Change, recordIds []string) error {
	if ch.ChangeId == "" || ch.VersionId == "" || ch.Local || ch.Injected {
		return nil
	}
	if ix.SkipsDataset(ch.Dataset) {
		return nil
	}
	hist, err := ix.historyColl(txCtx, ch.ObjectId, true)
	if err != nil {
		return err
	}
	recs, err := ix.recsColl(txCtx, ch.ObjectId, true)
	if err != nil {
		return err
	}

	a := &anyenc.Arena{}
	o := string(ch.VersionId)

	row := a.NewObject()
	row.Set("id", a.NewString(o)) // PK = OrderId: append-ordered inserts
	row.Set("c", a.NewString(ch.ChangeId))
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
	// The `__history_recs` rows below are the record-filter index; this
	// duplication trades a few bytes per change for a join-free list
	// path.
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
	if err := hist.UpsertOne(txCtx, row); err != nil {
		return fmt.Errorf("history: index change %s: %w", ch.ChangeId, err)
	}

	for _, rec := range recordIds {
		rrow := a.NewObject()
		rrow.Set("id", a.NewString(rec+keySep+o)) // record-prefix range scan, ordered by o
		rrow.Set("o", a.NewString(o))
		rrow.Set("c", a.NewString(ch.ChangeId))
		if err := recs.UpsertOne(txCtx, rrow); err != nil {
			return fmt.Errorf("history: index change record %s/%s: %w", ch.ChangeId, rec, err)
		}
	}

	for _, tr := range ch.TraceIds {
		trow := a.NewObject()
		trow.Set("id", a.NewString(ix.spaceId+keySep+tr+keySep+o+keySep+ch.ObjectId))
		trow.Set("o", a.NewString(o))
		trow.Set("obj", a.NewString(ch.ObjectId))
		trow.Set("c", a.NewString(ch.ChangeId))
		if err := ix.traces.UpsertOne(txCtx, trow); err != nil {
			return fmt.Errorf("history: index change trace %s/%s: %w", ch.ChangeId, tr, err)
		}
	}
	return nil
}

// MarkStale flags an object's index as incomplete (cold restore /
// re-index skipped the warm path, or a warm row failed). History
// queries must backfill before serving it.
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

// PurgeObject removes the object's space-level index state (trace rows
// and the meta row). The per-object `<objectId>__history*` collections
// are dropped by the store's `<objectId>_*` purge sweep alongside the
// dataset collections; traces have no per-object key prefix, so they
// need this scan (purge is rare, the traces collection is sparse).
func (ix *Index) PurgeObject(ctx context.Context, objectId string) error {
	iter, err := ix.traces.Find(map[string]any{"obj": objectId}).Iter(ctx)
	if err != nil {
		return err
	}
	var ids []string
	for iter.Next() {
		doc, derr := iter.Doc()
		if derr != nil {
			_ = iter.Close()
			return derr
		}
		ids = append(ids, string(doc.Value().GetStringBytes("id")))
	}
	if err := iter.Close(); err != nil {
		return err
	}
	for _, id := range ids {
		if err := ix.traces.DeleteId(ctx, id); err != nil && !errors.Is(err, anystore.ErrDocNotFound) {
			return err
		}
	}
	if err := ix.meta.DeleteId(ctx, objectId); err != nil && !errors.Is(err, anystore.ErrDocNotFound) {
		return err
	}
	return nil
}

// Backfill (re)builds the object's index rows by walking the tree —
// bounded batches, one WriteTx per batch, stale flag cleared in the
// final one (proposal §4.4). The caller provides a tree that is safe
// to iterate (a history tree, or the live tree under its lock).
// Synchronous; see the note above DefaultBackfillBatch.
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
	// OrderId is the route-specific pagination key (the row's o, plus
	// an object tail on the trace route); opaque to callers, never an
	// identity.
	OrderId string
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
// the opaque value returned by the previous page ("" = start) and is
// only meaningful with the same filter. Returns the page and the next
// cursor ("" = exhausted).
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
		return ix.listByRecord(ctx, f, limit, cursor)
	case f.TraceId != "":
		return ix.listByTrace(ctx, f, limit, cursor)
	default:
		return ix.listDirect(ctx, f, limit, cursor)
	}
}

// listDirect pages an object's `__history` collection: a reverse
// primary-key scan (PK = OrderId), residual ds/author filters applied
// by the query engine.
func (ix *Index) listDirect(ctx context.Context, f Filter, limit int, cursor string) ([]ChangeMeta, string, error) {
	coll, err := ix.historyColl(ctx, f.ObjectId, false)
	if err != nil || coll == nil {
		return nil, "", err
	}
	cond := map[string]any{}
	if f.Dataset != "" {
		cond["ds"] = f.Dataset
	}
	if f.Author != "" {
		cond["author"] = f.Author
	}
	if cursor != "" {
		cond["id"] = map[string]any{"$lt": cursor}
	}
	iter, err := coll.Find(cond).Sort("-id").Limit(uint(limit + 1)).Iter(ctx)
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
		out = append(out, changeMetaFromRow(doc.Value(), f.ObjectId))
	}
	if !hasMore || len(out) == 0 {
		return out, "", nil
	}
	return out, out[len(out)-1].OrderId, nil
}

// listByRecord pages the object's `__history_recs` collection by the
// record's PK prefix (rows are `rec\0o`, so the range is ordered by o)
// and joins each hit to its `__history` row by o. Cursor = last
// processed row's o.
func (ix *Index) listByRecord(ctx context.Context, f Filter, limit int, cursor string) ([]ChangeMeta, string, error) {
	recs, err := ix.recsColl(ctx, f.ObjectId, false)
	if err != nil || recs == nil {
		return nil, "", err
	}
	hist, err := ix.historyColl(ctx, f.ObjectId, false)
	if err != nil || hist == nil {
		return nil, "", err
	}

	upper := f.RecordId + keySepEnd
	if cursor != "" {
		upper = f.RecordId + keySep + cursor
	}
	cond := map[string]any{"id": map[string]any{"$gt": f.RecordId + keySep, "$lt": upper}}

	join := func(row *anyenc.Value) (ChangeMeta, bool, error) {
		doc, ferr := hist.FindId(ctx, string(row.GetStringBytes("o")))
		if ferr != nil {
			if errors.Is(ferr, anystore.ErrDocNotFound) {
				return ChangeMeta{}, false, nil // backfill in flight
			}
			return ChangeMeta{}, false, ferr
		}
		meta := changeMetaFromRow(doc.Value(), f.ObjectId)
		if meta.Dataset != f.Dataset {
			return ChangeMeta{}, false, nil // same record id in another dataset
		}
		return meta, true, nil
	}
	return ix.pageJoin(ctx, recs, cond, f, limit, join, func(row *anyenc.Value) string {
		return string(row.GetStringBytes("o"))
	})
}

// listByTrace pages the space-level traces collection by the trace's
// PK prefix and joins each hit to its object's `__history` row.
// OrderIds are only unique per tree, so the cursor is `o\0objectId` —
// the PK tail after the trace prefix.
func (ix *Index) listByTrace(ctx context.Context, f Filter, limit int, cursor string) ([]ChangeMeta, string, error) {
	prefix := ix.spaceId + keySep + f.TraceId + keySep
	upper := ix.spaceId + keySep + f.TraceId + keySepEnd
	if cursor != "" {
		upper = prefix + cursor
	}
	cond := map[string]any{"id": map[string]any{"$gt": prefix, "$lt": upper}}

	hists := map[string]anystore.Collection{} // per-object join targets
	join := func(row *anyenc.Value) (ChangeMeta, bool, error) {
		obj := string(row.GetStringBytes("obj"))
		if f.ObjectId != "" && obj != f.ObjectId {
			return ChangeMeta{}, false, nil
		}
		hist, ok := hists[obj]
		if !ok {
			var herr error
			hist, herr = ix.historyColl(ctx, obj, false)
			if herr != nil {
				return ChangeMeta{}, false, herr
			}
			hists[obj] = hist
		}
		if hist == nil {
			return ChangeMeta{}, false, nil
		}
		o := string(row.GetStringBytes("o"))
		doc, ferr := hist.FindId(ctx, o)
		if ferr != nil {
			if errors.Is(ferr, anystore.ErrDocNotFound) {
				return ChangeMeta{}, false, nil
			}
			return ChangeMeta{}, false, ferr
		}
		meta := changeMetaFromRow(doc.Value(), obj)
		meta.OrderId = o + keySep + obj // trace-route cursor tail
		return meta, true, nil
	}
	return ix.pageJoin(ctx, ix.traces, cond, f, limit, join, func(row *anyenc.Value) string {
		return string(row.GetStringBytes("o")) + keySep + string(row.GetStringBytes("obj"))
	})
}

// pageJoin drives one join-route page. One bookkeeping rule: cursorKey
// tracks the last PROCESSED side row, and the scan "has more" the
// moment we stop before natural exhaustion — either the page filled or
// the over-fetch window (fetch rows + 1 sentinel) still had rows. The
// next cursor is then always the last processed key, so a window whose
// rows were all rejected by post-join filters continues instead of
// terminating early.
func (ix *Index) pageJoin(
	ctx context.Context,
	side anystore.Collection,
	cond map[string]any,
	f Filter,
	limit int,
	join func(row *anyenc.Value) (ChangeMeta, bool, error),
	cursorKey func(row *anyenc.Value) string,
) ([]ChangeMeta, string, error) {
	fetch := limit
	if f.Author != "" || f.ObjectId != "" {
		// Post-join filters may reject rows: over-fetch the window.
		fetch = limit * 4
	}
	iter, err := side.Find(cond).Sort("-id").Limit(uint(fetch + 1)).Iter(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("history: list via %s: %w", side.Name(), err)
	}
	defer iter.Close()

	var (
		out     []ChangeMeta
		lastKey string
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
		row := doc.Value()
		lastKey = cursorKey(row)
		meta, ok, jerr := join(row)
		if jerr != nil {
			return nil, "", jerr
		}
		if !ok {
			continue
		}
		if f.Author != "" && meta.Author != f.Author {
			continue
		}
		out = append(out, meta)
	}
	if !hasMore {
		return out, "", nil
	}
	return out, lastKey, nil
}

// TouchedChangeIds returns every ChangeId known to touch (dataset,
// recordId) on the object — the RecordAt fast-path pre-filter, straight
// off the record's PK prefix range. Bounded by maxTouchedChangeIds; a
// record touched more often than that falls back to the
// decode-and-filter path (nil, nil).
func (ix *Index) TouchedChangeIds(ctx context.Context, objectId, dataset, recordId string) ([]string, error) {
	recs, err := ix.recsColl(ctx, objectId, false)
	if err != nil || recs == nil {
		return nil, err
	}
	iter, err := recs.Find(map[string]any{
		"id": map[string]any{"$gt": recordId + keySep, "$lt": recordId + keySepEnd},
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
		if c := string(doc.Value().GetStringBytes("c")); c != "" {
			out = append(out, c)
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
	coll, err := ix.historyColl(ctx, objectId, false)
	if err != nil || coll == nil {
		return false, err
	}
	iter, err := coll.Find(nil).Limit(1).Iter(ctx)
	if err != nil {
		return false, err
	}
	defer iter.Close()
	return iter.Next(), nil
}

// changeMetaFromRow assembles a ChangeMeta from a `__history` row
// (PK id = OrderId, ChangeId in `c`).
func changeMetaFromRow(row *anyenc.Value, objectId string) ChangeMeta {
	meta := ChangeMeta{
		Version:   string(row.GetStringBytes("c")),
		ObjectId:  objectId,
		Author:    string(row.GetStringBytes("author")),
		Timestamp: int64(row.GetFloat64("ts")),
		Dataset:   string(row.GetStringBytes("ds")),
		OrderId:   string(row.GetStringBytes("id")),
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
