package history

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
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
	// change, PK = OrderId. The row's `recIds` array (multikey-indexed)
	// doubles as the record-filter index — "changes touching record R"
	// is `{"recIds": R}`.
	HistoryCollectionSuffix = "__history"
	// HistoryTracesCollection — space-level: one row per (change,
	// trace), PK = spaceId + sep + traceId + sep + OrderId + sep +
	// objectId (OrderIds are only unique per tree, hence the object
	// tail).
	HistoryTracesCollection = "_history_traces"
	// HistoryMetaCollection — space-level per-object index state
	// (stale flag), id = objectId.
	HistoryMetaCollection = "_history_meta"
)

// keySep separates composite primary-key parts (traces only); keySepEnd
// is its exclusive upper bound for prefix range scans. Space (0x20) is
// the ONLY printable choice: the key tail is a lexid OrderId, lexids of
// different lengths can be prefix-related, and the lexid alphabet
// (CharsAllNoEscape) starts at '!' (0x21) — a separator sorting above
// any lexid char would invert bytewise key order versus OrderId order,
// breaking descending pagination (pinned by
// TestListByTraceOrderWithPrefixRelatedOrderIds). Trace ids must not
// contain a space — enforced nowhere today, but a violating id only
// mis-scopes its own history filter, nothing else. RECORD ids never
// enter composite keys: the record filter matches them as exact
// `recIds` array values, so their alphabet is unconstrained (a
// recordId may contain ':', spaces, anything).
const (
	keySep    = " "
	keySepEnd = "!"
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

	// touched tracks objects whose recIds index this process has
	// already ensured — one EnsureIndex per object per process, not
	// per change. Guarded by mu: apply hooks are single-writer, but
	// Backfill runs from read paths concurrently with them.
	mu      sync.Mutex
	touched map[string]struct{}
}

// OpenIndex opens (creating if needed) the space-level history
// collections. Per-object collections are created lazily on first
// write. skipDatasets lists datasets registered with SkipHistory.
func OpenIndex(ctx context.Context, db anystore.DB, spaceId string, skipDatasets []string) (*Index, error) {
	ix := &Index{
		spaceId: spaceId,
		db:      db,
		skip:    make(map[string]struct{}, len(skipDatasets)),
		touched: make(map[string]struct{}),
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

// historyColl resolves an object's history collection. create=false
// returns (nil, nil) when the collection doesn't exist — the object
// simply has no indexed history yet. create=true opens (creating if
// needed) and, once per object per process, ensures the recIds
// multikey index.
func (ix *Index) historyColl(ctx context.Context, objectId string, create bool) (anystore.Collection, error) {
	name := objectId + HistoryCollectionSuffix
	if !create {
		coll, err := ix.db.OpenCollection(ctx, name)
		if err != nil {
			if errors.Is(err, anystore.ErrCollectionNotFound) {
				return nil, nil
			}
			return nil, fmt.Errorf("history: open %s: %w", name, err)
		}
		return coll, nil
	}
	coll, err := ix.db.Collection(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("history: open %s: %w", name, err)
	}
	ix.mu.Lock()
	_, seen := ix.touched[objectId]
	ix.touched[objectId] = struct{}{}
	ix.mu.Unlock()
	if !seen {
		if err := coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"recIds"}, Sparse: true}); err != nil {
			return nil, fmt.Errorf("history: ensure recIds index on %s: %w", name, err)
		}
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
	// recIds is the flat, multikey-indexed record-filter mirror of
	// `recs` — anystore's index extraction doesn't descend array-of-
	// object paths, so the scalar duplicate is what makes
	// `{"recIds": R}` an index scan instead of a collection walk.
	if len(recordIds) > 0 {
		row.Set("recIds", stringArray(a, dedup(recordIds)))
	}
	if err := hist.UpsertOne(txCtx, row); err != nil {
		return fmt.Errorf("history: index change %s: %w", ch.ChangeId, err)
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
	iter, err := ix.traces.Find(query.Key{Path: []string{"obj"}, Filter: query.NewComp(query.CompOpEq, objectId)}).Iter(ctx)
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
// cursor ("" = exhausted). Filter fields combine as AND: the most
// selective field picks the scan route, the rest apply as residual
// checks on that route.
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

	if f.ObjectId == "" {
		return ix.listByTrace(ctx, f, limit, cursor)
	}
	return ix.listDirect(ctx, f, limit, cursor)
}

// idRange builds the typed PK range condition lo < id < hi — the
// prefix-scan shape every join route uses.
func idRange(lo, hi string) query.Filter {
	return query.And{
		query.Key{Path: idPath, Filter: query.NewComp(query.CompOpGt, lo)},
		query.Key{Path: idPath, Filter: query.NewComp(query.CompOpLt, hi)},
	}
}

var idPath = []string{"id"}

// listDirect pages an object's `__history` collection: a reverse
// primary-key scan (PK = OrderId), with ds/author equality plus the
// multikey recIds/traces array-membership filters applied by the
// query engine.
func (ix *Index) listDirect(ctx context.Context, f Filter, limit int, cursor string) ([]ChangeMeta, string, error) {
	coll, err := ix.historyColl(ctx, f.ObjectId, false)
	if err != nil || coll == nil {
		return nil, "", err
	}
	cond := query.And{}
	if f.Dataset != "" {
		cond = append(cond, query.Key{Path: []string{"ds"}, Filter: query.NewComp(query.CompOpEq, f.Dataset)})
	}
	if f.Author != "" {
		cond = append(cond, query.Key{Path: []string{"author"}, Filter: query.NewComp(query.CompOpEq, f.Author)})
	}
	if f.RecordId != "" {
		// Multikey membership on the indexed recIds array.
		cond = append(cond, query.Key{Path: []string{"recIds"}, Filter: query.NewComp(query.CompOpEq, f.RecordId)})
	}
	if f.TraceId != "" {
		cond = append(cond, query.Key{Path: []string{"traces"}, Filter: query.NewComp(query.CompOpEq, f.TraceId)})
	}
	if cursor != "" {
		cond = append(cond, query.Key{Path: idPath, Filter: query.NewComp(query.CompOpLt, cursor)})
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

var recIdsPath = []string{"recIds"}

// listByTrace pages the space-level traces collection by the trace's
// PK prefix and joins each hit to its object's `__history` row.
// OrderIds are only unique per tree, so the cursor is `o<sep>objectId` —
// the PK tail after the trace prefix. ObjectId, Dataset and Author
// apply as residual checks on the joined row.
func (ix *Index) listByTrace(ctx context.Context, f Filter, limit int, cursor string) ([]ChangeMeta, string, error) {
	prefix := ix.spaceId + keySep + f.TraceId + keySep
	upper := ix.spaceId + keySep + f.TraceId + keySepEnd
	if cursor != "" {
		upper = prefix + cursor
	}
	cond := idRange(prefix, upper)

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
		if f.Dataset != "" && meta.Dataset != f.Dataset {
			return ChangeMeta{}, false, nil
		}
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
	cond query.Filter,
	f Filter,
	limit int,
	join func(row *anyenc.Value) (ChangeMeta, bool, error),
	cursorKey func(row *anyenc.Value) string,
) ([]ChangeMeta, string, error) {
	fetch := limit
	if f.Author != "" || f.ObjectId != "" || f.Dataset != "" {
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

// TouchedChangeIds returns every ChangeId known to touch recordId on
// the object — the RecordAt fast-path pre-filter, a `{"recIds": R}`
// multikey-index scan over `__history`. The set is narrowed by
// dataset (the row carries it) but may still be a superset; that is
// sound — the replay re-checks dataset and record per change. What
// must NOT happen is a non-empty set MISSING changes of the requested
// dataset, so datasets opted out of history (SkipHistory — never
// indexed) return (nil, nil) and take the decode-and-filter fallback.
// Bounded by maxTouchedChangeIds; a record touched more often than
// that also falls back.
func (ix *Index) TouchedChangeIds(ctx context.Context, objectId, dataset, recordId string) ([]string, error) {
	if ix.SkipsDataset(dataset) {
		return nil, nil
	}
	hist, err := ix.historyColl(ctx, objectId, false)
	if err != nil || hist == nil {
		return nil, err
	}
	cond := query.And{
		query.Key{Path: recIdsPath, Filter: query.NewComp(query.CompOpEq, recordId)},
		query.Key{Path: []string{"ds"}, Filter: query.NewComp(query.CompOpEq, dataset)},
	}
	iter, err := hist.Find(cond).Iter(ctx)
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

// dedup returns items with duplicates removed, order preserved.
// Returns the input slice unchanged when already unique.
func dedup(items []string) []string {
	seen := make(map[string]struct{}, len(items))
	out := items[:0:0]
	unique := true
	for _, s := range items {
		if _, dup := seen[s]; dup {
			unique = false
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	if unique {
		return items
	}
	return out
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
