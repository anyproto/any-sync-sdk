// Package readstate owns the per-space read/unread engine: the unread
// entries, the per-object seen-heads frontier, per-tag counters, and
// the transitions log backing the read-state feed. See
// docs/read-tracking-proposal.md.
//
// The engine persists three space-shared collections (one set per DB,
// like _meta):
//
//   - _read_unread — one row per unread change, deleted when read.
//   - _read_state  — one row per tracked object: frontier heads,
//     pending remote heads, per-tag counters, and the object's last
//     stateSeq — the dirty-object feed (ChangedSince) reads THIS
//     table; there is no transitions log. A consumer re-pulls the
//     object's unread snapshot and diffs against what it holds.
//
// All mutating methods take the caller's tx context: the apply path
// calls Track inside the same WriteTx as the record mutations, so an
// unread entry is atomic with its change; marks and merges open their
// own tx at the service layer. The engine holds no locks and no
// in-memory state — callers serialize per object (the apply path via
// the tree lock, merges/marks via the service's per-object mutex).
//
// Read marking is forward-only: MarkRead covers the given changes and
// their causal ancestry. The ancestor walk runs over unread rows
// (each row carries its change's prevIds) and traverses gaps — read,
// self-authored, or untracked changes between unread ones — through
// the Resolver, pruning where a walked change's versionId drops below
// the object's minimum unread versionId (an ancestor's versionId is
// always smaller than its descendant's, so nothing unread can hide
// below that line). MarkReadUpTo never walks: range coverage by
// versionId is gap-immune.
package readstate

import (
	"context"
	"errors"
	"sort"
	"sync"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"

	"github.com/anyproto/any-sync-sdk/internal/fanout"
)

const (
	UnreadCollectionName = "_read_unread"
	StateCollectionName  = "_read_state"
)

const (
	fSpace    = "sp"
	fObject   = "ob"
	fChange   = "ch"
	fVersion  = "v"
	fAddSeq   = "q"
	fApplySeq = "as"
	fRecords  = "rs"
	fTags     = "tg"
	fPrevIds  = "p"
	fKey      = "k"
	fStateSeq = "ss"
	fFrontier = "h"
	fPending  = "ph"
	fCounters = "cnt"
	fDataset  = "ds"
	fSeeded   = "sd"
)

// SeqFunc allocates the next per-space stateSeq. Wired to the space's
// ApplySeqAllocator so marks, merges, and applies share one monotonic
// cursor axis. Must be called with the tx held (allocation order =
// commit order).
type SeqFunc func(ctx context.Context) (uint64, error)

// Resolver looks up a change the unread rows no longer (or never did)
// cover — the gap-traversal fallback backed by any-sync's change
// storage. ok=false means the change has not arrived on this device.
type Resolver func(ctx context.Context, objectId, changeId string) (prevIds []string, versionId string, ok bool, err error)

// Track is the apply-path input: one tracked change, classified.
type Track struct {
	ObjectId  string
	Dataset   string
	ChangeId  string
	VersionId string
	AddSeq    uint64
	ApplySeq  uint64 // stateSeq for the unread transition
	RecordIds []string
	Tags      []string
	Key       string // supersede key; "" = none
	PrevIds   []string
	// Tracked=false records no unread entry but still clears a
	// superseded row (Key) and resolves a pending head.
	Tracked bool
	// SelfAuthored changes are born read and advance the frontier
	// in place when their whole causal past is covered.
	SelfAuthored bool
}

// Entry is one unread change as persisted.
type Entry struct {
	ObjectId  string
	Dataset   string
	ChangeId  string
	VersionId string
	AddSeq    uint64
	ApplySeq  uint64
	RecordIds []string
	Tags      []string
	Key       string
	PrevIds   []string
	StateSeq  uint64
}

// ObjectState is one dirty-object feed element: an object whose read
// state changed, at the stateSeq that change advanced it to.
type ObjectState struct {
	ObjectId string
	StateSeq uint64
}

// MarkResult reports what a mark/merge covered.
type MarkResult struct {
	Removed  []Entry
	Frontier []string
	// Pending are marked ids not yet arrived on this device; they are
	// persisted and resolved when the change applies.
	Pending  []string
	StateSeq uint64 // 0 when nothing changed
}

type Engine struct {
	db      anystore.DB
	spaceId string
	seq     SeqFunc
	resolve Resolver

	// subs are the best-effort state pings (objectId, stateSeq).
	// Callers fire NotifyState AFTER their tx commits — never from
	// inside one — so a subscriber's ChangedSince pull sees the data.
	subs fanout.Registry[statePing]

	openOnce sync.Once
	openErr  error
	unread   anystore.Collection
	state    anystore.Collection
}

func New(db anystore.DB, spaceId string, seq SeqFunc, resolve Resolver) *Engine {
	return &Engine{db: db, spaceId: spaceId, seq: seq, resolve: resolve}
}

func (e *Engine) collections(ctx context.Context) error {
	e.openOnce.Do(func() {
		open := func(name string, indexes ...anystore.IndexInfo) (anystore.Collection, error) {
			coll, err := e.db.Collection(ctx, name)
			if err != nil {
				return nil, err
			}
			for _, idx := range indexes {
				if err = coll.EnsureIndex(ctx, idx); err != nil {
					return nil, err
				}
			}
			return coll, nil
		}
		e.unread, e.openErr = open(UnreadCollectionName,
			anystore.IndexInfo{Name: "idx__read_ob_v", Fields: []string{fObject, fVersion}},
			anystore.IndexInfo{Name: "idx__read_ob_k", Fields: []string{fObject, fKey}, Sparse: true},
		)
		if e.openErr != nil {
			return
		}
		e.state, e.openErr = open(StateCollectionName,
			anystore.IndexInfo{Name: "idx__read_state_sp_ss", Fields: []string{fSpace, fStateSeq}},
		)
	})
	return e.openErr
}

func unreadRowId(objectId, changeId string) string { return objectId + ":" + changeId }

// objState is the _read_state row, loaded whole and written whole —
// one row per object keeps every mutation a single upsert.
type objState struct {
	frontier map[string]struct{}
	pending  map[string]struct{}
	counters map[string]int
	stateSeq uint64
	// seeded records that first-sight seeding ran for this object —
	// durable, so a crash between restore and seed re-seeds on the
	// next load instead of silently skipping.
	seeded bool
}

func (e *Engine) loadState(ctx context.Context, objectId string) (*objState, error) {
	st := &objState{
		frontier: map[string]struct{}{},
		pending:  map[string]struct{}{},
		counters: map[string]int{},
	}
	doc, err := e.state.FindId(ctx, objectId)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return st, nil
		}
		return nil, err
	}
	v := doc.Value()
	for _, h := range v.GetArray(fFrontier) {
		st.frontier[string(h.GetStringBytes())] = struct{}{}
	}
	for _, h := range v.GetArray(fPending) {
		st.pending[string(h.GetStringBytes())] = struct{}{}
	}
	if cnt := v.Get(fCounters); cnt != nil && cnt.Type() == anyenc.TypeObject {
		obj, _ := cnt.Object()
		obj.Visit(func(k []byte, vv *anyenc.Value) {
			st.counters[string(k)] = vv.GetInt()
		})
	}
	st.stateSeq = uint64(v.GetInt(fStateSeq))
	st.seeded = v.GetBool(fSeeded)
	return st, nil
}

func (e *Engine) persistState(ctx context.Context, objectId string, st *objState) error {
	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		v.Set(fSpace, a.NewString(e.spaceId))
		v.Set(fFrontier, stringSetValue(a, st.frontier))
		v.Set(fPending, stringSetValue(a, st.pending))
		cnt := a.NewObject()
		for tag, n := range st.counters {
			if n > 0 {
				cnt.Set(tag, a.NewNumberInt(n))
			}
		}
		v.Set(fCounters, cnt)
		v.Set(fStateSeq, a.NewNumberInt(int(st.stateSeq)))
		if st.seeded {
			v.Set(fSeeded, a.NewTrue())
		}
		return v, true, nil
	})
	_, err := e.state.UpsertId(ctx, objectId, mod)
	return err
}

func stringSetValue(a *anyenc.Arena, set map[string]struct{}) *anyenc.Value {
	arr := a.NewArray()
	i := 0
	for s := range set {
		arr.SetArrayItem(i, a.NewString(s))
		i++
	}
	return arr
}

func stringsValue(a *anyenc.Arena, ss []string) *anyenc.Value {
	arr := a.NewArray()
	for i, s := range ss {
		arr.SetArrayItem(i, a.NewString(s))
	}
	return arr
}

func valueStrings(v *anyenc.Value, key string) []string {
	arr := v.GetArray(key)
	if len(arr) == 0 {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		out = append(out, string(item.GetStringBytes()))
	}
	return out
}

func entryFromDoc(v *anyenc.Value) Entry {
	return Entry{
		ObjectId:  string(v.GetStringBytes(fObject)),
		Dataset:   string(v.GetStringBytes(fDataset)),
		ChangeId:  string(v.GetStringBytes(fChange)),
		VersionId: string(v.GetStringBytes(fVersion)),
		AddSeq:    uint64(v.GetInt(fAddSeq)),
		ApplySeq:  uint64(v.GetInt(fApplySeq)),
		RecordIds: valueStrings(v, fRecords),
		Tags:      valueStrings(v, fTags),
		Key:       string(v.GetStringBytes(fKey)),
		PrevIds:   valueStrings(v, fPrevIds),
		StateSeq:  uint64(v.GetInt(fStateSeq)),
	}
}

// loadEntries returns the object's unread rows with versionId <= upTo
// (all when upTo == ""), ascending by versionId.
func (e *Engine) loadEntries(ctx context.Context, objectId, upTo string) ([]Entry, error) {
	filter := query.And{
		query.Key{Path: []string{fObject}, Filter: query.NewComp(query.CompOpEq, objectId)},
	}
	if upTo != "" {
		filter = append(filter, query.Key{Path: []string{fVersion}, Filter: query.NewComp(query.CompOpLte, upTo)})
	}
	it, err := e.unread.Find(filter).Sort(fVersion).Iter(ctx)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []Entry
	for it.Next() {
		doc, err := it.Doc()
		if err != nil {
			return nil, err
		}
		out = append(out, entryFromDoc(doc.Value()))
	}
	return out, nil
}

func (e *Engine) insertEntry(ctx context.Context, t Track) error {
	arena := &anyenc.Arena{}
	v := arena.NewObject()
	v.Set("id", arena.NewString(unreadRowId(t.ObjectId, t.ChangeId)))
	v.Set(fSpace, arena.NewString(e.spaceId))
	v.Set(fObject, arena.NewString(t.ObjectId))
	v.Set(fDataset, arena.NewString(t.Dataset))
	v.Set(fChange, arena.NewString(t.ChangeId))
	v.Set(fVersion, arena.NewString(t.VersionId))
	v.Set(fAddSeq, arena.NewNumberInt(int(t.AddSeq)))
	v.Set(fApplySeq, arena.NewNumberInt(int(t.ApplySeq)))
	v.Set(fRecords, stringsValue(arena, t.RecordIds))
	v.Set(fTags, stringsValue(arena, t.Tags))
	if t.Key != "" {
		v.Set(fKey, arena.NewString(t.Key))
	}
	v.Set(fPrevIds, stringsValue(arena, t.PrevIds))
	v.Set(fStateSeq, arena.NewNumberInt(int(t.ApplySeq)))
	return e.unread.UpsertOne(ctx, v)
}

// Track records one classified change from the apply path. Must run
// inside the apply's WriteTx (pass tx.Context()).
func (e *Engine) TrackChange(ctx context.Context, t Track) error {
	if err := e.collections(ctx); err != nil {
		return err
	}
	st, err := e.loadState(ctx, t.ObjectId)
	if err != nil {
		return err
	}
	dirty := false

	// Supersede: a new entry (or an untracked change) with a key
	// replaces/clears the previous same-key entry.
	if t.Key != "" {
		old, err := e.findByKey(ctx, t.ObjectId, t.Key)
		if err != nil {
			return err
		}
		if old != nil {
			if err = e.removeEntries(ctx, st, []Entry{*old}); err != nil {
				return err
			}
			dirty = true
		}
	}

	_, covered := st.frontier[t.ChangeId]
	_, wasPending := st.pending[t.ChangeId]

	if t.Tracked && !t.SelfAuthored && !covered && !wasPending {
		if err = e.insertEntry(ctx, t); err != nil {
			return err
		}
		for _, tag := range t.Tags {
			st.counters[tag]++
		}
		dirty = true
	}

	if t.SelfAuthored {
		// Advance the frontier in place when the change's whole causal
		// past is already covered — keeps the published frontier fresh
		// without a mark.
		allCovered := true
		for _, p := range t.PrevIds {
			if _, ok := st.frontier[p]; !ok {
				allCovered = false
				break
			}
		}
		if allCovered {
			for _, p := range t.PrevIds {
				delete(st.frontier, p)
			}
			st.frontier[t.ChangeId] = struct{}{}
			dirty = true
		}
	}

	if !dirty && !wasPending {
		return nil
	}
	if dirty {
		if t.ApplySeq > st.stateSeq {
			st.stateSeq = t.ApplySeq
		}
		if err = e.persistState(ctx, t.ObjectId, st); err != nil {
			return err
		}
	}

	if wasPending {
		// A previously-marked head arrived: another device read up to
		// here. Cover it and its ancestry now.
		delete(st.pending, t.ChangeId)
		if err = e.persistState(ctx, t.ObjectId, st); err != nil {
			return err
		}
		if _, err = e.markLocked(ctx, t.ObjectId, []string{t.ChangeId}, t.ApplySeq); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) findByKey(ctx context.Context, objectId, key string) (*Entry, error) {
	filter := query.And{
		query.Key{Path: []string{fObject}, Filter: query.NewComp(query.CompOpEq, objectId)},
		query.Key{Path: []string{fKey}, Filter: query.NewComp(query.CompOpEq, key)},
	}
	it, err := e.unread.Find(filter).Limit(1).Iter(ctx)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	if !it.Next() {
		return nil, nil
	}
	doc, err := it.Doc()
	if err != nil {
		return nil, err
	}
	entry := entryFromDoc(doc.Value())
	return &entry, nil
}

// removeEntries deletes rows and adjusts counters. It does not touch
// the frontier.
func (e *Engine) removeEntries(ctx context.Context, st *objState, entries []Entry) error {
	for _, en := range entries {
		if err := e.unread.DeleteId(ctx, unreadRowId(en.ObjectId, en.ChangeId)); err != nil {
			if errors.Is(err, anystore.ErrDocNotFound) {
				continue
			}
			return err
		}
		for _, tag := range en.Tags {
			if st.counters[tag] > 0 {
				st.counters[tag]--
			}
		}
	}
	return nil
}

// MarkRead covers the given change ids and their causal ancestry.
// Ids not yet arrived on this device are persisted as pending heads.
// Runs in the caller's tx; the caller serializes per object.
func (e *Engine) MarkRead(ctx context.Context, objectId string, changeIds []string) (MarkResult, error) {
	if err := e.collections(ctx); err != nil {
		return MarkResult{}, err
	}
	return e.markLocked(ctx, objectId, changeIds, 0)
}

func (e *Engine) markLocked(ctx context.Context, objectId string, changeIds []string, stateSeq uint64) (MarkResult, error) {
	st, err := e.loadState(ctx, objectId)
	if err != nil {
		return MarkResult{}, err
	}
	// Fast path for idempotent re-merges (startup reconcile replays
	// every published frontier): all ids already covered → one state
	// read, no row scan, no writes.
	todo := changeIds[:0:0]
	for _, id := range changeIds {
		if _, ok := st.frontier[id]; !ok {
			todo = append(todo, id)
		}
	}
	if len(todo) == 0 {
		return MarkResult{Frontier: setToSlice(st.frontier)}, nil
	}
	changeIds = todo
	entries, err := e.loadEntries(ctx, objectId, "")
	if err != nil {
		return MarkResult{}, err
	}
	if len(entries) == 0 {
		// Nothing to cover — never walk (with no unread rows the
		// versionId prune has no floor and a walk would chase prevIds
		// to the root). Known ids join the frontier, unknown ids park
		// as pending.
		return e.markNoEntries(ctx, objectId, st, changeIds, stateSeq)
	}
	byId := make(map[string]*Entry, len(entries))
	minUnreadV := ""
	for i := range entries {
		byId[entries[i].ChangeId] = &entries[i]
		if minUnreadV == "" || entries[i].VersionId < minUnreadV {
			minUnreadV = entries[i].VersionId
		}
	}

	var (
		covered  []Entry
		pending  []string
		visited  = map[string]struct{}{}
		stack    []string
		newHeads []string // marked ids that exist locally
	)
	for _, id := range changeIds {
		if _, ok := st.frontier[id]; ok {
			continue
		}
		if _, ok := byId[id]; ok {
			stack = append(stack, id)
			newHeads = append(newHeads, id)
			continue
		}
		// Not an unread row: already read here, or not yet synced.
		if _, _, ok, rerr := e.resolveGap(ctx, objectId, id); rerr != nil {
			return MarkResult{}, rerr
		} else if ok {
			// Exists locally and already read — still a frontier
			// candidate (another device may be behind).
			newHeads = append(newHeads, id)
			stack = append(stack, id)
		} else {
			pending = append(pending, id)
		}
	}

	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, ok := visited[id]; ok {
			continue
		}
		visited[id] = struct{}{}
		if _, ok := st.frontier[id]; ok {
			continue
		}
		var prevIds []string
		if en, ok := byId[id]; ok {
			covered = append(covered, *en)
			prevIds = en.PrevIds
		} else {
			// Gap: read/untracked change between unread ones. Resolve
			// its prevIds from change storage; prune when nothing
			// unread can sit below.
			p, v, ok, rerr := e.resolveGap(ctx, objectId, id)
			if rerr != nil {
				return MarkResult{}, rerr
			}
			if !ok || (minUnreadV != "" && v < minUnreadV) {
				continue
			}
			prevIds = p
		}
		stack = append(stack, prevIds...)
	}

	if len(covered) == 0 && len(pending) == 0 && len(newHeads) == 0 {
		return MarkResult{Frontier: setToSlice(st.frontier)}, nil
	}

	// Frontier: drop members referenced as prevIds of covered rows
	// (now redundant), then add the marked local ids. A marked id that
	// is itself a direct ancestor of another covered row gets dropped
	// by the same prevIds pass; deeper redundancy is tolerated — a
	// non-minimal frontier is correct, just larger.
	for _, en := range covered {
		for _, p := range en.PrevIds {
			delete(st.frontier, p)
		}
	}
	coveredPrev := map[string]struct{}{}
	for _, en := range covered {
		for _, p := range en.PrevIds {
			coveredPrev[p] = struct{}{}
		}
	}
	for _, h := range newHeads {
		if _, redundant := coveredPrev[h]; !redundant {
			st.frontier[h] = struct{}{}
		}
	}
	for _, id := range pending {
		st.pending[id] = struct{}{}
	}

	if err = e.removeEntries(ctx, st, covered); err != nil {
		return MarkResult{}, err
	}

	if stateSeq == 0 {
		if e.seq == nil {
			return MarkResult{}, errors.New("readstate: no seq allocator")
		}
		stateSeq, err = e.seq(ctx)
		if err != nil {
			return MarkResult{}, err
		}
	}
	if stateSeq > st.stateSeq {
		st.stateSeq = stateSeq
	}
	e.compactFrontier(ctx, objectId, st)
	if err = e.persistState(ctx, objectId, st); err != nil {
		return MarkResult{}, err
	}
	return MarkResult{
		Removed:  covered,
		Frontier: setToSlice(st.frontier),
		Pending:  pending,
		StateSeq: st.stateSeq,
	}, nil
}

// markNoEntries is markLocked's fully-read fast path: no unread rows
// exist, so marking only updates the frontier and pending sets.
func (e *Engine) markNoEntries(ctx context.Context, objectId string, st *objState, changeIds []string, stateSeq uint64) (MarkResult, error) {
	changed := false
	var pending []string
	for _, id := range changeIds {
		_, _, ok, err := e.resolveGap(ctx, objectId, id)
		if err != nil {
			return MarkResult{}, err
		}
		if ok {
			st.frontier[id] = struct{}{}
			changed = true
			continue
		}
		if _, dup := st.pending[id]; !dup {
			st.pending[id] = struct{}{}
			pending = append(pending, id)
			changed = true
		}
	}
	if !changed {
		return MarkResult{Frontier: setToSlice(st.frontier)}, nil
	}
	if stateSeq == 0 {
		if e.seq == nil {
			return MarkResult{}, errors.New("readstate: no seq allocator")
		}
		var err error
		if stateSeq, err = e.seq(ctx); err != nil {
			return MarkResult{}, err
		}
	}
	if stateSeq > st.stateSeq {
		st.stateSeq = stateSeq
	}
	e.compactFrontier(ctx, objectId, st)
	if err := e.persistState(ctx, objectId, st); err != nil {
		return MarkResult{}, err
	}
	return MarkResult{Frontier: setToSlice(st.frontier), Pending: pending, StateSeq: st.stateSeq}, nil
}

// maxFrontierSize caps the persisted (and published) frontier. Growth
// comes from merging other devices' independently-reduced frontiers;
// members accumulate because reduction only fires via covered rows.
const maxFrontierSize = 64

// compactFrontier drops the oldest members (smallest local versionId,
// resolved via the gap resolver; unresolvable members drop first) when
// the frontier exceeds the cap. Locally safe: membership is only a
// classify shortcut and a walk stop — coverage of applied changes is
// the watermark's job. The published-coverage trade-off: a dropped
// member that was NOT an ancestor of the remaining set shrinks what
// other devices mark read, costing them a bounded re-read, never
// corruption; oldest-first makes that case rare.
func (e *Engine) compactFrontier(ctx context.Context, objectId string, st *objState) {
	if len(st.frontier) <= maxFrontierSize {
		return
	}
	type member struct{ id, v string }
	members := make([]member, 0, len(st.frontier))
	for id := range st.frontier {
		_, v, ok, err := e.resolveGap(ctx, objectId, id)
		if err != nil || !ok {
			v = ""
		}
		members = append(members, member{id: id, v: v})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].v < members[j].v })
	for _, m := range members[:len(members)-maxFrontierSize] {
		delete(st.frontier, m.id)
	}
}

func (e *Engine) resolveGap(ctx context.Context, objectId, changeId string) ([]string, string, bool, error) {
	if e.resolve == nil {
		return nil, "", false, nil
	}
	return e.resolve(ctx, objectId, changeId)
}

// MarkReadUpTo covers every unread change with versionId <= upTo
// ("" = everything). Range coverage needs no ancestry walk.
func (e *Engine) MarkReadUpTo(ctx context.Context, objectId, upTo string) (MarkResult, error) {
	res, _, err := e.MarkReadUpToChunk(ctx, objectId, upTo, 0)
	return res, err
}

// MarkReadUpToChunk is MarkReadUpTo bounded to at most maxEntries
// covered entries (0 = unbounded); done=false means more remain below
// upTo. Covering a versionId prefix is a valid frontier advance
// (forward-only marking), so a caller can commit each chunk in its own
// tx and a crash mid-way just resumes — the huge-ReadAll path uses
// this to keep transactions bounded.
func (e *Engine) MarkReadUpToChunk(ctx context.Context, objectId, upTo string, maxEntries int) (MarkResult, bool, error) {
	if err := e.collections(ctx); err != nil {
		return MarkResult{}, true, err
	}
	st, err := e.loadState(ctx, objectId)
	if err != nil {
		return MarkResult{}, true, err
	}
	covered, err := e.loadEntries(ctx, objectId, upTo)
	if err != nil {
		return MarkResult{}, true, err
	}
	done := true
	if maxEntries > 0 && len(covered) > maxEntries {
		covered = covered[:maxEntries]
		done = false
	}
	if len(covered) == 0 {
		return MarkResult{Frontier: setToSlice(st.frontier)}, true, nil
	}

	// Frontier additions: covered ids not referenced as prevIds by
	// other covered rows (the maxima of the covered subgraph). Old
	// members referenced as prevIds of covered rows are redundant.
	inPrev := map[string]struct{}{}
	for _, en := range covered {
		for _, p := range en.PrevIds {
			inPrev[p] = struct{}{}
			delete(st.frontier, p)
		}
	}
	for _, en := range covered {
		if _, ok := inPrev[en.ChangeId]; !ok {
			st.frontier[en.ChangeId] = struct{}{}
		}
	}

	if err = e.removeEntries(ctx, st, covered); err != nil {
		return MarkResult{}, done, err
	}
	if e.seq == nil {
		return MarkResult{}, done, errors.New("readstate: no seq allocator")
	}
	stateSeq, err := e.seq(ctx)
	if err != nil {
		return MarkResult{}, done, err
	}
	if stateSeq > st.stateSeq {
		st.stateSeq = stateSeq
	}
	e.compactFrontier(ctx, objectId, st)
	if err = e.persistState(ctx, objectId, st); err != nil {
		return MarkResult{}, done, err
	}
	return MarkResult{Removed: covered, Frontier: setToSlice(st.frontier), StateSeq: st.stateSeq}, done, nil
}

// MergeHeads applies another device's published frontier. Identical
// semantics to MarkRead: known ids cover their ancestry, unknown ids
// park as pending heads.
func (e *Engine) MergeHeads(ctx context.Context, objectId string, heads []string) (MarkResult, error) {
	return e.MarkRead(ctx, objectId, heads)
}

// ClearRecords drops deleted records from unread entries: an entry
// referencing only deleted records is removed (counters and feed
// updated); an entry also touching surviving records just sheds the
// deleted ids.
func (e *Engine) ClearRecords(ctx context.Context, objectId string, recordIds []string, stateSeq uint64) error {
	if err := e.collections(ctx); err != nil {
		return err
	}
	deleted := map[string]struct{}{}
	for _, id := range recordIds {
		deleted[id] = struct{}{}
	}
	entries, err := e.loadEntries(ctx, objectId, "")
	if err != nil {
		return err
	}
	st, err := e.loadState(ctx, objectId)
	if err != nil {
		return err
	}
	dirty := false
	for _, en := range entries {
		var kept []string
		for _, r := range en.RecordIds {
			if _, gone := deleted[r]; !gone {
				kept = append(kept, r)
			}
		}
		if len(kept) == len(en.RecordIds) {
			continue
		}
		dirty = true
		if len(kept) == 0 {
			if err = e.removeEntries(ctx, st, []Entry{en}); err != nil {
				return err
			}
			continue
		}
		keptCopy := kept
		mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
			v.Set(fRecords, stringsValue(a, keptCopy))
			return v, true, nil
		})
		if _, err = e.unread.UpsertId(ctx, unreadRowId(en.ObjectId, en.ChangeId), mod); err != nil {
			return err
		}
	}
	if !dirty {
		return nil
	}
	if stateSeq == 0 {
		if e.seq == nil {
			return errors.New("readstate: no seq allocator")
		}
		if stateSeq, err = e.seq(ctx); err != nil {
			return err
		}
	}
	if stateSeq > st.stateSeq {
		st.stateSeq = stateSeq
	}
	return e.persistState(ctx, objectId, st)
}

// UnreadEntries returns the object's current unread set, ascending by
// versionId ("" upTo = all).
func (e *Engine) UnreadEntries(ctx context.Context, objectId string) ([]Entry, uint64, error) {
	if err := e.collections(ctx); err != nil {
		return nil, 0, err
	}
	st, err := e.loadState(ctx, objectId)
	if err != nil {
		return nil, 0, err
	}
	entries, err := e.loadEntries(ctx, objectId, "")
	if err != nil {
		return nil, 0, err
	}
	return entries, st.stateSeq, nil
}

// Counts returns the per-tag unread counters for an object.
func (e *Engine) Counts(ctx context.Context, objectId string) (map[string]int, error) {
	if err := e.collections(ctx); err != nil {
		return nil, err
	}
	st, err := e.loadState(ctx, objectId)
	if err != nil {
		return nil, err
	}
	return st.counters, nil
}

// Frontier returns the object's current seen-heads frontier and
// pending remote heads.
func (e *Engine) Frontier(ctx context.Context, objectId string) (heads, pending []string, err error) {
	if err = e.collections(ctx); err != nil {
		return nil, nil, err
	}
	st, err := e.loadState(ctx, objectId)
	if err != nil {
		return nil, nil, err
	}
	return setToSlice(st.frontier), setToSlice(st.pending), nil
}

// ChangedSince returns objects whose read state advanced past since,
// ascending by stateSeq, capped at limit (0 = no cap). One element per
// object (this reads the per-object state rows, not a log): the
// consumer re-pulls UnreadEntries for each dirty object and diffs
// against what it holds. Page by passing the last StateSeq back.
func (e *Engine) ChangedSince(ctx context.Context, since uint64, limit int) ([]ObjectState, error) {
	if err := e.collections(ctx); err != nil {
		return nil, err
	}
	filter := query.And{
		query.Key{Path: []string{fSpace}, Filter: query.NewComp(query.CompOpEq, e.spaceId)},
		query.Key{Path: []string{fStateSeq}, Filter: query.NewComp(query.CompOpGt, int(since))},
	}
	q := e.state.Find(filter).Sort(fSpace, fStateSeq)
	if limit > 0 {
		q = q.Limit(uint(limit))
	}
	it, err := q.Iter(ctx)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []ObjectState
	for it.Next() {
		doc, err := it.Doc()
		if err != nil {
			return nil, err
		}
		v := doc.Value()
		out = append(out, ObjectState{
			ObjectId: string(v.GetStringBytes("id")),
			StateSeq: uint64(v.GetInt(fStateSeq)),
		})
	}
	return out, nil
}

// SubscribeState registers a best-effort ping fired after a committed
// read-state change (new unread, mark, merge). cb runs synchronously
// on the notifying path — keep it small. Cancel is idempotent. A
// dropped ping is recovered by pulling TransitionsSince from the
// consumer's cursor.
func (e *Engine) SubscribeState(cb func(objectId string, stateSeq uint64)) (cancel func()) {
	if cb == nil {
		return func() {}
	}
	return e.subs.Add(func(p statePing) { cb(p.objectId, p.stateSeq) })
}

// statePing is the subs event payload.
type statePing struct {
	objectId string
	stateSeq uint64
}

// NotifyState fires the subscribed pings. Call AFTER the tx that
// produced stateSeq committed.
func (e *Engine) NotifyState(objectId string, stateSeq uint64) {
	e.subs.Dispatch(statePing{objectId: objectId, stateSeq: stateSeq})
}

// Seeded reports whether first-sight seeding already ran for the
// object. Durable: only SeedFrontier sets it, in the same tx as the
// seed itself, so the answer survives crashes on either side.
func (e *Engine) Seeded(ctx context.Context, objectId string) (bool, error) {
	if err := e.collections(ctx); err != nil {
		return false, err
	}
	st, err := e.loadState(ctx, objectId)
	if err != nil {
		return false, err
	}
	return st.seeded, nil
}

// MarkSeeded records first-sight seeding as done WITHOUT touching
// entries or frontier — the consult-KV seed path uses it: the
// account's published frontiers, merged separately in the same tx,
// are the real seed. Idempotent.
func (e *Engine) MarkSeeded(ctx context.Context, objectId string) error {
	if err := e.collections(ctx); err != nil {
		return err
	}
	st, err := e.loadState(ctx, objectId)
	if err != nil {
		return err
	}
	if st.seeded {
		return nil
	}
	st.seeded = true
	return e.persistState(ctx, objectId, st)
}

// SeedFrontier performs first-sight seeding: sets the frontier to the
// given heads (everything at or behind them is read), drops any
// unread entries that slipped in around the restore (their became-
// read transitions surface on the feed), preserves pending remote
// heads, and records the seed durably — all in the caller's tx.
// Idempotent: a second call on a seeded object is a no-op. Returns
// whether the seed ran.
func (e *Engine) SeedFrontier(ctx context.Context, objectId string, heads []string) (bool, error) {
	if err := e.collections(ctx); err != nil {
		return false, err
	}
	st, err := e.loadState(ctx, objectId)
	if err != nil {
		return false, err
	}
	if st.seeded {
		return false, nil
	}
	entries, err := e.loadEntries(ctx, objectId, "")
	if err != nil {
		return false, err
	}
	if err = e.removeEntries(ctx, st, entries); err != nil {
		return false, err
	}
	st.frontier = map[string]struct{}{}
	for _, h := range heads {
		st.frontier[h] = struct{}{}
	}
	st.seeded = true
	// Bump the dirty-feed watermark only when the seed actually flipped
	// entries — seeding an untouched object is not consumer-visible.
	if len(entries) > 0 {
		if e.seq == nil {
			return false, errors.New("readstate: no seq allocator")
		}
		stateSeq, serr := e.seq(ctx)
		if serr != nil {
			return false, serr
		}
		if stateSeq > st.stateSeq {
			st.stateSeq = stateSeq
		}
	}
	if err = e.persistState(ctx, objectId, st); err != nil {
		return false, err
	}
	return true, nil
}

// WriteTx runs fn inside a write transaction on the engine's DB —
// the tx opener for callers outside the apply path (marks, merges,
// reconcile). fn's error rolls the tx back and is returned.
func (e *Engine) WriteTx(ctx context.Context, fn func(txCtx context.Context) error) error {
	tx, err := e.db.WriteTx(ctx)
	if err != nil {
		return err
	}
	if err = fn(tx.Context()); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func setToSlice(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	return out
}
