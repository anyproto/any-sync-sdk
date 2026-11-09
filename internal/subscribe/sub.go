package subscribe

import (
	"bytes"
	"errors"
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/syncpool"
	"github.com/cheggaaa/mb/v3"

	"github.com/anyproto/any-sync-sdk/space"

	"github.com/anyproto/any-sync-sdk/internal/anyencx"
)

// querySub is the per-subscription state. Mutated only under
// Engine.mu — never lock-protected on its own. Read-side fields (mb,
// err) are racy-safe via atomic.Value / mb's own concurrency.
type querySub struct {
	id     uint64
	engine *Engine // parent; nil after Close
	cfg    SubConfig

	entries map[string]*entry
	minRef  *entry
	maxRef  *entry

	// Window state. complete: the window holds every match — its
	// snapshot started at offset 0 and found nothing past the window's
	// edge, and no match has been left past the edge since. unranked:
	// the sentinel took its slot without being ranked against the
	// matches past the edge (a held row's own sort move, an insert
	// below capacity), so promoting it could lift the wrong row into
	// view. departed: held rows that left during the current apply.
	// A refill (SubConfig.Refill) settles both from storage; a window
	// that cannot refill counts them as losses, and lost*100 at or
	// above lostLimit closes the sub.
	complete  bool
	unranked  bool
	departed  int
	lost      int
	lostLimit int

	docBuf *syncpool.DocBuffer

	mb     *mb.MB[space.SubscriptionEvent]
	closed bool         // guarded by engine.mu
	err    atomic.Value // error sentinel for Sub.Err()
}

func newSub(cfg SubConfig) *querySub {
	return &querySub{
		cfg:     cfg,
		entries: map[string]*entry{},
		docBuf:  docBufFor(),
		mb:      mb.New[space.SubscriptionEvent](cfg.MailboxCap),
	}
}

// release drops the held set of a closed sub. apply returns on closed
// before touching it, so nothing reads the window afterwards.
func (s *querySub) release() {
	s.entries = nil
	s.minRef, s.maxRef = nil, nil
}

// closeWith closes the mailbox with reason as the close error. The
// held set stays until the consumer's Close releases it.
func (s *querySub) closeWith(reason error) {
	s.err.Store(reason)
	_ = s.mb.Close()
	s.closed = true
}

// appendInitial is called by Engine.Subscribe once per snapshot row.
// Inserts the entry with what the sub keeps per row. Snapshot rows
// arrive in sort order (ascending tuple), so the last row appended is
// maxRef and the first is minRef when the loop completes.
func (s *querySub) appendInitial(id string, doc *anyenc.Value) {
	if doc == nil || id == "" {
		return
	}
	var raw []byte
	if s.windowed() {
		raw = encode(doc)
	}
	s.insertEntry(id, s.tupleFor(doc), raw)
}

// windowed reports whether the sub has a boundary (Limit > 0). A
// windowed sub keeps each held row's tuple and encoded doc: the
// sentinel is compared against arrivals and ships its doc when an
// unrelated event promotes it into view. An unbounded sub has no
// sentinel and tracks membership only — one entry per row, no doc and
// no tuple — so a subscription over a whole collection costs its id
// set, not a copy of every row for its lifetime.
func (s *querySub) windowed() bool {
	return s.cfg.Limit > 0
}

// withEncoding marshals doc into a pooled scratch buffer and hands the
// encoding to f for the duration of the call.
func withEncoding(doc *anyenc.Value, f func(enc []byte)) {
	bp := encBufs.Get().(*[]byte)
	*bp = doc.MarshalTo((*bp)[:0])
	f(*bp)
	if cap(*bp) <= maxPooledEncBuf {
		encBufs.Put(bp)
	}
}

// encode returns doc's encoding in a slice of its own. A held row
// costs its encoded size; a parsed copy would cost about ten times
// that for as long as the window holds it.
func encode(doc *anyenc.Value) (raw []byte) {
	withEncoding(doc, func(enc []byte) { raw = bytes.Clone(enc) })
	return raw
}

// encBufs pools the encoding scratch; a buffer grown past
// maxPooledEncBuf by one large row is dropped, not kept.
var encBufs = sync.Pool{New: func() any { return new([]byte) }}

const maxPooledEncBuf = 64 << 10

// parseHeld parses a held encoding in place: raw is never mutated
// once encoded, so the value may alias it. A row that does not parse
// back (nested past the parser's depth limit) ships no doc, as a
// failed clone does.
func parseHeld(raw []byte) *anyenc.Value {
	v, err := new(anyenc.Parser).ParseOwned(raw)
	if err != nil {
		return nil
	}
	return v
}

// capture takes what the sub needs of a post doc before the apply
// moves on: a windowed sub keeps the encoding on its entry and parses
// the event's copy from it only if an event ships the row; an
// unbounded sub keeps nothing and ships a clone.
func (s *querySub) capture(doc *anyenc.Value) (eventDoc *anyenc.Value, raw []byte) {
	if !s.windowed() {
		return anyencx.Clone(doc), nil
	}
	return nil, encode(doc)
}

// tupleFor builds the sort tuple for doc; nil for an unbounded sub,
// which has no boundary to compare against (and may have no Sort).
func (s *querySub) tupleFor(doc *anyenc.Value) anyenc.Tuple {
	if !s.windowed() {
		return nil
	}
	return s.cfg.Sort.AppendKey(nil, doc)
}

// atCapacity reports whether the held set has reached sentinel level.
// Only meaningful when cfg.Limit > 0.
func (s *querySub) atCapacity() bool {
	return s.cfg.Limit > 0 && len(s.entries) == s.cfg.Limit+1
}

// sentinel returns the sentinel entry (the largest-tuple holder) when
// the window is at capacity, else nil. Triggers ensureMaxRef when
// maxRef was lazily invalidated.
func (s *querySub) sentinel() *entry {
	if !s.atCapacity() {
		return nil
	}
	s.ensureMaxRef()
	return s.maxRef
}

// sentinelId returns the sentinel's id or "" when there's no sentinel.
func (s *querySub) sentinelId() string {
	if e := s.sentinel(); e != nil {
		return e.id
	}
	return ""
}

// subPending is the per-apply scratch — collects added/updated entries
// and removed ids before assembling the wire SubscriptionEvent. Lives
// on the stack of querySub.apply.
type subPending struct {
	added   []pendingRec
	updated []pendingRec
	removed []space.RemovedRecord
	seen    map[string]struct{}
	// unreadable holds the ids of this apply's records whose post value
	// could not be read; their transitions are unknown until settle.
	unreadable map[string]struct{}
}

type pendingRec struct {
	e   *entry
	doc *anyenc.Value   // the event's copy of the post doc (unbounded sub)
	raw []byte          // the held encoding the event parses its doc from (windowed sub)
	ops []space.EventOp // nil for implicit transitions
}

// value returns the doc the event ships for the record.
func (pr pendingRec) value() *anyenc.Value {
	if pr.doc != nil {
		return pr.doc
	}
	return parseHeld(pr.raw)
}

// apply processes one wire event. Builds at most one SubscriptionEvent
// per call. Per-record classification, sentinel transitions, drift +
// overflow checks all happen here under Engine.mu (so other engine
// methods observe consistent state).
func (s *querySub) apply(ev Event, postValue PostValueFn) {
	if s.closed {
		return
	}

	p := subPending{seen: map[string]struct{}{}}

	for i, rc := range ev.Records {
		if rc.Id == "" {
			continue
		}
		s.applyRecord(i, rc, postValue, &p)
	}
	if !s.settle(ev, &p) {
		return
	}

	if len(p.added) == 0 && len(p.updated) == 0 && len(p.removed) == 0 {
		s.checkDrift()
		return
	}

	subev := space.SubscriptionEvent{VersionId: ev.VersionId}
	if len(p.added) > 0 {
		subev.Added = make([]space.SubRecord, 0, len(p.added))
		for _, pr := range p.added {
			subev.Added = append(subev.Added, space.SubRecord{Id: pr.e.id, Doc: pr.value(), Ops: pr.ops})
		}
	}
	if len(p.updated) > 0 {
		subev.Updated = make([]space.SubRecord, 0, len(p.updated))
		for _, pr := range p.updated {
			subev.Updated = append(subev.Updated, space.SubRecord{Id: pr.e.id, Doc: pr.value(), Ops: pr.ops})
		}
	}
	if len(p.removed) > 0 {
		subev.Removed = p.removed
	}

	if err := s.mb.TryAdd(subev); err != nil {
		if errors.Is(err, mb.ErrOverflowed) {
			s.closeWith(space.ErrSubscriptionOverflow)
		}
		// Other errors (mb closed) — already shutting down.
		return
	}

	s.checkDrift()
}

// applyRecord classifies one EventRecord and updates p accordingly.
// Pulls postKey / postDoc lazily and short-circuits via the maxKey
// fast-reject when possible.
func (s *querySub) applyRecord(i int, rc EventRecord, postValue PostValueFn, p *subPending) {
	p.seen[rc.Id] = struct{}{}

	prevSentinelId := s.sentinelId()
	oldEntry := s.entries[rc.Id]
	wasHeld := oldEntry != nil
	wasVisible := wasHeld && oldEntry != s.sentinel()

	// Determine post-state: matches + tuple + doc.
	var matches bool
	var postKey anyenc.Tuple
	var postDoc *anyenc.Value
	if !rc.Deleted {
		var err error
		if postDoc, err = postValue(i); err != nil {
			// The row's state is unknown: no transition now; settle
			// re-reads the window or closes the sub.
			if p.unreadable == nil {
				p.unreadable = map[string]struct{}{}
			}
			p.unreadable[rc.Id] = struct{}{}
			return
		}
		if postDoc != nil {
			// Fast-reject: at capacity, not held, post tuple already
			// past the sentinel ⇒ no chance of entering window.
			// Skips allocation and mutation, and filter eval unless
			// the window is complete: a match landing past the edge
			// ends completeness.
			if !wasHeld && s.atCapacity() {
				s.ensureMaxRef()
				if s.maxRef != nil {
					postKey = s.tupleFor(postDoc)
					if bytes.Compare(postKey, s.maxRef.tuple) >= 0 {
						if s.complete && (s.cfg.Filter == nil || s.cfg.Filter.Ok(postDoc, s.docBuf)) {
							s.complete = false
						}
						return
					}
				}
			}
			matches = s.cfg.Filter == nil || s.cfg.Filter.Ok(postDoc, s.docBuf)
			if matches && postKey == nil {
				postKey = s.tupleFor(postDoc)
			}
		}
	}

	// Apply transition to held set. The event ships its own copy of
	// the post doc; a windowed sub keeps its encoding on the entry.
	var eventDoc *anyenc.Value
	var raw []byte
	switch {
	case wasHeld && matches:
		eventDoc, raw = s.capture(postDoc)
		s.updateEntry(oldEntry, postKey, raw)
		if s.windowed() && !s.complete && prevSentinelId != rc.Id && s.sentinel() == oldEntry {
			// The row's own sort move put it in the sentinel slot,
			// ahead of matches past the edge it was never ranked
			// against.
			s.unranked = true
		}
	case wasHeld && !matches:
		s.removeEntry(oldEntry)
		if s.windowed() && !s.complete {
			s.departed++
		}
	case !wasHeld && matches:
		if s.atCapacity() {
			s.ensureMaxRef()
			if s.maxRef != nil {
				// Not a loss: the evicted sentinel still matches,
				// now past the window's edge.
				s.removeEntry(s.maxRef)
				s.complete = false
			}
		} else if s.windowed() && !s.complete {
			// Below capacity with matches past the edge: the row's
			// rank among them is unknown.
			s.unranked = true
		}
		eventDoc, raw = s.capture(postDoc)
		s.insertEntry(rc.Id, postKey, raw)
	default:
		return
	}

	// Post-mutation visibility for this record.
	newEntry := s.entries[rc.Id]
	isHeld := newEntry != nil
	isVisible := isHeld && newEntry != s.sentinel()

	switch {
	case !wasVisible && isVisible:
		p.added = append(p.added, pendingRec{e: newEntry, doc: eventDoc, raw: raw, ops: rc.Ops})
	case wasVisible && isVisible:
		p.updated = append(p.updated, pendingRec{e: newEntry, doc: eventDoc, raw: raw, ops: rc.Ops})
	case wasVisible && !isVisible:
		// isHeld ⇒ the record updated and became the sentinel via its
		// own sort move: still matches, just slid out of view.
		// Otherwise it left the held set: tombstoned (or dropped) ⇒
		// deleted, else the post-apply doc failed the filter.
		reason := space.RemoveDisplaced
		if !isHeld {
			if rc.Deleted || postDoc == nil {
				reason = space.RemoveDeleted
			} else {
				reason = space.RemoveFilteredOut
			}
		}
		p.removed = append(p.removed, space.RemovedRecord{Id: rc.Id, Reason: reason})
	}

	// Implicit transitions caused by sentinel movement.
	newSentinelId := s.sentinelId()
	if prevSentinelId != newSentinelId {
		// Previous sentinel may have been promoted into visibility.
		if prevSentinelId != "" && prevSentinelId != rc.Id {
			if _, alreadySeen := p.seen[prevSentinelId]; !alreadySeen {
				if e := s.entries[prevSentinelId]; e != nil && e != s.sentinel() {
					p.added = append(p.added, pendingRec{e: e, raw: e.raw}) // implicit: no ops, the held doc
					p.seen[prevSentinelId] = struct{}{}
				}
			}
		}
		// New sentinel may have been demoted out of visibility.
		if newSentinelId != "" && newSentinelId != rc.Id {
			if _, alreadySeen := p.seen[newSentinelId]; !alreadySeen {
				if newSentinelId != prevSentinelId {
					// Demoted to sentinel by another record's arrival: still
					// matches the filter, just lost its visible slot.
					p.removed = append(p.removed, space.RemovedRecord{Id: newSentinelId, Reason: space.RemoveDisplaced})
					p.seen[newSentinelId] = struct{}{}
				}
			}
		}
	}
}

// settle restores the window's invariant after the records of one
// apply. A windowed sub at offset 0 holds either every match or
// limit+1 ranked rows; a departure from a window with matches past
// its edge, an unranked sentinel, and a row the apply could not read
// all break it. A refill re-reads the window from storage and rebuilds
// the pending event from the difference. Without one, departures
// count as losses, and an unreadable row closes the sub: its window
// can no longer be trusted to match storage. Reports whether the sub
// is still open.
func (s *querySub) settle(ev Event, p *subPending) bool {
	departed := s.departed
	s.departed = 0
	unreadable := len(p.unreadable) > 0
	if !unreadable && !s.needsRefill() {
		return true
	}
	if s.canRefill() && s.refillWindow(ev, p) == nil {
		return true
	}
	if unreadable {
		s.closeWith(space.ErrSubscriptionDrifted)
		return false
	}
	if s.unranked {
		s.unranked = false
		if departed == 0 {
			departed = 1
		}
	}
	s.lost += departed
	return true
}

// needsRefill reports whether the window may differ from storage past
// its edge: matches lie there and the window is below capacity or its
// sentinel is unranked.
func (s *querySub) needsRefill() bool {
	return s.windowed() && !s.complete && (s.unranked || len(s.entries) <= s.cfg.Limit)
}

// canRefill reports whether the window can be re-read: a refill was
// supplied and the window starts at offset 0. A window past an offset
// shifts with rows before it, which no re-read of its own rows shows.
func (s *querySub) canRefill() bool {
	return s.windowed() && s.cfg.Offset == 0 && s.cfg.Refill != nil
}

// freshRow is one row of a refill read, held until the read completes
// so a read that fails midway changes nothing.
type freshRow struct {
	id    string
	tuple anyenc.Tuple
	raw   []byte // nil for a held row whose stored value is unchanged
	held  bool
}

// refillWindow re-reads the window's rows from storage and reconciles
// the held set with them: rows past the edge enter, held rows storage
// no longer returns leave, and a held row whose stored value differs
// is updated. The pending event is then rebuilt from the difference
// between the view before this apply and the view after the refill. A
// read that returns at most limit rows leaves the window complete.
func (s *querySub) refillWindow(ev Event, p *subPending) error {
	visBefore := s.visibleBefore(p)
	rows := make([]freshRow, 0, s.cfg.Limit+1)
	read := 0
	err := s.cfg.Refill(func(id string, doc *anyenc.Value) {
		read++
		if id == "" || doc == nil {
			return
		}
		if e := s.entries[id]; e != nil {
			fr := freshRow{id: id, held: true}
			withEncoding(doc, func(enc []byte) {
				if !bytes.Equal(enc, e.raw) {
					fr.raw = bytes.Clone(enc)
					fr.tuple = s.tupleFor(doc)
				}
			})
			rows = append(rows, fr)
			return
		}
		rows = append(rows, freshRow{id: id, tuple: s.tupleFor(doc), raw: encode(doc)})
	})
	if err != nil {
		return err
	}

	fresh := make(map[string]struct{}, len(rows))
	changed := map[string]struct{}{}
	for _, fr := range rows {
		fresh[fr.id] = struct{}{}
		switch {
		case !fr.held:
			s.insertEntry(fr.id, fr.tuple, fr.raw)
		case fr.raw != nil:
			s.updateEntry(s.entries[fr.id], fr.tuple, fr.raw)
			changed[fr.id] = struct{}{}
		}
	}
	var absent []*entry
	for id, e := range s.entries {
		if _, ok := fresh[id]; !ok {
			absent = append(absent, e)
		}
	}
	for _, e := range absent {
		s.removeEntry(e)
	}
	s.complete = read <= s.cfg.Limit
	s.unranked = false
	s.rebuildPending(ev, p, visBefore, changed, absent, read > s.cfg.Limit)
	return nil
}

// visibleBefore reconstructs the ids visible when this apply began
// from the view now and the transitions recorded so far.
func (s *querySub) visibleBefore(p *subPending) map[string]struct{} {
	vis := make(map[string]struct{}, len(s.entries))
	sent := s.sentinel()
	for id, e := range s.entries {
		if e != sent {
			vis[id] = struct{}{}
		}
	}
	for _, pr := range p.added {
		delete(vis, pr.e.id)
	}
	for _, r := range p.removed {
		vis[r.Id] = struct{}{}
	}
	return vis
}

// rebuildPending replaces the transitions recorded record by record
// with the difference between the view before the apply and the view
// now: a row visible now and not before is added, a row visible both
// times is updated when a record of this apply or the refill changed
// it, a row visible before and not now is removed. A removed row's
// reason is its own record's when the apply had one for it. Otherwise
// it was displaced when it still sorts at or past the edge of a window
// that has one, and it stopped matching when it does not.
func (s *querySub) rebuildPending(ev Event, p *subPending, visBefore, changed map[string]struct{}, absent []*entry, hasEdge bool) {
	recs := make(map[string]EventRecord, len(ev.Records))
	for _, rc := range ev.Records {
		if rc.Id != "" {
			recs[rc.Id] = rc
		}
	}
	reasons := make(map[string]space.RemoveReason, len(p.removed))
	for _, r := range p.removed {
		reasons[r.Id] = r.Reason
	}
	gone := make(map[string]*entry, len(absent))
	for _, e := range absent {
		gone[e.id] = e
	}
	p.added, p.updated, p.removed = nil, nil, nil

	sent := s.sentinel()
	for id, e := range s.entries {
		if e == sent {
			continue
		}
		var ops []space.EventOp
		rc, touched := recs[id]
		if _, unread := p.unreadable[id]; unread {
			touched = false
		}
		if touched {
			ops = rc.Ops
		}
		_, wasVisible := visBefore[id]
		_, chg := changed[id]
		switch {
		case !wasVisible:
			p.added = append(p.added, pendingRec{e: e, raw: e.raw, ops: ops})
		case touched || chg:
			p.updated = append(p.updated, pendingRec{e: e, raw: e.raw, ops: ops})
		}
	}
	sortPending(p.added)
	sortPending(p.updated)

	for id := range visBefore {
		e := s.entries[id]
		if e != nil && e != sent {
			continue
		}
		reason, ok := reasons[id]
		if !ok || reason == space.RemoveDisplaced {
			reason = space.RemoveFilteredOut
			if e == nil {
				e = gone[id]
			}
			stillHeld := s.entries[id] != nil
			if stillHeld || (hasEdge && sent != nil && e != nil && bytes.Compare(e.tuple, sent.tuple) >= 0) {
				reason = space.RemoveDisplaced
			}
		}
		p.removed = append(p.removed, space.RemovedRecord{Id: id, Reason: reason})
	}
	sort.Slice(p.removed, func(i, j int) bool { return p.removed[i].Id < p.removed[j].Id })
}

// sortPending orders records by sort tuple, then id, so a rebuilt
// event lists them in the window's order.
func sortPending(rs []pendingRec) {
	sort.Slice(rs, func(i, j int) bool {
		if c := bytes.Compare(rs[i].e.tuple, rs[j].e.tuple); c != 0 {
			return c < 0
		}
		return rs[i].e.id < rs[j].e.id
	})
}

// checkDrift closes the sub with ErrSubscriptionDrifted when the
// lost-without-replacement counter crosses the configured budget.
func (s *querySub) checkDrift() {
	if s.cfg.Limit <= 0 || s.closed {
		return
	}
	if s.lost*100 >= s.lostLimit {
		s.closeWith(space.ErrSubscriptionDrifted)
	}
}

// lossLimit returns limit*budget, saturated at math.MaxInt: a budget
// large enough to overflow the product never closes the sub, where a
// wrapped negative product would close it on the first apply.
func lossLimit(limit, budget int) int {
	if budget > math.MaxInt/limit {
		return math.MaxInt
	}
	return limit * budget
}
