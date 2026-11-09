package subscribe

import (
	"bytes"
	"errors"
	"math"
	"sync"
	"sync/atomic"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/syncpool"
	"github.com/cheggaaa/mb/v3"

	"github.com/anyproto/any-sync-sdk/space"

	"github.com/anyproto/any-sync-sdk/internal/anyencx"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
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
	// snapshot started at offset 0 and held at most limit rows, and no
	// match has been left past the edge since. unranked: the sentinel
	// holds its slot with a key larger than the one it was ranked at
	// (a held row's own sort move, an insert below capacity), so
	// matches past the edge may sort below it and the next promotion
	// could lift the wrong row into view; unrankedLoss: that state has
	// been counted as a loss once. A refill (SubConfig.Refill) settles
	// both from storage; a window that cannot refill counts departures
	// as losses, and lost*100 at or above lostLimit closes the sub.
	complete     bool
	unranked     bool
	unrankedLoss bool
	lost         int
	lostLimit    int

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

// subPending is the per-apply scratch: the ids this apply touched, in
// first-touch order, each with whether it was visible when the apply
// began, and what the apply's records did to each. The event is built
// from it once the held set has settled, so a row touched twice in one
// apply (promoted and then removed, demoted and then promoted) ships
// the one transition from its view before the apply to its view after.
type subPending struct {
	order      []string
	before     map[string]bool
	recs       map[string]*recOutcome
	gone       map[string]*entry // held rows a refill found absent
	edge       bool              // the refilled window is at capacity: matches may lie past it
	departed   int               // held rows that left a window with matches past its edge
	unreadable int               // records whose post value could not be read
}

// recOutcome is what this apply did to one id: its records' ops in
// order and the last record's verdict, plus what a refill found.
type recOutcome struct {
	ops        []space.EventOp
	doc        *anyenc.Value // the event's copy of the post doc (unbounded sub)
	deleted    bool          // the last record tombstoned it
	present    bool          // the last record found a post doc
	matched    bool          // the last record's post doc passes the filter
	landed     bool          // a readable record wrote the held row
	unreadable bool          // a record's post value could not be read
	changed    bool          // a refill found its stored value changed
}

func newPending() subPending {
	return subPending{before: map[string]bool{}, recs: map[string]*recOutcome{}, gone: map[string]*entry{}}
}

// touch records id as touched by this apply with its visibility before
// the apply. The first touch wins: nothing moved the row before it, so
// its state then is its state when the apply began.
func (p *subPending) touch(id string, visible bool) {
	if _, ok := p.before[id]; ok {
		return
	}
	p.before[id] = visible
	p.order = append(p.order, id)
}

func (p *subPending) rec(id string) *recOutcome {
	r := p.recs[id]
	if r == nil {
		r = &recOutcome{}
		p.recs[id] = r
	}
	return r
}

// apply processes one wire event. Builds at most one SubscriptionEvent
// per call. Per-record classification, sentinel transitions, drift +
// overflow checks all happen here under Engine.mu (so other engine
// methods observe consistent state).
func (s *querySub) apply(ev Event, postValue PostValueFn) {
	if s.closed {
		return
	}

	p := newPending()
	for i, rc := range ev.Records {
		if rc.Id == "" {
			continue
		}
		s.applyRecord(i, rc, postValue, &p)
	}
	if !s.settle(&p) {
		return
	}

	subev := s.emit(ev.VersionId, &p)
	if len(subev.Added) == 0 && len(subev.Updated) == 0 && len(subev.Removed) == 0 {
		s.checkDrift()
		return
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

// applyRecord applies one EventRecord to the held set and records in p
// what it touched. Pulls postKey / postDoc lazily and short-circuits
// via the maxKey fast-reject when possible.
func (s *querySub) applyRecord(i int, rc EventRecord, postValue PostValueFn, p *subPending) {
	prevSentinel := s.sentinel()
	oldEntry := s.entries[rc.Id]
	wasHeld := oldEntry != nil
	p.touch(rc.Id, wasHeld && oldEntry != prevSentinel)
	if prevSentinel != nil {
		p.touch(prevSentinel.id, false)
	}
	out := p.rec(rc.Id)
	out.ops = append(out.ops, rc.Ops...)
	out.deleted, out.present, out.matched = rc.Deleted, false, false

	// Determine post-state: matches + tuple + doc.
	var matches bool
	var postKey anyenc.Tuple
	var postDoc *anyenc.Value
	if !rc.Deleted {
		var err error
		if postDoc, err = postValue(i); err != nil {
			// The row's state is unknown: no transition now; settle
			// re-reads the window or closes the sub.
			out.unreadable = true
			p.unreadable++
			return
		}
		if postDoc != nil {
			out.present = true
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
						// The filter is read only where the verdict
						// matters: completeness, or the reason for a
						// row this apply took out of view earlier.
						if s.complete || p.before[rc.Id] {
							out.matched = s.cfg.Filter == nil || s.cfg.Filter.Ok(postDoc, s.docBuf)
							if out.matched {
								s.complete = false
							}
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
	out.matched = matches

	// Apply transition to held set. The event ships its own copy of
	// the post doc; a windowed sub keeps its encoding on the entry.
	var raw []byte
	switch {
	case wasHeld && matches:
		out.doc, raw = s.capture(postDoc)
		out.landed = true
		oldTuple := oldEntry.tuple
		s.updateEntry(oldEntry, postKey, raw)
		if s.ranks() && s.sentinel() == oldEntry && bytes.Compare(postKey, oldTuple) > 0 {
			// The row's own sort move put or kept it in the sentinel
			// slot with a larger key than it was ranked at: matches
			// past the edge may now sort below it.
			s.unranked = true
		}
	case wasHeld && !matches:
		s.removeEntry(oldEntry)
		if s.windowed() && !s.complete {
			p.departed++
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
		} else if s.ranks() {
			// Below capacity with matches past the edge: the row's
			// rank among them is unknown.
			s.unranked = true
		}
		out.doc, raw = s.capture(postDoc)
		out.landed = true
		s.insertEntry(rc.Id, postKey, raw)
	default:
		return
	}

	if ns := s.sentinel(); ns != nil {
		// A row this record demoted to sentinel was visible before:
		// held, and not the sentinel. (A row touched earlier keeps
		// its recorded state.)
		p.touch(ns.id, true)
	}
}

// ranks reports whether the window keeps track of its sentinel's
// rank: it has matches past its edge, and it can refill to rank them.
func (s *querySub) ranks() bool {
	return s.windowed() && s.canRefill() && !s.complete
}

// emit builds the event from the touched ids: each ships the one
// transition from its view before the apply to its view now.
func (s *querySub) emit(versionId crdt.VersionId, p *subPending) space.SubscriptionEvent {
	sent := s.sentinel()
	subev := space.SubscriptionEvent{VersionId: versionId}
	for _, id := range p.order {
		was := p.before[id]
		e := s.entries[id]
		is := e != nil && e != sent
		out := p.recs[id]
		switch {
		case !was && is:
			subev.Added = append(subev.Added, s.record(e, out))
		case was && is:
			// A readable record wrote it, or a refill found it
			// changed; a row promoted and demoted back, or whose only
			// record could not be read and whose stored value did not
			// change, ships nothing.
			if out != nil && (out.landed || out.changed) {
				subev.Updated = append(subev.Updated, s.record(e, out))
			}
		case was && !is:
			subev.Removed = append(subev.Removed, space.RemovedRecord{Id: id, Reason: removeReason(id, e, out, p, sent)})
		}
	}
	return subev
}

// record builds the wire record for a held row: the held encoding
// parsed (windowed), or the event's copy (unbounded), with the ops of
// the apply's records on it when every one of them could be read.
func (s *querySub) record(e *entry, out *recOutcome) space.SubRecord {
	rec := space.SubRecord{Id: e.id}
	if s.windowed() {
		rec.Doc = parseHeld(e.raw)
	} else if out != nil {
		rec.Doc = out.doc
	}
	if out != nil && !out.unreadable {
		rec.Ops = out.ops
	}
	return rec
}

// removeReason classifies a row that was visible before the apply and
// is not now. Still held, it is the sentinel: displaced. Gone by its
// own readable record: deleted when the record tombstoned it or found
// no row, filtered out when the post doc failed the filter. Gone with
// no refill behind it, an arrival evicted it: displaced. Gone through
// a refill: displaced when its last known position is at or past the
// refilled window's edge, else it stopped matching.
func removeReason(id string, e *entry, out *recOutcome, p *subPending, sent *entry) space.RemoveReason {
	g := p.gone[id]
	switch {
	case e != nil:
		return space.RemoveDisplaced
	case out != nil && !out.unreadable && (out.deleted || !out.present):
		return space.RemoveDeleted
	case out != nil && !out.unreadable && !out.matched:
		return space.RemoveFilteredOut
	case g == nil:
		return space.RemoveDisplaced
	case p.edge && sent != nil && bytes.Compare(g.tuple, sent.tuple) >= 0:
		return space.RemoveDisplaced
	}
	return space.RemoveFilteredOut
}

// settle restores the window's invariant after the records of one
// apply. A windowed sub at offset 0 holds either every match or
// limit+1 ranked rows; a departure from a window with matches past
// its edge, an unranked sentinel, and a row the apply could not read
// all break it. A refill re-reads the window from storage and records
// the difference in p. A departure the window cannot refill counts as
// a loss, and an unranked sentinel it cannot rank counts once; the
// next apply tries the refill again at no further cost. An unreadable
// row with no refill closes the sub, whose window can no longer be
// trusted to match storage. Reports whether the sub is still open.
func (s *querySub) settle(p *subPending) bool {
	unreadable := p.unreadable > 0
	if unreadable || s.needsRefill() {
		if s.canRefill() && s.refillWindow(p) == nil {
			return true
		}
		if unreadable {
			s.closeWith(space.ErrSubscriptionDrifted)
			return false
		}
		if s.unranked && !s.unrankedLoss {
			s.unrankedLoss = true
			s.lost++
		}
	}
	s.lost += p.departed
	return true
}

// needsRefill reports whether the window may differ from storage past
// its edge: matches lie there and the window is below capacity or its
// sentinel is unranked. Only a window that can refill tracks either.
func (s *querySub) needsRefill() bool {
	return s.ranks() && (s.unranked || len(s.entries) <= s.cfg.Limit)
}

// canRefill reports whether the sub can be re-read: a refill was
// supplied and the window starts at offset 0. A window past an offset
// shifts with rows before it, which no re-read of its own rows shows.
func (s *querySub) canRefill() bool {
	return s.cfg.Refill != nil && s.cfg.Offset == 0
}

// freshRow is one row of a refill read, held until the read completes
// so a read that fails midway changes nothing.
type freshRow struct {
	id    string
	held  bool
	tuple anyenc.Tuple
	raw   []byte        // windowed: the encoding; nil for a held row whose stored value is unchanged
	doc   *anyenc.Value // unbounded: a copy for a row the event ships
}

// refillWindow re-reads the sub's rows from storage and reconciles the
// held set with them: rows storage returns enter, held rows it no
// longer returns leave, and a held row whose stored value differs is
// updated. Every row that moved is touched in p with its view before,
// so emit ships the difference. For a windowed sub a read whose rows
// fit within limit leaves the window complete; an unbounded sub holds
// no values, so only its membership is reconciled, plus the value of
// each row whose record could not be read.
func (s *querySub) refillWindow(p *subPending) error {
	s0 := s.sentinel()
	if s0 != nil {
		p.touch(s0.id, false)
	}
	rows := make([]freshRow, 0, s.cfg.Limit+1)
	err := s.cfg.Refill(func(id string, doc *anyenc.Value) {
		if id == "" || doc == nil {
			return
		}
		e := s.entries[id]
		fr := freshRow{id: id, held: e != nil}
		switch {
		case !s.windowed():
			if out := p.recs[id]; e == nil || (out != nil && out.unreadable) {
				fr.doc = anyencx.Clone(doc)
			}
		case e == nil:
			fr.tuple, fr.raw = s.tupleFor(doc), encode(doc)
		default:
			withEncoding(doc, func(enc []byte) {
				if !bytes.Equal(enc, e.raw) {
					fr.tuple, fr.raw = s.tupleFor(doc), bytes.Clone(enc)
				}
			})
		}
		rows = append(rows, fr)
	})
	if err != nil {
		return err
	}

	fresh := make(map[string]struct{}, len(rows))
	for _, fr := range rows {
		fresh[fr.id] = struct{}{}
		switch {
		case !fr.held:
			p.touch(fr.id, false)
			s.insertEntry(fr.id, fr.tuple, fr.raw)
			if fr.doc != nil {
				p.rec(fr.id).doc = fr.doc
			}
		case fr.raw != nil:
			e := s.entries[fr.id]
			p.touch(fr.id, e != s0)
			s.updateEntry(e, fr.tuple, fr.raw)
			p.rec(fr.id).changed = true
		case fr.doc != nil:
			out := p.rec(fr.id)
			out.doc, out.changed = fr.doc, true
		}
	}
	for id, e := range s.entries {
		if _, ok := fresh[id]; ok {
			continue
		}
		p.touch(id, e != s0)
		p.gone[id] = e
		s.removeEntry(e)
	}
	if s.windowed() {
		// Rows the read returned without an id or doc took no slot:
		// a window that holds at most limit rows holds every match
		// it can, as at Subscribe.
		s.complete = len(s.entries) <= s.cfg.Limit
		p.edge = s.atCapacity()
		s.unranked, s.unrankedLoss = false, false
	}
	if s1 := s.sentinel(); s1 != nil {
		// A row the refill demoted to sentinel was visible before.
		p.touch(s1.id, true)
	}
	return nil
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
