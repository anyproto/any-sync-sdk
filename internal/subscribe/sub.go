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
	seq     uint64 // the last place handed out (entry.seq)

	// Window state: complete, the window holds every match; edge, a key
	// every match the window does not hold sorts at or past; unranked, a
	// held row's rank against those matches is unknown; unrankedLoss,
	// that state was counted once; lost*100 at lostLimit closes the sub.
	complete     bool
	edge         anyenc.Tuple
	unranked     bool
	unrankedLoss bool
	lost         int
	lostLimit    int
	// deferred: the last refill found storage not final, pending Resync.
	// unread: rows of an unbounded sub the next refill re-reads.
	deferred bool
	unread   map[string]struct{}

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
// arrive in sort order (ascending tuple, ties in storage's order), so
// the last row appended is maxRef and the first is minRef when the
// loop completes.
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

// subPending is the per-apply scratch: the ids the apply touched in
// first-touch order, each row's view before the apply and after the
// records, what the records did to each, and why a refill dropped a row.
type subPending struct {
	source     any // Event.Source, handed to the refill
	order      []string
	before     map[string]bool
	mid        map[string]midState // each id's view after the records; nil until a refill starts
	refilled   bool                // a refill reconciled the held set
	recs       map[string]*recOutcome
	gone       map[string]space.RemoveReason // why a refill dropped a held row
	departed   int                           // held rows that left a window with matches past its edge
	moved      int                           // held rows whose key grew while the window held an unranked row
	unreadable int                           // records whose post value could not be read
}

// midState is a row's view after the apply's records, before a refill.
type midState struct {
	visible bool
	raw     []byte // the held encoding then (windowed sub)
}

// recOutcome is what the apply did to one id: its records' ops, the
// last settled verdict, and what a refill found.
type recOutcome struct {
	ops        []space.EventOp
	doc        *anyenc.Value // the event's copy of the post doc (unbounded sub)
	verdict    bool          // a record settled deleted / present / matched
	deleted    bool          // the last settled record tombstoned it
	present    bool          // the last settled record found a post doc
	matched    bool          // the last settled record's post doc passes the filter
	landed     bool          // a readable record wrote the held row
	unreadable bool          // a record's post value could not be read
	changed    bool          // a refill found its stored value changed
}

func newPending(source any) subPending {
	return subPending{source: source, before: map[string]bool{}, recs: map[string]*recOutcome{}}
}

// touch records id as touched by this apply with its visibility now.
// The first touch wins: nothing moved the row before it, so its state
// then is its state when the apply began; once a refill has started,
// the first touch also fixes its state after the records.
func (p *subPending) touch(id string, visible bool) {
	if _, ok := p.before[id]; !ok {
		p.before[id] = visible
		p.order = append(p.order, id)
	}
	if p.mid != nil {
		if _, ok := p.mid[id]; !ok {
			p.mid[id] = midState{visible: visible}
		}
	}
}

// beginRefill fixes each touched id's view after the apply's records:
// the state the refill's event ships transitions from.
func (p *subPending) beginRefill(s *querySub) {
	p.mid = make(map[string]midState, len(p.order))
	sent := s.sentinel()
	for _, id := range p.order {
		e := s.entries[id]
		m := midState{visible: e != nil && e != sent}
		if m.visible {
			m.raw = e.raw
		}
		p.mid[id] = m
	}
}

// markUnreadable records that a record on id could not be read.
func (p *subPending) markUnreadable(id string) {
	p.rec(id).unreadable = true
	p.unreadable++
}

func (p *subPending) rec(id string) *recOutcome {
	r := p.recs[id]
	if r == nil {
		r = &recOutcome{}
		p.recs[id] = r
	}
	return r
}

// apply processes one wire event. Builds at most two SubscriptionEvents
// per call: the change's own, and the refill's when one ran.
// Per-record classification, sentinel transitions, drift + overflow
// checks all happen here under Engine.mu (so other engine methods
// observe consistent state).
func (s *querySub) apply(ev Event, postValue PostValueFn) {
	if s.closed {
		return
	}

	p := newPending(ev.Source)
	for i, rc := range ev.Records {
		if rc.Id == "" {
			continue
		}
		s.applyRecord(i, rc, postValue, &p)
	}
	if !s.settle(&p) {
		return
	}

	own, resync := s.emit(ev.VersionId, &p)
	if !s.deliver(own) || !s.deliver(resync) {
		return
	}
	s.checkDrift()
}

// resync re-reads the window outside any apply and ships the difference
// as a resync event. A window that cannot re-read, or whose read fails,
// closes; a deferred read waits for the next Resync.
func (s *querySub) resync(source any) {
	if !s.canRefill() {
		s.closeWith(space.ErrSubscriptionDrifted)
		return
	}
	p := newPending(source)
	p.beginRefill(s)
	if err := s.refillWindow(&p); err != nil {
		s.readFailed(err)
		return
	}
	s.refilled()
	p.refilled = true
	_, resync := s.emit("", &p)
	if s.deliver(resync) {
		s.checkDrift()
	}
}

// resettleRow re-reads one held row by lookup after its object's
// rebuild. Present and matching: nothing. Gone or filtered out: an
// unbounded sub drops it with storage's reason, a sub that cannot
// re-read its window closes. No lookup: the whole window.
func (s *querySub) resettleRow(id string, source any) {
	if s.cfg.Refill == nil {
		s.closeWith(space.ErrSubscriptionDrifted)
		return
	}
	read, err := s.cfg.Refill(source)
	if err != nil {
		s.readFailed(err)
		return
	}
	if read.Lookup == nil {
		// No way to classify one row: the whole window, which a sub
		// that cannot refill closes over.
		read.Close()
		s.resync(source)
		return
	}
	doc, err := read.Lookup(id)
	read.Close()
	if err != nil {
		s.readFailed(err)
		return
	}
	reason := space.RemoveDisplaced
	switch {
	case doc == nil:
		reason = space.RemoveDeleted
	case s.cfg.Filter != nil && !s.cfg.Filter.Ok(doc, s.docBuf):
		reason = space.RemoveFilteredOut
	default:
		return
	}
	if !s.canRefill() {
		s.closeWith(space.ErrSubscriptionDrifted)
		return
	}
	p := newPending(source)
	p.beginRefill(s)
	p.touch(id, true)
	p.gone = map[string]space.RemoveReason{id: reason}
	s.removeEntry(s.entries[id])
	p.refilled = true
	_, resync := s.emit("", &p)
	s.deliver(resync)
}

// readFailed settles a re-read that could not run: deferred, the window
// waits for the next Resync; failed otherwise, it closes.
func (s *querySub) readFailed(err error) {
	if errors.Is(err, ErrRefillDeferred) {
		if s.canRefill() {
			s.deferred = true
		}
		return
	}
	s.closeWith(space.ErrSubscriptionDrifted)
}

// deliver queues one event unless it is empty. Reports whether the sub
// is still open.
func (s *querySub) deliver(subev space.SubscriptionEvent) bool {
	if len(subev.Added) == 0 && len(subev.Updated) == 0 && len(subev.Removed) == 0 {
		return true
	}
	if err := s.mb.TryAdd(subev); err != nil {
		if errors.Is(err, mb.ErrOverflowed) {
			s.closeWith(space.ErrSubscriptionOverflow)
		}
		// Other errors (mb closed) — already shutting down.
		return false
	}
	return true
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

	// Determine post-state: matches + tuple + doc.
	var matches bool
	var postKey anyenc.Tuple
	var postDoc *anyenc.Value
	if !rc.Deleted {
		if rc.Unreadable {
			p.markUnreadable(rc.Id)
			return
		}
		var err error
		if postDoc, err = postValue(i); err != nil {
			// The row's state is unknown: no transition now; settle
			// re-reads the window or closes the sub.
			p.markUnreadable(rc.Id)
			return
		}
	}
	out.verdict, out.deleted, out.present, out.matched = true, rc.Deleted, postDoc != nil, false
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
		if s.ranks() && bytes.Compare(postKey, oldTuple) > 0 {
			if s.unranked && !(oldEntry == prevSentinel && s.sentinel() == oldEntry) {
				// A move past rows the window does not hold may put
				// the row where one of them belongs; a sentinel that
				// stays the sentinel moves nothing into view.
				p.moved++
			}
			switch {
			case s.atCapacity():
				// The row's own sort move put or kept it in the
				// sentinel slot with a larger key than it was ranked
				// at: matches past the edge may now sort below it.
				if s.sentinel() == oldEntry {
					s.unranked = true
				}
			case s.edge == nil || bytes.Compare(postKey, s.edge) >= 0:
				// Below capacity, the row moved to or past the edge:
				// it sorts among the matches the window does not hold.
				s.unranked = true
			}
		}
	case wasHeld && !matches:
		if prevSentinel != nil && s.ranks() && !s.unranked {
			// The window leaves capacity ranked: every match it does
			// not hold sorts at or past its sentinel.
			s.edge = prevSentinel.tuple
		}
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
		} else if s.ranks() && (s.edge == nil || bytes.Compare(postKey, s.edge) >= 0) {
			// Below capacity with matches past the edge, and the row
			// sorts among them: its rank is unknown.
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

// emit builds the change's own event (each id's transition from before
// the apply to after its records, with ops) and the refill's resync
// event (from there to now, no VersionId, no ops).
func (s *querySub) emit(versionId crdt.VersionId, p *subPending) (own, resync space.SubscriptionEvent) {
	own.VersionId = versionId
	sent := s.sentinel()
	for _, id := range p.order {
		was := p.before[id]
		out := p.recs[id]
		e := s.entries[id]
		now := e != nil && e != sent
		mid, midRaw := now, []byte(nil)
		if e != nil {
			midRaw = e.raw
		}
		if p.refilled {
			m := p.mid[id]
			mid, midRaw = m.visible, m.raw
		}
		switch {
		case !was && mid:
			own.Added = append(own.Added, s.ownRecord(id, midRaw, out))
		case was && mid:
			// A readable record wrote it; a row promoted and demoted
			// back, or whose only record could not be read, ships
			// nothing.
			if out != nil && out.landed {
				own.Updated = append(own.Updated, s.ownRecord(id, midRaw, out))
			}
		case was && !mid:
			own.Removed = append(own.Removed, space.RemovedRecord{Id: id, Reason: recordReason(id, out, p)})
		}
		if !p.refilled {
			continue
		}
		switch {
		case !mid && now:
			resync.Added = append(resync.Added, s.storedRecord(e, out))
		case mid && now:
			if out != nil && out.changed {
				resync.Updated = append(resync.Updated, s.storedRecord(e, out))
			}
		case mid && !now:
			// Still held, it is the sentinel the refill demoted it to;
			// gone, storage said why.
			reason := space.RemoveDisplaced
			if r, ok := p.gone[id]; e == nil && ok {
				reason = r
			}
			resync.Removed = append(resync.Removed, space.RemovedRecord{Id: id, Reason: reason})
		}
	}
	return own, resync
}

// ownRecord is the change's record for a row: its doc after the records,
// with the records' ops when every one of them could be read.
func (s *querySub) ownRecord(id string, raw []byte, out *recOutcome) space.SubRecord {
	rec := space.SubRecord{Id: id}
	if s.windowed() {
		rec.Doc = parseHeld(raw)
	} else if out != nil {
		rec.Doc = out.doc
	}
	if out != nil && !out.unreadable {
		rec.Ops = out.ops
	}
	return rec
}

// storedRecord builds the refill's record for a held row: its stored
// doc and no ops.
func (s *querySub) storedRecord(e *entry, out *recOutcome) space.SubRecord {
	rec := space.SubRecord{Id: e.id}
	if s.windowed() {
		rec.Doc = parseHeld(e.raw)
	} else if out != nil {
		rec.Doc = out.doc
	}
	return rec
}

// recordReason classifies a row the apply's records took out of view:
// deleted when the last settled record on it tombstoned it or found no
// row, filtered out when that record's post doc failed the filter.
// With no verdict, another row's arrival evicted it or its own sort
// move demoted it: it still matches past the edge, unless a refill
// looked it up and storage said otherwise.
func recordReason(id string, out *recOutcome, p *subPending) space.RemoveReason {
	switch {
	case out == nil || !out.verdict:
		if r, ok := p.gone[id]; ok {
			return r
		}
		return space.RemoveDisplaced
	case out.deleted || !out.present:
		return space.RemoveDeleted
	case !out.matched:
		return space.RemoveFilteredOut
	}
	return space.RemoveDisplaced
}

// settle restores the window's invariant after one apply's records with
// a refill where the window is short, unranked, deferred or holds a row
// the apply could not read. A refill deferred by a rebuild counts
// nothing. Otherwise a failed refill counts every departure unless the
// window ends at capacity and ranked, a key growth while unranked, and
// the unranked state once; an unreadable row with no refill closes the
// sub. Reports whether the sub is still open.
func (s *querySub) settle(p *subPending) bool {
	unreadable := p.unreadable > 0
	if !unreadable && !s.needsRefill() && !(s.deferred && s.windowed()) {
		if !s.ranks() {
			// No refill behind the window: every departure is a loss.
			s.lost += p.departed
		}
		return true
	}
	if s.canRefill() {
		p.beginRefill(s)
		err := s.refillWindow(p)
		if err == nil {
			s.refilled()
			p.refilled = true
			return true
		}
		if errors.Is(err, ErrRefillDeferred) {
			s.deferred = true
			if !s.windowed() {
				// The stored value of a row no record could read is
				// shipped by the re-read, whenever it runs.
				for id, out := range p.recs {
					if out.unreadable {
						if s.unread == nil {
							s.unread = map[string]struct{}{}
						}
						s.unread[id] = struct{}{}
					}
				}
			}
			return true
		}
	}
	if unreadable {
		s.closeWith(space.ErrSubscriptionDrifted)
		return false
	}
	if s.unranked && !s.unrankedLoss {
		s.unrankedLoss = true
		s.lost++
	}
	if len(s.entries) <= s.cfg.Limit || s.unranked {
		// Short of rows, or holding a row it cannot rank: what
		// replaced each departure may be the wrong row.
		s.lost += p.departed
	}
	s.lost += p.moved
	return true
}

// needsRefill reports whether the window may differ from storage past
// its edge: matches lie there and the window is below capacity or
// holds an unranked row. Only a window that can refill tracks either.
func (s *querySub) needsRefill() bool {
	return s.ranks() && (s.unranked || len(s.entries) <= s.cfg.Limit)
}

// refilled clears what a successful refill settles.
func (s *querySub) refilled() {
	s.deferred = false
	s.unread = nil
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

// refillWindow reconciles the held set with a fresh read: rows the read
// returns enter or update, held rows it no longer returns leave with
// the reason a lookup gives (deleted, filtered out, else displaced),
// each touched in p so emit ships the difference. A failed read
// changes nothing.
func (s *querySub) refillWindow(p *subPending) error {
	read, err := s.cfg.Refill(p.source)
	if err != nil {
		return err
	}
	defer read.Close()
	rows := make([]freshRow, 0, len(s.entries)+1)
	err = read.Rows(func(id string, doc *anyenc.Value) {
		if id == "" || doc == nil {
			return
		}
		e := s.entries[id]
		fr := freshRow{id: id, held: e != nil}
		switch {
		case !s.windowed():
			_, unread := s.unread[id]
			if out := p.recs[id]; e == nil || unread || (out != nil && out.unreadable) {
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
	}
	// Look up every held row the read did not return, and every row the
	// records took out of view without a verdict, before anything
	// changes; the reasons reach p only once every lookup succeeded.
	var gone map[string]space.RemoveReason
	lookup := func(id string) error {
		doc, err := read.Lookup(id)
		if err != nil {
			return err
		}
		reason := space.RemoveDisplaced
		switch {
		case doc == nil:
			reason = space.RemoveDeleted
		case s.cfg.Filter != nil && !s.cfg.Filter.Ok(doc, s.docBuf):
			reason = space.RemoveFilteredOut
		}
		if gone == nil {
			gone = map[string]space.RemoveReason{}
		}
		gone[id] = reason
		return nil
	}
	if read.Lookup != nil {
		for id := range s.entries {
			if _, ok := fresh[id]; ok {
				continue
			}
			if err := lookup(id); err != nil {
				return err
			}
		}
		for _, id := range p.order {
			if _, ok := fresh[id]; ok || s.entries[id] != nil || !p.before[id] || p.mid[id].visible {
				continue
			}
			if out := p.recs[id]; out == nil || !out.verdict {
				if err := lookup(id); err != nil {
					return err
				}
			}
		}
	}
	p.gone = gone

	s0 := s.sentinel()
	if s0 != nil {
		p.touch(s0.id, false)
	}
	for _, fr := range rows {
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
			// An unbounded sub: held is visible, and the stored value
			// of a row no record could read ships.
			p.touch(fr.id, true)
			out := p.rec(fr.id)
			out.doc, out.changed = fr.doc, true
		}
		if s.windowed() {
			// Ties take the read's order, whatever place they held.
			s.place(s.entries[fr.id])
		}
	}
	for id, e := range s.entries {
		if _, ok := fresh[id]; ok {
			continue
		}
		p.touch(id, e != s0)
		s.removeEntry(e)
	}
	if s.windowed() {
		// The boundaries follow the new places.
		s.minRef, s.maxRef = nil, nil
		// Rows the read returned without an id or doc took no slot:
		// a window that holds at most limit rows holds every match
		// it can, as at Subscribe.
		s.complete = len(s.entries) <= s.cfg.Limit
		s.edge = nil
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
