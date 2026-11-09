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

	// Drift accounting. complete: the window holds every match — its
	// snapshot started at offset 0 and found nothing past the window's
	// edge, and no match has been left past the edge since. A row that
	// leaves a complete window has nothing to be backfilled from, so it
	// is not a loss. lostLimit: lost*100 at or above it closes the sub.
	complete  bool
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

// encode returns doc's encoding in a slice of its own. A held row
// costs its encoded size; a parsed copy would cost about ten times
// that for as long as the window holds it.
func encode(doc *anyenc.Value) []byte {
	bp := encBufs.Get().(*[]byte)
	*bp = doc.MarshalTo((*bp)[:0])
	raw := bytes.Clone(*bp)
	if cap(*bp) <= maxPooledEncBuf {
		encBufs.Put(bp)
	}
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
			s.err.Store(space.ErrSubscriptionOverflow)
			_ = s.mb.Close()
			s.closed = true
			return
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
		postDoc = postValue(i)
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
	case wasHeld && !matches:
		s.removeEntry(oldEntry)
		if !s.complete {
			s.lost++
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

// checkDrift closes the sub with ErrSubscriptionDrifted when the
// lost-without-replacement counter crosses the configured budget.
func (s *querySub) checkDrift() {
	if s.cfg.Limit <= 0 || s.closed {
		return
	}
	if s.lost*100 >= s.lostLimit {
		s.err.Store(space.ErrSubscriptionDrifted)
		_ = s.mb.Close()
		s.closed = true
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
