package subscribe

import (
	"bytes"
	"errors"
	"sync/atomic"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/anyenc/anyencutil"
	"github.com/anyproto/any-store/v2/syncpool"
	"github.com/cheggaaa/mb/v3"

	"github.com/anyproto/any-sync-sdk/space"
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

	// Drift accounting.
	initialHeld    int
	lost           int
	driftBudgetAbs int // precomputed limit*budget/100 (not used; we compare via lost*100 vs limit*budget)

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

// appendInitial is called by Engine.Subscribe once per snapshot row.
// Computes the sort tuple, deep-clones the doc, inserts the entry.
// Snapshot rows arrive in sort order (ascending tuple), so the last
// row appended is maxRef and the first is minRef when the loop
// completes.
func (s *querySub) appendInitial(id string, doc *anyenc.Value) {
	if doc == nil || id == "" {
		return
	}
	tuple := s.tupleFor(doc)
	cloned := cloneValue(doc)
	s.insertEntry(id, tuple, cloned)
}

// tupleFor builds the sort tuple for doc. With cfg.Sort==nil (only
// allowed when Limit==0), the tuple is empty — we don't need
// ordering when there's no boundary.
func (s *querySub) tupleFor(doc *anyenc.Value) anyenc.Tuple {
	if s.cfg.Sort == nil {
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
	removed []string
	seen    map[string]struct{}
}

type pendingRec struct {
	e   *entry
	ops []space.EventOp // nil for implicit transitions
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
			subev.Added = append(subev.Added, space.SubRecord{Id: pr.e.id, Doc: pr.e.doc, Ops: pr.ops})
		}
	}
	if len(p.updated) > 0 {
		subev.Updated = make([]space.SubRecord, 0, len(p.updated))
		for _, pr := range p.updated {
			subev.Updated = append(subev.Updated, space.SubRecord{Id: pr.e.id, Doc: pr.e.doc, Ops: pr.ops})
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
			// Skips filter eval, allocation, mutation.
			if !wasHeld && s.atCapacity() {
				s.ensureMaxRef()
				if s.maxRef != nil {
					postKey = s.tupleFor(postDoc)
					if bytes.Compare(postKey, s.maxRef.tuple) >= 0 {
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

	// Apply transition to held set.
	switch {
	case wasHeld && matches:
		s.updateEntry(oldEntry, postKey, cloneValue(postDoc))
	case wasHeld && !matches:
		s.removeEntry(oldEntry)
		s.lost++
	case !wasHeld && matches:
		if s.atCapacity() {
			s.ensureMaxRef()
			if s.maxRef != nil {
				s.removeEntry(s.maxRef)
				// Not a "loss" — steady-state add.
			}
		}
		s.insertEntry(rc.Id, postKey, cloneValue(postDoc))
	default:
		return
	}

	// Post-mutation visibility for this record.
	newEntry := s.entries[rc.Id]
	isHeld := newEntry != nil
	isVisible := isHeld && newEntry != s.sentinel()

	switch {
	case !wasVisible && isVisible:
		p.added = append(p.added, pendingRec{e: newEntry, ops: rc.Ops})
	case wasVisible && isVisible:
		p.updated = append(p.updated, pendingRec{e: newEntry, ops: rc.Ops})
	case wasVisible && !isVisible:
		p.removed = append(p.removed, rc.Id)
	}

	// Implicit transitions caused by sentinel movement.
	newSentinelId := s.sentinelId()
	if prevSentinelId != newSentinelId {
		// Previous sentinel may have been promoted into visibility.
		if prevSentinelId != "" && prevSentinelId != rc.Id {
			if _, alreadySeen := p.seen[prevSentinelId]; !alreadySeen {
				if e := s.entries[prevSentinelId]; e != nil && e != s.sentinel() {
					p.added = append(p.added, pendingRec{e: e}) // implicit: no ops
					p.seen[prevSentinelId] = struct{}{}
				}
			}
		}
		// New sentinel may have been demoted out of visibility.
		if newSentinelId != "" && newSentinelId != rc.Id {
			if _, alreadySeen := p.seen[newSentinelId]; !alreadySeen {
				if newSentinelId != prevSentinelId {
					p.removed = append(p.removed, newSentinelId)
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
	if s.lost*100 >= s.cfg.Limit*s.cfg.DriftBudget {
		s.err.Store(space.ErrSubscriptionDrifted)
		_ = s.mb.Close()
		s.closed = true
	}
}

// cloneValue deep-copies v onto a fresh parser-owned arena via
// anyencutil.Value.FillCopy. Mirrors event.go's clonePayload. nil-safe.
func cloneValue(v *anyenc.Value) *anyenc.Value {
	if v == nil {
		return nil
	}
	var w anyencutil.Value
	w.FillCopy(v)
	return w.Value
}
