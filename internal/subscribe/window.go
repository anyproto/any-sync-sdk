package subscribe

import (
	"bytes"

	"github.com/anyproto/any-store/v2/anyenc"
)

// entry is one held record in a querySub's window. The tuple is a
// freshly-allocated []byte produced by query.Sort.AppendKey — owned
// by this struct, safe to retain. doc is the deep-cloned post-apply
// value; owned (cloned onto a fresh arena via anyencutil.FillCopy
// elsewhere). We need doc here so implicit transitions (a record
// promoted from sentinel into visibility by an unrelated event) can
// still ship a SubRecord with the record's current state — postValue
// only covers ids in the triggering ch.Records.
type entry struct {
	id    string
	tuple anyenc.Tuple
	doc   *anyenc.Value
}

// insertEntry adds a fresh entry to the window. Caller is responsible
// for ensuring id isn't already in entries (use updateEntry otherwise).
// Updates minRef/maxRef cheaply if the new tuple extends a known
// boundary; if a boundary pointer was lazily invalidated (set to nil
// after a removal) and there are other entries still in the set, runs
// a fresh O(|entries|) scan rather than guessing — otherwise we'd
// install the new entry as the boundary even when an existing one is
// extremal.
func (s *querySub) insertEntry(id string, tuple anyenc.Tuple, doc *anyenc.Value) {
	e := &entry{id: id, tuple: tuple, doc: doc}
	s.entries[id] = e
	switch {
	case s.maxRef == nil && len(s.entries) > 1:
		// Boundary was invalidated and existing entries may exceed e.
		s.ensureMaxRef()
	case s.maxRef == nil:
		s.maxRef = e
	case bytes.Compare(e.tuple, s.maxRef.tuple) > 0:
		s.maxRef = e
	}
	switch {
	case s.minRef == nil && len(s.entries) > 1:
		s.ensureMinRef()
	case s.minRef == nil:
		s.minRef = e
	case bytes.Compare(e.tuple, s.minRef.tuple) < 0:
		s.minRef = e
	}
}

// updateEntry overwrites an existing entry's tuple + doc. If the
// entry was the boundary holder and moved out of its position,
// invalidates the pointer for lazy recompute on the next access.
func (s *querySub) updateEntry(e *entry, tuple anyenc.Tuple, doc *anyenc.Value) {
	old := e.tuple
	e.tuple = tuple
	e.doc = doc
	// Was the max — did it stay the max?
	if e == s.maxRef {
		if bytes.Compare(tuple, old) < 0 {
			s.maxRef = nil // may no longer be max; recompute lazily
		}
		// Else still the max (key grew or stayed) — no change.
	} else if s.maxRef != nil && bytes.Compare(tuple, s.maxRef.tuple) > 0 {
		// Wasn't max before, but now exceeds it. Promote.
		s.maxRef = e
	}
	// Symmetric for min.
	if e == s.minRef {
		if bytes.Compare(tuple, old) > 0 {
			s.minRef = nil
		}
	} else if s.minRef != nil && bytes.Compare(tuple, s.minRef.tuple) < 0 {
		s.minRef = e
	}
}

// removeEntry drops an entry from the window. Boundary invalidation
// follows the same lazy rule: if the entry was the boundary holder,
// the pointer is cleared.
func (s *querySub) removeEntry(e *entry) {
	delete(s.entries, e.id)
	if e == s.maxRef {
		s.maxRef = nil
	}
	if e == s.minRef {
		s.minRef = nil
	}
}

// ensureMaxRef recomputes maxRef on demand if it was invalidated. O(N)
// scan over entries comparing tuples. For limit ≤ 10⁴ this is
// microseconds; no heap.
func (s *querySub) ensureMaxRef() {
	if s.maxRef != nil || len(s.entries) == 0 {
		return
	}
	var best *entry
	for _, e := range s.entries {
		if best == nil || bytes.Compare(e.tuple, best.tuple) > 0 {
			best = e
		}
	}
	s.maxRef = best
}

// ensureMinRef is the dual of ensureMaxRef. We don't use min for the
// fast-reject path today, but keep it consistent for future
// optimisations and for diagnostics.
func (s *querySub) ensureMinRef() {
	if s.minRef != nil || len(s.entries) == 0 {
		return
	}
	var best *entry
	for _, e := range s.entries {
		if best == nil || bytes.Compare(e.tuple, best.tuple) < 0 {
			best = e
		}
	}
	s.minRef = best
}
