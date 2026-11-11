package subscribe

import (
	"bytes"

	"github.com/anyproto/any-store/v2/anyenc"
)

// entry is one held record in a querySub's window. tuple is the sort
// key (query.Sort.AppendKey, owned) and raw the encoding of the
// record's last post value (owned, never mutated); a windowed sub
// keeps both — an implicit transition (a record promoted from
// sentinel into visibility by an unrelated event) ships raw parsed,
// since postValue covers only the ids of the triggering change. An
// unbounded sub tracks membership only (querySub.windowed): id alone,
// tuple and raw nil. seq orders entries with equal tuples: storage's
// order for rows a read returned, arrival order for the rest.
type entry struct {
	id    string
	tuple anyenc.Tuple
	raw   []byte
	seq   uint64
}

// after reports whether e sorts after o: a larger tuple, or an equal
// one placed later.
func (e *entry) after(o *entry) bool {
	c := bytes.Compare(e.tuple, o.tuple)
	return c > 0 || (c == 0 && e.seq > o.seq)
}

// place puts e last among the entries that share its tuple.
func (s *querySub) place(e *entry) {
	s.seq++
	e.seq = s.seq
}

// insertEntry adds a fresh entry to the window. Caller is responsible
// for ensuring id isn't already in entries (use updateEntry otherwise).
// Updates minRef/maxRef cheaply if the new tuple extends a known
// boundary; if a boundary pointer was lazily invalidated (set to nil
// after a removal) and there are other entries still in the set, runs
// a fresh O(|entries|) scan rather than guessing — otherwise we'd
// install the new entry as the boundary even when an existing one is
// extremal.
func (s *querySub) insertEntry(id string, tuple anyenc.Tuple, raw []byte) {
	e := &entry{id: id, tuple: tuple, raw: raw}
	s.place(e)
	s.entries[id] = e
	switch {
	case s.maxRef == nil && len(s.entries) > 1:
		// Boundary was invalidated and existing entries may exceed e.
		s.ensureMaxRef()
	case s.maxRef == nil:
		s.maxRef = e
	case e.after(s.maxRef):
		s.maxRef = e
	}
	switch {
	case s.minRef == nil && len(s.entries) > 1:
		s.ensureMinRef()
	case s.minRef == nil:
		s.minRef = e
	case s.minRef.after(e):
		s.minRef = e
	}
}

// updateEntry overwrites an existing entry's tuple + raw. A changed
// tuple places the entry last among its new ties; an unchanged one
// keeps its place. If the entry was the boundary holder and moved out
// of its position, invalidates the pointer for lazy recompute on the
// next access.
func (s *querySub) updateEntry(e *entry, tuple anyenc.Tuple, raw []byte) {
	old := e.tuple
	e.tuple = tuple
	e.raw = raw
	if !bytes.Equal(tuple, old) {
		s.place(e)
	}
	// Was the max — did it stay the max?
	if e == s.maxRef {
		if bytes.Compare(tuple, old) < 0 {
			s.maxRef = nil // may no longer be max; recompute lazily
		}
		// Else still the max (key grew or stayed) — no change.
	} else if s.maxRef != nil && e.after(s.maxRef) {
		// Wasn't max before, but now exceeds it. Promote.
		s.maxRef = e
	}
	// Symmetric for min.
	if e == s.minRef {
		if bytes.Compare(tuple, old) > 0 {
			s.minRef = nil
		}
	} else if s.minRef != nil && s.minRef.after(e) {
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
// scan over entries comparing tuples, then places. For limit ≤ 10⁴ this is
// microseconds; no heap.
func (s *querySub) ensureMaxRef() {
	if s.maxRef != nil || len(s.entries) == 0 {
		return
	}
	var best *entry
	for _, e := range s.entries {
		if best == nil || e.after(best) {
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
		if best == nil || best.after(e) {
			best = e
		}
	}
	s.minRef = best
}
