package spaceobjects

import (
	"errors"
	"time"

	"github.com/anyproto/any-sync/app/ocache"
)

// releaseIdle is how long a released Object must go without a Get
// before it closes, so that what its load set off (the read-state
// materializer, the account mirror, the drainer, inbound head updates)
// finds it. releaseKeep is how long a closed one stays released: a
// consumer that trails the sync, like an indexer, loads it again, and
// that load closes the same way, once. releaseTick is how often the
// release loop looks.
const (
	releaseIdle = time.Second
	releaseKeep = objectCacheTTL
	releaseTick = 250 * time.Millisecond
)

// releasedObject is a released id's state: the Release or the last Get
// since, when the release loop closed the Object (zero while resident),
// and whether it was loaded again after a close; that load's close ends
// the release.
type releasedObject struct {
	last, closed time.Time
	reloaded     bool
}

// Release closes the cached Object for objectId once it has gone
// releaseIdle without a Get, instead of at the cache's TTL: the sync
// job loads a backlog faster than the TTL evicts it, and loads each
// tree only to replay it. A Get restarts the wait, and a load within
// releaseKeep of the close is released once more. As under GC eviction, a
// holder that keeps the Object past its last Get may find it closed.
// An Object whose tree is locked declines and is tried on the next tick.
// The space's spaceIndex is never released: it is one object, in use for
// the space's life.
func (s *Store) Release(objectId string) {
	s.releaseMu.Lock()
	defer s.releaseMu.Unlock()
	if s.releaseClosed || objectId == s.spaceIndexObjId {
		return
	}
	if s.released == nil {
		s.released = map[string]releasedObject{}
	}
	if s.releaseStop == nil {
		s.releaseStop = make(chan struct{})
	}
	s.released[objectId] = releasedObject{last: time.Now()}
	if !s.releaseRunning {
		s.releaseRunning = true
		s.releaseWG.Add(1)
		go s.releaseLoop(s.releaseStop)
	}
}

// touchReleased restarts the wait of a released Object; Get calls it.
func (s *Store) touchReleased(objectId string) {
	s.releaseMu.Lock()
	if r, ok := s.released[objectId]; ok {
		s.released[objectId] = releasedObject{last: time.Now(), reloaded: r.reloaded || !r.closed.IsZero()}
	}
	s.releaseMu.Unlock()
}

// releaseLoop runs while an id is released, and ends once none is or
// the store closes.
func (s *Store) releaseLoop(stop <-chan struct{}) {
	defer s.releaseWG.Done()
	tick := time.NewTicker(releaseTick)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-tick.C:
			if !s.sweepReleased(now) {
				return
			}
		}
	}
}

// sweepReleased closes the released Objects that have gone releaseIdle
// without a Get at now, drops the ids closed releaseKeep ago, and
// reports whether any id is still released. One that declines stays
// for the next tick; one the cache no longer holds counts as closed. A
// closing store stops it between ids.
func (s *Store) sweepReleased(now time.Time) bool {
	s.releaseMu.Lock()
	var due []string
	for id, r := range s.released {
		switch {
		case !r.closed.IsZero():
			if now.Sub(r.closed) >= releaseKeep {
				delete(s.released, id)
			}
		case now.Sub(r.last) >= releaseIdle:
			due = append(due, id)
		}
	}
	s.releaseMu.Unlock()
	for _, id := range due {
		s.releaseMu.Lock()
		r, ok := s.released[id]
		stopped := s.releaseClosed
		s.releaseMu.Unlock()
		if stopped {
			return false
		}
		if !ok || !r.closed.IsZero() || now.Sub(r.last) < releaseIdle {
			continue
		}
		closed, err := s.cache.TryRemove(id)
		if !closed && err == nil {
			continue
		}
		s.releaseMu.Lock()
		if cur, ok := s.released[id]; ok && cur == r {
			if (closed || errors.Is(err, ocache.ErrNotExists)) && !r.reloaded {
				s.released[id] = releasedObject{last: r.last, closed: now}
			} else {
				delete(s.released, id)
			}
		}
		s.releaseMu.Unlock()
	}
	s.releaseMu.Lock()
	defer s.releaseMu.Unlock()
	if len(s.released) > 0 {
		return true
	}
	s.released = nil
	s.releaseRunning = false
	return false
}

// stopRelease ends the release loop and waits for it. A later Release
// is dropped: closing the cache closes every Object.
func (s *Store) stopRelease() {
	s.releaseMu.Lock()
	if !s.releaseClosed {
		s.releaseClosed = true
		if s.releaseStop != nil {
			close(s.releaseStop)
		}
	}
	s.releaseMu.Unlock()
	s.releaseWG.Wait()
}
