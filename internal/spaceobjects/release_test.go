package spaceobjects

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app/ocache"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/object"
)

// releaseObject declines TryClose while busy, the way an Object does
// while a handler holds its tree locked.
type releaseObject struct {
	busy   atomic.Bool
	closed atomic.Int32
}

func (o *releaseObject) Close() error {
	o.closed.Add(1)
	return nil
}

func (o *releaseObject) TryClose(time.Duration) (bool, error) {
	if o.busy.Load() {
		return false, nil
	}
	o.closed.Add(1)
	return true, nil
}

// releaseStore is a Store whose cache holds objs; loads counts the
// cache's loads by id.
func releaseStore(t *testing.T, objs map[string]*releaseObject) (*Store, map[string]int) {
	loads := map[string]int{}
	s := &Store{cache: ocache.New(
		func(_ context.Context, id string) (ocache.Object, error) {
			loads[id]++
			if o, ok := objs[id]; ok {
				return o, nil
			}
			return nil, ocache.ErrNotExists
		},
		ocache.WithTTL(objectCacheTTL),
		ocache.WithGCPeriod(objectCacheGC),
	)}
	t.Cleanup(func() {
		s.stopRelease()
		_ = s.cache.Close()
	})
	for id := range objs {
		_, err := s.cache.Get(t.Context(), id)
		require.NoError(t, err)
	}
	return s, loads
}

// setReleased sets an id's release state as the loop would find it.
func setReleased(s *Store, id string, r releasedObject) {
	s.releaseMu.Lock()
	defer s.releaseMu.Unlock()
	if s.released == nil {
		s.released = map[string]releasedObject{}
	}
	s.released[id] = r
}

// A released Object closes once it has gone releaseIdle without a Get,
// and a Get restarts the wait. One whose tree is locked declines and is
// tried again. A load within releaseKeep of the close is released once
// more, and a load after that is not; an id the cache does not hold is
// not loaded.
func TestStoreRelease(t *testing.T) {
	objs := map[string]*releaseObject{"idle": {}, "busy": {}, "held": {}}
	objs["busy"].busy.Store(true)
	s, loads := releaseStore(t, objs)
	t0 := time.Now().Add(-time.Hour)
	for _, id := range []string{"idle", "busy", "held", "absent"} {
		setReleased(s, id, releasedObject{last: t0})
	}

	require.True(t, s.sweepReleased(t0.Add(releaseIdle-time.Millisecond)))
	for id, o := range objs {
		require.Zero(t, o.closed.Load(), id)
	}

	s.touchReleased("held")
	now := time.Now()
	require.True(t, s.sweepReleased(now))
	require.EqualValues(t, 1, objs["idle"].closed.Load())
	_, err := s.cache.Pick(t.Context(), "idle")
	require.ErrorIs(t, err, ocache.ErrNotExists)
	require.Zero(t, objs["busy"].closed.Load())
	_, err = s.cache.Pick(t.Context(), "busy")
	require.NoError(t, err)
	require.Zero(t, objs["held"].closed.Load())

	_, err = s.cache.Get(t.Context(), "idle")
	require.NoError(t, err)
	s.touchReleased("idle")
	objs["busy"].busy.Store(false)
	later := time.Now().Add(releaseIdle)
	require.True(t, s.sweepReleased(later))
	require.EqualValues(t, 2, objs["idle"].closed.Load(), "the reload is released once more")
	require.EqualValues(t, 1, objs["busy"].closed.Load())
	require.EqualValues(t, 1, objs["held"].closed.Load())
	require.ElementsMatch(t, []string{"busy", "held", "absent"}, releasedIds(s))

	_, err = s.cache.Get(t.Context(), "idle")
	require.NoError(t, err)
	s.touchReleased("idle")
	require.False(t, s.sweepReleased(later.Add(releaseKeep)))
	require.Nil(t, s.released)
	require.EqualValues(t, 2, objs["idle"].closed.Load(), "a second reload stays")
	require.Equal(t, map[string]int{"idle": 3, "busy": 1, "held": 1}, loads)
}

// Release starts the loop that closes released Objects; it ends once no
// id is released, and a Release after that starts it again. Once the
// store stops it, a Release is dropped. The spaceIndex is never
// released.
func TestStoreReleaseLoop(t *testing.T) {
	objs := map[string]*releaseObject{"a": {}, "index": {}}
	s, _ := releaseStore(t, objs)
	s.spaceIndexObjId = "index"

	s.Release("index")
	require.False(t, releaseRunning(s))
	require.Empty(t, releasedIds(s))

	s.Release("a")
	require.True(t, releaseRunning(s))
	setReleased(s, "a", releasedObject{last: time.Now().Add(-releaseIdle)})
	require.Eventually(t, func() bool { return objs["a"].closed.Load() == 1 }, 5*releaseTick, 10*time.Millisecond)
	setReleased(s, "a", releasedObject{closed: time.Now().Add(-releaseKeep)})
	require.Eventually(t, func() bool { return !releaseRunning(s) }, 5*releaseTick, 10*time.Millisecond)
	require.Empty(t, releasedIds(s))

	s.Release("a")
	require.True(t, releaseRunning(s))
	s.stopRelease()
	s.Release("b")
	require.ElementsMatch(t, []string{"a"}, releasedIds(s))
	setReleased(s, "a", releasedObject{last: time.Now().Add(-releaseIdle)})
	require.False(t, s.sweepReleased(time.Now()), "a stopped store sweeps nothing")
	require.EqualValues(t, 1, objs["a"].closed.Load())
}

// Store.Get restarts a released Object's wait, and Store.Close stops the
// release loop.
func TestStoreReleaseGetAndClose(t *testing.T) {
	s := NewStore(nil, nil, nil, "spaceA", nil, nil, nil, nil)
	_ = s.cache.Close()
	s.cache = ocache.New(func(context.Context, string) (ocache.Object, error) { return &object.Object{}, nil })

	s.Release("a")
	setReleased(s, "a", releasedObject{last: time.Now().Add(-time.Hour)})
	before := time.Now()
	_, err := s.Get(t.Context(), "a")
	require.NoError(t, err)
	s.releaseMu.Lock()
	last := s.released["a"].last
	s.releaseMu.Unlock()
	require.False(t, last.Before(before))

	require.NoError(t, s.Close())
	s.Release("b")
	require.ElementsMatch(t, []string{"a"}, releasedIds(s))
}

func releasedIds(s *Store) []string {
	s.releaseMu.Lock()
	defer s.releaseMu.Unlock()
	ids := make([]string, 0, len(s.released))
	for id := range s.released {
		ids = append(ids, id)
	}
	return ids
}

func releaseRunning(s *Store) bool {
	s.releaseMu.Lock()
	defer s.releaseMu.Unlock()
	return s.releaseRunning
}
