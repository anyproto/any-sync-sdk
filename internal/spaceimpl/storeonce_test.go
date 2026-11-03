package spaceimpl

import (
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
)

func newStoreOnceService(t *testing.T) (*Service, func(spaceId string) *spaceobjects.Store) {
	t.Helper()
	db, err := anystore.Open(t.Context(), filepath.Join(t.TempDir(), "sdk.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	s := &Service{stores: map[string]*spaceobjects.Store{}, building: map[string]chan struct{}{}}
	bare := func(spaceId string) *spaceobjects.Store {
		st := spaceobjects.NewStoreWithConfig(spaceobjects.StoreConfig{
			DB: db, SpaceId: spaceId, Alloc: object.NewVersionAllocator(""), DisableHistory: true,
		})
		t.Cleanup(func() { _ = st.Close() })
		return st
	}
	return s, bare
}

// requireClosed: a closed store's object cache refuses loads.
func requireClosed(t *testing.T, st *spaceobjects.Store) {
	t.Helper()
	_, err := st.Get(t.Context(), "obj")
	require.ErrorIs(t, err, ocache.ErrClosed)
}

// requireBlocked: fn has not returned after a grace period.
func requireBlocked(t *testing.T, returned <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-returned:
		t.Fatalf("%s returned while the build was in flight", what)
	case <-time.After(50 * time.Millisecond):
	}
}

func requireReturned(t *testing.T, returned <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not return after the build published", what)
	}
}

func TestStoreOnce_OneBuildPerSpace(t *testing.T) {
	s, bare := newStoreOnceService(t)
	release := make(chan struct{})
	var builds atomic.Int32
	build := func(spaceId string, _ bool) *spaceobjects.Store {
		builds.Add(1)
		<-release
		return bare(spaceId)
	}
	const callers = 8
	results := make(chan *spaceobjects.Store, callers)
	for range callers {
		go func() { results <- s.storeOnce("a", build) }()
	}
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return builds.Load() == 1 && s.building["a"] != nil
	}, 5*time.Second, time.Millisecond)
	close(release)
	first := <-results
	for i := 1; i < callers; i++ {
		require.Same(t, first, <-results)
	}
	require.Equal(t, int32(1), builds.Load())
	require.Same(t, first, s.storeOnce("a", build), "cached after publish")
	s.mu.Lock()
	require.Empty(t, s.building)
	s.mu.Unlock()
}

func TestStoreOnce_SpacesBuildIndependently(t *testing.T) {
	s, bare := newStoreOnceService(t)
	started, release := make(chan struct{}), make(chan struct{})
	build := func(spaceId string, _ bool) *spaceobjects.Store {
		if spaceId == "a" {
			close(started)
			<-release
		}
		return bare(spaceId)
	}
	aDone := make(chan struct{})
	go func() {
		s.storeOnce("a", build)
		close(aDone)
	}()
	<-started
	bDone := make(chan struct{})
	go func() {
		require.NotNil(t, s.storeOnce("b", build))
		close(bDone)
	}()
	requireReturned(t, bDone, "storeOnce(b)")
	close(release)
	<-aDone
}

func TestCloseStores_WaitsForTheBuildAndClosesIt(t *testing.T) {
	s, bare := newStoreOnceService(t)
	started, release := make(chan struct{}), make(chan struct{})
	var sawClosing atomic.Bool
	build := func(spaceId string, closing bool) *spaceobjects.Store {
		sawClosing.Store(closing)
		close(started)
		<-release
		return bare(spaceId)
	}
	published := make(chan *spaceobjects.Store, 1)
	go func() { published <- s.storeOnce("a", build) }()
	<-started
	require.False(t, sawClosing.Load(), "the build started before closing")

	// Close's head, then its store pass.
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	closed := make(chan struct{})
	go func() {
		s.closeStores()
		close(closed)
	}()
	requireBlocked(t, closed, "closeStores")
	close(release)
	requireReturned(t, closed, "closeStores")
	st := <-published
	s.mu.Lock()
	require.Same(t, st, s.stores["a"])
	s.mu.Unlock()
	requireClosed(t, st)

	// A build after Close: the store is published closed.
	late := s.storeOnce("b", func(_ string, closing bool) *spaceobjects.Store {
		sawClosing.Store(closing)
		return bare("b")
	})
	require.True(t, sawClosing.Load())
	requireClosed(t, late)
	require.Same(t, late, s.storeOnce("b", build), "a closed store stays published")
}

func TestAwaitBuildLocked_WaitsForTheBuild(t *testing.T) {
	s, bare := newStoreOnceService(t)
	started, release := make(chan struct{}), make(chan struct{})
	build := func(spaceId string, _ bool) *spaceobjects.Store {
		close(started)
		<-release
		return bare(spaceId)
	}
	published := make(chan *spaceobjects.Store, 1)
	go func() { published <- s.storeOnce("a", build) }()
	<-started

	// The offload's critical section: take the store once no build is in flight.
	taken := make(chan *spaceobjects.Store, 1)
	go func() {
		s.mu.Lock()
		s.awaitBuildLocked("a")
		st := s.stores["a"]
		delete(s.stores, "a")
		s.mu.Unlock()
		taken <- st
	}()
	returned := make(chan struct{})
	go func() {
		<-taken
		close(returned)
	}()
	requireBlocked(t, returned, "awaitBuildLocked")
	close(release)
	requireReturned(t, returned, "awaitBuildLocked")
	st := <-published
	s.mu.Lock()
	_, present := s.stores["a"]
	s.mu.Unlock()
	require.False(t, present, "the offload took the store the build published")
	require.NotNil(t, st)
}
