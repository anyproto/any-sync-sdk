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

type storeBuild = func(spaceId string, closing bool) *spaceobjects.Store

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

// blockingBuild returns a build that signals started and parks on
// release before it returns bare(spaceId).
func blockingBuild(bare func(string) *spaceobjects.Store) (build storeBuild, started, release chan struct{}) {
	started, release = make(chan struct{}), make(chan struct{})
	return func(spaceId string, _ bool) *spaceobjects.Store {
		close(started)
		<-release
		return bare(spaceId)
	}, started, release
}

// requireClosed: a closed store's object cache refuses loads.
func requireClosed(t *testing.T, st *spaceobjects.Store) {
	t.Helper()
	_, err := st.Get(t.Context(), "obj")
	require.ErrorIs(t, err, ocache.ErrClosed)
}

// requireBlocked: returned has not fired after a grace period.
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
		t.Fatalf("%s did not return", what)
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
	buildA, started, release := blockingBuild(bare)
	aDone := make(chan struct{})
	go func() {
		s.storeOnce("a", buildA)
		close(aDone)
	}()
	<-started
	b := make(chan *spaceobjects.Store, 1)
	go func() { b <- s.storeOnce("b", func(id string, _ bool) *spaceobjects.Store { return bare(id) }) }()
	select {
	case st := <-b:
		require.NotNil(t, st)
	case <-time.After(5 * time.Second):
		t.Fatal("storeOnce(b) waited on a's build")
	}
	close(release)
	<-aDone
}

func TestStoreOnce_PanickingBuildReleasesWaiters(t *testing.T) {
	s, bare := newStoreOnceService(t)
	started, release := make(chan struct{}), make(chan struct{})
	var builds atomic.Int32
	build := func(spaceId string, _ bool) *spaceobjects.Store {
		if builds.Add(1) == 1 {
			close(started)
			<-release
			panic("build failed")
		}
		return bare(spaceId)
	}
	panicked := make(chan any, 1)
	go func() {
		defer func() { panicked <- recover() }()
		s.storeOnce("a", build)
	}()
	<-started
	waiter := make(chan *spaceobjects.Store, 1)
	go func() { waiter <- s.storeOnce("a", build) }()
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.building["a"] != nil
	}, 5*time.Second, time.Millisecond)
	close(release)
	require.Equal(t, "build failed", <-panicked, "the panic reaches the builder's caller")
	select {
	case st := <-waiter:
		require.NotNil(t, st, "the waiter rebuilt")
	case <-time.After(5 * time.Second):
		t.Fatal("the waiter stayed parked on the panicked build")
	}
	require.Equal(t, int32(2), builds.Load())
}

func TestCloseStores_WaitsForTheBuildAndClosesIt(t *testing.T) {
	s, bare := newStoreOnceService(t)
	var sawClosing atomic.Bool
	started, release := make(chan struct{}), make(chan struct{})
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

	// Close's head, then its store pass; a same-space waiter arrives in between.
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	waiter := make(chan *spaceobjects.Store, 1)
	go func() { waiter <- s.storeOnce("a", build) }()
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
	require.Same(t, st, <-waiter, "the waiter gets the store closed")

	// A build after Close: the store is published closed.
	late := s.storeOnce("b", func(_ string, closing bool) *spaceobjects.Store {
		sawClosing.Store(closing)
		return bare("b")
	})
	require.True(t, sawClosing.Load())
	requireClosed(t, late)
	noRebuild := func(string, bool) *spaceobjects.Store { t.Error("rebuilt a published store"); return nil }
	require.Same(t, late, s.storeOnce("b", noRebuild), "a closed store stays published")
}

func TestTakeSpaceRuntime_WaitsForTheBuild(t *testing.T) {
	s, bare := newStoreOnceService(t)
	build, started, release := blockingBuild(bare)
	published := make(chan *spaceobjects.Store, 1)
	go func() { published <- s.storeOnce("a", build) }()
	<-started

	taken := make(chan *spaceobjects.Store, 1)
	go func() { taken <- s.takeSpaceRuntime("a") }()
	returned := make(chan struct{})
	go func() {
		<-taken
		close(returned)
	}()
	requireBlocked(t, returned, "takeSpaceRuntime")
	close(release)
	requireReturned(t, returned, "takeSpaceRuntime")
}

func TestTakeSpaceRuntime_ReturnsThePublishedStore(t *testing.T) {
	s, bare := newStoreOnceService(t)
	build, started, release := blockingBuild(bare)
	published := make(chan *spaceobjects.Store, 1)
	go func() { published <- s.storeOnce("a", build) }()
	<-started
	taken := make(chan *spaceobjects.Store, 1)
	go func() { taken <- s.takeSpaceRuntime("a") }()
	close(release)
	st := <-published
	require.Same(t, st, <-taken, "the offload takes the store the build published")
	s.mu.Lock()
	_, present := s.stores["a"]
	require.Empty(t, s.building)
	s.mu.Unlock()
	require.False(t, present)
}
