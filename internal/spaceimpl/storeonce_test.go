package spaceimpl

import (
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
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
	go s.storeOnce("a", build)
	<-started
	defer close(release)
	done := make(chan *spaceobjects.Store, 1)
	go func() { done <- s.storeOnce("b", build) }()
	select {
	case st := <-done:
		require.NotNil(t, st)
	case <-time.After(5 * time.Second):
		t.Fatal("storeOnce(b) waited on a's build")
	}
}

func TestStoreOnce_CloseWaitsForTheBuild(t *testing.T) {
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

	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	waited := make(chan struct{})
	go func() { s.waitStoreBuilds(); close(waited) }()
	select {
	case <-waited:
		t.Fatal("waitStoreBuilds returned while the build was in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("waitStoreBuilds did not return after the build published")
	}
	st := <-published
	s.mu.Lock()
	require.Same(t, st, s.stores["a"])
	s.mu.Unlock()

	// After closing, a build starts with closing=true.
	require.Same(t, s.storeOnce("a", build), st)
	go func() {
		published <- s.storeOnce("b", func(_ string, closing bool) *spaceobjects.Store {
			sawClosing.Store(closing)
			return bare("b")
		})
	}()
	<-published
	require.True(t, sawClosing.Load())
}
