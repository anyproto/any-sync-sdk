package readsync

import (
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonspace/object/keyvalue/keyvaluestorage/innerstorage"
	"github.com/stretchr/testify/require"
)

// MarkReadUpTo holds the object lock while publish takes the store
// mutex, so reconcile must not take an object lock while Iterate holds
// that mutex. The schedule makes the two meet: Iterate parks after
// taking the store mutex until MarkReadUpTo is about to take it inside
// Set, then reconcile merges the same object.
func TestReconcile_ConcurrentMark_LockOrder(t *testing.T) {
	f := newFixture(t)
	f.trackUnread(t, "c1", "v1", nil)
	key := kvKey(testSpace, testObj)
	f.kv.rows[key] = []innerstorage.KeyValue{frontierKV(key, "peer-other", []string{"c1"})}

	iterating := make(chan struct{})
	setting := make(chan struct{})
	var once sync.Once
	f.kv.inIterate = func() {
		close(iterating)
		<-setting
	}
	f.kv.beforeSet = func() { once.Do(func() { close(setting) }) }

	done := make(chan error, 2)
	go func() { done <- f.Reconcile(ctx, testSpace) }()
	<-iterating
	go func() {
		_, err := f.MarkReadUpTo(ctx, testSpace, testObj, "")
		done <- err
	}()
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatalf("deadlock: Reconcile and MarkReadUpTo did not finish\n%s", readsyncStacks())
		}
	}
}

// readsyncStacks returns the goroutines parked inside the Service.
func readsyncStacks() string {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	var out []string
	for _, g := range strings.Split(string(buf[:n]), "\n\n") {
		if strings.Contains(g, "readsync.(*Service)") {
			out = append(out, g)
		}
	}
	return strings.Join(out, "\n\n")
}
