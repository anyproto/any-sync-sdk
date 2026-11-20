package fanout

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestAddDispatchCancel(t *testing.T) {
	r := New[int]()
	var got []int
	cancel := r.Add(func(v int) { got = append(got, v) })
	if !r.HasSubscribers() {
		t.Fatal("expected subscriber")
	}
	r.Dispatch(1)
	r.Dispatch(2)
	cancel()
	cancel() // idempotent
	r.Dispatch(3)
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("got %v", got)
	}
	if r.HasSubscribers() {
		t.Fatal("expected no subscribers")
	}
}

func TestZeroValue(t *testing.T) {
	var r Registry[string]
	r.Dispatch("ignored") // no subscribers, no panic
	var got string
	cancel := r.Add(func(v string) { got = v })
	r.Dispatch("hi")
	cancel()
	if got != "hi" {
		t.Fatalf("got %q", got)
	}
}

func TestNilCallbackAndClose(t *testing.T) {
	r := New[int]()
	cancel := r.Add(nil)
	cancel()
	if r.HasSubscribers() {
		t.Fatal("nil cb must not register")
	}
	r.Close()
	cancel = r.Add(func(int) {})
	cancel()
	if r.HasSubscribers() {
		t.Fatal("Add after Close must not register")
	}
	r.Dispatch(1) // must not panic
}

// A callback may add / cancel on the same registry mid-dispatch —
// snapshot-then-call means no re-entry deadlock.
func TestReentrantCallback(t *testing.T) {
	r := New[int]()
	var inner atomic.Int64
	var cancel func()
	cancel = r.Add(func(int) {
		cancel()
		r.Add(func(int) { inner.Add(1) })()
	})
	r.Dispatch(1)
	r.Dispatch(2)
	if inner.Load() != 0 {
		t.Fatalf("inner fired %d times", inner.Load())
	}
}

func TestConcurrent(t *testing.T) {
	r := New[int]()
	var n atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				cancel := r.Add(func(int) { n.Add(1) })
				r.Dispatch(j)
				cancel()
			}
		}()
	}
	wg.Wait()
	r.Close()
	if n.Load() == 0 {
		t.Fatal("no callbacks fired")
	}
}
