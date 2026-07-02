package status

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recorder struct {
	mu     sync.Mutex
	runs   []Job
	events []string // "job.id done" transitions
	err    func(job Job, attempt int) error
}

func (r *recorder) run(_ context.Context, job Job) error {
	r.mu.Lock()
	r.runs = append(r.runs, job)
	n := 0
	for _, j := range r.runs {
		if j.id() == job.id() {
			n++
		}
	}
	fn := r.err
	r.mu.Unlock()
	if fn != nil {
		return fn(job, n)
	}
	return nil
}

func (r *recorder) onChange(job Job, done bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, fmt.Sprintf("%s %v", job.id(), done))
}

func (r *recorder) runCount(id string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, j := range r.runs {
		if j.id() == id {
			n++
		}
	}
	return n
}

func newQueue(t *testing.T, rec *recorder) (*Queue, anystore.DB) {
	t.Helper()
	db, err := anystore.Open(context.Background(), filepath.Join(t.TempDir(), "meta.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	q, err := NewQueue(context.Background(), db, rec.run, rec.onChange)
	require.NoError(t, err)
	t.Cleanup(q.Close)
	return q, db
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition never held")
}

func TestQueueRunSuccess(t *testing.T) {
	ctx := context.Background()
	rec := &recorder{}
	q, _ := newQueue(t, rec)
	q.Run()

	require.NoError(t, q.Enqueue(ctx, KindDurable, "sp1", "f1"))
	waitFor(t, func() bool { return rec.runCount("durable/sp1/f1") == 1 })
	waitFor(t, func() bool {
		_, ok, err := q.Get(ctx, KindDurable, "sp1", "f1")
		return err == nil && !ok
	})
	rec.mu.Lock()
	defer rec.mu.Unlock()
	assert.Contains(t, rec.events, "durable/sp1/f1 false", "enqueue event")
	assert.Contains(t, rec.events, "durable/sp1/f1 true", "done event")
}

func TestQueueBackoffAndManualKick(t *testing.T) {
	ctx := context.Background()
	rec := &recorder{err: func(job Job, attempt int) error {
		if attempt == 1 {
			return errors.New("transient")
		}
		return nil
	}}
	q, _ := newQueue(t, rec)
	q.Run()

	require.NoError(t, q.Enqueue(ctx, KindDurable, "sp1", "f1"))
	waitFor(t, func() bool { return rec.runCount("durable/sp1/f1") == 1 })
	waitFor(t, func() bool {
		job, ok, err := q.Get(ctx, KindDurable, "sp1", "f1")
		return err == nil && ok && job.Attempts == 1
	})
	job, ok, err := q.Get(ctx, KindDurable, "sp1", "f1")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "transient", job.LastErr)
	assert.False(t, job.Limited)
	assert.True(t, job.NextAt.After(time.Now()), "failed job must back off")
	assert.Equal(t, 1, rec.runCount("durable/sp1/f1"), "must not retry before NextAt")

	// Manual retry bypasses the backoff; the second attempt succeeds.
	kicked, err := q.KickJob(ctx, KindDurable, "sp1", "f1")
	require.NoError(t, err)
	require.True(t, kicked)
	waitFor(t, func() bool {
		_, ok, err := q.Get(ctx, KindDurable, "sp1", "f1")
		return err == nil && !ok
	})
	assert.Equal(t, 2, rec.runCount("durable/sp1/f1"))
}

func TestQueueLimited(t *testing.T) {
	ctx := context.Background()
	rec := &recorder{err: func(Job, int) error { return fmt.Errorf("%w: f1", ErrLimited) }}
	q, _ := newQueue(t, rec)
	q.Run()

	require.NoError(t, q.Enqueue(ctx, KindDurable, "sp1", "f1"))
	waitFor(t, func() bool {
		job, ok, err := q.Get(ctx, KindDurable, "sp1", "f1")
		return err == nil && ok && job.Limited
	})
	job, _, err := q.Get(ctx, KindDurable, "sp1", "f1")
	require.NoError(t, err)
	assert.True(t, job.NextAt.After(time.Now().Add(time.Minute)), "limited parks on the slow cadence")
}

func TestQueuePersistsAcrossRestart(t *testing.T) {
	ctx := context.Background()
	rec := &recorder{err: func(Job, int) error { return errors.New("down") }}
	q, db := newQueue(t, rec)
	q.Run()
	require.NoError(t, q.Enqueue(ctx, KindPin, "sp1", "f1"))
	waitFor(t, func() bool { return rec.runCount("pin/sp1/f1") == 1 })
	q.Close()

	// New process: the persisted job is picked up and, once kicked due,
	// runs to success.
	rec2 := &recorder{}
	q2, err := NewQueue(ctx, db, rec2.run, rec2.onChange)
	require.NoError(t, err)
	t.Cleanup(q2.Close)
	q2.Run()
	kicked, err := q2.KickJob(ctx, KindPin, "sp1", "f1")
	require.NoError(t, err)
	require.True(t, kicked)
	waitFor(t, func() bool { return rec2.runCount("pin/sp1/f1") == 1 })
	waitFor(t, func() bool {
		_, ok, err := q2.Get(ctx, KindPin, "sp1", "f1")
		return err == nil && !ok
	})
}

func TestQueueListSpaceAndRemove(t *testing.T) {
	ctx := context.Background()
	rec := &recorder{err: func(Job, int) error { return errors.New("down") }}
	q, _ := newQueue(t, rec)
	// Not running: exercise the bookkeeping only.
	require.NoError(t, q.Enqueue(ctx, KindDurable, "sp1", "f1"))
	require.NoError(t, q.Enqueue(ctx, KindPin, "sp1", "f2"))
	require.NoError(t, q.Enqueue(ctx, KindDurable, "sp2", "f3"))

	jobs, err := q.ListSpace(ctx, "sp1")
	require.NoError(t, err)
	require.Len(t, jobs, 2)

	require.NoError(t, q.Remove(ctx, KindDurable, "sp1", "f1"))
	jobs, err = q.ListSpace(ctx, "sp1")
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, "f2", jobs[0].FileId)

	kicked, err := q.KickJob(ctx, KindDurable, "sp1", "missing")
	require.NoError(t, err)
	assert.False(t, kicked)

	// Space deletion drops every remaining job of that space only.
	require.NoError(t, q.RemoveSpace(ctx, "sp1"))
	jobs, err = q.ListSpace(ctx, "sp1")
	require.NoError(t, err)
	assert.Empty(t, jobs)
	jobs, err = q.ListSpace(ctx, "sp2")
	require.NoError(t, err)
	assert.Len(t, jobs, 1)
}
