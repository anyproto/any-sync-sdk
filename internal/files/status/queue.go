// Package status is the files background-work engine (SYN-29): one
// persistent queue with two job kinds — drive-toward-durable (every
// registered row whose backup hasn't succeeded yet) and pin (full
// background fetches). Jobs survive restarts, retry with backoff, and
// report every transition so the status surface can push events.
package status

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-sync/app/logger"
	"go.uber.org/zap"
)

var log = logger.NewNamed("sdk.files.status")

// QueueCollection persists the pending jobs:
// {id: "<kind>/<spaceId>/<fileId>", kind, sp, fid, att, next, lastErr, lim}.
const QueueCollection = "files_queue"

// Job kinds.
const (
	KindDurable = "durable" // drive an unsigned row to a verified receipt
	KindPin     = "pin"     // fetch every block of a file into the local store
)

// Job is one unit of pending background work.
type Job struct {
	Kind    string
	SpaceId string
	FileId  string

	Attempts int
	NextAt   time.Time
	LastErr  string
	// Limited: the last attempt was refused for storage limit. Retried
	// on the slow cadence (headroom appears by quota raise / deletes),
	// or immediately via Kick.
	Limited bool
}

func (j Job) id() string { return j.Kind + "/" + j.SpaceId + "/" + j.FileId }

// ErrLimited is returned by a Runner to mark a limit refusal (mapped
// from the broker's per-item outcome).
var ErrLimited = errors.New("filestatus: storage limit exceeded")

// ErrNoFileNodes is returned by a Runner when the network lists no file
// nodes (a local-only network): nothing was attempted, so the job waits
// for nodes to appear without counting a failure.
var ErrNoFileNodes = errors.New("filestatus: no file nodes in the network")

// Runner executes one job. Success removes the job; ErrLimited parks
// it on the slow cadence; ErrNoFileNodes parks it uncounted; any other
// error backs off exponentially.
type Runner func(ctx context.Context, job Job) error

// OnChange observes job transitions (enqueue, attempt failure, done).
// done=true means the job left the queue (succeeded or was removed).
type OnChange func(job Job, done bool)

// Retry cadences.
const (
	scanInterval   = 15 * time.Second
	baseBackoff    = 30 * time.Second
	maxBackoff     = 10 * time.Minute
	limitedBackoff = 10 * time.Minute
	noNodesBackoff = time.Minute
	// pinTimeout bounds one pin attempt; fetched blocks persist, so the
	// next attempt resumes.
	pinTimeout = 5 * time.Minute
	// durableTimeout bounds one backup attempt. An upload cannot
	// resume, so the cap is generous; a stalled transfer is the
	// uploader's to detect.
	durableTimeout = 6 * time.Hour
	// workers is how many jobs run at once.
	workers = 4
	// dueBatch is how many due jobs one scan looks at.
	dueBatch = 64
)

// triedJobs matches every job that ran and did not succeed (failed,
// limited or parked): those carry a lastErr.
var triedJobs = query.Key{Path: []string{fieldLastErr}, Filter: query.NewComp(query.CompOpGt, "")}

// Queue is the persistent work queue. One per SDK.
type Queue struct {
	coll     anystore.Collection
	runner   Runner
	onChange OnChange

	kick   chan struct{}
	slots  chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	running  map[string]context.CancelFunc // job id → cancel of its attempt
	lastKind string
}

// NewQueue opens the queue over the shared files DB. Call Run to start
// the worker; runner and onChange must be set before Run.
func NewQueue(ctx context.Context, db anystore.DB, runner Runner, onChange OnChange) (*Queue, error) {
	coll, err := db.Collection(ctx, QueueCollection)
	if err != nil {
		return nil, fmt.Errorf("filestatus: open %s: %w", QueueCollection, err)
	}
	qctx, cancel := context.WithCancel(context.Background())
	return &Queue{
		coll:     coll,
		runner:   runner,
		onChange: onChange,
		kick:     make(chan struct{}, 1),
		slots:    make(chan struct{}, workers),
		running:  map[string]context.CancelFunc{},
		ctx:      qctx,
		cancel:   cancel,
	}, nil
}

// Run starts the worker loop. Persisted jobs from a previous session
// pick up on the first scan, and a backoff earned in that session does
// not carry over: the network may be back, so every job that ran
// before is due now. A job that never ran keeps its schedule (takeover
// stagger).
func (q *Queue) Run() {
	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		v.Set(fieldNext, a.NewNumberFloat64(0))
		return v, true, nil
	})
	if _, err := q.coll.Find(triedJobs).Update(q.ctx, mod); err != nil {
		log.Warn("reset backoff failed", zap.Error(err))
	}
	q.wg.Add(1)
	go q.loop()
	q.Kick()
}

// Close stops the workers and waits for the in-flight jobs.
func (q *Queue) Close() {
	q.cancel()
	q.wg.Wait()
}

// Kick wakes the worker immediately (app events, manual retry).
func (q *Queue) Kick() {
	select {
	case q.kick <- struct{}{}:
	default:
	}
}

// Enqueue adds (or refreshes) a job due immediately. Re-enqueueing an
// existing job resets its schedule but keeps its attempt count.
func (q *Queue) Enqueue(ctx context.Context, kind, spaceId, fileId string) error {
	job := Job{Kind: kind, SpaceId: spaceId, FileId: fileId}
	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		v.Set(fieldKind, a.NewString(kind))
		v.Set(fieldSpace, a.NewString(spaceId))
		v.Set(fieldFile, a.NewString(fileId))
		v.Set(fieldNext, a.NewNumberFloat64(0)) // due now
		v.Del(fieldLimited)
		v.Del(fieldLastErr)
		return v, true, nil
	})
	if err := q.upsert(ctx, job, mod); err != nil {
		return err
	}
	q.Kick()
	return nil
}

// EnqueueDelayed schedules a job to first run no sooner than `delay`
// from now (via the same NextAt the backoff uses). Used to stagger
// durability takeover: a device that DOWNLOADED a non-durable file
// waits before uploading it, so the creator (which enqueues durability
// immediately) usually wins and the delayed job then no-ops on its
// NetworkSign re-check — avoiding two devices uploading the same file
// at once. The worker's periodic scan picks it up when due, so no Kick.
func (q *Queue) EnqueueDelayed(ctx context.Context, kind, spaceId, fileId string, delay time.Duration) error {
	if delay < 0 {
		delay = 0
	}
	next := float64(time.Now().Add(delay).Unix())
	job := Job{Kind: kind, SpaceId: spaceId, FileId: fileId, NextAt: time.Now().Add(delay)}
	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		// Don't pull an already-due job forward or push it later: only
		// set NextAt when creating the job. An existing durable job
		// (e.g. the creator's) keeps its own schedule.
		if v.Get(fieldKind) != nil {
			return v, false, nil
		}
		v.Set(fieldKind, a.NewString(kind))
		v.Set(fieldSpace, a.NewString(spaceId))
		v.Set(fieldFile, a.NewString(fileId))
		v.Set(fieldNext, a.NewNumberFloat64(next))
		return v, true, nil
	})
	return q.upsert(ctx, job, mod)
}

// upsert validates the job identity, applies mod, and notifies. Shared
// by Enqueue / EnqueueDelayed.
func (q *Queue) upsert(ctx context.Context, job Job, mod query.ModifyFunc) error {
	if job.Kind != KindDurable && job.Kind != KindPin {
		return fmt.Errorf("filestatus: unknown job kind %q", job.Kind)
	}
	if job.SpaceId == "" || job.FileId == "" {
		return errors.New("filestatus: spaceId and fileId required")
	}
	if _, err := q.coll.UpsertId(ctx, job.id(), mod); err != nil {
		return err
	}
	q.notify(job, false)
	return nil
}

// Remove drops a job (its work became unnecessary — e.g. the file was
// deleted) and cancels its attempt when one is running.
func (q *Queue) Remove(ctx context.Context, kind, spaceId, fileId string) error {
	job := Job{Kind: kind, SpaceId: spaceId, FileId: fileId}
	err := q.coll.DeleteId(ctx, job.id())
	q.mu.Lock()
	if cancel := q.running[job.id()]; cancel != nil {
		cancel()
	}
	q.mu.Unlock()
	if err != nil && !errors.Is(err, anystore.ErrDocNotFound) {
		return err
	}
	if err == nil {
		q.notify(job, true)
	}
	return nil
}

// Get reads one pending job.
func (q *Queue) Get(ctx context.Context, kind, spaceId, fileId string) (Job, bool, error) {
	doc, err := q.coll.FindId(ctx, Job{Kind: kind, SpaceId: spaceId, FileId: fileId}.id())
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return Job{}, false, nil
		}
		return Job{}, false, err
	}
	return jobFromValue(doc.Value()), true, nil
}

// KickJob makes one job due immediately (manual retry) and wakes the
// worker. False when no such job is pending.
func (q *Queue) KickJob(ctx context.Context, kind, spaceId, fileId string) (bool, error) {
	id := Job{Kind: kind, SpaceId: spaceId, FileId: fileId}.id()
	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		v.Set(fieldNext, a.NewNumberFloat64(0))
		v.Del(fieldLimited)
		return v, true, nil
	})
	if _, err := q.coll.UpdateId(ctx, id, mod); err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return false, nil
		}
		return false, err
	}
	q.Kick()
	return true, nil
}

// RemoveSpace drops every pending job of a space (space deletion —
// nothing left to drive).
func (q *Queue) RemoveSpace(ctx context.Context, spaceId string) error {
	jobs, err := q.ListSpace(ctx, spaceId)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if err := q.Remove(ctx, job.Kind, job.SpaceId, job.FileId); err != nil {
			return err
		}
	}
	return nil
}

// ListSpace returns the pending jobs of one space (aggregate counts).
func (q *Queue) ListSpace(ctx context.Context, spaceId string) ([]Job, error) {
	filter := query.Key{Path: []string{fieldSpace}, Filter: query.NewComp(query.CompOpEq, spaceId)}
	iter, err := q.coll.Find(filter).Iter(ctx)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []Job
	for iter.Next() {
		doc, err := iter.Doc()
		if err != nil {
			return nil, err
		}
		out = append(out, jobFromValue(doc.Value()))
	}
	return out, nil
}

// Row field keys.
const (
	fieldKind    = "kind"
	fieldSpace   = "sp"
	fieldFile    = "fid"
	fieldAtt     = "att"
	fieldNext    = "next"
	fieldLastErr = "lastErr"
	fieldLimited = "lim"
)

func jobFromValue(v *anyenc.Value) Job {
	return Job{
		Kind:     string(v.GetStringBytes(fieldKind)),
		SpaceId:  string(v.GetStringBytes(fieldSpace)),
		FileId:   string(v.GetStringBytes(fieldFile)),
		Attempts: v.GetInt(fieldAtt),
		NextAt:   time.Unix(int64(v.GetFloat64(fieldNext)), 0),
		LastErr:  string(v.GetStringBytes(fieldLastErr)),
		Limited:  v.GetBool(fieldLimited),
	}
}

// loop is the dispatcher: hand every due job to a worker, then sleep
// until the next deadline (bounded by scanInterval) or a kick.
func (q *Queue) loop() {
	defer q.wg.Done()
	for {
		q.dispatchDue()
		select {
		case <-q.ctx.Done():
			return
		case <-q.kick:
		case <-time.After(scanInterval):
		}
	}
}

// dispatchDue starts due jobs until none is left, waiting for a free
// worker when all are busy. Each pass re-queries, so a job re-enqueued
// while running is observed.
func (q *Queue) dispatchDue() {
	for {
		job, ok := q.nextDue()
		if !ok {
			return
		}
		select {
		case q.slots <- struct{}{}:
		case <-q.ctx.Done():
			return
		}
		ctx, cancel := context.WithTimeout(q.ctx, jobTimeout(job.Kind))
		q.mu.Lock()
		q.running[job.id()] = cancel
		q.lastKind = job.Kind
		q.mu.Unlock()
		q.wg.Add(1)
		go func() {
			defer q.wg.Done()
			q.runOne(ctx, job)
			cancel()
			q.mu.Lock()
			delete(q.running, job.id())
			q.mu.Unlock()
			<-q.slots
			q.Kick()
		}()
	}
}

func jobTimeout(kind string) time.Duration {
	if kind == KindDurable {
		return durableTimeout
	}
	return pinTimeout
}

// nextDue picks a due job that is not running. Kinds alternate when
// both are due, so a backlog of one never starves the other.
func (q *Queue) nextDue() (Job, bool) {
	// The read and the running check share one critical section: a
	// worker leaves q.running only after its job's row is settled, so a
	// job read as due here is either still marked running or truly due.
	q.mu.Lock()
	defer q.mu.Unlock()
	now := float64(time.Now().Unix())
	filter := query.Key{Path: []string{fieldNext}, Filter: query.NewComp(query.CompOpLte, now)}
	iter, err := q.coll.Find(filter).Limit(dueBatch).Iter(q.ctx)
	if err != nil {
		return Job{}, false
	}
	var due []Job
	for iter.Next() {
		doc, err := iter.Doc()
		if err != nil {
			break
		}
		due = append(due, jobFromValue(doc.Value()))
	}
	_ = iter.Close()

	var pick *Job
	for i := range due {
		if _, busy := q.running[due[i].id()]; busy {
			continue
		}
		if due[i].Kind != q.lastKind {
			return due[i], true
		}
		if pick == nil {
			pick = &due[i]
		}
	}
	if pick == nil {
		return Job{}, false
	}
	return *pick, true
}

func (q *Queue) runOne(ctx context.Context, job Job) {
	err := q.runner(ctx, job)
	if err == nil {
		if derr := q.coll.DeleteId(q.ctx, job.id()); derr != nil && !errors.Is(derr, anystore.ErrDocNotFound) {
			log.Warn("dequeue failed", zap.String("job", job.id()), zap.Error(derr))
		}
		q.notify(job, true)
		return
	}
	job.LastErr = err.Error()
	job.Limited = errors.Is(err, ErrLimited)
	var delay time.Duration
	if errors.Is(err, ErrNoFileNodes) {
		delay = noNodesBackoff
		log.Debug("job parked: no file nodes", zap.String("job", job.id()))
	} else {
		job.Attempts++
		delay = baseBackoff << min(job.Attempts-1, 5)
		if delay > maxBackoff {
			delay = maxBackoff
		}
		if job.Limited {
			delay = limitedBackoff
		}
		log.Info("job attempt failed", zap.String("job", job.id()),
			zap.Int("attempts", job.Attempts), zap.Bool("limited", job.Limited), zap.Error(err))
	}
	job.NextAt = time.Now().Add(delay)
	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		v.Set(fieldAtt, a.NewNumberInt(job.Attempts))
		v.Set(fieldNext, a.NewNumberFloat64(float64(job.NextAt.Unix())))
		v.Set(fieldLastErr, a.NewString(job.LastErr))
		if job.Limited {
			v.Set(fieldLimited, a.NewTrue())
		} else {
			v.Del(fieldLimited)
		}
		return v, true, nil
	})
	if _, uerr := q.coll.UpdateId(q.ctx, job.id(), mod); uerr != nil && !errors.Is(uerr, anystore.ErrDocNotFound) {
		log.Warn("job update failed", zap.String("job", job.id()), zap.Error(uerr))
	}
	q.notify(job, false)
}

func (q *Queue) notify(job Job, done bool) {
	if q.onChange != nil {
		q.onChange(job, done)
	}
}
