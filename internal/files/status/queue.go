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

// Runner executes one job. Success removes the job; ErrLimited parks
// it on the slow cadence; any other error backs off exponentially.
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
	jobTimeout     = 5 * time.Minute
)

// Queue is the persistent work queue. One per SDK.
type Queue struct {
	coll     anystore.Collection
	runner   Runner
	onChange OnChange

	kick   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
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
		ctx:      qctx,
		cancel:   cancel,
	}, nil
}

// Run starts the worker loop (persisted jobs from a previous session
// pick up on the first scan).
func (q *Queue) Run() {
	q.wg.Add(1)
	go q.loop()
	q.Kick()
}

// Close stops the worker and waits for the in-flight job.
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
	if kind != KindDurable && kind != KindPin {
		return fmt.Errorf("filestatus: unknown job kind %q", kind)
	}
	if spaceId == "" || fileId == "" {
		return errors.New("filestatus: spaceId and fileId required")
	}
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
	if _, err := q.coll.UpsertId(ctx, job.id(), mod); err != nil {
		return err
	}
	q.notify(job, false)
	q.Kick()
	return nil
}

// Remove drops a job (its work became unnecessary — e.g. the durable
// phase succeeded inline).
func (q *Queue) Remove(ctx context.Context, kind, spaceId, fileId string) error {
	job := Job{Kind: kind, SpaceId: spaceId, FileId: fileId}
	err := q.coll.DeleteId(ctx, job.id())
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

// loop is the worker: run every due job, then sleep until the next
// deadline (bounded by scanInterval) or a kick.
func (q *Queue) loop() {
	defer q.wg.Done()
	for {
		q.drainDue()
		select {
		case <-q.ctx.Done():
			return
		case <-q.kick:
		case <-time.After(scanInterval):
		}
	}
}

// drainDue runs due jobs one at a time until none are due (each pass
// re-queries so a job re-enqueued while running is observed).
func (q *Queue) drainDue() {
	for {
		if q.ctx.Err() != nil {
			return
		}
		job, ok := q.nextDue()
		if !ok {
			return
		}
		q.runOne(job)
	}
}

func (q *Queue) nextDue() (Job, bool) {
	now := float64(time.Now().Unix())
	filter := query.Key{Path: []string{fieldNext}, Filter: query.NewComp(query.CompOpLte, now)}
	iter, err := q.coll.Find(filter).Limit(1).Iter(q.ctx)
	if err != nil {
		return Job{}, false
	}
	defer iter.Close()
	if !iter.Next() {
		return Job{}, false
	}
	doc, err := iter.Doc()
	if err != nil {
		return Job{}, false
	}
	return jobFromValue(doc.Value()), true
}

func (q *Queue) runOne(job Job) {
	ctx, cancel := context.WithTimeout(q.ctx, jobTimeout)
	err := q.runner(ctx, job)
	cancel()
	if err == nil {
		if derr := q.coll.DeleteId(q.ctx, job.id()); derr != nil && !errors.Is(derr, anystore.ErrDocNotFound) {
			log.Warn("dequeue failed", zap.String("job", job.id()), zap.Error(derr))
		}
		q.notify(job, true)
		return
	}
	job.Attempts++
	job.LastErr = err.Error()
	job.Limited = errors.Is(err, ErrLimited)
	delay := baseBackoff << min(job.Attempts-1, 5)
	if delay > maxBackoff {
		delay = maxBackoff
	}
	if job.Limited {
		delay = limitedBackoff
	}
	job.NextAt = time.Now().Add(delay)
	log.Info("job attempt failed", zap.String("job", job.id()),
		zap.Int("attempts", job.Attempts), zap.Bool("limited", job.Limited), zap.Error(err))
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
