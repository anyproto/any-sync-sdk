package anysyncx

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/anyproto/any-sync/net/peer"
	"go.uber.org/zap"
)

// The sync job: one loop per space syncs the trees its headsync rounds
// name, in passes, under the space's lifetime.
//
// A round (SyncAll) finds the trees missing or changed against a peer.
// Handled within the round, the work was paced by the caller: a
// periodic round is time-boxed to a minute, so a backlog of thousands
// spanned many rounds with an idle diff between each, and a round a
// caller kicked with a short context parked every tree it had not
// reached when the context ended. So a round queues its trees on the
// space's job and waits for them for as long as its caller lets it;
// the job goes on until the queue is empty.
//
// A pass handles the queue in groups, each done before the next starts:
//
//  1. the trees the registry names first (SpaceRegistry.PullFirst) —
//     the spaceIndex, the bundle roots it lists, the local definitions
//     — one at a time, each fetched and materialized in one step. Asked
//     twice: handling the spaceIndex can add bundle roots to the answer.
//  2. the other missing trees, fetched into storage (treefetch.go)
//     treeFetchWorkers at a time, nothing materialized. The root read on
//     the way in says what each tree is.
//  3. the fetched trees whose root is a type or a collection
//     (SpaceRegistry.PullFirstTypes), materialized one at a time.
//  4. the other fetched trees, materialized treeSyncWorkers at a time.
//  5. the changed trees, loaded and pinged with a full-sync request.
//  6. the retries of trees parked by earlier failures.
//
// Within a pass no object replays ahead of a definition the diff
// offered, and no definition replays next to a tree that may look it
// up: that is what keeps the apply gate's parking (crdt.md § Datasets)
// for the cases the job cannot see — a tree a peer's head update pulls
// on its own, a definition arriving in a later pass.

// treeFetchWorkers bounds the trees one pass fetches at once. A fetch is
// a round trip to the peer and a storage write, nothing else, so the
// peer's request budget (peerLimits) is what paces it; this only caps
// the goroutines.
var treeFetchWorkers = 8

// fetchTimeout bounds one tree request. A tree cut short keeps what
// arrived: its storage was created on the first answer, so the next
// diff offers it as changed and a full-sync request completes it.
var fetchTimeout = 2 * time.Minute

// syncKind is why a round queued a tree.
type syncKind uint8

const (
	// syncMissing: the diff found the tree on the peer only.
	syncMissing syncKind = iota
	// syncChanged: both sides have it with different heads.
	syncChanged
	// syncRetry: an earlier round parked it.
	syncRetry
)

// syncItem is one queued tree. One per id: rounds that overlap queue
// much the same trees, and each waits for the same item.
type syncItem struct {
	id   string
	kind syncKind
	peer peer.Peer
	// seq orders the items the way rounds queued them.
	seq uint64
	// busy: a worker is on it.
	busy bool
	// fetched: the tree is in storage, not yet materialized; changeType
	// is its root's.
	fetched    bool
	changeType string
	// done is closed when the item is handled: materialized, parked or
	// skipped.
	done chan struct{}
}

type syncJob struct {
	t *treeSyncerAdapter

	// ctx is the job's lifetime, ended by close.
	ctx    context.Context
	cancel context.CancelFunc
	// wake carries one token: the queue grew.
	wake chan struct{}
	// exited is closed when the loop returns.
	exited          chan struct{}
	running, closed bool

	mu    sync.Mutex
	items map[string]*syncItem
	seq   uint64
}

func newSyncJob(t *treeSyncerAdapter) *syncJob {
	ctx, cancel := context.WithCancel(context.Background())
	return &syncJob{
		t:      t,
		ctx:    ctx,
		cancel: cancel,
		wake:   make(chan struct{}, 1),
		exited: make(chan struct{}),
		items:  map[string]*syncItem{},
	}
}

// add queues a round's trees — in the order given, missing and changed
// ids first, then the parked ones, an id once — and returns the items
// the round waits for. A tree already queued is shared; a queued retry
// the diff now offers becomes a fetch or a ping.
func (j *syncJob) add(p peer.Peer, existing, missing, parked []string) []*syncItem {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []*syncItem
	seen := make(map[string]struct{}, len(existing)+len(missing)+len(parked))
	queue := func(ids []string, kind syncKind) {
		for _, id := range ids {
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			it, ok := j.items[id]
			if !ok {
				j.seq++
				it = &syncItem{id: id, kind: kind, peer: p, seq: j.seq, done: make(chan struct{})}
				j.items[id] = it
			} else if !it.busy {
				it.peer = p
				if it.kind == syncRetry {
					it.kind = kind
				}
			}
			out = append(out, it)
		}
	}
	queue(missing, syncMissing)
	queue(existing, syncChanged)
	queue(parked, syncRetry)
	if len(out) == 0 || j.closed {
		return out
	}
	if !j.running {
		j.running = true
		go j.loop()
	}
	select {
	case j.wake <- struct{}{}:
	default:
	}
	return out
}

// wait returns once every item is handled, or the caller's context
// ends (its error), or the job closes (nil: the space is going away).
func (j *syncJob) wait(ctx context.Context, items []*syncItem) error {
	for _, it := range items {
		select {
		case <-it.done:
		case <-ctx.Done():
			return ctx.Err()
		case <-j.ctx.Done():
			return nil
		}
	}
	return nil
}

// count reports the queued trees, the ones in flight included.
func (j *syncJob) count() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.items)
}

// close ends the job and waits for its workers. Items left in the queue
// stay counted: a tree fetched and not materialized is in storage
// without being in the projection, and the close-time watermark gate
// reads the count (App.SyncingTreeCount).
func (j *syncJob) close() {
	j.mu.Lock()
	j.closed = true
	running := j.running
	j.mu.Unlock()
	j.cancel()
	if running {
		<-j.exited
	}
}

func (j *syncJob) loop() {
	defer close(j.exited)
	for j.ctx.Err() == nil {
		if j.pass() {
			continue
		}
		select {
		case <-j.wake:
		case <-j.ctx.Done():
		}
	}
}

// pass works through what is queued, group by group, and reports
// whether there was anything.
func (j *syncJob) pass() (worked bool) {
	t := j.t
	for i := 0; i < 2; i++ {
		first := j.takeFirst()
		if len(first) == 0 {
			break
		}
		worked = true
		j.each(first, 1, j.handle)
	}
	if missing := j.take(func(it *syncItem) bool { return it.kind == syncMissing && !it.fetched }); len(missing) > 0 {
		worked = true
		start := time.Now()
		j.each(missing, treeFetchWorkers, j.fetch)
		t.logPass("fetched missing trees", len(missing), start)
	}
	if fetched := j.take(func(it *syncItem) bool { return it.fetched }); len(fetched) > 0 {
		worked = true
		start := time.Now()
		types := t.registry.PullFirstTypes(t.spaceId)
		var defs, rest []*syncItem
		for _, it := range fetched {
			if slices.Contains(types, it.changeType) {
				defs = append(defs, it)
			} else {
				rest = append(rest, it)
			}
		}
		j.each(defs, 1, j.materialize)
		j.each(rest, treeSyncWorkers, j.materialize)
		t.logPass("materialized fetched trees", len(fetched), start, zap.Int("definitions", len(defs)))
	}
	for _, kind := range []syncKind{syncChanged, syncRetry} {
		if items := j.take(func(it *syncItem) bool { return it.kind == kind }); len(items) > 0 {
			worked = true
			j.each(items, 1, j.handle)
		}
	}
	return worked
}

// take snapshots the queued items pick accepts, in queue order.
func (j *syncJob) take(pick func(*syncItem) bool) []*syncItem {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []*syncItem
	for _, it := range j.items {
		if pick(it) {
			out = append(out, it)
		}
	}
	slices.SortFunc(out, func(a, b *syncItem) int { return int(a.seq - b.seq) })
	return out
}

// takeFirst picks the queued items the registry names first, in its
// order. A queue of one has nothing to order and asks nothing.
func (j *syncJob) takeFirst() []*syncItem {
	if j.count() < 2 {
		return nil
	}
	first := j.t.registry.PullFirst(j.ctx, j.t.spaceId)
	if len(first) == 0 {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []*syncItem
	for _, id := range first {
		if it, ok := j.items[id]; ok {
			out = append(out, it)
		}
	}
	return out
}

// each runs fn over items on up to workers goroutines and returns when
// every call has returned; a closing job runs nothing more.
func (j *syncJob) each(items []*syncItem, workers int, fn func(*syncItem)) {
	eachOf(items, workers, func(it *syncItem) {
		if j.ctx.Err() != nil {
			return
		}
		j.mu.Lock()
		it.busy = true
		j.mu.Unlock()
		fn(it)
	})
}

// finish takes the item off the queue and releases the rounds waiting
// for it.
func (j *syncJob) finish(it *syncItem) {
	j.mu.Lock()
	delete(j.items, it.id)
	j.mu.Unlock()
	close(it.done)
}

// handle syncs one tree in one step, the way a round did on its own:
// fetched when missing, loaded and pinged otherwise.
func (j *syncJob) handle(it *syncItem) {
	if j.t.syncOne(j.ctx, it.peer, it.id, it.kind == syncMissing || j.t.parkedMissing(it.id)) {
		j.finish(it)
	}
}

// fetch brings one missing tree into storage. A failure parks the tree
// for a retry; success leaves the item queued as fetched.
func (j *syncJob) fetch(it *syncItem) {
	t := j.t
	changeType, err := t.fetchTree(j.ctx, it.peer.Id(), it.id)
	if err != nil {
		if j.ctx.Err() != nil {
			return
		}
		t.markPending(it.id, true)
		t.logParked(it.peer.Id(), it.id, err)
		j.finish(it)
		return
	}
	j.mu.Lock()
	it.fetched, it.changeType, it.busy = true, changeType, false
	j.mu.Unlock()
}

// materialize replays one fetched tree into the projection. The load
// takes no request slot and names no peer: the tree is local, and one
// that is not any more (deleted meanwhile) fails and parks rather than
// fetch outside the budget.
func (j *syncJob) materialize(it *syncItem) {
	t := j.t
	tree, err := t.registry.GetTree(j.ctx, t.spaceId, it.id)
	if err != nil {
		if j.ctx.Err() != nil {
			return
		}
		t.markPending(it.id, false)
		t.logParked(it.peer.Id(), it.id, err)
		j.finish(it)
		return
	}
	t.recovered(it.peer.Id(), it.id)
	t.reportFetched(it.peer.Id(), it.id, tree)
	j.finish(it)
}
