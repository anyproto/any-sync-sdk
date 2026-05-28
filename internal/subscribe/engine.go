package subscribe

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
	"github.com/cheggaaa/mb/v3"

	"github.com/anyproto/any-sync-sdk/space"
)

// defaultMailboxCap is the default per-sub mailbox capacity when
// SubConfig.MailboxCap is 0.
const defaultMailboxCap = 256

// minMailboxCap is the floor; capacities below this round up. Tiny
// mailboxes overflow almost immediately and the resulting churn (close
// + resubscribe) overwhelms callers — pick a reasonable minimum.
const minMailboxCap = 16

// defaultDriftBudgetPercent is the default drift threshold when
// SubConfig.DriftBudget is 0.
const defaultDriftBudgetPercent = 30

// SubConfig is the input to Engine.Subscribe. Built by queryImpl from
// the chained Filter / Sort / Limit + caller's QueryOpts. The engine
// owns the parsed query.Filter / query.Sort after registration; the
// caller must not mutate them.
type SubConfig struct {
	Scope       Scope
	Filter      query.Filter // already ANDed with `_deletedAt missing`
	Sort        query.Sort   // required when Limit > 0
	Limit       int          // 0 = unbounded; no sentinel, no fast-reject
	MailboxCap  int          // 0 → defaultMailboxCap; clamped to >= minMailboxCap
	DriftBudget int          // percent of limit; 0 → defaultDriftBudgetPercent; ignored when Limit == 0
}

// SnapshotFn is the caller-supplied populator. It runs UNDER engine.mu
// (held by Subscribe), so apply events fired during the read block
// waiting on engine.mu and process after the new sub is registered.
// The callback yields each result row's (id, post-doc) to the engine,
// which extracts the sort tuple via cfg.Sort and inserts the entry.
type SnapshotFn func(yield func(id string, doc *anyenc.Value)) error

// ErrEngineClosed is returned by Subscribe after Close.
var ErrEngineClosed = errors.New("subscribe: engine closed")

// ErrLimitWithoutSort fires when a caller asks for Limit > 0 without
// supplying a Sort — the engine needs a total order to define
// "boundary" / "sentinel". Limit == 0 (unbounded) is fine without
// Sort.
var ErrLimitWithoutSort = errors.New("subscribe: Limit > 0 requires Sort")

// Engine is the per-space live-query engine. Cheap when no subs are
// registered: HasSubscribers is a single atomic load and the
// afterApply hook short-circuits on it.
type Engine struct {
	spaceId string

	mu      sync.Mutex
	subs    map[uint64]*querySub
	scope   scopeIndex
	nextId  atomic.Uint64
	counter atomic.Int64 // live sub count; reads gate the apply hook
	closed  atomic.Bool

	scratch []*querySub // OnApply reuses to avoid per-event allocation
}

// New constructs an empty Engine for spaceId.
func New(spaceId string) *Engine {
	return &Engine{
		spaceId: spaceId,
		subs:    map[uint64]*querySub{},
		scope:   newScopeIndex(),
	}
}

// SpaceId returns the id of the space this engine serves.
func (e *Engine) SpaceId() string { return e.spaceId }

// HasSubscribers reports whether any sub is live. Single atomic load
// — call before paying the cost of building the wire Event in
// afterApply.
func (e *Engine) HasSubscribers() bool {
	return e.counter.Load() > 0
}

// Subscribe registers a new querySub. The snapshot callback runs
// under engine.mu so the apply path is fenced between the snapshot
// read and the sub's registration — no events are missed and no
// events fire against an unregistered sub.
//
// Returns space.ErrSubscribeUnsupported when the engine is closed.
// Returns ErrLimitWithoutSort when cfg.Limit > 0 and cfg.Sort is nil.
func (e *Engine) Subscribe(cfg SubConfig, snapshot SnapshotFn) (*Sub, error) {
	if cfg.Limit > 0 && cfg.Sort == nil {
		return nil, ErrLimitWithoutSort
	}
	if cfg.MailboxCap <= 0 {
		cfg.MailboxCap = defaultMailboxCap
	}
	if cfg.MailboxCap < minMailboxCap {
		cfg.MailboxCap = minMailboxCap
	}
	if cfg.DriftBudget <= 0 {
		cfg.DriftBudget = defaultDriftBudgetPercent
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed.Load() {
		return nil, ErrEngineClosed
	}

	sub := newSub(cfg)
	if err := snapshot(func(id string, doc *anyenc.Value) {
		sub.appendInitial(id, doc)
	}); err != nil {
		return nil, err
	}
	sub.initialHeld = len(sub.entries)
	if cfg.Limit > 0 {
		sub.driftBudgetAbs = (cfg.Limit * cfg.DriftBudget) / 100
		if sub.driftBudgetAbs < 1 {
			sub.driftBudgetAbs = 1 // at least one loss triggers when limit*budget rounds to 0
		}
	}

	sub.id = e.nextId.Add(1)
	sub.engine = e
	e.subs[sub.id] = sub
	e.scope.add(sub)
	e.counter.Add(1)
	return &Sub{querySub: sub}, nil
}

// OnApply is the apply-path hook. afterApply calls this with the same
// Event built once for the dispatcher, plus the PostValueFn that
// lets the engine evaluate filter / sort against the post-apply doc.
//
// Cheap when there are no subs (HasSubscribers check returns false
// before this is even invoked by the caller). When subs exist, takes
// engine.mu briefly to fan to the matching ones.
func (e *Engine) OnApply(ev Event, postValue PostValueFn) {
	if e.closed.Load() {
		return
	}
	if e.counter.Load() == 0 {
		return
	}
	if postValue == nil {
		// Apply path couldn't provide a post-apply lookup (rare; see
		// gate.go.postValueFor). No filter / sort eval possible — skip.
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.scratch = e.scope.matches(ev, e.scratch[:0])
	if len(e.scratch) == 0 {
		return
	}
	for _, sub := range e.scratch {
		if sub.closed {
			continue
		}
		sub.apply(ev, postValue)
	}
}

// closeSub removes a sub from the engine. Called via Sub.Close.
func (e *Engine) closeSub(s *querySub) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.subs[s.id]; !ok {
		return
	}
	if !s.closed {
		s.closed = true
		_ = s.mb.Close()
	}
	delete(e.subs, s.id)
	e.scope.remove(s)
	e.counter.Add(-1)
}

// Close cancels every live sub. Idempotent.
func (e *Engine) Close() error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, s := range e.subs {
		if !s.closed {
			s.closed = true
			_ = s.mb.Close()
		}
	}
	e.subs = map[uint64]*querySub{}
	e.scope = newScopeIndex()
	e.counter.Store(0)
	return nil
}

// Sub is the consumer-side handle returned by Engine.Subscribe. It
// implements space.QuerySubscription. The engine guarantees the
// mailbox carries only post-snapshot transitions (no replay of the
// snapshot itself).
type Sub struct {
	*querySub
}

// Events returns the underlying mb/v3 mailbox. Use Wait for batched
// delivery or WaitOne for single events.
func (s *Sub) Events() *mb.MB[space.SubscriptionEvent] { return s.mb }

// Err returns the close reason — nil while live or after user Close,
// space.ErrSubscriptionOverflow on mailbox overflow,
// space.ErrSubscriptionDrifted on drift-budget exceeded.
func (s *Sub) Err() error {
	if v, ok := s.err.Load().(error); ok {
		return v
	}
	return nil
}

// Close releases the subscription. Idempotent.
func (s *Sub) Close() error {
	if s.engine != nil {
		s.engine.closeSub(s.querySub)
	}
	return nil
}

// Compile-time interface check.
var _ space.QuerySubscription = (*Sub)(nil)

// docBufFor builds a fresh per-sub DocBuffer for filter.Ok eval. We
// don't share these across subs to keep ownership clean; the buffer
// is mutated during eval.
func docBufFor() *syncpool.DocBuffer {
	return &syncpool.DocBuffer{
		Arena:  &anyenc.Arena{},
		Parser: &anyenc.Parser{},
	}
}
