package anysyncsdk

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/anyproto/any-sync/commonfile/fileproto/fileprotov2"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/files/broker"
	"github.com/anyproto/any-sync-sdk/space"
)

const (
	// limitsWait bounds how long a Limits call waits for the network.
	limitsWait = 5 * time.Second
	// limitsFetchTimeout bounds one fetch. It outlasts limitsWait, so an
	// answer that comes late is there for the next call.
	limitsFetchTimeout = 30 * time.Second
	// limitsRetry is the pause between fetches while the network fails.
	limitsRetry = 10 * time.Second
	// limitsFresh is how long an answer is returned without asking
	// again: the network accounts the limits far slower than this.
	limitsFresh = 10 * time.Second
)

// Stored answers are keyed per part, network and account.
const (
	storageLimitsKey      = "accountLimits/storage/"
	sharedSpacesLimitsKey = "accountLimits/sharedSpaces/"
)

// errNoPoolView — the file nodes answered without the account pool.
var errNoPoolView = errors.New("anysyncsdk: file nodes hold no account pool view")

// limitsSource is the network side of Limits.
type limitsSource interface {
	// scope names the network and the account the answers belong to.
	scope() string
	// hasFileNodes is false on a network that stores no files.
	hasFileNodes() bool
	storage(ctx context.Context) (limitBytes, usageBytes uint64, err error)
	sharedSpaces(ctx context.Context) (limit uint32, err error)
}

// limitsKV keeps the last answers on the device.
type limitsKV interface {
	GetKV(ctx context.Context, key string) (value string, ok bool, err error)
	SetKV(ctx context.Context, key, value string) error
}

// storedLimits is an answer as kept on the device. Its field names are
// its own, so the stored form does not follow the public types.
type storedLimits struct {
	LimitBytes uint64    `json:"limitBytes,omitempty"`
	UsageBytes uint64    `json:"usageBytes,omitempty"`
	Limit      uint32    `json:"limit,omitempty"`
	FetchedAt  time.Time `json:"fetchedAt"`
}

// networkLimits reads the shared-spaces limit from the coordinator and
// the storage pool from the file nodes. The pool is account-wide, so any
// space reports it; the tech space is the one every account has.
type networkLimits struct {
	app     *anysyncx.App
	files   *broker.Client
	spaceId func() string
	account func() string
}

func (n networkLimits) scope() string { return n.app.NetworkId() + "/" + n.account() }

func (n networkLimits) hasFileNodes() bool { return len(n.app.FileV2Peers()) > 0 }

func (n networkLimits) storage(ctx context.Context) (uint64, uint64, error) {
	spaceId := n.spaceId()
	if spaceId == "" {
		return 0, 0, errors.New("anysyncsdk: tech space not open")
	}
	quota, err := n.files.SpaceQuota(ctx, spaceId, hasAccountPool)
	if err != nil {
		return 0, 0, err
	}
	if !hasAccountPool(quota) {
		return 0, 0, errNoPoolView
	}
	return quota.GetAccountLimitBytes(), quota.GetAccountUsageBytes(), nil
}

func (n networkLimits) sharedSpaces(ctx context.Context) (uint32, error) {
	return n.app.SharedSpacesLimit(ctx)
}

// hasAccountPool is false for a node that holds no pool view: it runs on
// its config default, or its coordinator lookup failed. Such a node
// reports a zero pool beside a non-zero space allowance, which is no
// answer, not an empty pool.
func hasAccountPool(q *fileprotov2.SpaceQuota) bool {
	return q.GetAccountLimitBytes() != 0 || q.GetAccountUsageBytes() != 0 || q.GetLimitBytes() == 0
}

// limitsPart keeps one part of the limits: its last answer, in memory
// and on the device, and at most one fetch at a time, shared by every
// caller.
type limitsPart[T any] struct {
	key   string
	fetch func(ctx context.Context) (*T, error)
	// encode and decode convert an answer to and from its stored form.
	encode             func(*T) storedLimits
	decode             func(storedLimits) *T
	wait, retry, fresh time.Duration

	mu     sync.Mutex
	loaded bool      // cur was read from the device
	cur    *T        // nil: the network never answered
	curAt  time.Time // when this process fetched cur; zero for one read from the device
	run    *limitsRun
	// noWait: the last fetch failed or outlasted wait. Callers then take
	// the last answer at once and the next fetch runs behind them.
	noWait  bool
	retryAt time.Time
	closed  bool
}

// limitsRun is one fetch in flight.
type limitsRun struct {
	started  time.Time
	answered chan struct{} // closed once the fetch has answered or failed
	done     chan struct{} // closed once the answer is stored too
}

func (a *accountImpl) initLimits() {
	st, sh := &a.storageLimits, &a.sharedSpacesLimits
	st.key, sh.key = storageLimitsKey, sharedSpacesLimitsKey
	st.fetch = func(ctx context.Context) (*space.StorageLimits, error) {
		limit, usage, err := a.limits.storage(ctx)
		if err != nil {
			return nil, err
		}
		return &space.StorageLimits{LimitBytes: limit, UsageBytes: usage, FetchedAt: time.Now().UTC()}, nil
	}
	st.encode = func(v *space.StorageLimits) storedLimits {
		return storedLimits{LimitBytes: v.LimitBytes, UsageBytes: v.UsageBytes, FetchedAt: v.FetchedAt}
	}
	st.decode = func(s storedLimits) *space.StorageLimits {
		return &space.StorageLimits{LimitBytes: s.LimitBytes, UsageBytes: s.UsageBytes, FetchedAt: s.FetchedAt}
	}
	sh.fetch = func(ctx context.Context) (*space.SharedSpacesLimits, error) {
		limit, err := a.limits.sharedSpaces(ctx)
		if err != nil {
			return nil, err
		}
		return &space.SharedSpacesLimits{Limit: limit, FetchedAt: time.Now().UTC()}, nil
	}
	sh.encode = func(v *space.SharedSpacesLimits) storedLimits {
		return storedLimits{Limit: v.Limit, FetchedAt: v.FetchedAt}
	}
	sh.decode = func(s storedLimits) *space.SharedSpacesLimits {
		return &space.SharedSpacesLimits{Limit: s.Limit, FetchedAt: s.FetchedAt}
	}
	st.wait, st.retry, st.fresh = limitsWait, limitsRetry, limitsFresh
	sh.wait, sh.retry, sh.fresh = limitsWait, limitsRetry, limitsFresh
}

// Limits reads both parts at once. See AccountAPI.Limits.
func (a *accountImpl) Limits(ctx context.Context) (space.AccountLimits, error) {
	var (
		out       space.AccountLimits
		sharedErr error
		wg        sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		out.SharedSpaces, sharedErr = a.sharedSpacesLimits.get(ctx, a)
	}()
	var storageErr error
	if a.limits.hasFileNodes() {
		out.Storage, storageErr = a.storageLimits.get(ctx, a)
	}
	wg.Wait()
	if err := errors.Join(storageErr, sharedErr); err != nil {
		return space.AccountLimits{}, err
	}
	return out, nil
}

// get returns the part's answer: a new one when the network gives it
// within wait, the last one otherwise. While the network is failing or
// slower than wait, the last answer is returned at once and the fetch
// runs behind the call.
func (p *limitsPart[T]) get(ctx context.Context, a *accountImpl) (*T, error) {
	// A cancelled caller gets its error. An expired deadline is no
	// error: it only means there is no time to wait.
	if err := ctx.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return nil, err
	}
	key := p.key + a.limits.scope()
	p.mu.Lock()
	if !p.loaded {
		cur, err := loadLimits(context.WithoutCancel(ctx), a.limitsKV, key, p.decode)
		if err != nil {
			p.mu.Unlock()
			return nil, err
		}
		p.cur, p.loaded = cur, true
	}
	now := time.Now()
	if p.run == nil && !p.closed && now.Sub(p.curAt) >= p.fresh && !now.Before(p.retryAt) {
		p.run = &limitsRun{started: now, answered: make(chan struct{}), done: make(chan struct{})}
		a.wg.Add(1)
		go p.runFetch(a, p.run, key)
	}
	run, wait := p.run, p.run != nil && !p.noWait
	p.mu.Unlock()

	if wait {
		if err := p.await(ctx, run); err != nil {
			return nil, err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cur == nil {
		return nil, nil
	}
	out := *p.cur
	return &out, nil
}

// await waits for run to answer, no longer than wait from its start. The
// caller's deadline bounds the wait the same way; its cancellation is an
// error.
func (p *limitsPart[T]) await(ctx context.Context, run *limitsRun) error {
	timer := time.NewTimer(time.Until(run.started.Add(p.wait)))
	defer timer.Stop()
	select {
	case <-run.answered:
	case <-timer.C:
		select {
		case <-run.answered: // answered just in time
		default:
			p.mu.Lock()
			if p.run == run {
				p.noWait = true
			}
			p.mu.Unlock()
		}
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ctx.Err()
		}
	}
	return nil
}

// runFetch asks the network once, on the account's lifetime rather than
// a caller's, and keeps the answer.
func (p *limitsPart[T]) runFetch(a *accountImpl, run *limitsRun, key string) {
	defer a.wg.Done()
	defer close(run.done)
	ctx, cancel := context.WithTimeout(a.closeCtx, limitsFetchTimeout)
	defer cancel()
	v, err := p.fetch(ctx)

	p.mu.Lock()
	if err != nil {
		log.Debug("account limits: fetch failed", zap.String("part", p.key), zap.Error(err))
		p.noWait, p.retryAt, p.run = true, time.Now().Add(p.retry), nil
		p.mu.Unlock()
		close(run.answered)
		return
	}
	// An answer that outlasted the wait is kept, but the next call does
	// not wait for the next one.
	p.cur, p.curAt, p.noWait = v, time.Now(), time.Since(run.started) > p.wait
	p.mu.Unlock()
	close(run.answered)

	// Stored outside the lock, so a slow device write holds no caller.
	// The run ends after it: answers are stored in the order given.
	if err := storeLimits(a.closeCtx, a.limitsKV, key, p.encode(v)); err != nil {
		log.Warn("account limits: store failed", zap.String("part", p.key), zap.Error(err))
	}
	p.mu.Lock()
	p.run = nil
	p.mu.Unlock()
}

// stop keeps new fetches from starting. Called by close before it joins
// the running ones.
func (p *limitsPart[T]) stop() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
}

// loadLimits reads the stored answer; nil when there is none. A value
// this code did not store is as good as none.
func loadLimits[T any](ctx context.Context, kv limitsKV, key string, decode func(storedLimits) *T) (*T, error) {
	raw, ok, err := kv.GetKV(ctx, key)
	if err != nil || !ok {
		return nil, err
	}
	var s storedLimits
	if json.Unmarshal([]byte(raw), &s) != nil || s.FetchedAt.IsZero() {
		return nil, nil
	}
	return decode(s), nil
}

func storeLimits(ctx context.Context, kv limitsKV, key string, s storedLimits) error {
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return kv.SetKV(ctx, key, string(raw))
}
