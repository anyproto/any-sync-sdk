package anysyncsdk

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonfile/fileproto/fileprotov2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// fakeLimits is a network that answers, fails, or hangs until released.
type fakeLimits struct {
	mu                     sync.Mutex
	account                string
	noFileNodes            bool
	limitBytes, usageBytes uint64
	shared                 uint32
	storageErr, sharedErr  error
	hang                   chan struct{} // non-nil: every fetch blocks on it
	storageCalls           int
	sharedCalls            int
}

func (f *fakeLimits) set(fn func(f *fakeLimits)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func (f *fakeLimits) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.storageCalls + f.sharedCalls
}

func (f *fakeLimits) scope() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return "net/" + f.account
}

func (f *fakeLimits) hasFileNodes() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.noFileNodes
}

func (f *fakeLimits) block(ctx context.Context) error {
	f.mu.Lock()
	hang := f.hang
	f.mu.Unlock()
	if hang == nil {
		return nil
	}
	select {
	case <-hang:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *fakeLimits) storage(ctx context.Context) (uint64, uint64, error) {
	f.set(func(f *fakeLimits) { f.storageCalls++ })
	if err := f.block(ctx); err != nil {
		return 0, 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.limitBytes, f.usageBytes, f.storageErr
}

func (f *fakeLimits) sharedSpaces(ctx context.Context) (uint32, error) {
	f.set(func(f *fakeLimits) { f.sharedCalls++ })
	if err := f.block(ctx); err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.shared, f.sharedErr
}

// memKV is the device store; getErr and setErr make it fail.
type memKV struct {
	mu             sync.Mutex
	m              map[string]string
	getErr, setErr error
}

func newMemKV() *memKV { return &memKV{m: map[string]string{}} }

func (k *memKV) GetKV(_ context.Context, key string) (string, bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.getErr != nil {
		return "", false, k.getErr
	}
	v, ok := k.m[key]
	return v, ok, nil
}

func (k *memKV) SetKV(_ context.Context, key, value string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.setErr != nil {
		return k.setErr
	}
	k.m[key] = value
	return nil
}

// newLimitsAccount is an account with only the limits wired: one device
// on net, keeping its answers in kv. Every call asks the network again
// unless a test sets the parts' fresh window.
func newLimitsAccount(t *testing.T, net limitsSource, kv limitsKV) *accountImpl {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	acc := &accountImpl{limits: net, limitsKV: kv, closeCtx: ctx, closeCancel: cancel}
	acc.initLimits()
	acc.storageLimits.fresh, acc.sharedSpacesLimits.fresh = 0, 0
	t.Cleanup(acc.close)
	return acc
}

func limitsRunOf[T any](p *limitsPart[T]) *limitsRun {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.run
}

// settle waits until the fetches in flight have ended and stored.
func settle(acc *accountImpl) {
	for _, run := range []*limitsRun{limitsRunOf(&acc.storageLimits), limitsRunOf(&acc.sharedSpacesLimits)} {
		if run != nil {
			<-run.done
		}
	}
}

// retryNow ends the pause after a failed fetch.
func retryNow(acc *accountImpl) {
	acc.storageLimits.mu.Lock()
	acc.storageLimits.retryAt = time.Time{}
	acc.storageLimits.mu.Unlock()
	acc.sharedSpacesLimits.mu.Lock()
	acc.sharedSpacesLimits.retryAt = time.Time{}
	acc.sharedSpacesLimits.mu.Unlock()
}

var errDown = errors.New("unable to connect")

func down(f *fakeLimits) { f.storageErr, f.sharedErr = errDown, errDown }

func TestAccountLimits_Live(t *testing.T) {
	ctx := context.Background()
	net := &fakeLimits{limitBytes: 1 << 30, usageBytes: 42, shared: 3}
	acc := newLimitsAccount(t, net, newMemKV())

	got, err := acc.Limits(ctx)
	require.NoError(t, err)
	require.NotNil(t, got.Storage)
	require.NotNil(t, got.SharedSpaces)
	assert.Equal(t, uint64(1<<30), got.Storage.LimitBytes)
	assert.Equal(t, uint64(42), got.Storage.UsageBytes)
	assert.Equal(t, uint32(3), got.SharedSpaces.Limit)
	assert.WithinDuration(t, time.Now(), got.Storage.FetchedAt, time.Minute)
	assert.WithinDuration(t, time.Now(), got.SharedSpaces.FetchedAt, time.Minute)

	// Past the fresh window a call on a healthy network is a new answer.
	settle(acc)
	net.set(func(f *fakeLimits) { f.usageBytes, f.shared = 43, 4 })
	next, err := acc.Limits(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(43), next.Storage.UsageBytes)
	assert.Equal(t, uint32(4), next.SharedSpaces.Limit)
	assert.False(t, next.Storage.FetchedAt.Before(got.Storage.FetchedAt))

	// The caller owns what it got.
	settle(acc)
	next.Storage.LimitBytes = 1
	again, err := acc.Limits(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(1<<30), again.Storage.LimitBytes)
}

// Within the fresh window the network is not asked again.
func TestAccountLimits_Fresh(t *testing.T) {
	ctx := context.Background()
	net := &fakeLimits{limitBytes: 100, shared: 3}
	acc := newLimitsAccount(t, net, newMemKV())
	acc.storageLimits.fresh, acc.sharedSpacesLimits.fresh = time.Hour, time.Hour

	first, err := acc.Limits(ctx)
	require.NoError(t, err)
	settle(acc)
	net.set(func(f *fakeLimits) { f.limitBytes = 200 })
	for range 5 {
		got, err := acc.Limits(ctx)
		require.NoError(t, err)
		assert.Equal(t, first, got)
	}
	assert.Equal(t, 2, net.calls())
}

func TestAccountLimits_Offline(t *testing.T) {
	ctx := context.Background()
	net := &fakeLimits{storageErr: errDown, sharedErr: errDown}
	kv := newMemKV()
	acc := newLimitsAccount(t, net, kv)

	// Never reached the network: no part is known, and it is no error.
	got, err := acc.Limits(ctx)
	require.NoError(t, err)
	assert.Nil(t, got.Storage)
	assert.Nil(t, got.SharedSpaces)

	// While the network fails it is not asked on every call.
	calls := net.calls()
	for range 3 {
		got, err = acc.Limits(ctx)
		require.NoError(t, err)
		assert.Nil(t, got.Storage)
	}
	assert.Equal(t, calls, net.calls())

	// The network is back. The next fetch runs behind the call, which
	// still has nothing to return.
	net.set(func(f *fakeLimits) {
		f.storageErr, f.sharedErr = nil, nil
		f.limitBytes, f.usageBytes, f.shared = 100, 10, 3
	})
	retryNow(acc)
	got, err = acc.Limits(ctx)
	require.NoError(t, err)
	assert.Nil(t, got.Storage)
	assert.Nil(t, got.SharedSpaces)
	settle(acc)

	// Its answer is kept: offline again, that answer is what a call
	// returns, with the time it was given.
	net.set(down)
	kept, err := acc.Limits(ctx)
	require.NoError(t, err)
	require.NotNil(t, kept.Storage)
	require.NotNil(t, kept.SharedSpaces)
	assert.Equal(t, uint64(100), kept.Storage.LimitBytes)
	assert.Equal(t, uint64(10), kept.Storage.UsageBytes)
	assert.Equal(t, uint32(3), kept.SharedSpaces.Limit)
	settle(acc)
	again, err := acc.Limits(ctx)
	require.NoError(t, err)
	assert.Equal(t, kept, again)

	// The answer survives a restart of the device, offline.
	restarted := newLimitsAccount(t, net, kv)
	stored, err := restarted.Limits(ctx)
	require.NoError(t, err)
	require.NotNil(t, stored.Storage)
	require.NotNil(t, stored.SharedSpaces)
	assert.Equal(t, kept.Storage.LimitBytes, stored.Storage.LimitBytes)
	assert.Equal(t, kept.Storage.UsageBytes, stored.Storage.UsageBytes)
	assert.Equal(t, kept.SharedSpaces.Limit, stored.SharedSpaces.Limit)
	assert.True(t, kept.Storage.FetchedAt.Equal(stored.Storage.FetchedAt))
	assert.True(t, kept.SharedSpaces.FetchedAt.Equal(stored.SharedSpaces.FetchedAt))

	// Another account on the same device and network has its own answers.
	other := &fakeLimits{account: "other", storageErr: errDown, sharedErr: errDown}
	none, err := newLimitsAccount(t, other, kv).Limits(ctx)
	require.NoError(t, err)
	assert.Nil(t, none.Storage)
	assert.Nil(t, none.SharedSpaces)
}

func TestAccountLimits_Parts(t *testing.T) {
	ctx := context.Background()
	net := &fakeLimits{limitBytes: 100, shared: 3}
	acc := newLimitsAccount(t, net, newMemKV())
	live, err := acc.Limits(ctx)
	require.NoError(t, err)
	settle(acc)

	// One part down: the other is still live.
	net.set(func(f *fakeLimits) { f.storageErr, f.shared = errDown, 5 })
	mixed, err := acc.Limits(ctx)
	require.NoError(t, err)
	assert.Equal(t, live.Storage, mixed.Storage)
	assert.Equal(t, uint32(5), mixed.SharedSpaces.Limit)
	settle(acc)

	// File nodes that hold no pool view give no answer: the last one stands.
	net.set(func(f *fakeLimits) { f.storageErr = errNoPoolView })
	retryNow(acc)
	_, err = acc.Limits(ctx)
	require.NoError(t, err)
	settle(acc)
	noView, err := acc.Limits(ctx)
	require.NoError(t, err)
	assert.Equal(t, live.Storage, noView.Storage)
	settle(acc)

	// A network without file nodes has no pool, whatever was stored, and
	// is not asked for one.
	net.set(func(f *fakeLimits) { f.noFileNodes = true })
	calls := net.storageCalls
	none, err := acc.Limits(ctx)
	require.NoError(t, err)
	assert.Nil(t, none.Storage)
	assert.NotNil(t, none.SharedSpaces)
	assert.Equal(t, calls, net.storageCalls)
}

// A network that never answers holds one call for the wait bound. Later
// calls return at once, and the answer that comes late is kept.
func TestAccountLimits_Hang(t *testing.T) {
	ctx := context.Background()
	release := make(chan struct{})
	net := &fakeLimits{limitBytes: 5, shared: 2, hang: release}
	acc := newLimitsAccount(t, net, newMemKV())
	const wait = 300 * time.Millisecond
	acc.storageLimits.wait, acc.sharedSpacesLimits.wait = wait, wait

	start := time.Now()
	got, err := acc.Limits(ctx)
	require.NoError(t, err)
	assert.Nil(t, got.Storage)
	assert.Nil(t, got.SharedSpaces)
	assert.GreaterOrEqual(t, time.Since(start), wait)
	assert.Less(t, time.Since(start), wait+2*time.Second)

	start = time.Now()
	_, err = acc.Limits(ctx)
	require.NoError(t, err)
	assert.Less(t, time.Since(start), wait)
	calls := net.calls()

	// The late answer lands. With the network down again, it is what the
	// next call returns: nothing else could have produced it.
	close(release)
	settle(acc)
	net.set(func(f *fakeLimits) { f.hang = nil; down(f) })
	kept, err := acc.Limits(ctx)
	require.NoError(t, err)
	require.NotNil(t, kept.Storage)
	require.NotNil(t, kept.SharedSpaces)
	assert.Equal(t, uint64(5), kept.Storage.LimitBytes)
	assert.Equal(t, uint32(2), kept.SharedSpaces.Limit)
	assert.Equal(t, calls, 2)
}

// A network that answers, but slower than the wait bound, is not waited
// for on every call: the last answer is returned at once while the next
// fetch runs. A fetch that answers in time brings the wait back.
func TestAccountLimits_Slow(t *testing.T) {
	ctx := context.Background()
	const wait = 200 * time.Millisecond
	release := make(chan struct{})
	net := &fakeLimits{limitBytes: 1, shared: 1, hang: release}
	acc := newLimitsAccount(t, net, newMemKV())
	acc.storageLimits.wait, acc.sharedSpacesLimits.wait = wait, wait

	// First fetch: succeeds, but only after the wait bound.
	_, err := acc.Limits(ctx)
	require.NoError(t, err)
	close(release)
	settle(acc)

	// The next fetch hangs too; the call does not wait for it.
	release = make(chan struct{})
	net.set(func(f *fakeLimits) { f.hang, f.limitBytes, f.shared = release, 2, 2 })
	start := time.Now()
	got, err := acc.Limits(ctx)
	require.NoError(t, err)
	assert.Less(t, time.Since(start), wait)
	require.NotNil(t, got.Storage)
	assert.Equal(t, uint64(1), got.Storage.LimitBytes)

	// It answers at once this time, well inside the bound.
	close(release)
	settle(acc)
	net.set(func(f *fakeLimits) { f.hang, f.limitBytes, f.shared = nil, 3, 3 })
	_, err = acc.Limits(ctx)
	require.NoError(t, err)
	settle(acc)

	// So the network is waited for again.
	release = make(chan struct{})
	defer close(release)
	net.set(func(f *fakeLimits) { f.hang = release })
	start = time.Now()
	got, err = acc.Limits(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start), wait)
	require.NotNil(t, got.Storage)
	assert.Equal(t, uint64(3), got.Storage.LimitBytes)
}

// The caller's deadline bounds the wait like the wait bound does, even
// when it has already passed; its cancellation is an error and asks
// nothing.
func TestAccountLimits_CallerContext(t *testing.T) {
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()

	// A first call on an expired deadline is no error either.
	fresh := newLimitsAccount(t, &fakeLimits{storageErr: errDown, sharedErr: errDown}, newMemKV())
	none, err := fresh.Limits(expired)
	require.NoError(t, err)
	assert.Nil(t, none.Storage)
	assert.Nil(t, none.SharedSpaces)

	net := &fakeLimits{limitBytes: 5, shared: 2}
	acc := newLimitsAccount(t, net, newMemKV())
	live, err := acc.Limits(context.Background())
	require.NoError(t, err)
	settle(acc)

	release := make(chan struct{})
	defer close(release)
	net.set(func(f *fakeLimits) { f.hang = release })

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	got, err := acc.Limits(ctx)
	require.NoError(t, err)
	assert.Equal(t, live, got)
	assert.Less(t, time.Since(start), 2*time.Second)

	got, err = acc.Limits(expired)
	require.NoError(t, err)
	assert.Equal(t, live, got)

	calls := net.calls()
	cancelled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	_, err = acc.Limits(cancelled)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, calls, net.calls())
}

// Calls made at once share one fetch.
func TestAccountLimits_SharedFetch(t *testing.T) {
	release := make(chan struct{})
	net := &fakeLimits{limitBytes: 5, shared: 2, hang: release}
	acc := newLimitsAccount(t, net, newMemKV())

	var wg sync.WaitGroup
	results := make([]space.AccountLimits, 8)
	for i := range results {
		wg.Go(func() {
			got, err := acc.Limits(context.Background())
			assert.NoError(t, err)
			results[i] = got
		})
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	for _, got := range results {
		require.NotNil(t, got.Storage)
		assert.Equal(t, uint64(5), got.Storage.LimitBytes)
	}
	assert.Equal(t, 2, net.calls())
}

// The device store failing: an unreadable store is an error, since the
// last answer cannot be told; an unwritable one still serves the answer.
func TestAccountLimits_StoreFailure(t *testing.T) {
	ctx := context.Background()
	errDisk := errors.New("disk")

	kv := newMemKV()
	kv.getErr = errDisk
	acc := newLimitsAccount(t, &fakeLimits{limitBytes: 5, shared: 2}, kv)
	_, err := acc.Limits(ctx)
	require.ErrorIs(t, err, errDisk)
	// The read is tried again on the next call.
	kv.mu.Lock()
	kv.getErr = nil
	kv.mu.Unlock()
	got, err := acc.Limits(ctx)
	require.NoError(t, err)
	require.NotNil(t, got.Storage)

	kv = newMemKV()
	kv.setErr = errDisk
	acc = newLimitsAccount(t, &fakeLimits{limitBytes: 5, shared: 2}, kv)
	got, err = acc.Limits(ctx)
	require.NoError(t, err)
	require.NotNil(t, got.Storage)
	assert.Equal(t, uint64(5), got.Storage.LimitBytes)
}

// A stored value this code did not write is no answer, whatever it
// decodes to.
func TestAccountLimits_StoredGarbage(t *testing.T) {
	net := &fakeLimits{storageErr: errDown, sharedErr: errDown}
	for _, raw := range []string{`null`, `{}`, `not json`, `{"limitBytesRenamed":5}`, `{"limitBytes":5}`, `[]`} {
		kv := newMemKV()
		kv.m[storageLimitsKey+net.scope()] = raw
		kv.m[sharedSpacesLimitsKey+net.scope()] = raw
		got, err := newLimitsAccount(t, net, kv).Limits(context.Background())
		require.NoError(t, err, raw)
		assert.Nil(t, got.Storage, raw)
		assert.Nil(t, got.SharedSpaces, raw)
	}
}

// No fetch starts once the account is closing.
func TestAccountLimits_Closed(t *testing.T) {
	net := &fakeLimits{limitBytes: 5, shared: 2}
	acc := newLimitsAccount(t, net, newMemKV())
	acc.close()
	got, err := acc.Limits(context.Background())
	require.NoError(t, err)
	assert.Nil(t, got.Storage)
	assert.Zero(t, net.calls())
}

// Closing while a call waits ends the fetch and the call.
func TestAccountLimits_CloseWhileWaiting(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	net := &fakeLimits{limitBytes: 5, shared: 2, hang: release}
	acc := newLimitsAccount(t, net, newMemKV())

	done := make(chan error, 1)
	go func() {
		_, err := acc.Limits(context.Background())
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	acc.close()
	assert.Less(t, time.Since(start), 2*time.Second)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("the waiting call did not return after close")
	}
}

func TestHasAccountPool(t *testing.T) {
	for _, tc := range []struct {
		name  string
		quota *fileprotov2.SpaceQuota
		want  bool
	}{
		{"granted pool", &fileprotov2.SpaceQuota{LimitBytes: 90, AccountLimitBytes: 100, AccountUsageBytes: 10}, true},
		{"empty pool", &fileprotov2.SpaceQuota{}, true},
		{"revoked pool with usage", &fileprotov2.SpaceQuota{LimitBytes: 10, AccountUsageBytes: 10}, true},
		{"node without a pool view", &fileprotov2.SpaceQuota{LimitBytes: 1 << 30}, false},
	} {
		assert.Equal(t, tc.want, hasAccountPool(tc.quota), tc.name)
	}
}
