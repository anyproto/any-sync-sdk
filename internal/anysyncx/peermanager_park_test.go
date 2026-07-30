package anysyncx

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/net/streampool"
	"github.com/cheggaaa/mb/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"storj.io/drpc"
)

// fakeSendPool overflows the first failN Sends, then accepts; fixed
// err (when set) wins over both.
type fakeSendPool struct {
	mu    sync.Mutex
	failN int
	err   error
	sent  []drpc.Message
}

func (f *fakeSendPool) Send(_ context.Context, msg drpc.Message, _ streampool.PeerGetter) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	if f.failN > 0 {
		f.failN--
		return mb.ErrOverflowed
	}
	f.sent = append(f.sent, msg)
	return nil
}

func (f *fakeSendPool) Streams(...string) []drpc.Stream { return nil }

func (f *fakeSendPool) sentCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent)
}

// parkMsg carries an id and a fake size for bound tests.
type parkMsg struct {
	drpc.Message
	id   string
	size uint64
}

func (p parkMsg) MsgSize() uint64 { return p.size }

func msg(id string) parkMsg { return parkMsg{id: id} }

// disableRetryWorker makes park() bookkeeping observable without the
// retry worker concurrently popping the buffer: it consumes parkStart
// and pre-closes parkDone so Close stays correct.
func disableRetryWorker(m *spacePeerManager) {
	m.parkStart.Do(func() {})
	close(m.parkDone)
}

func TestBroadcastPark_OverflowParksAndReportsSuccess(t *testing.T) {
	m := newTestManager(nil, nil, nil)
	defer func() { require.NoError(t, m.Close(context.Background())) }()
	disableRetryWorker(m)
	m.streamPool = &fakeSendPool{err: mb.ErrOverflowed}

	require.NoError(t, m.BroadcastMessage(context.Background(), msg("m0")),
		"a parked broadcast reports success")

	m.parkMu.Lock()
	defer m.parkMu.Unlock()
	require.Len(t, m.parked, 1)
	assert.Equal(t, "m0", m.parked[0].(parkMsg).id)
}

func TestBroadcastPark_RetriesUntilDelivered(t *testing.T) {
	m := newTestManager(nil, nil, nil)
	defer func() { require.NoError(t, m.Close(context.Background())) }()
	pool := &fakeSendPool{}
	m.streamPool = pool

	// Park directly: overflow-then-succeed through BroadcastMessage
	// would race the worker's own Send attempts for the failN budget.
	for i := 0; i < 3; i++ {
		m.park(msg(fmt.Sprintf("m%d", i)))
	}
	require.Eventually(t, func() bool { return pool.sentCount() == 3 },
		2*time.Second, 10*time.Millisecond, "parked broadcasts retried and delivered")

	pool.mu.Lock()
	defer pool.mu.Unlock()
	for i, sent := range pool.sent {
		assert.Equal(t, fmt.Sprintf("m%d", i), sent.(parkMsg).id, "FIFO order preserved")
	}
}

func TestBroadcastPark_ClosedPoolNotParked(t *testing.T) {
	m := newTestManager(nil, nil, nil)
	defer func() { require.NoError(t, m.Close(context.Background())) }()
	m.streamPool = &fakeSendPool{err: mb.ErrClosed}

	require.ErrorIs(t, m.BroadcastMessage(context.Background(), msg("m0")), mb.ErrClosed)

	m.parkMu.Lock()
	defer m.parkMu.Unlock()
	assert.Empty(t, m.parked, "terminal errors are not parked")
	assert.False(t, m.parkStarted, "no retry worker for terminal errors")
}

func TestBroadcastPark_CountBoundDropsOldest(t *testing.T) {
	m := newTestManager(nil, nil, nil)
	defer func() { require.NoError(t, m.Close(context.Background())) }()
	disableRetryWorker(m)
	m.streamPool = &fakeSendPool{err: mb.ErrOverflowed}

	for i := 0; i < parkedMaxCount+10; i++ {
		require.NoError(t, m.BroadcastMessage(context.Background(), msg(fmt.Sprintf("m%03d", i))))
	}

	m.parkMu.Lock()
	defer m.parkMu.Unlock()
	assert.LessOrEqual(t, len(m.parked), parkedMaxCount)
	assert.Equal(t, "m010", m.parked[0].(parkMsg).id, "oldest dropped first")
}

func TestBroadcastPark_ByteBoundDropsOldest(t *testing.T) {
	m := newTestManager(nil, nil, nil)
	defer func() { require.NoError(t, m.Close(context.Background())) }()
	disableRetryWorker(m)
	m.streamPool = &fakeSendPool{err: mb.ErrOverflowed}

	big := func(id string) parkMsg { return parkMsg{id: id, size: 3 << 20} }
	for _, id := range []string{"m0", "m1", "m2"} {
		require.NoError(t, m.BroadcastMessage(context.Background(), big(id)))
	}

	m.parkMu.Lock()
	defer m.parkMu.Unlock()
	require.Len(t, m.parked, 2, "9MiB exceeds the 8MiB bound — oldest evicted")
	assert.Equal(t, "m1", m.parked[0].(parkMsg).id)
	assert.LessOrEqual(t, m.parkedBytes, parkedMaxBytes)
}

func TestBroadcastPark_CloseStopsWorkerPromptly(t *testing.T) {
	m := newTestManager(nil, nil, nil)
	m.streamPool = &fakeSendPool{failN: 1 << 30} // never succeeds

	require.NoError(t, m.BroadcastMessage(context.Background(), msg("m0")))

	done := make(chan struct{})
	go func() {
		_ = m.Close(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not stop the retry worker")
	}
}

func TestNextRetryDelay(t *testing.T) {
	assert.Equal(t, 100*time.Millisecond, nextRetryDelay(0))
	assert.Equal(t, 500*time.Millisecond, nextRetryDelay(1))
	assert.Equal(t, 2*time.Second, nextRetryDelay(2))
	assert.Equal(t, retrySteady, nextRetryDelay(3))
	assert.Equal(t, retrySteady, nextRetryDelay(100))
}
