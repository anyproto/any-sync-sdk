package anysyncsdk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// fakeDeletion is a coordinator: it holds the account's status, moves
// it on request and revert like the real one, and can fail, refuse or
// hang.
type fakeDeletion struct {
	mu       sync.Mutex
	account  string
	st       netAccountState
	deleteAt time.Time
	period   time.Duration
	// stateErr, requestErr and revertErr make the matching call fail.
	stateErr, requestErr, revertErr error
	// refuse makes request and revert refused whatever the status.
	refuse bool
	// lostReply makes request fail after the status moved: the
	// coordinator took it and the answer never arrived.
	lostReply   bool
	hang        chan struct{} // non-nil: state blocks on it
	requestGate chan struct{} // non-nil: request blocks on it
	stateCalls  int
	revertCalls int
}

func newFakeDeletion(st netAccountState) *fakeDeletion {
	return &fakeDeletion{account: "acc", st: st, period: 30 * 24 * time.Hour}
}

func (f *fakeDeletion) set(fn func(f *fakeDeletion)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func (f *fakeDeletion) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stateCalls
}

func (f *fakeDeletion) scope() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return "net/" + f.account
}

func (f *fakeDeletion) state(ctx context.Context) (netAccountState, time.Time, error) {
	f.mu.Lock()
	f.stateCalls++
	hang := f.hang
	// The status is read before the hang: a hung read answers stale.
	st, deleteAt, err := f.st, f.deleteAt, f.stateErr
	f.mu.Unlock()
	if hang != nil {
		select {
		case <-hang:
		case <-ctx.Done():
			return 0, time.Time{}, ctx.Err()
		}
	}
	return st, deleteAt, err
}

func (f *fakeDeletion) request(ctx context.Context) (time.Time, bool, error) {
	f.mu.Lock()
	gate := f.requestGate
	f.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return time.Time{}, false, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.requestErr != nil {
		return time.Time{}, false, f.requestErr
	}
	if f.refuse || f.st != netAccountActive {
		return time.Time{}, true, nil
	}
	f.st, f.deleteAt = netAccountPending, time.Now().Add(f.period).UTC().Truncate(time.Second)
	if f.lostReply {
		return time.Time{}, false, errors.New("connection closed")
	}
	return f.deleteAt, false, nil
}

func (f *fakeDeletion) revert(context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revertCalls++
	if f.revertErr != nil {
		return false, f.revertErr
	}
	if f.refuse || f.st != netAccountPending {
		return true, nil
	}
	f.st, f.deleteAt = netAccountActive, time.Time{}
	return false, nil
}

// newDeletionAccount is an account with only the deletion state wired:
// one device on net, keeping the state in kv. The watch is not started.
func newDeletionAccount(t *testing.T, net deletionSource, kv limitsKV) *accountImpl {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	acc := &accountImpl{closeCtx: ctx, closeCancel: cancel}
	acc.initDeletion(net, kv)
	acc.deletion.poll, acc.deletion.retry = 10*time.Millisecond, 10*time.Millisecond
	t.Cleanup(acc.close)
	return acc
}

func deletionOf(t *testing.T, acc *accountImpl) *space.AccountDeletion {
	t.Helper()
	st, err := acc.Deletion(context.Background())
	require.NoError(t, err)
	return st
}

func TestAccountDeletion_NoAnswer(t *testing.T) {
	net := newFakeDeletion(netAccountActive)
	acc := newDeletionAccount(t, net, newMemKV())
	assert.Nil(t, deletionOf(t, acc), "the network never answered")
	assert.Zero(t, net.calls(), "Deletion does not ask the network")
}

func TestAccountDeletion_RequestAndRevert(t *testing.T) {
	ctx := context.Background()
	net, kv := newFakeDeletion(netAccountActive), newMemKV()
	acc := newDeletionAccount(t, net, kv)

	st, err := acc.RequestDeletion(ctx)
	require.NoError(t, err)
	assert.Equal(t, space.AccountDeletionPending, st.State)
	assert.WithinDuration(t, time.Now().Add(net.period), st.DeleteAt, time.Minute)
	assert.False(t, st.FetchedAt.IsZero())
	kept := deletionOf(t, acc)
	require.NotNil(t, kept)
	assert.Equal(t, st, *kept)

	// Another process of the device reads the stored state with the
	// network gone.
	net.set(func(f *fakeDeletion) { f.stateErr = errors.New("offline") })
	again := newDeletionAccount(t, net, kv)
	kept = deletionOf(t, again)
	require.NotNil(t, kept)
	assert.Equal(t, space.AccountDeletionPending, kept.State)
	assert.True(t, st.DeleteAt.Equal(kept.DeleteAt))

	st, err = acc.RevertDeletion(ctx)
	require.NoError(t, err)
	assert.Equal(t, space.AccountActive, st.State)
	assert.True(t, st.DeleteAt.IsZero())
	kept = deletionOf(t, acc)
	require.NotNil(t, kept)
	assert.Equal(t, space.AccountActive, kept.State)
}

func TestAccountDeletion_Refused(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		st      netAccountState
		refuse  bool
		revert  bool
		wantErr error
		want    space.AccountDeletionState
	}{
		{"request while pending", netAccountPending, false, false, space.ErrAccountDeletionPending, space.AccountDeletionPending},
		{"request once deleted", netAccountDeleted, false, false, space.ErrAccountDeleted, space.AccountDeleted},
		{"request for an unregistered account", netAccountUnknown, false, false, space.ErrAccountNotOnNetwork, space.AccountActive},
		{"request refused while active", netAccountActive, true, false, space.ErrNetworkUnavailable, space.AccountActive},
		{"revert while active", netAccountActive, false, true, space.ErrAccountDeletionNotPending, space.AccountActive},
		{"revert once deleted", netAccountDeleted, false, true, space.ErrAccountDeleted, space.AccountDeleted},
		{"revert for an unregistered account", netAccountUnknown, false, true, space.ErrAccountNotOnNetwork, space.AccountActive},
		{"revert refused while pending", netAccountPending, true, true, space.ErrNetworkUnavailable, space.AccountDeletionPending},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			net := newFakeDeletion(c.st)
			net.refuse = c.refuse
			acc := newDeletionAccount(t, net, newMemKV())
			var err error
			if c.revert {
				_, err = acc.RevertDeletion(ctx)
			} else {
				_, err = acc.RequestDeletion(ctx)
			}
			require.ErrorIs(t, err, c.wantErr)
			// The status read to name the refusal is kept.
			kept := deletionOf(t, acc)
			require.NotNil(t, kept)
			assert.Equal(t, c.want, kept.State)
		})
	}
}

func TestAccountDeletion_Unavailable(t *testing.T) {
	down := errors.New("unable to connect")
	net := newFakeDeletion(netAccountActive)
	net.requestErr, net.revertErr = down, down
	acc := newDeletionAccount(t, net, newMemKV())

	_, err := acc.RequestDeletion(context.Background())
	require.ErrorIs(t, err, space.ErrNetworkUnavailable)
	require.ErrorIs(t, err, down)
	_, err = acc.RevertDeletion(context.Background())
	require.ErrorIs(t, err, space.ErrNetworkUnavailable)
	assert.Nil(t, deletionOf(t, acc), "a failed call keeps nothing")

	// The refusal cannot be named while the status read fails.
	net.set(func(f *fakeDeletion) { f.requestErr, f.refuse, f.stateErr = nil, true, down })
	_, err = acc.RequestDeletion(context.Background())
	require.ErrorIs(t, err, space.ErrNetworkUnavailable)

	// The caller's cancellation is its own error.
	net.set(func(f *fakeDeletion) { f.requestErr = context.Canceled })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = acc.RequestDeletion(ctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.NotErrorIs(t, err, space.ErrNetworkUnavailable)
}

func TestAccountDeletion_Watch(t *testing.T) {
	net := newFakeDeletion(netAccountActive)
	net.stateErr = errors.New("offline")
	acc := newDeletionAccount(t, net, newMemKV())

	var mu sync.Mutex
	var got []space.AccountDeletion
	cancel := acc.SubscribeDeletion(func(st space.AccountDeletion) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, st)
	})
	states := func() []space.AccountDeletionState {
		mu.Lock()
		defer mu.Unlock()
		out := make([]space.AccountDeletionState, len(got))
		for i, st := range got {
			out[i] = st.State
		}
		return out
	}

	acc.startDeletionWatch()
	require.Eventually(t, func() bool { return net.calls() >= 2 }, time.Second, time.Millisecond, "retries while the network fails")
	assert.Nil(t, deletionOf(t, acc))
	assert.Empty(t, states())

	// The network answers: the first answer is a change.
	net.set(func(f *fakeDeletion) { f.stateErr = nil })
	require.Eventually(t, func() bool { return len(states()) == 1 }, time.Second, time.Millisecond)
	assert.Equal(t, []space.AccountDeletionState{space.AccountActive}, states())

	// Another device of the account requests the deletion, then
	// reverts it.
	deleteAt := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	net.set(func(f *fakeDeletion) { f.st, f.deleteAt = netAccountPending, deleteAt })
	require.Eventually(t, func() bool { return len(states()) == 2 }, time.Second, time.Millisecond)
	kept := deletionOf(t, acc)
	require.NotNil(t, kept)
	assert.Equal(t, space.AccountDeletionPending, kept.State)
	assert.True(t, deleteAt.Equal(kept.DeleteAt))

	net.set(func(f *fakeDeletion) { f.st, f.deleteAt = netAccountActive, time.Time{} })
	require.Eventually(t, func() bool { return len(states()) == 3 }, time.Second, time.Millisecond)
	assert.Equal(t, []space.AccountDeletionState{space.AccountActive, space.AccountDeletionPending, space.AccountActive}, states())

	// An unchanged answer is no event, and a cancelled subscriber gets
	// none.
	n := net.calls()
	require.Eventually(t, func() bool { return net.calls() >= n+3 }, time.Second, time.Millisecond)
	assert.Len(t, states(), 3)
	cancel()
	net.set(func(f *fakeDeletion) { f.st = netAccountDeleted })
	require.Eventually(t, func() bool {
		st := deletionOf(t, acc)
		return st != nil && st.State == space.AccountDeleted
	}, time.Second, time.Millisecond)
	assert.Len(t, states(), 3)
}

// A read that started before RequestDeletion answered is dropped: its
// stale "active" must not replace "pending".
func TestAccountDeletion_StaleRead(t *testing.T) {
	net := newFakeDeletion(netAccountActive)
	hang := make(chan struct{})
	net.hang = hang
	acc := newDeletionAccount(t, net, newMemKV())
	acc.deletion.poll = time.Hour

	acc.startDeletionWatch()
	require.Eventually(t, func() bool { return net.calls() == 1 }, time.Second, time.Millisecond)
	_, err := acc.RequestDeletion(context.Background())
	require.NoError(t, err)

	net.set(func(f *fakeDeletion) { f.hang = nil })
	close(hang)
	// The dropped read is followed by a fresh one at once.
	require.Eventually(t, func() bool { return net.calls() >= 2 }, time.Second, time.Millisecond)
	kept := deletionOf(t, acc)
	require.NotNil(t, kept)
	assert.Equal(t, space.AccountDeletionPending, kept.State)
}

func TestAccountDeletion_StoredGarbage(t *testing.T) {
	net, kv := newFakeDeletion(netAccountActive), newMemKV()
	for _, raw := range []string{"not json", `{"state":"pending"}`, `{"state":"gone","fetchedAt":"2026-01-02T03:04:05Z"}`} {
		kv.m[deletionKey+net.scope()] = raw
		acc := newDeletionAccount(t, net, kv)
		assert.Nil(t, deletionOf(t, acc), raw)
	}
	kv.getErr = errors.New("device read failed")
	acc := newDeletionAccount(t, net, kv)
	_, err := acc.Deletion(context.Background())
	require.Error(t, err)
	// An answer replaces what could not be read.
	kv.setErr = errors.New("device write failed")
	st, err := acc.RequestDeletion(context.Background())
	require.NoError(t, err, "a failed store does not fail the call")
	kept := deletionOf(t, acc)
	require.NotNil(t, kept)
	assert.Equal(t, st, *kept)
}

func TestAccountDeletion_Closed(t *testing.T) {
	net := newFakeDeletion(netAccountPending)
	acc := newDeletionAccount(t, net, newMemKV())
	acc.close()
	acc.startDeletionWatch()
	cancel := acc.SubscribeDeletion(func(space.AccountDeletion) { t.Error("a closed account delivers nothing") })
	defer cancel()
	// A change after the close is kept and reaches no subscriber.
	_, err := acc.RevertDeletion(context.Background())
	require.NoError(t, err)
	time.Sleep(30 * time.Millisecond)
	assert.Zero(t, net.calls(), "a closed account starts no watch")
	kept := deletionOf(t, acc)
	require.NotNil(t, kept)
	assert.Equal(t, space.AccountActive, kept.State)
}

// A change that lands between a read of the state and the first
// subscription is delivered: reading first loses nothing.
func TestAccountDeletion_ReadThenSubscribe(t *testing.T) {
	net := newFakeDeletion(netAccountPending)
	hang := make(chan struct{})
	net.hang = hang
	acc := newDeletionAccount(t, net, newMemKV())
	acc.deletion.poll = time.Hour

	acc.startDeletionWatch()
	require.Eventually(t, func() bool { return net.calls() == 1 }, time.Second, time.Millisecond)
	assert.Nil(t, deletionOf(t, acc), "the read is still in flight")
	net.set(func(f *fakeDeletion) { f.hang = nil })
	close(hang)
	require.Eventually(t, func() bool { return deletionOf(t, acc) != nil }, time.Second, time.Millisecond)

	got := make(chan space.AccountDeletion, 1)
	defer acc.SubscribeDeletion(func(st space.AccountDeletion) { got <- st })()
	select {
	case st := <-got:
		assert.Equal(t, space.AccountDeletionPending, st.State)
	case <-time.After(time.Second):
		t.Fatal("the first answer never reached the subscriber")
	}
}

// A callback may close the account: close does not wait for the
// goroutine that runs it.
func TestAccountDeletion_CloseFromCallback(t *testing.T) {
	net := newFakeDeletion(netAccountActive)
	acc := newDeletionAccount(t, net, newMemKV())
	closed := make(chan struct{})
	acc.SubscribeDeletion(func(space.AccountDeletion) {
		acc.close()
		close(closed)
	})
	_, err := acc.RequestDeletion(context.Background())
	require.NoError(t, err)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("close from a callback never returned")
	}
}

// A request the coordinator took, whose answer never arrived, fails as
// an unavailable network and is read back at once.
func TestAccountDeletion_LostReply(t *testing.T) {
	net := newFakeDeletion(netAccountActive)
	net.lostReply = true
	acc := newDeletionAccount(t, net, newMemKV())
	acc.deletion.poll = time.Hour
	acc.startDeletionWatch()
	require.Eventually(t, func() bool {
		st := deletionOf(t, acc)
		return st != nil && st.State == space.AccountActive
	}, time.Second, time.Millisecond)

	_, err := acc.RequestDeletion(context.Background())
	require.ErrorIs(t, err, space.ErrNetworkUnavailable)
	require.Eventually(t, func() bool {
		st := deletionOf(t, acc)
		return st != nil && st.State == space.AccountDeletionPending
	}, time.Second, time.Millisecond, "the state the network holds is read without waiting for the poll")
}

// A request and a revert never overlap, so their answers are kept in
// the order the coordinator gave them.
func TestAccountDeletion_CallsInOrder(t *testing.T) {
	net := newFakeDeletion(netAccountActive)
	gate := make(chan struct{})
	net.requestGate = gate
	acc := newDeletionAccount(t, net, newMemKV())

	requested, reverted := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := acc.RequestDeletion(context.Background())
		requested <- err
	}()
	require.Eventually(t, func() bool { return len(acc.deletion.call) == 1 }, time.Second, time.Millisecond)
	go func() {
		_, err := acc.RevertDeletion(context.Background())
		reverted <- err
	}()
	time.Sleep(30 * time.Millisecond)
	net.mu.Lock()
	revertCalls := net.revertCalls
	net.mu.Unlock()
	assert.Zero(t, revertCalls, "the revert ran while the request was in flight")

	close(gate)
	require.NoError(t, <-requested)
	require.NoError(t, <-reverted)
	kept := deletionOf(t, acc)
	require.NotNil(t, kept)
	assert.Equal(t, space.AccountActive, kept.State)

	// A call waiting for its turn honours its caller's cancellation.
	acc.deletion.call <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := acc.RevertDeletion(ctx)
	require.ErrorIs(t, err, context.Canceled)
	<-acc.deletion.call
}

// A status this code does not know keeps the state it has and is not
// asked for again before the next poll.
func TestAccountDeletion_UnknownStatus(t *testing.T) {
	net := newFakeDeletion(netAccountPending)
	acc := newDeletionAccount(t, net, newMemKV())
	acc.deletion.poll = time.Hour
	_, err := acc.RequestDeletion(context.Background())
	require.ErrorIs(t, err, space.ErrAccountDeletionPending)

	net.set(func(f *fakeDeletion) { f.stateErr = fmt.Errorf("%w 99", errUnknownAccountStatus) })
	acc.startDeletionWatch()
	require.Eventually(t, func() bool { return net.calls() >= 2 }, time.Second, time.Millisecond)
	n := net.calls()
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, n, net.calls(), "an unknown status is not a failed read")
	kept := deletionOf(t, acc)
	require.NotNil(t, kept)
	assert.Equal(t, space.AccountDeletionPending, kept.State)
}

func TestAccountStateOf(t *testing.T) {
	deleteAt := time.Date(2026, 12, 27, 10, 0, 0, 0, time.UTC)
	cases := []struct {
		status coordinatorproto.SpaceStatus
		want   netAccountState
		date   time.Time
	}{
		{coordinatorproto.SpaceStatus_SpaceStatusCreated, netAccountActive, time.Time{}},
		{coordinatorproto.SpaceStatus_SpaceStatusPendingDeletion, netAccountPending, deleteAt},
		{coordinatorproto.SpaceStatus_SpaceStatusDeletionStarted, netAccountDeleted, time.Time{}},
		{coordinatorproto.SpaceStatus_SpaceStatusDeleted, netAccountDeleted, time.Time{}},
		{coordinatorproto.SpaceStatus_SpaceStatusNotExists, netAccountUnknown, time.Time{}},
	}
	for _, c := range cases {
		// The coordinator sends the date with every status but Created.
		st, date, err := accountStateOf(&coordinatorproto.SpaceStatusPayload{Status: c.status, DeletionTimestamp: deleteAt.Unix()})
		require.NoError(t, err, c.status)
		assert.Equal(t, c.want, st, c.status)
		assert.True(t, c.date.Equal(date), "%v: date %s", c.status, date)
	}
	_, _, err := accountStateOf(&coordinatorproto.SpaceStatusPayload{Status: coordinatorproto.SpaceStatus(99)})
	require.ErrorIs(t, err, errUnknownAccountStatus)
}

// A call that waits for its turn still has its full time for the
// coordinator once it runs.
func TestAccountDeletion_QueuedCallKeepsItsTime(t *testing.T) {
	net := newFakeDeletion(netAccountActive)
	gate := make(chan struct{})
	net.requestGate = gate
	acc := newDeletionAccount(t, net, newMemKV())

	requested := make(chan error, 1)
	go func() {
		_, err := acc.RequestDeletion(context.Background())
		requested <- err
	}()
	require.Eventually(t, func() bool { return len(acc.deletion.call) == 1 }, time.Second, time.Millisecond)

	// The caller's deadline bounds the wait for the turn, as an
	// unavailable network.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := acc.RevertDeletion(ctx)
	require.ErrorIs(t, err, space.ErrNetworkUnavailable)
	net.mu.Lock()
	revertCalls := net.revertCalls
	net.mu.Unlock()
	assert.Zero(t, revertCalls, "a call that never got its turn does not reach the coordinator")

	close(gate)
	require.NoError(t, <-requested)
}
