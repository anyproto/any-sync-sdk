package anysyncsdk

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

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
	refuse     bool
	hang       chan struct{} // non-nil: state blocks on it
	stateCalls int
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

func (f *fakeDeletion) request(context.Context) (time.Time, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.requestErr != nil {
		return time.Time{}, false, f.requestErr
	}
	if f.refuse || f.st != netAccountActive {
		return time.Time{}, true, nil
	}
	f.st, f.deleteAt = netAccountPending, time.Now().Add(f.period).UTC().Truncate(time.Second)
	return f.deleteAt, false, nil
}

func (f *fakeDeletion) revert(context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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
	acc.startDeletionWatch() // one watch only
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
	time.Sleep(30 * time.Millisecond)
	assert.Zero(t, net.calls(), "a closed account starts no watch")
}
