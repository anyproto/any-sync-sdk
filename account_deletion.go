package anysyncsdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/space"
)

const (
	// deletionPoll is how often the deletion state is read from the
	// network.
	deletionPoll = 300 * time.Second
	// deletionRetry is the pause before the next read while the network
	// does not answer.
	deletionRetry = 30 * time.Second
	// deletionCallTimeout bounds one coordinator call.
	deletionCallTimeout = 30 * time.Second
)

// The stored state is keyed per network and account.
const deletionKey = "accountDeletion/"

// netAccountState is the coordinator's status of the account's tech
// space.
type netAccountState uint8

const (
	// netAccountUnknown: the coordinator has never registered the
	// account.
	netAccountUnknown netAccountState = iota
	netAccountActive
	netAccountPending
	netAccountDeleted
)

// errUnknownAccountStatus: the coordinator answered a status this code
// does not know. The kept state stands.
var errUnknownAccountStatus = errors.New("anysyncsdk: unknown tech space status")

// deletionSource is the network side of the account's deletion.
type deletionSource interface {
	// scope names the network and the account the state belongs to.
	scope() string
	// state reads the account's status; deleteAt is set while pending.
	state(ctx context.Context) (st netAccountState, deleteAt time.Time, err error)
	// request and revert report refused when the coordinator answered
	// that the account is in no state for the call; state tells why.
	request(ctx context.Context) (deleteAt time.Time, refused bool, err error)
	revert(ctx context.Context) (refused bool, err error)
}

// networkDeletion talks to the coordinator. The account's status is the
// status of its tech space.
type networkDeletion struct {
	app     *anysyncx.App
	spaceId func() string
	account func() string
}

func (n networkDeletion) scope() string { return n.app.NetworkId() + "/" + n.account() }

func (n networkDeletion) state(ctx context.Context) (netAccountState, time.Time, error) {
	spaceId := n.spaceId()
	if spaceId == "" {
		return 0, time.Time{}, errors.New("anysyncsdk: tech space not open")
	}
	st, err := n.app.SpaceStatus(ctx, spaceId)
	if errors.Is(err, coordinatorproto.ErrSpaceNotExists) {
		return netAccountUnknown, time.Time{}, nil
	}
	if err != nil {
		return 0, time.Time{}, err
	}
	return accountStateOf(st)
}

// accountStateOf maps the tech space's coordinator status to the
// account's; deleteAt is set while pending.
func accountStateOf(st *coordinatorproto.SpaceStatusPayload) (netAccountState, time.Time, error) {
	switch st.GetStatus() {
	case coordinatorproto.SpaceStatus_SpaceStatusCreated:
		return netAccountActive, time.Time{}, nil
	case coordinatorproto.SpaceStatus_SpaceStatusPendingDeletion:
		return netAccountPending, time.Unix(st.GetDeletionTimestamp(), 0).UTC(), nil
	case coordinatorproto.SpaceStatus_SpaceStatusDeletionStarted,
		coordinatorproto.SpaceStatus_SpaceStatusDeleted:
		return netAccountDeleted, time.Time{}, nil
	case coordinatorproto.SpaceStatus_SpaceStatusNotExists:
		return netAccountUnknown, time.Time{}, nil
	default:
		return 0, time.Time{}, fmt.Errorf("%w %d", errUnknownAccountStatus, st.GetStatus())
	}
}

// The coordinator answers ErrAccountIsDeleted to a request for an
// account that is pending, deleted or unregistered.
func (n networkDeletion) request(ctx context.Context) (time.Time, bool, error) {
	deleteAt, err := n.app.AccountDelete(ctx)
	if errors.Is(err, coordinatorproto.ErrAccountIsDeleted) {
		return time.Time{}, true, nil
	}
	return deleteAt, false, err
}

// The coordinator answers ErrUnexpected to a revert for an account that
// is not pending.
func (n networkDeletion) revert(ctx context.Context) (bool, error) {
	err := n.app.AccountRevertDeletion(ctx)
	if errors.Is(err, coordinatorproto.ErrUnexpected) {
		return true, nil
	}
	return false, err
}

// storedDeletion is the state as kept on the device. Its field names
// are its own, so the stored form does not follow the public type.
type storedDeletion struct {
	State     string     `json:"state"`
	DeleteAt  *time.Time `json:"deleteAt,omitempty"`
	FetchedAt time.Time  `json:"fetchedAt"`
}

// deletionState keeps the account's deletion state: the last answer of
// the network, in memory and on the device.
type deletionState struct {
	src         deletionSource
	kv          limitsKV
	poll, retry time.Duration

	// call holds one RequestDeletion or RevertDeletion at a time, so
	// their answers are kept in the order the coordinator gave them.
	call chan struct{}
	// setMu orders the writers: an answer is kept and stored whole
	// before the next one.
	setMu sync.Mutex

	mu     sync.Mutex
	loaded bool // cur was read from the device
	cur    *space.AccountDeletion
	// gen counts the answers RequestDeletion and RevertDeletion gave: a
	// read that started before one of them is dropped.
	gen         uint64
	subs        map[uint64]func(space.AccountDeletion)
	nextSub     uint64
	watching    bool
	dispatching bool
	closed      bool

	kick    chan struct{} // wakes the watch for a read now
	changed chan struct{} // wakes the dispatcher
}

func (a *accountImpl) initDeletion(src deletionSource, kv limitsKV) {
	d := &a.deletion
	d.src, d.kv = src, kv
	d.poll, d.retry = deletionPoll, deletionRetry
	d.subs = map[uint64]func(space.AccountDeletion){}
	d.call = make(chan struct{}, 1)
	d.kick = make(chan struct{}, 1)
	d.changed = make(chan struct{}, 1)
}

// stop keeps new goroutines from starting. Called by close before it
// joins the watch.
func (d *deletionState) stop() {
	d.mu.Lock()
	d.closed = true
	d.mu.Unlock()
}

// readNow wakes the watch for a read now.
func (d *deletionState) readNow() {
	select {
	case d.kick <- struct{}{}:
	default:
	}
}

// loadLocked reads the stored state once. Caller holds d.mu.
func (d *deletionState) loadLocked(ctx context.Context) error {
	if d.loaded {
		return nil
	}
	raw, ok, err := d.kv.GetKV(ctx, deletionKey+d.src.scope())
	if err != nil {
		return err
	}
	d.loaded = true
	if !ok {
		return nil
	}
	// A value this code did not store is as good as none.
	var s storedDeletion
	if json.Unmarshal([]byte(raw), &s) != nil || s.FetchedAt.IsZero() {
		return nil
	}
	cur := space.AccountDeletion{FetchedAt: s.FetchedAt}
	switch s.State {
	case space.AccountActive.String():
		cur.State = space.AccountActive
	case space.AccountDeletionPending.String():
		cur.State = space.AccountDeletionPending
	case space.AccountDeleted.String():
		cur.State = space.AccountDeleted
	default:
		return nil
	}
	if s.DeleteAt != nil {
		cur.DeleteAt = *s.DeleteAt
	}
	d.cur = &cur
	return nil
}

// Deletion returns the kept state. See AccountAPI.Deletion.
func (a *accountImpl) Deletion(ctx context.Context) (*space.AccountDeletion, error) {
	d := &a.deletion
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.loadLocked(context.WithoutCancel(ctx)); err != nil {
		return nil, err
	}
	if d.cur == nil {
		return nil, nil
	}
	out := *d.cur
	return &out, nil
}

// keepDeletion keeps st as the network's answer and wakes the
// subscribers when the state or its date changed. gen is the generation
// a background read started at; an answer RequestDeletion or
// RevertDeletion gave (own) outranks it. Returns false for a dropped
// read.
func (a *accountImpl) keepDeletion(st space.AccountDeletion, gen uint64, own bool) bool {
	d := &a.deletion
	d.setMu.Lock()
	defer d.setMu.Unlock()

	d.mu.Lock()
	if !own && gen != d.gen {
		d.mu.Unlock()
		return false
	}
	if own {
		d.gen++
	}
	// The answer replaces the stored state, read or not.
	_ = d.loadLocked(context.Background())
	d.loaded = true
	changed := d.cur == nil || d.cur.State != st.State || !d.cur.DeleteAt.Equal(st.DeleteAt)
	d.cur = &st
	d.mu.Unlock()

	stored := storedDeletion{State: st.State.String(), FetchedAt: st.FetchedAt}
	if !st.DeleteAt.IsZero() {
		stored.DeleteAt = &st.DeleteAt
	}
	if raw, err := json.Marshal(stored); err != nil {
		log.Warn("account deletion: encode failed", zap.Error(err))
	} else if err := d.kv.SetKV(context.Background(), deletionKey+d.src.scope(), string(raw)); err != nil {
		log.Warn("account deletion: store failed", zap.Error(err))
	}
	if changed {
		select {
		case d.changed <- struct{}{}:
		default:
		}
	}
	return true
}

// publicDeletion is a network status as the API reports it. An account
// the coordinator has never registered has nothing scheduled: active.
func publicDeletion(st netAccountState, deleteAt time.Time) space.AccountDeletion {
	out := space.AccountDeletion{State: space.AccountActive, FetchedAt: time.Now().UTC()}
	switch st {
	case netAccountPending:
		out.State, out.DeleteAt = space.AccountDeletionPending, deleteAt
	case netAccountDeleted:
		out.State = space.AccountDeleted
	}
	return out
}

// startDeletionWatch starts reading the deletion state from the
// network: now, then every poll, sooner while the network does not
// answer.
func (a *accountImpl) startDeletionWatch() {
	d := &a.deletion
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.watching || d.closed {
		return
	}
	d.watching = true
	a.wg.Add(1)
	go a.watchDeletion()
}

func (a *accountImpl) watchDeletion() {
	defer a.wg.Done()
	d := &a.deletion
	for {
		wait := d.poll
		if !a.readDeletion() {
			wait = d.retry
		}
		t := time.NewTimer(wait)
		select {
		case <-a.closeCtx.Done():
			t.Stop()
			return
		case <-d.kick:
			t.Stop()
		case <-t.C:
		}
	}
}

// readDeletion asks the network once and keeps the answer. False when
// the network did not answer.
func (a *accountImpl) readDeletion() bool {
	d := &a.deletion
	d.mu.Lock()
	gen := d.gen
	d.mu.Unlock()
	ctx, cancel := context.WithTimeout(a.closeCtx, deletionCallTimeout)
	defer cancel()
	st, deleteAt, err := d.src.state(ctx)
	if errors.Is(err, errUnknownAccountStatus) {
		// The network answered: asking sooner changes nothing.
		log.Warn("account deletion: read", zap.Error(err))
		return true
	}
	if err != nil {
		log.Debug("account deletion: read failed", zap.Error(err))
		return false
	}
	if !a.keepDeletion(publicDeletion(st, deleteAt), gen, false) {
		// A request or a revert answered meanwhile: read again.
		d.readNow()
	}
	return true
}

// enterDeletionCall waits for the turn of a request or a revert.
func (a *accountImpl) enterDeletionCall(ctx context.Context) error {
	select {
	case a.deletion.call <- struct{}{}:
		return nil
	case <-ctx.Done():
		return deletionCallError(ctx, ctx.Err())
	}
}

// RequestDeletion asks the network to delete the account. See
// AccountAPI.RequestDeletion.
func (a *accountImpl) RequestDeletion(ctx context.Context) (space.AccountDeletion, error) {
	d := &a.deletion
	callCtx, cancel := context.WithTimeout(ctx, deletionCallTimeout)
	defer cancel()
	if err := a.enterDeletionCall(callCtx); err != nil {
		return space.AccountDeletion{}, err
	}
	defer func() { <-d.call }()
	deleteAt, refused, err := d.src.request(callCtx)
	if err != nil {
		// The coordinator may have taken the request: read the state.
		d.readNow()
		return space.AccountDeletion{}, deletionCallError(ctx, err)
	}
	if refused {
		return space.AccountDeletion{}, a.deletionRefused(ctx, callCtx, netAccountPending, space.ErrAccountDeletionPending)
	}
	st := space.AccountDeletion{State: space.AccountDeletionPending, DeleteAt: deleteAt, FetchedAt: time.Now().UTC()}
	a.keepDeletion(st, 0, true)
	return st, nil
}

// RevertDeletion cancels a pending deletion. See
// AccountAPI.RevertDeletion.
func (a *accountImpl) RevertDeletion(ctx context.Context) (space.AccountDeletion, error) {
	d := &a.deletion
	callCtx, cancel := context.WithTimeout(ctx, deletionCallTimeout)
	defer cancel()
	if err := a.enterDeletionCall(callCtx); err != nil {
		return space.AccountDeletion{}, err
	}
	defer func() { <-d.call }()
	refused, err := d.src.revert(callCtx)
	if err != nil {
		// The coordinator may have taken the revert: read the state.
		d.readNow()
		return space.AccountDeletion{}, deletionCallError(ctx, err)
	}
	if refused {
		return space.AccountDeletion{}, a.deletionRefused(ctx, callCtx, netAccountActive, space.ErrAccountDeletionNotPending)
	}
	st := space.AccountDeletion{State: space.AccountActive, FetchedAt: time.Now().UTC()}
	a.keepDeletion(st, 0, true)
	return st, nil
}

// deletionRefused names why the coordinator refused a request or a
// revert: it reads the account's status, keeps it, and returns
// refusedErr when the status is refusedIn.
func (a *accountImpl) deletionRefused(ctx, callCtx context.Context, refusedIn netAccountState, refusedErr error) error {
	st, deleteAt, err := a.deletion.src.state(callCtx)
	if err != nil {
		a.deletion.readNow()
		return deletionCallError(ctx, err)
	}
	a.keepDeletion(publicDeletion(st, deleteAt), 0, true)
	switch st {
	case refusedIn:
		return refusedErr
	case netAccountDeleted:
		return space.ErrAccountDeleted
	case netAccountUnknown:
		return space.ErrAccountNotOnNetwork
	default:
		// The status allows the call: the coordinator failed it.
		return fmt.Errorf("%w: the coordinator refused the call", space.ErrNetworkUnavailable)
	}
}

// deletionCallError is a failed coordinator call as the caller sees it:
// its own cancellation, or an unavailable network.
func deletionCallError(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return ctx.Err()
	}
	return fmt.Errorf("%w: %w", space.ErrNetworkUnavailable, err)
}

// SubscribeDeletion registers cb. See AccountAPI.SubscribeDeletion.
func (a *accountImpl) SubscribeDeletion(cb func(space.AccountDeletion)) (cancel func()) {
	d := &a.deletion
	d.mu.Lock()
	defer d.mu.Unlock()
	id := d.nextSub
	d.nextSub++
	d.subs[id] = cb
	if !d.dispatching && !d.closed {
		d.dispatching = true
		go a.dispatchDeletion()
	}
	return func() {
		d.mu.Lock()
		delete(d.subs, id)
		d.mu.Unlock()
	}
}

// dispatchDeletion delivers the kept state to the subscribers after
// each change, from one goroutine: deliveries are ordered and the last
// one carries the latest state. It ends with the account, and close
// does not join it: a callback may be the one that closes the SDK.
func (a *accountImpl) dispatchDeletion() {
	d := &a.deletion
	for {
		select {
		case <-a.closeCtx.Done():
			return
		case <-d.changed:
		}
		d.mu.Lock()
		if d.cur == nil {
			d.mu.Unlock()
			continue
		}
		cur := *d.cur
		ids := make([]uint64, 0, len(d.subs))
		for id := range d.subs {
			ids = append(ids, id)
		}
		d.mu.Unlock()
		for _, id := range ids {
			// Read again: a subscriber cancelled meanwhile gets nothing.
			d.mu.Lock()
			cb := d.subs[id]
			d.mu.Unlock()
			if cb != nil && a.closeCtx.Err() == nil {
				cb(cur)
			}
		}
	}
}
