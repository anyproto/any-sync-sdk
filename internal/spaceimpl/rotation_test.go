package spaceimpl

import (
	"context"
	"errors"
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/list/listtest"
	"github.com/anyproto/any-sync/commonspace/object/acl/recordverifier"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errStaleHead = errors.New("record is not built on the head")

// aclLog is the ACL of one space as the network holds it: a linear log
// that takes a record only when it is built on the head.
type aclLog struct {
	t       *testing.T
	records []*consensusproto.RawRecordWithId
}

func (l *aclLog) head() string { return l.records[len(l.records)-1].Id }

func (l *aclLog) accept(rec *consensusproto.RawRecord) (*consensusproto.RawRecordWithId, error) {
	var payload consensusproto.Record
	require.NoError(l.t, payload.UnmarshalVT(rec.Payload))
	if payload.PrevId != l.head() {
		return nil, errStaleHead
	}
	accepted := listtest.WrapAclRecord(rec)
	l.records = append(l.records, accepted)
	return accepted, nil
}

// device builds the ACL as a device of keys holds it: a reader of the
// log the network accepted.
func (l *aclLog) device(keys *accountdata.AccountKeys) list.AclList {
	st, err := list.NewInMemoryStorage(l.records[0].Id, l.records)
	require.NoError(l.t, err)
	acl, err := list.BuildAclListWithIdentity(keys, st, recordverifier.NewValidateAcceptedLog())
	require.NoError(l.t, err)
	return acl
}

// sync applies the records the device has not seen.
func (l *aclLog) sync(acl list.AclList) {
	acl.Lock()
	defer acl.Unlock()
	require.NoError(l.t, acl.AddRawRecords(l.records))
}

// publish posts a record of the device and applies it there, as the
// space's ACL client does. published counts the accepted ones.
func (l *aclLog) publish(acl list.AclList, published *int) func(context.Context, *consensusproto.RawRecord) error {
	return func(_ context.Context, rec *consensusproto.RawRecord) error {
		accepted, err := l.accept(rec)
		if err != nil {
			return err
		}
		*published++
		acl.Lock()
		defer acl.Unlock()
		return acl.AddRawRecord(accepted)
	}
}

// rotationFixture is a child space with an Owner, an Admin, a Writer
// and one more member, whom an Admin of the parent space removes
// without the read key.
type rotationFixture struct {
	log                           *aclLog
	owner, admin, writer, removed *accountdata.AccountKeys
}

func newRotationFixture(t *testing.T) *rotationFixture {
	t.Helper()
	newKeys := func() *accountdata.AccountKeys {
		keys, err := accountdata.NewRandom()
		require.NoError(t, err)
		return keys
	}
	f := &rotationFixture{owner: newKeys(), admin: newKeys(), writer: newKeys(), removed: newKeys()}

	metadataKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	masterKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	builder := list.NewAclRecordBuilder("", crypto.NewKeyStorage(), f.owner, recordverifier.NewValidateFull())
	root, err := builder.BuildRoot(list.RootContent{
		PrivKey:   f.owner.SignKey,
		MasterKey: masterKey,
		SpaceId:   "child",
		Change:    list.ReadKeyChangePayload{MetadataKey: metadataKey, ReadKey: crypto.NewAES()},
	})
	require.NoError(t, err)
	f.log = &aclLog{t: t, records: []*consensusproto.RawRecordWithId{root}}

	ownerAcl := f.log.device(f.owner)
	add, err := ownerAcl.RecordBuilder().BuildAccountsAdd(list.AccountsAddPayload{Additions: []list.AccountAdd{
		{Identity: f.admin.SignKey.GetPublic(), Permissions: list.AclPermissionsAdmin},
		{Identity: f.writer.SignKey.GetPublic(), Permissions: list.AclPermissionsWriter},
		{Identity: f.removed.SignKey.GetPublic(), Permissions: list.AclPermissionsWriter},
	}})
	require.NoError(t, err)
	_, err = f.log.accept(add)
	require.NoError(t, err)
	return f
}

// removeWithoutKey is the removal an Admin of the parent space writes:
// an account that is not a member of the child.
func (f *rotationFixture) removeWithoutKey(t *testing.T) {
	t.Helper()
	parentAdmin, err := accountdata.NewRandom()
	require.NoError(t, err)
	rec, err := f.log.device(parentAdmin).RecordBuilder().BuildAccountRemoveNoRotate(
		[]crypto.PubKey{f.removed.SignKey.GetPublic()})
	require.NoError(t, err)
	_, err = f.log.accept(rec)
	require.NoError(t, err)
}

func rotationPending(acl list.AclList) bool {
	acl.RLock()
	defer acl.RUnlock()
	return acl.AclState().RotationPending()
}

// A device of the Owner or of an Admin that holds the read key ends a
// pending rotation with one read key change.
func TestRotateIfPending_OwnerAndAdminRotate(t *testing.T) {
	ctx := context.Background()
	for _, role := range []string{"owner", "admin"} {
		t.Run(role, func(t *testing.T) {
			f := newRotationFixture(t)
			keys := map[string]*accountdata.AccountKeys{"owner": f.owner, "admin": f.admin}[role]
			acl := f.log.device(keys)
			published := 0
			publish := f.log.publish(acl, &published)

			// Nothing pending: nothing written.
			require.NoError(t, rotateIfPending(ctx, acl, publish))
			assert.Zero(t, published)

			f.removeWithoutKey(t)
			f.log.sync(acl)
			require.True(t, rotationPending(acl))
			keyBefore := acl.AclState().CurrentReadKeyId()

			require.NoError(t, rotateIfPending(ctx, acl, publish))
			assert.Equal(t, 1, published)
			assert.False(t, rotationPending(acl))
			assert.NotEqual(t, keyBefore, acl.AclState().CurrentReadKeyId())

			// A later pass finds it done.
			require.NoError(t, rotateIfPending(ctx, acl, publish))
			assert.Equal(t, 1, published)

			// The new key is not for the removed member.
			newKey, _ := f.log.device(f.removed).AclState().CurrentReadKey()
			assert.Nil(t, newKey)
			memberKey, err := f.log.device(f.writer).AclState().CurrentReadKey()
			require.NoError(t, err)
			assert.NotNil(t, memberKey)
		})
	}
}

// A Writer holds the read key and may not change it: its devices write
// nothing and wait for an Admin or the Owner.
func TestRotateIfPending_WriterDoesNotRotate(t *testing.T) {
	ctx := context.Background()
	f := newRotationFixture(t)
	f.removeWithoutKey(t)
	acl := f.log.device(f.writer)
	require.True(t, rotationPending(acl))

	published := 0
	require.NoError(t, rotateIfPending(ctx, acl, f.log.publish(acl, &published)))
	assert.Zero(t, published)
	assert.True(t, rotationPending(acl))
}

// Two devices that see the pending rotation at once both build the
// change on the same head. The network takes one; the other fails to
// publish, and its next pass finds the rotation done.
func TestRotateIfPending_TwoDevicesRotateOnce(t *testing.T) {
	ctx := context.Background()
	f := newRotationFixture(t)
	f.removeWithoutKey(t)
	first, second := f.log.device(f.owner), f.log.device(f.admin)
	published := 0

	// The second device builds before the first one's record reaches
	// it: its publish runs when the first has been accepted.
	var raced bool
	err := rotateIfPending(ctx, second, func(ctx context.Context, rec *consensusproto.RawRecord) error {
		require.NoError(t, rotateIfPending(ctx, first, f.log.publish(first, &published)))
		raced = true
		return f.log.publish(second, &published)(ctx, rec)
	})
	require.True(t, raced)
	require.ErrorIs(t, err, errStaleHead, "the loser's record is refused")
	assert.Equal(t, 1, published)
	assert.True(t, rotationPending(second), "until the winner's record arrives")

	f.log.sync(second)
	require.NoError(t, rotateIfPending(ctx, second, f.log.publish(second, &published)))
	assert.Equal(t, 1, published, "one rotation for one removal")
	assert.False(t, rotationPending(first))
	assert.False(t, rotationPending(second))
	assert.Equal(t, first.AclState().CurrentReadKeyId(), second.AclState().CurrentReadKeyId())
}

// A publish that fails leaves the rotation pending for the next pass.
func TestRotateIfPending_FailedPublishIsRetried(t *testing.T) {
	ctx := context.Background()
	f := newRotationFixture(t)
	f.removeWithoutKey(t)
	acl := f.log.device(f.owner)

	offline := errors.New("unable to connect")
	require.ErrorIs(t, rotateIfPending(ctx, acl, func(context.Context, *consensusproto.RawRecord) error { return offline }), offline)
	assert.True(t, rotationPending(acl))

	published := 0
	require.NoError(t, rotateIfPending(ctx, acl, f.log.publish(acl, &published)))
	assert.Equal(t, 1, published)
	assert.False(t, rotationPending(acl))
}

// Past its retry budget the watcher goes on retrying only for a
// rotation it could not publish.
func TestAclMirrorWatcher_RotationRetriesPastTheBudget(t *testing.T) {
	w := &aclMirrorWatcher{kickCh: make(chan struct{}, 1), stopCh: make(chan struct{})}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	defer w.stop()
	// armed reports whether a retry was scheduled, and takes it back.
	armed := func() bool {
		w.retryMu.Lock()
		defer w.retryMu.Unlock()
		timer := w.retryTimer
		if timer != nil {
			timer.Stop()
			w.retryTimer = nil
		}
		return timer != nil
	}

	for range aclMirrorRetryMax {
		w.scheduleRetry(false)
		require.True(t, armed())
	}
	w.scheduleRetry(false)
	assert.False(t, armed(), "the budget is spent")
	w.scheduleRetry(true)
	assert.True(t, armed(), "a rotation is still owed")

	// An applied ACL record gives the budget back.
	w.UpdateAcl(nil)
	w.scheduleRetry(false)
	assert.True(t, armed())
}

// A rotation that found no network is tried again when a tree node
// answers a head-sync round of the space, once.
func TestService_NodeRoundKicksARotationThatAwaitsTheNetwork(t *testing.T) {
	w := &aclMirrorWatcher{id: "space.1", kickCh: make(chan struct{}, 1), stopCh: make(chan struct{})}
	s := &Service{aclMirrorWatchers: map[string]*aclMirrorWatcher{w.id: w}}
	kicked := func() bool {
		select {
		case <-w.kickCh:
			return true
		default:
			return false
		}
	}

	s.nodeRound(w.id)
	assert.False(t, kicked(), "nothing awaits the network")

	w.awaitsNetwork.Store(true)
	s.nodeRound("space.2")
	assert.False(t, kicked(), "a round of another space")
	s.nodeRound(w.id)
	assert.True(t, kicked())
	s.nodeRound(w.id)
	assert.False(t, kicked(), "one kick for one failure")
}
