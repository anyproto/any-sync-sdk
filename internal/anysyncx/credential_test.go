package anysyncx

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// signCoordinator answers SpaceSign with err and counts the requests.
type signCoordinator struct {
	coordinatorclient.CoordinatorClient
	err   error
	calls int
}

func (c *signCoordinator) SpaceSign(context.Context, coordinatorclient.SpaceSignPayload) (*coordinatorproto.SpaceReceiptWithSignature, error) {
	c.calls++
	if c.err != nil {
		return nil, c.err
	}
	return &coordinatorproto.SpaceReceiptWithSignature{}, nil
}

type signAnswer struct {
	spaceId string
	refusal space.Refusal
}

// signFixture is a credential provider over a fake coordinator and a
// clock the test moves.
type signFixture struct {
	*credentialProvider
	coord   *signCoordinator
	clock   time.Time
	answers []signAnswer
	// rows is the index: the refusal each space's row holds. Each
	// answer is written to it, as the space service does. nil: the
	// spaces have no row.
	rows map[string]space.Refusal
	// deleted holds the spaces whose row reads deleted.
	deleted map[string]bool
}

func newSignFixture() *signFixture {
	f := &signFixture{coord: &signCoordinator{}, clock: time.Unix(1_700_000_000, 0)}
	f.credentialProvider = newCredentialProvider(newLocalOnlySpaces())
	f.coordClient = f.coord
	f.now = func() time.Time { return f.clock }
	onAnswer := func(_ context.Context, spaceId string, refusal space.Refusal) {
		f.answers = append(f.answers, signAnswer{spaceId, refusal})
		if f.rows != nil {
			f.rows[spaceId] = refusal
		}
	}
	f.onChildSign.Store(&onAnswer)
	recorded := func(spaceId string) (space.Refusal, bool, bool) {
		refusal, ok := f.rows[spaceId]
		return refusal, f.deleted[spaceId], ok
	}
	f.recorded.Store(&recorded)
	return f
}

// restart is the provider of the next process over the same index.
func (f *signFixture) restart() {
	f.refused = make(map[string]*signRefusal)
}

func testSpaceHeader(t *testing.T, parentSpaceId string) *spacesyncproto.RawSpaceHeaderWithId {
	t.Helper()
	keys, err := accountdata.NewRandom()
	require.NoError(t, err)
	metadataKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	payload, err := spacepayloads.StoragePayloadForSpaceCreateV1(spacepayloads.SpaceCreatePayload{
		SigningKey:    keys.SignKey,
		MasterKey:     keys.SignKey,
		ReadKey:       crypto.NewAES(),
		MetadataKey:   metadataKey,
		SpaceType:     space.SpaceTypeAny,
		ParentSpaceId: parentSpaceId,
	})
	require.NoError(t, err)
	return payload.SpaceHeaderWithId
}

// Each refusal of the coordinator is reported with the check it names.
func TestCredentialProvider_ChildRefusalNamesTheCheck(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		err  error
		want space.Refusal
	}{
		{coordinatorproto.ErrParentSpaceNotAvailable, space.RefusalParentNotAvailable},
		{coordinatorproto.ErrParentSpaceInvalid, space.RefusalParentInvalid},
		{coordinatorproto.ErrParentPermissionDenied, space.RefusalParentPermissionDenied},
		{coordinatorproto.ErrChildSpacesLimitReached, space.RefusalChildLimitReached},
	}
	for _, tc := range cases {
		t.Run(tc.want.String(), func(t *testing.T) {
			f := newSignFixture()
			child := testSpaceHeader(t, "parent.1")
			f.coord.err = tc.err
			_, err := f.GetCredential(ctx, child)
			require.ErrorIs(t, err, tc.err)
			assert.Equal(t, []signAnswer{{child.Id, tc.want}}, f.answers)
		})
	}
}

// While a child is refused, its pushes do not reach the coordinator:
// the wait starts at signRetryMin and doubles up to signRetryMax.
func TestCredentialProvider_RefusedChildWaits(t *testing.T) {
	ctx := context.Background()
	f := newSignFixture()
	child := testSpaceHeader(t, "parent.1")
	f.coord.err = coordinatorproto.ErrChildSpacesLimitReached

	// Head sync asks on every round and for every node.
	push := func() {
		for range 100 {
			_, err := f.GetCredential(ctx, child)
			require.ErrorIs(t, err, coordinatorproto.ErrChildSpacesLimitReached)
		}
	}
	push()
	assert.Equal(t, 1, f.coord.calls)

	wait := signRetryMin
	for calls := 2; calls <= 12; calls++ {
		f.clock = f.clock.Add(wait - time.Second)
		push()
		assert.Equal(t, calls-1, f.coord.calls, "the wait of %v is not over", wait)
		f.clock = f.clock.Add(time.Second)
		push()
		assert.Equal(t, calls, f.coord.calls, "one request after a wait of %v", wait)
		wait = min(wait*2, signRetryMax)
	}
	assert.Equal(t, signRetryMax, wait)
	assert.Len(t, f.answers, f.coord.calls, "only an answer of the coordinator is reported")
}

// A parent that cannot be a parent stays one: the coordinator is asked
// again only on request.
func TestCredentialProvider_InvalidParentIsNotRetried(t *testing.T) {
	ctx := context.Background()
	f := newSignFixture()
	child := testSpaceHeader(t, "parent.1")
	f.coord.err = coordinatorproto.ErrParentSpaceInvalid

	_, err := f.GetCredential(ctx, child)
	require.ErrorIs(t, err, coordinatorproto.ErrParentSpaceInvalid)
	f.clock = f.clock.Add(1000 * signRetryMax)
	_, err = f.GetCredential(ctx, child)
	require.ErrorIs(t, err, coordinatorproto.ErrParentSpaceInvalid)
	assert.Equal(t, 1, f.coord.calls)

	_, err = f.sign(ctx, child, true)
	require.ErrorIs(t, err, coordinatorproto.ErrParentSpaceInvalid)
	assert.Equal(t, 2, f.coord.calls)
}

// A request made now skips the wait, and a signed child is asked about
// freely again.
func TestCredentialProvider_SignNowClearsTheRefusal(t *testing.T) {
	ctx := context.Background()
	f := newSignFixture()
	child := testSpaceHeader(t, "parent.1")
	f.coord.err = coordinatorproto.ErrParentPermissionDenied
	_, err := f.GetCredential(ctx, child)
	require.Error(t, err)

	f.coord.err = nil
	_, err = f.GetCredential(ctx, child)
	require.ErrorIs(t, err, coordinatorproto.ErrParentPermissionDenied, "the push still waits")
	_, err = f.sign(ctx, child, true)
	require.NoError(t, err)
	assert.Equal(t, signAnswer{child.Id, space.RefusalNone}, f.answers[len(f.answers)-1])

	for range 3 {
		_, err = f.GetCredential(ctx, child)
		require.NoError(t, err)
	}
	assert.Equal(t, 5, f.coord.calls)

	// A refusal after that starts from the shortest wait.
	f.coord.err = coordinatorproto.ErrParentPermissionDenied
	_, err = f.GetCredential(ctx, child)
	require.Error(t, err)
	f.clock = f.clock.Add(signRetryMin)
	_, err = f.GetCredential(ctx, child)
	require.Error(t, err)
	assert.Equal(t, 7, f.coord.calls)
}

// An error that names no check is no answer: nothing is recorded and
// the next push asks again. That holds for a coordinator out of reach
// and for a child the coordinator reports as deleted.
func TestCredentialProvider_UnreachableCoordinatorIsNoRefusal(t *testing.T) {
	ctx := context.Background()
	f := newSignFixture()
	child := testSpaceHeader(t, "parent.1")
	for _, noAnswer := range []error{
		errors.New("unable to connect"),
		coordinatorproto.ErrSpaceIsDeleted,
		coordinatorproto.ErrSpaceDeletionPending,
	} {
		f.coord.err = noAnswer
		_, err := f.GetCredential(ctx, child)
		require.ErrorIs(t, err, noAnswer)
	}
	assert.Equal(t, 3, f.coord.calls)
	assert.Empty(t, f.answers)

	// It does not shorten or end a wait either.
	f.coord.err = coordinatorproto.ErrChildSpacesLimitReached
	_, err := f.GetCredential(ctx, child)
	require.Error(t, err)
	f.coord.err = errors.New("unable to connect")
	_, err = f.sign(ctx, child, true)
	require.Error(t, err)
	_, err = f.GetCredential(ctx, child)
	require.ErrorIs(t, err, coordinatorproto.ErrChildSpacesLimitReached)
	assert.Equal(t, 5, f.coord.calls)
}

// A top-level space is signed as before: no wait, nothing reported.
func TestCredentialProvider_TopLevelSpaceIsNotGated(t *testing.T) {
	ctx := context.Background()
	f := newSignFixture()
	top := testSpaceHeader(t, "")
	f.coord.err = coordinatorproto.ErrSpaceLimitReached
	for range 3 {
		_, err := f.GetCredential(ctx, top)
		require.ErrorIs(t, err, coordinatorproto.ErrSpaceLimitReached)
	}
	f.coord.err = nil
	_, err := f.GetCredential(ctx, top)
	require.NoError(t, err)
	assert.Equal(t, 4, f.coord.calls)
	assert.Empty(t, f.answers)
}

// The index holds the refusal for the account's devices and across
// restarts; the waits are the process's own. A restart asks once, and
// then waits again.
func TestCredentialProvider_RestartAsksOnce(t *testing.T) {
	ctx := context.Background()
	f := newSignFixture()
	f.rows = map[string]space.Refusal{}
	child := testSpaceHeader(t, "parent.1")
	f.rows[child.Id] = space.RefusalNone
	f.coord.err = coordinatorproto.ErrChildSpacesLimitReached
	_, err := f.GetCredential(ctx, child)
	require.Error(t, err)
	require.Equal(t, space.RefusalChildLimitReached, f.rows[child.Id])

	f.restart()
	for range 10 {
		_, err = f.GetCredential(ctx, child)
		require.ErrorIs(t, err, coordinatorproto.ErrChildSpacesLimitReached)
	}
	assert.Equal(t, 2, f.coord.calls)
}

// A child refused for a parent that cannot be one is not asked about
// again by itself, in this process or after a restart.
func TestCredentialProvider_InvalidParentIsNotRetriedAfterRestart(t *testing.T) {
	ctx := context.Background()
	f := newSignFixture()
	f.rows = map[string]space.Refusal{}
	child := testSpaceHeader(t, "parent.1")
	f.rows[child.Id] = space.RefusalParentInvalid

	f.clock = f.clock.Add(1000 * signRetryMax)
	_, err := f.GetCredential(ctx, child)
	require.ErrorIs(t, err, coordinatorproto.ErrParentSpaceInvalid)
	assert.Zero(t, f.coord.calls)

	f.coord.err = coordinatorproto.ErrParentSpaceInvalid
	_, err = f.sign(ctx, child, true)
	require.ErrorIs(t, err, coordinatorproto.ErrParentSpaceInvalid)
	assert.Equal(t, 1, f.coord.calls, "asked on request")
}

// A refusal the index no longer holds is over: another device was
// signed, or the coordinator reports the space registered. The wait
// goes with it.
func TestCredentialProvider_ClearedRefusalDropsTheWait(t *testing.T) {
	ctx := context.Background()
	f := newSignFixture()
	f.rows = map[string]space.Refusal{}
	child := testSpaceHeader(t, "parent.1")
	f.rows[child.Id] = space.RefusalNone
	f.coord.err = coordinatorproto.ErrParentPermissionDenied
	_, err := f.GetCredential(ctx, child)
	require.Error(t, err)
	_, err = f.GetCredential(ctx, child)
	require.Error(t, err)
	require.Equal(t, 1, f.coord.calls, "waiting")

	f.rows[child.Id] = space.RefusalNone
	f.coord.err = nil
	_, err = f.GetCredential(ctx, child)
	require.NoError(t, err)
	assert.Equal(t, 2, f.coord.calls)
	assert.Empty(t, f.refused)
}

// A child whose row reads deleted is not asked for, on a push or on
// request: another device of the account deleted it while this one
// still holds it. Signed, it would be pushed and listed again.
func TestCredentialProvider_DeletedChildIsNotAskedFor(t *testing.T) {
	ctx := context.Background()
	f := newSignFixture()
	f.rows = map[string]space.Refusal{}
	refused := testSpaceHeader(t, "parent.1")
	f.coord.err = coordinatorproto.ErrParentPermissionDenied
	_, err := f.GetCredential(ctx, refused)
	require.ErrorIs(t, err, coordinatorproto.ErrParentPermissionDenied)
	require.Equal(t, 1, f.coord.calls)

	// The row is deleted, the cause of the refusal goes and the wait
	// ends: the coordinator would sign.
	f.deleted = map[string]bool{refused.Id: true}
	f.coord.err = nil
	f.clock = f.clock.Add(signRetryMax)
	for _, now := range []bool{false, true} {
		_, err = f.sign(ctx, refused, now)
		require.ErrorIs(t, err, space.ErrSpaceDeleted)
	}

	// The same for a child that was never asked for, after a restart
	// too.
	fresh := testSpaceHeader(t, "parent.1")
	f.rows[fresh.Id] = space.RefusalNone
	f.deleted[fresh.Id] = true
	f.restart()
	for _, child := range []*spacesyncproto.RawSpaceHeaderWithId{refused, fresh} {
		_, err = f.GetCredential(ctx, child)
		require.ErrorIs(t, err, space.ErrSpaceDeleted)
	}
	assert.Equal(t, 1, f.coord.calls, "the coordinator is not asked")
	assert.Len(t, f.answers, 1, "and nothing is recorded")
}

// A child of a parent the account owns and this device stores is not
// asked for before the network holds the parent: the coordinator would
// answer that the parent is not available. It is asked as soon as a
// tree node answered a round of the parent.
func TestCredentialProvider_ChildWaitsForItsOwnParent(t *testing.T) {
	ctx := context.Background()
	f := newSignFixture()
	const parent = "parent.1"
	ownStored := func(spaceId string) bool { return spaceId == parent }
	f.ownStored.Store(&ownStored)
	var kicked [][]string
	f.kick = func(spaceIds []string) { kicked = append(kicked, spaceIds) }
	child := testSpaceHeader(t, parent)

	for _, now := range []bool{false, true} {
		_, err := f.sign(ctx, child, now)
		require.ErrorIs(t, err, coordinatorproto.ErrParentSpaceNotAvailable)
	}
	assert.Zero(t, f.coord.calls, "the coordinator is not asked")
	assert.Empty(t, f.answers, "and nothing is recorded as a refusal")
	assert.Empty(t, kicked)

	f.networkHolds(parent)
	require.Equal(t, [][]string{{child.Id}}, kicked, "the child's push starts at once")
	_, err := f.GetCredential(ctx, child)
	require.NoError(t, err)
	assert.Equal(t, 1, f.coord.calls)

	f.networkHolds(parent)
	assert.Len(t, kicked, 1, "a later round of the parent kicks nothing")
}

// A parent the account does not own is on the network already: its
// child is asked for at once, and "parent not available" is a refusal.
func TestCredentialProvider_ForeignParentIsNotWaitedFor(t *testing.T) {
	ctx := context.Background()
	f := newSignFixture()
	ownStored := func(string) bool { return false }
	f.ownStored.Store(&ownStored)
	child := testSpaceHeader(t, "parent.1")
	f.coord.err = coordinatorproto.ErrParentSpaceNotAvailable

	_, err := f.GetCredential(ctx, child)
	require.ErrorIs(t, err, coordinatorproto.ErrParentSpaceNotAvailable)
	assert.Equal(t, 1, f.coord.calls)
	assert.Equal(t, []signAnswer{{child.Id, space.RefusalParentNotAvailable}}, f.answers)
}

// Only a round with a tree node of the space says the network holds
// it and is in reach: a LAN peer or another node does not.
func TestApp_NodeAnsweredMarksTheSpace(t *testing.T) {
	a := &App{cred: newCredentialProvider(newLocalOnlySpaces()), nodeConf: &fakeNodeConf{nodeIds: []string{"node-1", "node-2"}}}
	var rounds []string
	a.OnNodeRound(func(spaceId string) { rounds = append(rounds, spaceId) })
	a.nodeAnswered("space.1", "lan-peer")
	assert.False(t, a.cred.holds("space.1"))
	assert.Empty(t, rounds)
	a.nodeAnswered("space.1", "node-2")
	a.nodeAnswered("space.1", "node-1")
	assert.True(t, a.cred.holds("space.1"))
	assert.False(t, a.cred.holds("space.2"))
	assert.Equal(t, []string{"space.1", "space.1"}, rounds, "every round with a tree node is reported")
}
