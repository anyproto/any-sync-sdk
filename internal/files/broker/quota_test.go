package broker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonfile/fileproto/fileprotov2"
	"github.com/anyproto/any-sync/commonfile/fileproto/fileprotov2/fileprotov2err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHedgeQuota(t *testing.T) {
	good := &fileprotov2.SpaceQuota{AccountLimitBytes: 100}
	blind := &fileprotov2.SpaceQuota{LimitBytes: 1}
	usable := func(q *fileprotov2.SpaceQuota) bool { return q.GetAccountLimitBytes() > 0 }
	errDown := errors.New("unable to connect")
	const hedge = 200 * time.Millisecond

	// peer is one node's behaviour: a quota, an error, or nil to hang.
	type peer struct {
		id     string
		answer any
	}
	type result struct {
		quota  *fileprotov2.SpaceQuota
		peerId string
		err    error
		asked  []string
		took   time.Duration
	}
	run := func(peers ...peer) result {
		ids := make([]string, len(peers))
		byId := map[string]any{}
		for i, p := range peers {
			ids[i], byId[p.id] = p.id, p.answer
		}
		var (
			mu    sync.Mutex
			asked []string
		)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		start := time.Now()
		quota, peerId, err := hedgeQuota(ctx, ids, hedge, func(ctx context.Context, peerId string) (*fileprotov2.SpaceQuota, error) {
			mu.Lock()
			asked = append(asked, peerId)
			mu.Unlock()
			switch v := byId[peerId].(type) {
			case *fileprotov2.SpaceQuota:
				return v, nil
			case error:
				return nil, v
			default:
				<-ctx.Done()
				return nil, ctx.Err()
			}
		}, usable)
		took := time.Since(start)
		mu.Lock()
		defer mu.Unlock()
		return result{quota, peerId, err, append([]string(nil), asked...), took}
	}

	// A healthy first node is the only one asked.
	r := run(peer{"a", good}, peer{"b", good})
	require.NoError(t, r.err)
	assert.Same(t, good, r.quota)
	assert.Equal(t, "a", r.peerId)
	assert.Equal(t, []string{"a"}, r.asked)

	// A silent node passes the turn after the hedge, not before.
	r = run(peer{"a", nil}, peer{"b", good})
	require.NoError(t, r.err)
	assert.Same(t, good, r.quota)
	assert.Equal(t, "b", r.peerId)
	assert.GreaterOrEqual(t, r.took, hedge)
	assert.Less(t, r.took, hedge+time.Second)

	// A node that fails, or answers without the pool, passes it at once.
	r = run(peer{"a", errDown}, peer{"b", blind}, peer{"c", good})
	require.NoError(t, r.err)
	assert.Same(t, good, r.quota)
	assert.Equal(t, "c", r.peerId)
	assert.Less(t, r.took, hedge)

	// With no usable answer an unusable one is still an answer.
	r = run(peer{"a", errDown}, peer{"b", blind})
	require.NoError(t, r.err)
	assert.Same(t, blind, r.quota)
	assert.Equal(t, "b", r.peerId)

	// A real failure outranks a refusal of the space.
	r = run(peer{"a", fileprotov2err.ErrNotResponsible}, peer{"b", errDown})
	require.ErrorIs(t, r.err, errDown)
	r = run(peer{"a", errDown}, peer{"b", fileprotov2err.ErrNotResponsible})
	require.ErrorIs(t, r.err, errDown)

	// Only a fleet that refuses the space as one says so.
	r = run(peer{"a", fileprotov2err.ErrNotResponsible}, peer{"b", fileprotov2err.ErrNotResponsible})
	require.ErrorIs(t, r.err, fileprotov2err.ErrNotResponsible)
	assert.Equal(t, []string{"a", "b"}, r.asked)
}

// A caller that gives up ends the walk with its error.
func TestHedgeQuota_Cancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	_, _, err := hedgeQuota(ctx, []string{"a", "b"}, time.Hour, func(ctx context.Context, _ string) (*fileprotov2.SpaceQuota, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}, func(*fileprotov2.SpaceQuota) bool { return true })
	require.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(start), time.Second)
}

// The walk starts with the node that last gave a usable answer, and
// only such a node is remembered.
func TestQuotaFrom_RemembersUsablePeer(t *testing.T) {
	good := &fileprotov2.SpaceQuota{AccountLimitBytes: 100}
	blind := &fileprotov2.SpaceQuota{LimitBytes: 1}
	usable := func(q *fileprotov2.SpaceQuota) bool { return q.GetAccountLimitBytes() > 0 }
	errDown := errors.New("unable to connect")

	var (
		mu      sync.Mutex
		asked   []string
		answers = map[string]any{"a": errDown, "b": good, "c": good}
	)
	ask := func(_ context.Context, peerId string) (*fileprotov2.SpaceQuota, error) {
		mu.Lock()
		defer mu.Unlock()
		asked = append(asked, peerId)
		if err, ok := answers[peerId].(error); ok {
			return nil, err
		}
		return answers[peerId].(*fileprotov2.SpaceQuota), nil
	}
	c := &Client{peers: func() []string { return []string{"a", "b", "c"} }}
	run := func() (*fileprotov2.SpaceQuota, []string, error) {
		mu.Lock()
		asked = nil
		mu.Unlock()
		q, err := c.quotaFrom(context.Background(), usable, ask)
		mu.Lock()
		defer mu.Unlock()
		return q, append([]string(nil), asked...), err
	}

	// "a" is down, "b" answers: "b" is remembered and asked first, alone.
	q, order, err := run()
	require.NoError(t, err)
	assert.Same(t, good, q)
	assert.Equal(t, []string{"a", "b"}, order)
	_, order, err = run()
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, order)

	// "b" loses its pool view: the walk moves on and remembers "c".
	mu.Lock()
	answers["b"] = blind
	mu.Unlock()
	q, order, err = run()
	require.NoError(t, err)
	assert.Same(t, good, q)
	assert.Equal(t, []string{"b", "a", "c"}, order)
	_, order, _ = run()
	assert.Equal(t, []string{"c"}, order)

	// A node that answers without the pool is an answer, but is not
	// remembered: the next walk still starts with "c".
	mu.Lock()
	answers["c"] = errDown
	mu.Unlock()
	q, order, err = run()
	require.NoError(t, err)
	assert.Same(t, blind, q)
	assert.Equal(t, []string{"c", "a", "b"}, order)
	_, order, _ = run()
	assert.Equal(t, []string{"c", "a", "b"}, order)

	// No file nodes at all.
	_, err = (&Client{peers: func() []string { return nil }}).quotaFrom(context.Background(), usable, ask)
	require.ErrorIs(t, err, ErrNoFileNodes)
}

func TestPeerFirst(t *testing.T) {
	peers := []string{"a", "b", "c"}
	assert.Equal(t, []string{"a", "b", "c"}, peerFirst(peers, ""))
	assert.Equal(t, []string{"a", "b", "c"}, peerFirst(peers, "a"))
	assert.Equal(t, []string{"c", "a", "b"}, peerFirst(peers, "c"))
	assert.Equal(t, []string{"b", "a", "c"}, peerFirst(peers, "b"))
	// A node that left the fleet changes nothing.
	assert.Equal(t, []string{"a", "b", "c"}, peerFirst(peers, "gone"))
	assert.Equal(t, []string{"a", "b", "c"}, peers)
}

func TestSpaceQuotaOf(t *testing.T) {
	quota := &fileprotov2.SpaceQuota{LimitBytes: 1}
	resp := &fileprotov2.SpaceInfoResponse{Results: []*fileprotov2.SpaceInfoResult{
		{SpaceId: "other", Quota: &fileprotov2.SpaceQuota{LimitBytes: 2}},
		{SpaceId: "sp", Quota: quota},
		{SpaceId: "misrouted", Code: fileprotov2.ErrCode_ErrNotResponsible},
		{SpaceId: "empty"},
	}}
	got, code := spaceQuotaOf(resp, "sp")
	assert.Equal(t, fileprotov2.ErrCode_Ok, code)
	assert.Same(t, quota, got)

	_, code = spaceQuotaOf(resp, "misrouted")
	assert.Equal(t, fileprotov2.ErrCode_ErrNotResponsible, code)

	// An Ok result with no quota, and a space the node left out, are
	// both no answer.
	_, code = spaceQuotaOf(resp, "empty")
	assert.Equal(t, fileprotov2.ErrCode_ErrUnexpected, code)
	_, code = spaceQuotaOf(resp, "absent")
	assert.Equal(t, fileprotov2.ErrCode_ErrUnexpected, code)
	_, code = spaceQuotaOf(nil, "sp")
	assert.Equal(t, fileprotov2.ErrCode_ErrUnexpected, code)
}
