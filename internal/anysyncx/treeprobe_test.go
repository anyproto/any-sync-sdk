package anysyncx

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/objecttreebuilder"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/stretchr/testify/require"
)

// rawRootOf builds a raw root change of the given changeType.
func rawRootOf(t *testing.T, id, changeType string) *treechangeproto.RawTreeChangeWithId {
	t.Helper()
	payload, err := (&treechangeproto.RootChange{ChangeType: changeType, SpaceId: "space1"}).MarshalVT()
	require.NoError(t, err)
	raw, err := (&treechangeproto.RawTreeChange{Payload: payload}).MarshalVT()
	require.NoError(t, err)
	return &treechangeproto.RawTreeChangeWithId{RawChange: raw, Id: id}
}

// scriptedProber answers a probe with a root of the id's changeType
// ("object" when unlisted), the way a peer does: through the request's
// validator. full makes it answer like a peer that ignores the probe
// flag, with change bodies. fail makes a probe fail before any answer.
type scriptedProber struct {
	t     *testing.T
	types map[string]string
	fail  map[string]error
	full  bool
	// onProbe runs inside every probe, outside the lock.
	onProbe func(id string)

	mu    sync.Mutex
	calls []string
}

func (p *scriptedProber) BuildTree(_ context.Context, id string, opts objecttreebuilder.BuildTreeOpts) (objecttree.ObjectTree, error) {
	p.mu.Lock()
	p.calls = append(p.calls, id)
	p.mu.Unlock()
	if p.onProbe != nil {
		p.onProbe(id)
	}
	if !opts.Probe || opts.TreeValidator == nil {
		return nil, errors.New("not a probe")
	}
	if err, ok := p.fail[id]; ok {
		return nil, err
	}
	changeType, ok := p.types[id]
	if !ok {
		changeType = "object"
	}
	root := rawRootOf(p.t, id, changeType)
	payload := treestorage.TreeStorageCreatePayload{RootRawChange: root, Heads: []string{id}}
	if p.full {
		payload.Changes = []*treechangeproto.RawTreeChangeWithId{root}
	}
	return opts.TreeValidator(payload, nil, nil)
}

func (p *scriptedProber) probed() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

// probeSyncer is a tree syncer over reg that probes through prober and
// probes any backlog of two or more.
func probeSyncer(t *testing.T, reg *scriptedRegistry, prober *scriptedProber) *treeSyncerAdapter {
	t.Helper()
	prevMin := probeMinMissing
	probeMinMissing = 2
	t.Cleanup(func() { probeMinMissing = prevMin })
	prober.t = t
	ts := newTreeSyncer("space1", reg, nil)
	ts.prober = prober
	return ts
}

func backlog(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("m%02d", i)
	}
	return ids
}

// A round with a backlog probes the roots of its missing trees and
// fetches the types and collections among them first, after the trees
// the registry names and in diff order; everything else keeps its
// place.
func TestTreeSyncerFetchesProbedDefinitionsFirst(t *testing.T) {
	reg := &scriptedRegistry{first: []string{"index"}, firstTypes: []string{"type", "collection"}}
	prober := &scriptedProber{types: map[string]string{"m07": "collection", "m03": "type", "m18": "type"}}
	ts := probeSyncer(t, reg, prober)

	missing := append(backlog(20), "index")
	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, []string{"e1"}, missing))

	want := []string{"index", "m03", "m07", "m18"}
	for _, id := range backlog(20) {
		if _, def := prober.types[id]; !def {
			want = append(want, id)
		}
	}
	want = append(want, "e1")
	require.Equal(t, want, reg.calls)
	require.ElementsMatch(t, backlog(20), prober.probed(), "every missing tree is probed once; the handled index is not")
}

// A probe answer is kept while its tree is missing: the next round
// probes only what is new, and still fetches a known definition first.
func TestTreeSyncerProbesEachRootOnce(t *testing.T) {
	reg := &scriptedRegistry{firstTypes: []string{"type"}, fail: map[string]error{"m05": errors.New("boom")}}
	prober := &scriptedProber{types: map[string]string{"m05": "type"}}
	ts := probeSyncer(t, reg, prober)
	p := fakePeer{id: "peer1"}

	require.NoError(t, ts.SyncAll(context.Background(), p, nil, backlog(10)))
	require.Len(t, prober.probed(), 10)

	// The diff offers the unfetched trees again, plus a new one.
	delete(reg.fail, "m05")
	reg.calls = nil
	require.NoError(t, ts.SyncAll(context.Background(), p, nil, []string{"m02", "m05", "new"}))
	require.Equal(t, []string{"m05", "m02", "new"}, reg.calls)
	require.Len(t, prober.probed(), 11, "only the new tree is probed")
}

// Rounds of one space overlap and each sees its own diff, so a probe
// answer survives rounds that do not offer its tree and is dropped only
// once no round has offered it for a while.
func TestTreeSyncerKeepsProbeAnswersAcrossOtherDiffs(t *testing.T) {
	prevKeep := probeKeepRounds
	probeKeepRounds = 2
	t.Cleanup(func() { probeKeepRounds = prevKeep })

	reg := &scriptedRegistry{firstTypes: []string{"type"}, fail: map[string]error{}}
	for _, id := range backlog(6) {
		reg.fail[id] = errors.New("boom") // nothing is fetched: the diffs keep offering
	}
	prober := &scriptedProber{}
	ts := probeSyncer(t, reg, prober)

	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "a"}, nil, backlog(6)))
	// Another peer's diff offers other trees.
	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "b"}, nil, []string{"x1", "x2"}))
	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "a"}, nil, backlog(6)))
	require.Len(t, prober.probed(), 8, "the first peer's answers outlived the other peer's round")

	// Unoffered for longer than the keep window, the answers go.
	for i := 0; i < 3; i++ {
		require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "b"}, nil, []string{"x1", "x2"}))
	}
	ts.probeMu.Lock()
	_, kept := ts.probed["m00"]
	ts.probeMu.Unlock()
	require.False(t, kept)
}

// Nothing is probed when the registry names no changeType, when the
// backlog is small, or for a tree that is already local.
func TestTreeSyncerProbeSkips(t *testing.T) {
	p := fakePeer{id: "peer1"}

	t.Run("no pull-first types", func(t *testing.T) {
		reg := &scriptedRegistry{}
		prober := &scriptedProber{types: map[string]string{"m03": "type"}}
		ts := probeSyncer(t, reg, prober)
		require.NoError(t, ts.SyncAll(context.Background(), p, nil, backlog(6)))
		require.Empty(t, prober.probed())
		require.Equal(t, backlog(6), reg.calls)
	})

	t.Run("small backlog", func(t *testing.T) {
		reg := &scriptedRegistry{firstTypes: []string{"type"}}
		prober := &scriptedProber{types: map[string]string{"m03": "type"}}
		ts := probeSyncer(t, reg, prober)
		probeMinMissing = 16
		require.NoError(t, ts.SyncAll(context.Background(), p, nil, backlog(6)))
		require.Empty(t, prober.probed())
		require.Equal(t, backlog(6), reg.calls)
	})

	t.Run("tree already local", func(t *testing.T) {
		reg := &scriptedRegistry{firstTypes: []string{"type"}}
		prober := &scriptedProber{types: map[string]string{"m03": "type"}}
		ts := probeSyncer(t, reg, prober)
		ts.hasTree = func(_ context.Context, id string) (bool, error) { return id == "m03", nil }
		require.NoError(t, ts.SyncAll(context.Background(), p, nil, backlog(6)))
		require.NotContains(t, prober.probed(), "m03")
		// Unclassified, it follows the trees whose root was read.
		require.Equal(t, []string{"m00", "m01", "m02", "m04", "m05", "m03"}, reg.calls)
	})
}

// A tree whose probe fails is unclassified: it syncs after the trees
// whose root was read, one at a time. It is probed again, after a wait
// that doubles per failure: a tree the peer cannot serve must not cost
// a probe every round, and no failure settles a tree — the probe that
// gets through still classifies it.
func TestTreeSyncerFailedProbeIsRetriedWithBackoff(t *testing.T) {
	reg := &scriptedRegistry{firstTypes: []string{"type"}, fail: map[string]error{}}
	for _, id := range backlog(6) {
		reg.fail[id] = errors.New("boom") // nothing is fetched: the diffs keep offering
	}
	prober := &scriptedProber{
		types: map[string]string{"m01": "type", "m04": "type"},
		fail:  map[string]error{"m04": errors.New("refused")},
	}
	ts := probeSyncer(t, reg, prober)
	p := fakePeer{id: "peer1"}
	probesOf := func(id string) (n int) {
		for _, probed := range prober.probed() {
			if probed == id {
				n++
			}
		}
		return n
	}

	require.NoError(t, ts.SyncAll(context.Background(), p, nil, backlog(6)))
	require.Equal(t, []string{"m01", "m00", "m02", "m03", "m05", "m04"}, reg.calls)

	// Rounds 2..9: the failing tree is probed in rounds 3 and 7 only.
	for i := 0; i < 8; i++ {
		require.NoError(t, ts.SyncAll(context.Background(), p, nil, backlog(6)))
	}
	require.Equal(t, 3, probesOf("m04"))
	require.Len(t, prober.probed(), 6+2, "the trees whose root was read are not probed again")

	// The failure passes: the next probe that is due classifies the tree.
	delete(prober.fail, "m04")
	for i := 0; i < int(probeMaxBackoff); i++ {
		require.NoError(t, ts.SyncAll(context.Background(), p, nil, backlog(6)))
	}
	reg.calls = nil
	require.NoError(t, ts.SyncAll(context.Background(), p, nil, backlog(6)))
	require.Equal(t, []string{"m01", "m04"}, reg.calls[:2])
}

// When probe after probe fails and none is answered, the link or the
// peer is failing, not the trees: the round stops probing that peer,
// and a later round, with the peer back, classifies the backlog.
func TestTreeSyncerStopsProbingAFailingPeer(t *testing.T) {
	prevWorkers, prevGiveUp := probeWorkers, probeGiveUp
	probeWorkers, probeGiveUp = 1, 3
	t.Cleanup(func() { probeWorkers, probeGiveUp = prevWorkers, prevGiveUp })

	reg := &scriptedRegistry{firstTypes: []string{"type"}, fail: map[string]error{}}
	prober := &scriptedProber{types: map[string]string{"m10": "type"}, fail: map[string]error{}}
	for _, id := range backlog(12) {
		reg.fail[id] = errors.New("boom")
		prober.fail[id] = errors.New("connection closed")
	}
	ts := probeSyncer(t, reg, prober)
	p := fakePeer{id: "peer1"}

	require.NoError(t, ts.SyncAll(context.Background(), p, nil, backlog(12)))
	require.Less(t, len(prober.probed()), 12, "the round gave up on the peer")

	prober.fail = nil
	for i := 0; i < int(probeMaxBackoff)+1; i++ {
		require.NoError(t, ts.SyncAll(context.Background(), p, nil, backlog(12)))
	}
	reg.calls = nil
	require.NoError(t, ts.SyncAll(context.Background(), p, nil, backlog(12)))
	require.Equal(t, "m10", reg.calls[0], "nothing was settled by the failures")
}

// A peer that turns a probe away as busy — over its request cap, or
// already serving that tree to us — has said nothing about the tree: it
// is probed again next round, however often that happens, and a refusal
// over the cap narrows the peer's budget.
func TestTreeSyncerBusyPeerIsProbedAgain(t *testing.T) {
	reg := &scriptedRegistry{firstTypes: []string{"type"}, fail: map[string]error{}}
	for _, id := range backlog(6) {
		reg.fail[id] = errors.New("boom")
	}
	prober := &scriptedProber{
		types: map[string]string{"m02": "type", "m04": "type"},
		fail: map[string]error{
			"m02": spacesyncproto.ErrTooManyRequestsFromPeer,
			"m04": spacesyncproto.ErrDuplicateRequest,
		},
	}
	ts := probeSyncer(t, reg, prober)
	p := fakePeer{id: "peer1"}

	const rounds = 5
	for i := 0; i < rounds; i++ {
		require.NoError(t, ts.SyncAll(context.Background(), p, nil, backlog(6)))
	}
	require.Len(t, prober.probed(), 4+2*rounds, "the two refused trees are probed every round")
	require.Less(t, ts.limits.of("peer1").width, peerLimitMax, "refusals over the cap narrow the budget")

	prober.fail = nil
	reg.calls = nil
	require.NoError(t, ts.SyncAll(context.Background(), p, nil, backlog(6)))
	require.Equal(t, []string{"m02", "m04"}, reg.calls[:2])
}

// Rounds overlap. A failure reported by one round's probe does not
// replace the answer another round already read for that tree.
func TestTreeSyncerProbeFailureKeepsAnAnswer(t *testing.T) {
	ts := newTreeSyncer("space1", &scriptedRegistry{}, nil)
	ts.probeRecord("t1", probeIsFirst)
	ts.probeRecord("t1", probeFailed)
	ts.probeRecord("t1", probeIsOther)
	require.True(t, ts.probed["t1"].first)
	require.True(t, ts.probed["t1"].done)
	require.Zero(t, ts.probed["t1"].fails)
}

// A probe is in flight for a tree at most once, and a round does not
// go on to its fetches while an overlapping round is still probing a
// tree of its diff: the definition that round finds is fetched first
// here too.
func TestTreeSyncerWaitsForAnOverlappingRoundsProbes(t *testing.T) {
	reg := &scriptedRegistry{firstTypes: []string{"type"}}
	release := make(chan struct{})
	started := make(chan struct{}, 64)
	prober := &scriptedProber{types: map[string]string{"m02": "type"}}
	prober.onProbe = func(string) {
		started <- struct{}{}
		<-release
	}
	ts := probeSyncer(t, reg, prober)

	done := make(chan error, 2)
	go func() { done <- ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, nil, backlog(4)) }()
	for i := 0; i < 4; i++ {
		<-started // the first round holds a probe of every tree
	}
	go func() { done <- ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, nil, backlog(4)) }()

	select {
	case err := <-done:
		t.Fatalf("a round finished while probes of its diff were in flight: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	reg.mu.Lock()
	require.Empty(t, reg.calls, "nothing is fetched before the probes answer")
	reg.mu.Unlock()

	close(release)
	require.NoError(t, <-done)
	require.NoError(t, <-done)
	require.ElementsMatch(t, backlog(4), prober.probed(), "each root probed once across both rounds")
	require.Equal(t, "m02", reg.calls[0], "the definition the other round found goes first")
	require.Len(t, reg.calls, 8)
}

// A probe cut short by the round leaves its tree unclassified, to be
// probed by a later round; no probe starts once the round is over.
func TestTreeSyncerCancelledRoundProbesNothing(t *testing.T) {
	reg := &scriptedRegistry{firstTypes: []string{"type"}}
	prober := &scriptedProber{types: map[string]string{"m01": "type"}}
	ts := probeSyncer(t, reg, prober)
	var checked atomic.Int64
	ts.hasTree = func(context.Context, string) (bool, error) { checked.Add(1); return false, nil }

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, ts.SyncAll(ctx, fakePeer{id: "peer1"}, nil, backlog(8)))
	require.Empty(t, prober.probed())
	require.Zero(t, checked.Load(), "a finished round does not touch storage")
	require.Empty(t, ts.probed)

	reg.calls = nil
	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, nil, backlog(8)))
	require.Len(t, prober.probed(), 8)
	require.Equal(t, "m01", reg.calls[0])
}

// Answers age out once the backlog is gone, not only while a round
// still probes.
func TestTreeSyncerProbeAnswersAgeWithoutABacklog(t *testing.T) {
	prevKeep := probeKeepRounds
	probeKeepRounds = 2
	t.Cleanup(func() { probeKeepRounds = prevKeep })

	reg := &scriptedRegistry{firstTypes: []string{"type"}, fail: map[string]error{}}
	for _, id := range backlog(6) {
		reg.fail[id] = errors.New("boom")
	}
	ts := probeSyncer(t, reg, &scriptedProber{})
	p := fakePeer{id: "peer1"}
	require.NoError(t, ts.SyncAll(context.Background(), p, nil, backlog(6)))
	require.Len(t, ts.probed, 6)
	for i := 0; i < 3; i++ {
		require.NoError(t, ts.SyncAll(context.Background(), p, nil, nil))
	}
	require.Empty(t, ts.probed)
}

// A peer that ignores the probe flag streams the whole tree. The first
// such answer stops the probing of that peer: probing it would download
// every tree twice.
func TestTreeSyncerStopsProbingPeerThatIgnoresTheFlag(t *testing.T) {
	prevWorkers := probeWorkers
	probeWorkers = 1
	t.Cleanup(func() { probeWorkers = prevWorkers })

	reg := &scriptedRegistry{firstTypes: []string{"type"}}
	prober := &scriptedProber{types: map[string]string{"m00": "type", "m05": "type"}, full: true}
	ts := probeSyncer(t, reg, prober)

	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "old"}, nil, backlog(8)))
	require.Less(t, len(prober.probed()), 8, "probing stops after the first full answer")
	require.Equal(t, "m00", reg.calls[0], "the root it did read still orders the fetch")
	require.ElementsMatch(t, backlog(8), reg.calls)

	// The next round does not probe this peer again.
	before := len(prober.probed())
	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "old"}, nil, append(backlog(8), "x1", "x2")))
	require.Len(t, prober.probed(), before)
}
