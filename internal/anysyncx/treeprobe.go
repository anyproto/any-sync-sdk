package anysyncx

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/objecttreebuilder"
	"github.com/anyproto/any-sync/net/peer"
	"go.uber.org/zap"
)

// Root probing: which missing trees a round fetches first.
//
// A tree's root carries its changeType in plain text, but the headsync
// diff names a missing tree by id and head alone. A probe request asks
// the peer for a tree's root and heads without its changes, so a round
// with a backlog probes the roots of its missing trees — several at a
// time: an answer is a root and its heads, and nothing is built here
// (the responder loads the tree to answer, as it will for the fetch) —
// and fetches the ones of a
// pull-first changeType (SpaceRegistry.PullFirstTypes: types and
// collections) ahead of the rest. Objects that arrive after their type
// apply at once; arriving before it, their changes park until the type
// lands and replay then.
//
// The answer orders the fetches, and tells which of them may run side
// by side (SyncAll): the trees whose root is neither a type nor a
// collection. A tree that was not probed, or whose probe failed, syncs
// after those, one at a time.

// Vars, not consts, so tests don't depend on them.
var (
	// probeMinMissing is the backlog below which a round fetches in
	// diff order: probing a handful of trees costs about what fetching
	// them does.
	probeMinMissing = 16
	// probeWorkers bounds the probes one round has in flight. Rounds
	// overlap, and with them their probes: the peer's request budget
	// (peerLimits) is what bounds them all.
	probeWorkers = 8
	// probeBudget bounds one round's probing; trees it did not reach
	// keep their diff position and are probed by a later round.
	probeBudget = 15 * time.Second
	// probeKeepRounds is how many rounds a probe answer outlives the
	// last diff that offered its tree. Rounds of one space overlap (the
	// periodic loop, a kicked round, one per peer) and each sees its
	// own diff, so an answer is dropped by age, not by one round's
	// missing list.
	probeKeepRounds uint64 = 16
	// probeMaxBackoff caps, in rounds, how long a tree whose probe
	// failed waits for the next one. The wait doubles per failure: a
	// tree the peer cannot serve must not cost a probe every round, and
	// a failure of the link or the peer, which hits every tree at once,
	// must not settle anything.
	probeMaxBackoff uint64 = 8
	// probeGiveUp is how many probes in a row may fail, with none
	// answered, before a round stops probing the peer: the link or the
	// peer is failing, not the trees.
	probeGiveUp int64 = 8
)

// errProbed ends a probe fetch once its root is read; nothing is built
// or stored.
var errProbed = errors.New("anysyncx: root probed")

// probeAnswer is what is known of a probed root.
type probeAnswer struct {
	// done: the tree is not probed again. first: its root was read and
	// its changeType is a pull-first one. other: its root was read and
	// its changeType is not. Neither: the tree arrived before a probe
	// could read its root.
	done, first, other bool
	// fails counts the failed probes; retryAt is the round from which
	// the tree is probed again.
	fails   uint8
	retryAt uint64
	// round is the last round whose diff offered the tree.
	round uint64
}

// rootProber is the slice of the space's tree builder a probe uses.
type rootProber interface {
	BuildTree(ctx context.Context, id string, opts objecttreebuilder.BuildTreeOpts) (objecttree.ObjectTree, error)
}

// probeFirst returns the missing trees, in diff order, whose root is of
// a pull-first changeType, probing the roots that are not classified
// yet.
func (t *treeSyncerAdapter) probeFirst(ctx context.Context, p peer.Peer, missing []string, seen map[string]struct{}) []string {
	if !t.probeAge(missing) || len(missing) < probeMinMissing || t.prober == nil || ctx.Err() != nil {
		return nil
	}
	types := t.registry.PullFirstTypes(t.spaceId)
	if len(types) == 0 {
		return nil
	}
	if todo := t.probeTodo(p.Id(), missing, seen); len(todo) > 0 {
		t.probeRoots(ctx, p.Id(), todo, types)
	}
	// An overlapping round may still be probing trees of this diff: a
	// definition among them must not be overtaken by its objects.
	t.probeWait(ctx, missing)
	t.probeMu.Lock()
	defer t.probeMu.Unlock()
	var out []string
	for _, id := range missing {
		if _, handled := seen[id]; !handled && t.probed[id].first {
			out = append(out, id)
		}
	}
	return out
}

// probedOther returns the missing trees, in diff order, whose root was
// read and is not of a pull-first changeType: the ones a round syncs
// side by side (see SyncAll).
func (t *treeSyncerAdapter) probedOther(missing []string, seen map[string]struct{}) []string {
	t.probeMu.Lock()
	defer t.probeMu.Unlock()
	if len(t.probed) == 0 {
		return nil
	}
	var out []string
	for _, id := range missing {
		if _, handled := seen[id]; !handled && t.probed[id].other {
			out = append(out, id)
		}
	}
	return out
}

// probeAge counts a round: it renews the answers of the trees the
// round's diff still offers and drops the ones no recent round's diff
// did. It reports whether there is anything to probe or to use: a
// backlog, or answers kept.
func (t *treeSyncerAdapter) probeAge(missing []string) bool {
	t.probeMu.Lock()
	defer t.probeMu.Unlock()
	if len(t.probed) == 0 {
		return len(missing) >= probeMinMissing
	}
	t.probeRound++
	for _, id := range missing {
		if a, ok := t.probed[id]; ok {
			a.round = t.probeRound
			t.probed[id] = a
		}
	}
	for id, a := range t.probed {
		if t.probeRound-a.round > probeKeepRounds {
			delete(t.probed, id)
		}
	}
	return true
}

// probeTodo returns the missing trees still to probe on peerId — none
// when the peer ignores probes.
func (t *treeSyncerAdapter) probeTodo(peerId string, missing []string, seen map[string]struct{}) []string {
	t.probeMu.Lock()
	defer t.probeMu.Unlock()
	if _, off := t.noProbe[peerId]; off {
		return nil
	}
	var todo []string
	for _, id := range missing {
		if _, handled := seen[id]; handled {
			continue
		}
		if a := t.probed[id]; !a.done && t.probeRound >= a.retryAt {
			todo = append(todo, id)
		}
	}
	return todo
}

// probeRoots probes ids on peerId with a bounded pool and records each
// answer. A probe cut short by the budget, or turned away by a busy
// peer, leaves its id for a later round.
func (t *treeSyncerAdapter) probeRoots(ctx context.Context, peerId string, ids, types []string) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(peer.CtxWithPeerId(ctx, peerId), probeBudget)
	defer cancel()
	limit := t.limits.of(peerId)
	work := make(chan string)
	var (
		wg                      sync.WaitGroup
		probed, found, busy     atomic.Int64
		failedInARow            atomic.Int64
		answered, peerIsFailing atomic.Bool
	)
	for w := 0; w < min(probeWorkers, len(ids)); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range work {
				if !t.probeClaim(id) {
					continue // an overlapping round is on it
				}
				res := probeUnanswered
				if slot, err := limit.acquire(ctx); err == nil {
					res = t.probeRoot(ctx, peerId, id, types)
					limit.release(slot, res == probeTooMany)
				}
				switch res {
				case probeIsFirst, probeIsOther:
					probed.Add(1)
					answered.Store(true)
					failedInARow.Store(0)
					if res == probeIsFirst {
						found.Add(1)
					}
				case probeBusy, probeTooMany:
					busy.Add(1)
				case probeFailed:
					if failedInARow.Add(1) >= probeGiveUp && !answered.Load() {
						peerIsFailing.Store(true)
					}
				}
				// Recorded before the claim goes: a round waiting on the
				// claim reads the answer.
				t.probeRecord(id, res)
				t.probeRelease(id)
			}
		}()
	}
	for _, id := range ids {
		if ctx.Err() != nil || peerIsFailing.Load() || t.probeOff(peerId) {
			break
		}
		select {
		case work <- id:
		case <-ctx.Done():
		}
	}
	close(work)
	wg.Wait()
	t.log.Debug("probed missing tree roots",
		zap.String("spaceId", t.spaceId), zap.String("peerId", peerId),
		zap.Int("missing", len(ids)), zap.Int64("probed", probed.Load()),
		zap.Int64("first", found.Load()), zap.Int64("peerBusy", busy.Load()),
		zap.Bool("peerFailing", peerIsFailing.Load()),
		zap.Duration("dur", time.Since(start)))
}

// probeWait returns once no probe is in flight for a tree of missing,
// or the probing budget is spent.
func (t *treeSyncerAdapter) probeWait(ctx context.Context, missing []string) {
	var (
		inDiff map[string]struct{}
		expire <-chan time.Time
	)
	for {
		t.probeMu.Lock()
		waiting := false
		if len(t.probing) > 0 {
			if inDiff == nil {
				inDiff = make(map[string]struct{}, len(missing))
				for _, id := range missing {
					inDiff[id] = struct{}{}
				}
			}
			for id := range t.probing {
				if _, ok := inDiff[id]; ok {
					waiting = true
					break
				}
			}
		}
		idle := t.probeIdle
		t.probeMu.Unlock()
		if !waiting {
			return
		}
		if expire == nil {
			timer := time.NewTimer(probeBudget)
			defer timer.Stop()
			expire = timer.C
		}
		select {
		case <-idle:
		case <-expire:
			return
		case <-ctx.Done():
			return
		}
	}
}

// probeResult is the outcome of one probe.
type probeResult uint8

const (
	// probeUnanswered: cut short (round budget, cancel) — probe again.
	probeUnanswered probeResult = iota
	// probeBusy, probeTooMany: the peer turned the request away without
	// looking at the tree — probe again.
	probeBusy
	probeTooMany
	// probeFailed: the probe failed — the tree's own fault (deleted,
	// refused) or the link's; probe again after a wait.
	probeFailed
	// probeGone: nothing to probe — the tree arrived meanwhile.
	probeGone
	// probeIsFirst, probeIsOther: the root was read.
	probeIsFirst
	probeIsOther
)

// probeRecord stores a probe's outcome. A classified tree stays
// classified: rounds overlap, and a late failure says nothing about an
// answer already read.
func (t *treeSyncerAdapter) probeRecord(id string, res probeResult) {
	if res == probeUnanswered || res == probeBusy || res == probeTooMany {
		return
	}
	t.probeMu.Lock()
	defer t.probeMu.Unlock()
	a := t.probed[id]
	if a.done {
		return
	}
	a.round = t.probeRound
	switch res {
	case probeIsFirst:
		a.done, a.first = true, true
	case probeIsOther:
		a.done, a.other = true, true
	case probeGone:
		a.done = true
	case probeFailed:
		if a.fails < 16 {
			a.fails++
		}
		a.retryAt = t.probeRound + min(uint64(1)<<a.fails, probeMaxBackoff)
	}
	t.probed[id] = a
}

// probeRoot asks peerId for id's root and classifies it against types.
func (t *treeSyncerAdapter) probeRoot(ctx context.Context, peerId, id string, types []string) probeResult {
	if ctx.Err() != nil {
		return probeUnanswered
	}
	// A tree that arrived since the diff (a head update pulls it) has
	// nothing to probe, and BuildTree would open it without the SDK's
	// listener.
	if t.hasTree != nil {
		has, err := t.hasTree(ctx, id)
		if err != nil {
			return probeUnanswered
		}
		if has {
			return probeGone
		}
	}
	var (
		changeType string
		read, full bool
	)
	tree, err := t.prober.BuildTree(ctx, id, objecttreebuilder.BuildTreeOpts{
		Probe: true,
		TreeValidator: func(payload treestorage.TreeStorageCreatePayload, _ objecttree.TreeStorageCreator, _ list.AclList) (objecttree.ObjectTree, error) {
			// A probe answer carries no changes; a peer that ignores
			// the flag streams the tree, root first.
			full = len(payload.Changes) > 0
			ct, perr := rootChangeType(payload.RootRawChange)
			if perr != nil {
				return nil, perr
			}
			changeType, read = ct, true
			return nil, errProbed
		},
	})
	if tree != nil {
		// The tree landed between the check above and the request, and
		// BuildTree opened it. Closing this second handle drops what is
		// queued for the tree; the next round's diff brings it back.
		_ = tree.Close()
		return probeGone
	}
	if full {
		t.probeMu.Lock()
		if _, known := t.noProbe[peerId]; !known {
			t.noProbe[peerId] = struct{}{}
			t.log.Info("peer ignores tree probes; missing trees sync in diff order",
				zap.String("spaceId", t.spaceId), zap.String("peerId", peerId))
		}
		t.probeMu.Unlock()
	}
	switch {
	case read:
		for _, tp := range types {
			if tp == changeType {
				return probeIsFirst
			}
		}
		return probeIsOther
	case peerTooMany(err):
		return probeTooMany
	case peerBusy(err):
		return probeBusy
	case ctx.Err() != nil || err == nil:
		return probeUnanswered
	default:
		return probeFailed
	}
}

// probeClaim marks id as being probed; false when an overlapping
// round's probe of it is in flight or it has been classified since.
func (t *treeSyncerAdapter) probeClaim(id string) bool {
	t.probeMu.Lock()
	defer t.probeMu.Unlock()
	if _, busy := t.probing[id]; busy || t.probed[id].done {
		return false
	}
	t.probing[id] = struct{}{}
	return true
}

func (t *treeSyncerAdapter) probeRelease(id string) {
	t.probeMu.Lock()
	delete(t.probing, id)
	close(t.probeIdle)
	t.probeIdle = make(chan struct{})
	t.probeMu.Unlock()
}

func (t *treeSyncerAdapter) probeOff(peerId string) bool {
	t.probeMu.Lock()
	defer t.probeMu.Unlock()
	_, off := t.noProbe[peerId]
	return off
}

// rootChangeType reads the changeType off a raw root change. The cid is
// not checked: a mislabelled root changes the order of the fetches and
// which of them run side by side, and the fetch itself validates the
// tree.
func rootChangeType(raw *treechangeproto.RawTreeChangeWithId) (string, error) {
	if raw == nil || len(raw.GetRawChange()) == 0 {
		return "", errors.New("anysyncx: probe answer carries no root")
	}
	rawChange := &treechangeproto.RawTreeChange{}
	if err := rawChange.UnmarshalVT(raw.RawChange); err != nil {
		return "", fmt.Errorf("anysyncx: unmarshal raw root: %w", err)
	}
	root := &treechangeproto.RootChange{}
	if err := root.UnmarshalVT(rawChange.Payload); err != nil {
		return "", fmt.Errorf("anysyncx: unmarshal root: %w", err)
	}
	return root.ChangeType, nil
}
