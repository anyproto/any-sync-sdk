// Package readsync moves read state between an account's devices: it
// publishes each object's seen-heads frontier to the TECH space's
// key-value store and merges other devices' published frontiers into
// the local per-space readstate engines. The tech space keeps read
// positions private (owner-only ACL) — nothing here is visible to
// other members of the target space.
//
// One KV key per tracked object, `read/<spaceId>/<objectId>`; the
// store keys rows per (key, peerId), so every device owns its row
// (LWW on the writer timestamp) and the logical frontier is the union
// of all rows, folded in by the engine's idempotent MergeHeads.
//
// A merge NEVER loads the object: it runs against the readstate rows
// (plus point lookups into any-sync's change storage for gap
// traversal), serialized per object by keyed mutexes, in its own
// write transaction. The live hook (App.OnKeyValues) hands work to a
// single worker goroutine; durability comes from Reconcile, which
// replays every published frontier for a space through the same
// idempotent merge (one state read per already-merged object).
package readsync

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/object/keyvalue/keyvaluestorage"
	"github.com/anyproto/any-sync/commonspace/object/keyvalue/keyvaluestorage/innerstorage"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/readstate"
)

var log = logger.NewNamed("sdk.readsync")

const keyPrefix = "read/"

// Key codec: read/<spaceId>/<objectId>. Space ids and object ids are
// content-addressable (no '/'), so the split is unambiguous.
func kvKey(spaceId, objectId string) string { return keyPrefix + spaceId + "/" + objectId }

func parseKey(key string) (spaceId, objectId string, ok bool) {
	rest, found := strings.CutPrefix(key, keyPrefix)
	if !found {
		return "", "", false
	}
	spaceId, objectId, ok = strings.Cut(rest, "/")
	return spaceId, objectId, ok && spaceId != "" && objectId != ""
}

// frontierValue is the published KV payload.
type frontierValue struct {
	Heads []string `json:"h"`
}

// EngineFor routes a merge to the target space's readstate engine.
// nil = space unknown here or tracks nothing (the value is dropped;
// Reconcile picks it up if the space registers tracking later).
type EngineFor func(spaceId string) *readstate.Engine

// KVStore returns the tech space's default key-value store.
type KVStore func(ctx context.Context) (keyvaluestorage.Storage, error)

type Service struct {
	engineFor  EngineFor
	kvStore    KVStore
	selfPeerId string

	// objMu serializes merges and marks per object — the engine holds
	// no locks of its own. Entries are reference-counted and evicted
	// on last release, so the map doesn't grow with every object ever
	// touched.
	mu    sync.Mutex
	objMu map[string]*objLock

	queue  chan mergeJob
	stop   chan struct{}
	worker sync.WaitGroup
}

type mergeJob struct {
	spaceId  string
	objectId string
	heads    []string
}

func New(engineFor EngineFor, kvStore KVStore, selfPeerId string) *Service {
	// The peerId identifies this device's row in every published key;
	// logging it once makes cross-boot row ownership checkable from
	// device logs (a regenerated device key orphans all previous rows).
	log.Info("readsync: starting", zap.String("selfPeerId", selfPeerId))
	s := &Service{
		engineFor:  engineFor,
		kvStore:    kvStore,
		selfPeerId: selfPeerId,
		objMu:      map[string]*objLock{},
		queue:      make(chan mergeJob, 256),
		stop:       make(chan struct{}),
	}
	s.worker.Add(1)
	go s.run()
	return s
}

func (s *Service) Close() {
	close(s.stop)
	s.worker.Wait()
}

type objLock struct {
	mu   sync.Mutex
	refs int
}

func (s *Service) lockObject(objectId string) func() {
	s.mu.Lock()
	l := s.objMu[objectId]
	if l == nil {
		l = &objLock{}
		s.objMu[objectId] = l
	}
	l.refs++
	s.mu.Unlock()
	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		s.mu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(s.objMu, objectId)
		}
		s.mu.Unlock()
	}
}

// MarkRead covers the given changes (and ancestry) locally, then
// publishes the advanced frontier. The local commit is the durable
// step; publishing writes the device's KV row, which any-sync
// persists locally and syncs when connectivity allows.
func (s *Service) MarkRead(ctx context.Context, spaceId, objectId string, changeIds []string) (readstate.MarkResult, error) {
	return s.mark(ctx, spaceId, objectId, func(eng *readstate.Engine, txCtx context.Context) (readstate.MarkResult, error) {
		return eng.MarkRead(txCtx, objectId, changeIds)
	})
}

// markChunkSize bounds one MarkReadUpTo transaction. Forward-only
// marking makes a versionId-prefix chunk a valid frontier advance, so
// each chunk commits durably and a crash mid-way resumes on retry.
const markChunkSize = 2048

// MarkReadUpTo covers everything at or below upTo in local display
// order ("" = all) in bounded per-chunk transactions, then publishes
// the final frontier once.
func (s *Service) MarkReadUpTo(ctx context.Context, spaceId, objectId, upTo string) (readstate.MarkResult, error) {
	eng := s.engineFor(spaceId)
	if eng == nil {
		return readstate.MarkResult{}, ErrUntracked
	}
	unlock := s.lockObject(objectId)
	defer unlock()
	var agg readstate.MarkResult
	for {
		var (
			res  readstate.MarkResult
			done bool
		)
		err := eng.WriteTx(ctx, func(txCtx context.Context) error {
			var opErr error
			res, done, opErr = eng.MarkReadUpToChunk(txCtx, objectId, upTo, markChunkSize)
			return opErr
		})
		if err != nil {
			return agg, err
		}
		agg.Removed = append(agg.Removed, res.Removed...)
		agg.Frontier = res.Frontier
		if res.StateSeq != 0 {
			agg.StateSeq = res.StateSeq
		}
		if done {
			break
		}
	}
	if agg.StateSeq != 0 {
		s.publish(ctx, spaceId, objectId, agg.Frontier)
		eng.NotifyState(objectId, agg.StateSeq)
	}
	return agg, nil
}

func (s *Service) mark(ctx context.Context, spaceId, objectId string, op func(*readstate.Engine, context.Context) (readstate.MarkResult, error)) (readstate.MarkResult, error) {
	eng := s.engineFor(spaceId)
	if eng == nil {
		return readstate.MarkResult{}, ErrUntracked
	}
	unlock := s.lockObject(objectId)
	defer unlock()
	var res readstate.MarkResult
	err := eng.WriteTx(ctx, func(txCtx context.Context) error {
		var opErr error
		res, opErr = op(eng, txCtx)
		return opErr
	})
	if err != nil {
		return readstate.MarkResult{}, err
	}
	if res.StateSeq != 0 {
		s.publish(ctx, spaceId, objectId, res.Frontier)
		eng.NotifyState(objectId, res.StateSeq)
	}
	return res, nil
}

// publish writes this device's frontier row. Best-effort: the local
// mark already committed, and an unpublished frontier is re-derivable
// (the next mark republishes a superset).
func (s *Service) publish(ctx context.Context, spaceId, objectId string, heads []string) {
	// Liveness re-check at publish time: the mark resolved its engine
	// before any concurrent removal flipped the space's row, and a row
	// published after the removal's prune watermark would resurrect the
	// prefix on every replica until a later reconcile pass re-prunes.
	if s.engineFor(spaceId) == nil {
		return
	}
	store, err := s.kvStore(ctx)
	if err != nil {
		log.Warn("publish: tech kv store", zap.Error(err))
		return
	}
	raw, err := json.Marshal(frontierValue{Heads: heads})
	if err != nil {
		log.Warn("publish: marshal", zap.Error(err))
		return
	}
	if err = store.Set(ctx, kvKey(spaceId, objectId), raw); err != nil {
		log.Warn("publish: set", zap.String("objectId", objectId), zap.Error(err))
	}
}

// PublishedFrontiers returns every device's published frontier for the
// object — own rows included (a rebuilt device's previous publishes
// are valid coverage). nil when nothing was published. Used by the
// first-load seed to prefer the account's real read state over
// first-sight seeding.
func (s *Service) PublishedFrontiers(ctx context.Context, spaceId, objectId string) ([][]string, error) {
	store, err := s.kvStore(ctx)
	if err != nil {
		return nil, err
	}
	var sets [][]string
	err = store.GetAll(ctx, kvKey(spaceId, objectId), func(decryptor keyvaluestorage.Decryptor, values []innerstorage.KeyValue) error {
		for _, kv := range values {
			raw, decErr := decryptor(kv)
			if decErr != nil {
				log.Warn("published frontiers: decrypt", zap.String("key", kv.Key), zap.Error(decErr))
				continue
			}
			var val frontierValue
			if decErr = json.Unmarshal(raw, &val); decErr != nil {
				log.Warn("published frontiers: decode", zap.String("key", kv.Key), zap.Error(decErr))
				continue
			}
			if len(val.Heads) > 0 {
				sets = append(sets, val.Heads)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return sets, nil
}

// OnKeyValues is the live hook for the tech space's applied KV writes
// (wire via App.OnKeyValues). Runs on any-sync's apply path — it only
// decodes and enqueues; merging happens on the service worker.
func (s *Service) OnKeyValues(decryptor keyvaluestorage.Decryptor, kvs []innerstorage.KeyValue) {
	for _, kv := range kvs {
		if kv.PeerId == s.selfPeerId {
			continue // our own published row
		}
		spaceId, objectId, ok := parseKey(kv.Key)
		if !ok {
			continue
		}
		raw, err := decryptor(kv)
		if err != nil {
			log.Warn("decrypt read frontier", zap.String("key", kv.Key), zap.Error(err))
			continue
		}
		var val frontierValue
		if err = json.Unmarshal(raw, &val); err != nil {
			log.Warn("decode read frontier", zap.String("key", kv.Key), zap.Error(err))
			continue
		}
		if len(val.Heads) == 0 {
			continue
		}
		select {
		case s.queue <- mergeJob{spaceId: spaceId, objectId: objectId, heads: val.Heads}:
		default:
			// Queue full — drop; Reconcile replays it on next boot, and
			// any further row update re-enqueues.
			log.Warn("merge queue full, dropping", zap.String("key", kv.Key))
		}
	}
}

func (s *Service) run() {
	defer s.worker.Done()
	for {
		select {
		case <-s.stop:
			return
		case job := <-s.queue:
			s.merge(context.Background(), job)
		}
	}
}

func (s *Service) merge(ctx context.Context, job mergeJob) {
	eng := s.engineFor(job.spaceId)
	if eng == nil {
		return
	}
	unlock := s.lockObject(job.objectId)
	defer unlock()
	var res readstate.MarkResult
	err := eng.WriteTx(ctx, func(txCtx context.Context) error {
		var mergeErr error
		res, mergeErr = eng.MergeHeads(txCtx, job.objectId, job.heads)
		return mergeErr
	})
	if err != nil {
		log.Warn("merge frontier", zap.String("objectId", job.objectId), zap.Error(err))
		return
	}
	if res.StateSeq != 0 {
		eng.NotifyState(job.objectId, res.StateSeq)
	}
}

// Reconcile replays every frontier published for spaceId through the
// idempotent merge — the durable complement to the best-effort live
// hook. Call on boot after the space's store is up. Already-merged
// frontiers cost one state read each (engine fast path).
//
// Own-device rows are merged too: after a local DB rebuild our own
// published frontier is the record of this device's reads. And when
// the local frontier has advanced past our published row (a publish
// that failed after its mark committed, or marks made before a
// rebuild), Reconcile republishes — so boot is the healing pass for
// both directions of divergence.
func (s *Service) Reconcile(ctx context.Context, spaceId string) error {
	if s.engineFor(spaceId) == nil {
		return nil
	}
	return s.reconcile(ctx, spaceId)
}

// ReconcileAll is the boot form: ONE pass over the tech-space store
// covering every space (keys route by their embedded spaceId; spaces
// without a tracked engine are skipped), instead of a full store scan
// per space.
func (s *Service) ReconcileAll(ctx context.Context) error {
	return s.reconcile(ctx, "")
}

func (s *Service) reconcile(ctx context.Context, onlySpaceId string) error {
	store, err := s.kvStore(ctx)
	if err != nil {
		return err
	}
	// keyState accumulates one key's rows across ALL Iterate callbacks.
	// storage.Iterate groups only CONSECUTIVE rows of a key over an
	// unsorted (insertion-order) scan, so one key's rows can arrive
	// split into several callbacks. The republish decision must see the
	// whole key: deciding per callback makes every group without the
	// own row look divergent, republishing the same keys on every boot,
	// once per own-row-less group.
	type keyState struct {
		spaceId  string
		objectId string
		ownHeads []string
	}
	states := map[string]*keyState{}
	// Publish order follows first-seen order; map iteration is random.
	var order []string
	// Spaces with no engine (deleted / pending / untracked) are skipped
	// silently per key; one debug line per space keeps boot logs quiet
	// even when a dead space left hundreds of frontier keys behind.
	skipped := map[string]struct{}{}
	err = store.Iterate(ctx, func(decryptor keyvaluestorage.Decryptor, key string, values []innerstorage.KeyValue) (bool, error) {
		spaceId, objectId, ok := parseKey(key)
		if !ok || (onlySpaceId != "" && spaceId != onlySpaceId) {
			return true, nil
		}
		if s.engineFor(spaceId) == nil {
			if _, seen := skipped[spaceId]; !seen {
				skipped[spaceId] = struct{}{}
				log.Debug("reconcile: no engine for space, skipping its keys", zap.String("spaceId", spaceId))
			}
			return true, nil
		}
		st := states[key]
		if st == nil {
			st = &keyState{spaceId: spaceId, objectId: objectId}
			states[key] = st
			order = append(order, key)
		}
		for _, kv := range values {
			raw, decErr := decryptor(kv)
			if decErr != nil {
				log.Warn("reconcile: decrypt", zap.String("key", key), zap.Error(decErr))
				continue
			}
			var val frontierValue
			if decErr = json.Unmarshal(raw, &val); decErr != nil {
				log.Warn("reconcile: decode", zap.String("key", key), zap.Error(decErr))
				continue
			}
			if kv.PeerId == s.selfPeerId {
				st.ownHeads = val.Heads
			}
			if len(val.Heads) == 0 {
				continue
			}
			s.merge(ctx, mergeJob{spaceId: spaceId, objectId: objectId, heads: val.Heads})
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	// Frontier reads and republishes run after the full iteration: every
	// row has merged by now, so the published row is the converged union
	// — and republishing writes to the same store Iterate was reading.
	republished := 0
	for _, key := range order {
		st := states[key]
		eng := s.engineFor(st.spaceId)
		if eng == nil {
			continue
		}
		heads, _, ferr := eng.Frontier(ctx, st.objectId)
		if ferr != nil {
			return ferr
		}
		if len(heads) == 0 || sameSet(heads, st.ownHeads) {
			continue
		}
		republished++
		log.Debug("reconcile: republish divergent frontier",
			zap.String("key", key),
			zap.Int("frontier", len(heads)),
			zap.Int("published", len(st.ownHeads)))
		s.publish(ctx, st.spaceId, st.objectId, heads)
	}
	if republished > 0 {
		log.Info("reconcile: republished divergent frontiers",
			zap.Int("republished", republished), zap.Int("keys", len(order)))
	}
	return nil
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, s := range a {
		set[s] = struct{}{}
	}
	for _, s := range b {
		if _, ok := set[s]; !ok {
			return false
		}
	}
	return true
}

// PruneSpace publishes a deletion watermark for the space's read/ keys
// when any visible frontier row remains — the cleanup for removed
// spaces (SYN-104). Joining re-seeds read state, so removed spaces'
// frontiers have no restore value; the watermark physically drops them
// on every replica. Idempotent and self-clearing: once applied the
// prefix reads empty and further calls are no-ops, and a row published
// later by a device that had not yet seen the removal makes the next
// reconcile pass re-issue the watermark.
func (s *Service) PruneSpace(ctx context.Context, spaceId string) error {
	store, err := s.kvStore(ctx)
	if err != nil {
		return err
	}
	prefix := keyPrefix + spaceId + "/"
	// A watermark drops only rows strictly older than itself, and ours is
	// stamped with the local clock — so only rows older than now are
	// coverable. Rows stamped ahead of our clock (a skewed writer) would
	// survive any watermark we issue; re-issuing for them every pass is
	// pure churn, and they fall due once wall time passes their stamp.
	nowMicro := time.Now().UnixMicro()
	var coverable bool
	err = store.GetAll(ctx, prefix, func(_ keyvaluestorage.Decryptor, values []innerstorage.KeyValue) error {
		for _, kv := range values {
			if kv.TimestampMicro < nowMicro {
				coverable = true
				break
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !coverable {
		return nil
	}
	log.Info("pruning read frontiers of removed space", zap.String("spaceId", spaceId))
	err = store.DeletePrefix(ctx, prefix)
	if errors.Is(err, keyvaluestorage.ErrCoveredByWatermark) {
		// An existing watermark is stamped ahead of our clock: any row it
		// left visible is newer still, so a watermark of ours could not
		// cover it either. Benign — a device with a further clock (or
		// simply later wall time) completes the prune.
		return nil
	}
	return err
}

// ErrUntracked is returned by marks when EngineFor yields no engine:
// the space tracks no datasets, or the liveness gate dropped it
// (deleted / pending / unknown at mark time). spaceimpl maps it to
// the public space.ErrSpaceNotTracked.
var ErrUntracked = errors.New("readsync: space read state not tracked")
