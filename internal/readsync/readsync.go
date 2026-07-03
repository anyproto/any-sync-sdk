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
	// no locks of its own.
	mu    sync.Mutex
	objMu map[string]*sync.Mutex

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
	s := &Service{
		engineFor:  engineFor,
		kvStore:    kvStore,
		selfPeerId: selfPeerId,
		objMu:      map[string]*sync.Mutex{},
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

func (s *Service) lockObject(objectId string) func() {
	s.mu.Lock()
	m := s.objMu[objectId]
	if m == nil {
		m = &sync.Mutex{}
		s.objMu[objectId] = m
	}
	s.mu.Unlock()
	m.Lock()
	return m.Unlock
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

// MarkReadUpTo covers everything at or below upTo in local display
// order ("" = all), then publishes.
func (s *Service) MarkReadUpTo(ctx context.Context, spaceId, objectId, upTo string) (readstate.MarkResult, error) {
	return s.mark(ctx, spaceId, objectId, func(eng *readstate.Engine, txCtx context.Context) (readstate.MarkResult, error) {
		return eng.MarkReadUpTo(txCtx, objectId, upTo)
	})
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
	}
	return res, nil
}

// publish writes this device's frontier row. Best-effort: the local
// mark already committed, and an unpublished frontier is re-derivable
// (the next mark republishes a superset).
func (s *Service) publish(ctx context.Context, spaceId, objectId string, heads []string) {
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
	err := eng.WriteTx(ctx, func(txCtx context.Context) error {
		_, mergeErr := eng.MergeHeads(txCtx, job.objectId, job.heads)
		return mergeErr
	})
	if err != nil {
		log.Warn("merge frontier", zap.String("objectId", job.objectId), zap.Error(err))
	}
}

// Reconcile replays every frontier published for spaceId through the
// idempotent merge — the durable complement to the best-effort live
// hook. Call on boot after the space's store is up. Already-merged
// frontiers cost one state read each (engine fast path).
func (s *Service) Reconcile(ctx context.Context, spaceId string) error {
	eng := s.engineFor(spaceId)
	if eng == nil {
		return nil
	}
	store, err := s.kvStore(ctx)
	if err != nil {
		return err
	}
	prefix := keyPrefix + spaceId + "/"
	return store.Iterate(ctx, func(decryptor keyvaluestorage.Decryptor, key string, values []innerstorage.KeyValue) (bool, error) {
		if !strings.HasPrefix(key, prefix) {
			return true, nil
		}
		_, objectId, ok := parseKey(key)
		if !ok {
			return true, nil
		}
		for _, kv := range values {
			if kv.PeerId == s.selfPeerId {
				continue
			}
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
			if len(val.Heads) == 0 {
				continue
			}
			s.merge(ctx, mergeJob{spaceId: spaceId, objectId: objectId, heads: val.Heads})
		}
		return true, nil
	})
}

// ErrUntracked is returned by marks on a space with no tracked datasets.
var ErrUntracked = errors.New("readsync: space tracks no datasets")
