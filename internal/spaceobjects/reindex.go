package spaceobjects

import (
	"context"
	"errors"
	"fmt"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// Re-index — rebuilding an object's materialized rows from its DAG.
//
// Every apply stamps dataset→handler version onto the object's _meta row.
// A handler whose compiled-in logic changes bumps its HandlerReg.Version;
// the Controller reports the dataset stale at construction
// (crdt/reindex.go) and the load path rebuilds the object before anything
// reads it: wipe the materialized rows, rewind the watermark, and let the
// existing cold restore replay the whole tree through the current
// handlers.
//
// Lazy, per object, on load — not an eager account-wide sweep. The work is
// the cold-restore work, paid on first touch, on a path that opens the
// tree anyway.
//
// applySeq keeps climbing across a rebuild (the allocator seeds past the
// space's high-water mark), so consumer cursors stay valid: a re-applied
// row simply surfaces again as changed. That is why a rebuild does not
// mint a new space Generation — only a wiped sdk.db, which restarts the
// axis at zero, does.
//
// What a replay cannot reproduce is local-scope state: it never entered
// the DAG. Those leaves are captured before the wipe and re-applied after
// the replay. Account-scope values need no capture — the mirror replays
// its carrier records when the row re-materializes. Read-tracking flags
// need none either: the materializer recomputes them from unread entries
// and self-heals.
const (
	// reindexLocalLeafCap bounds the local-scope capture. Past it the
	// rebuild proceeds and the excess leaves are lost — a bounded,
	// logged loss beats an unbounded read on a boot path.
	reindexLocalLeafCap = 20000
)

// Sweep pacing. Vars, not consts, so tests don't wait on them.
var (
	// reindexSweepStartDelay keeps the sweep off the boot path's back:
	// the first seconds after a space store appears belong to catch-up
	// and the caller's own first reads.
	reindexSweepStartDelay = 2 * time.Second
	// reindexSweepPace spaces out the loads. A rebuild is a full tree
	// replay, so the sweep is deliberately unhurried — nothing waits on
	// it, and the lazy path still rebuilds on demand whatever it has not
	// reached yet.
	reindexSweepPace = 50 * time.Millisecond
)

// localLeaf is one captured local-scope value. The document it came from
// is only valid during iteration, so the value travels as its own bytes.
type localLeaf struct {
	dataset  string
	recordId string
	path     []string
	value    []byte
}

// reindexPrepare captures local-scope state, rewinds the controller and
// wipes the object's materialized rows. The caller then cold-restores,
// which replays the whole tree, and finally calls reindexFinish.
//
// The rewind is persisted before the wipe: a crash in between must leave
// a watermark of zero over missing rows, so the next load rebuilds again.
// The reverse order would strand rows that no watermark asks for.
func (s *Store) reindexPrepare(ctx context.Context, objectId string, ctrl *crdt.Controller, stale []string) ([]localLeaf, error) {
	storeLog.Info("reindex: rebuilding object",
		zap.String("objectId", objectId),
		zap.Strings("datasets", stale))
	leaves := s.captureLocalLeaves(ctx, objectId, ctrl)
	if err := ctrl.ResetForReindex(ctx); err != nil {
		return nil, fmt.Errorf("spaceobjects: reindex rewind %s: %w", objectId, err)
	}
	s.wipeMaterialized(ctx, objectId, ctrl)
	return leaves, nil
}

// reindexFinish re-applies the captured local leaves and stamps the
// current handler versions. The stamp matters for an object whose replay
// re-applied nothing — without it the object would be found stale on
// every load, rebuilding forever.
func (s *Store) reindexFinish(ctx context.Context, obj *object.Object, ctrl *crdt.Controller, leaves []localLeaf) {
	s.restoreLocalLeaves(ctx, obj, leaves)
	if err := ctrl.PersistVersions(ctx); err != nil {
		storeLog.Warn("reindex: persist handler versions",
			zap.String("objectId", ctrl.ObjectId()), zap.Error(err))
	}
}

// captureLocalLeaves reads the object's local-scope values. Best-effort:
// a read failure costs those leaves, not the rebuild.
func (s *Store) captureLocalLeaves(ctx context.Context, objectId string, ctrl *crdt.Controller) []localLeaf {
	var out []localLeaf
	for _, dataset := range ctrl.RegisteredDatasets() {
		var err error
		if ctrl.IsShared(dataset) {
			// One row per object in a per-space collection: read this
			// object's row directly, and resolve the dynamic per-property
			// heads through the type registry (the objects dataset carries
			// per-property scopes the schema can't declare).
			coll := ctrl.Collection(ctx, dataset)
			if coll == nil {
				continue // never materialized
			}
			out, err = s.captureSharedRow(ctx, coll, objectId, dataset, ctrl, out)
		} else {
			fields := ctrl.LocalFields(dataset)
			if len(fields) == 0 {
				continue
			}
			// Deliberately NOT ctrl.Collection: that caches the handle on
			// the controller, and the wipe drops the collection out from
			// under it — the replay would then write through a dead
			// handle. This one is opened, read and closed right here.
			coll, oerr := s.OpenObjectCollection(ctx, objectId, dataset)
			if oerr != nil {
				continue // never materialized
			}
			out, err = captureRecordFields(ctx, coll, dataset, fields, out)
			if cerr := coll.Close(); cerr != nil {
				storeLog.Warn("reindex: close capture handle",
					zap.String("objectId", objectId), zap.String("dataset", dataset), zap.Error(cerr))
			}
		}
		if err != nil {
			storeLog.Warn("reindex: capture local values",
				zap.String("objectId", objectId), zap.String("dataset", dataset), zap.Error(err))
		}
		if len(out) >= reindexLocalLeafCap {
			storeLog.Warn("reindex: local-value capture truncated; excess device-local values are lost",
				zap.String("objectId", objectId), zap.Int("cap", reindexLocalLeafCap))
			return out
		}
	}
	return out
}

// captureSharedRow captures the object's own row in a shared collection:
// declared local fields plus every per-property leaf the type registry
// marks local.
func (s *Store) captureSharedRow(ctx context.Context, coll anystore.Collection, objectId, dataset string, ctrl *crdt.Controller, out []localLeaf) ([]localLeaf, error) {
	doc, err := coll.FindId(ctx, objectId)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return out, nil
		}
		return out, err
	}
	v := doc.Value()
	for _, field := range ctrl.LocalFields(dataset) {
		if leaf := v.Get(field); leaf != nil {
			out = append(out, localLeaf{dataset: dataset, recordId: objectId,
				path: []string{field}, value: leaf.MarshalTo(nil)})
		}
	}
	if s.reg == nil {
		return out, nil
	}
	obj, err := v.Object()
	if err != nil {
		return out, nil
	}
	var typeIds []string
	obj.Visit(func(key []byte, item *anyenc.Value) {
		if item.Type() == anyenc.TypeObject {
			typeIds = append(typeIds, string(key))
		}
	})
	for _, typeId := range typeIds {
		props, ok := s.reg.PropsOf(typeId)
		if !ok {
			continue
		}
		for _, p := range props {
			if p.Scope != schema.ScopeLocal {
				continue
			}
			leaf := v.Get(typeId, p.Id)
			if leaf == nil {
				continue
			}
			out = append(out, localLeaf{dataset: dataset, recordId: objectId,
				path: []string{typeId, p.Id}, value: leaf.MarshalTo(nil)})
		}
	}
	return out, nil
}

// captureRecordFields captures one declared local field across every
// record of a per-object dataset that carries it.
func captureRecordFields(ctx context.Context, coll anystore.Collection, dataset string, fields []string, out []localLeaf) ([]localLeaf, error) {
	for _, field := range fields {
		filter := query.Key{Path: []string{field}, Filter: query.Exists{}}
		iter, err := coll.Find(filter).Iter(ctx)
		if err != nil {
			return out, err
		}
		for iter.Next() {
			doc, derr := iter.Doc()
			if derr != nil {
				_ = iter.Close()
				return out, derr
			}
			v := doc.Value()
			leaf := v.Get(field)
			if leaf == nil {
				continue
			}
			out = append(out, localLeaf{
				dataset:  dataset,
				recordId: string(v.GetStringBytes("id")),
				path:     []string{field},
				value:    leaf.MarshalTo(nil),
			})
			if len(out) >= reindexLocalLeafCap {
				break
			}
		}
		err = iter.Err()
		_ = iter.Close()
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

// wipeMaterialized removes everything the replay will rebuild: the
// object's row in each shared collection, its per-object dataset
// collections (the `<objectId>_*` sweep, which takes the per-object
// history collection with it), and its space-level history rows.
//
// Deliberately not the deletion path: no `del` stamp, no Removed events.
// The object is being rebuilt, not deleted, and a del stamp is sticky —
// it would evict the object from every consumer index permanently.
func (s *Store) wipeMaterialized(ctx context.Context, objectId string, ctrl *crdt.Controller) {
	for _, dataset := range ctrl.RegisteredDatasets() {
		if !ctrl.IsShared(dataset) {
			continue
		}
		coll := ctrl.Collection(ctx, dataset)
		if coll == nil {
			continue
		}
		if err := coll.DeleteId(ctx, objectId); err != nil && !errors.Is(err, anystore.ErrDocNotFound) {
			storeLog.Warn("reindex: clear shared row",
				zap.String("objectId", objectId), zap.String("dataset", dataset), zap.Error(err))
		}
	}
	s.dropObjectCollections(ctx, objectId)
	s.purgeHistoryForReindex(ctx, objectId)
}

// purgeHistoryForReindex clears the object's space-level history rows
// (trace rows, the stale-flag row) when the index is open. Deliberately
// NOT purgeHistoryRows: that one queues a deferred purge when the index
// is unavailable, and a deferred purge firing later would wipe the rows
// the post-replay backfill has just rebuilt. The per-object history
// collection went with the `<objectId>_*` sweep either way, and the load
// path marks the object stale after the replay, so the backfill rewrites
// whatever is left.
func (s *Store) purgeHistoryForReindex(ctx context.Context, objectId string) {
	ix := s.historyIx.Load()
	if ix == nil {
		return
	}
	if err := ix.PurgeObject(ctx, objectId); err != nil {
		storeLog.Warn("reindex: history index cleanup",
			zap.String("objectId", objectId), zap.Error(err))
	}
}

// restoreLocalLeaves re-applies captured local values through the normal
// LocalSet route, one change per dataset. Best-effort: a failure costs
// device-local state that no peer can restore, so it is logged loudly and
// never fails the load.
func (s *Store) restoreLocalLeaves(ctx context.Context, obj *object.Object, leaves []localLeaf) {
	if len(leaves) == 0 {
		return
	}
	// dataset → recordId → ops, preserving first-seen record order.
	byDataset := map[string][]crdt.RecordChange{}
	index := map[string]map[string]int{}
	for _, leaf := range leaves {
		val, err := anyenc.Parse(leaf.value)
		if err != nil {
			storeLog.Warn("reindex: decode captured local value",
				zap.String("dataset", leaf.dataset), zap.String("recordId", leaf.recordId), zap.Error(err))
			continue
		}
		op := crdt.Op{Type: crdt.OpSet, Path: leaf.path, Payload: val}
		recs := index[leaf.dataset]
		if recs == nil {
			recs = map[string]int{}
			index[leaf.dataset] = recs
		}
		if i, ok := recs[leaf.recordId]; ok {
			byDataset[leaf.dataset][i].Ops = append(byDataset[leaf.dataset][i].Ops, op)
			continue
		}
		recs[leaf.recordId] = len(byDataset[leaf.dataset])
		byDataset[leaf.dataset] = append(byDataset[leaf.dataset], crdt.RecordChange{
			Id: leaf.recordId, Ops: []crdt.Op{op},
		})
	}
	for dataset, records := range byDataset {
		if _, err := obj.LocalSet(ctx, crdt.Change{
			Dataset:     dataset,
			DataVersion: s.dataVersions[dataset],
			Records:     records,
		}); err != nil {
			storeLog.Warn("reindex: restore local values",
				zap.String("objectId", obj.Id()), zap.String("dataset", dataset), zap.Error(err))
		}
	}
}

// StartReindexSweep rebuilds, in the background, every object this store
// materialized with a stale handler version, instead of waiting for each
// one to be opened.
//
// The lazy path alone would leave the per-space `objects` collection
// mixing rows built by the old handler with rows built by the new one for
// as long as some object stays unopened — and a sort or filter over a
// rebuilt field reads both shapes. The sweep closes that window.
//
// Idempotent, one sweep per store. A store with nothing stale pays one
// _meta scan and stops. Errors are logged, never fatal: whatever the
// sweep misses, the lazy path still rebuilds on first touch.
func (s *Store) StartReindexSweep() {
	if s == nil {
		return
	}
	s.sweepOnce.Do(func() { go s.reindexSweep() })
}

func (s *Store) reindexSweep() {
	select {
	case <-s.sweepStop:
		return
	case <-time.After(reindexSweepStartDelay):
	}
	ctx := context.Background()
	regs, _, err := s.buildRegs()
	if err != nil {
		storeLog.Warn("reindex sweep: build regs", zap.String("spaceId", s.spaceId), zap.Error(err))
		return
	}
	registered := make(map[string]int, len(regs))
	for _, reg := range regs {
		v := reg.Version
		if v == 0 {
			v = 1
		}
		registered[reg.Name] = v
	}
	metaColl, err := s.metaCollection(ctx)
	if err != nil {
		storeLog.Warn("reindex sweep: open _meta", zap.String("spaceId", s.spaceId), zap.Error(err))
		return
	}
	ids, err := crdt.StaleObjects(ctx, metaColl, s.spaceId, registered)
	if err != nil {
		storeLog.Warn("reindex sweep: scan", zap.String("spaceId", s.spaceId), zap.Error(err))
		return
	}
	if len(ids) == 0 {
		return
	}
	storeLog.Info("reindex sweep: rebuilding objects",
		zap.String("spaceId", s.spaceId), zap.Int("objects", len(ids)))
	done := 0
	for _, id := range ids {
		select {
		case <-s.sweepStop:
			storeLog.Info("reindex sweep: stopped early",
				zap.String("spaceId", s.spaceId), zap.Int("done", done), zap.Int("total", len(ids)))
			return
		case <-time.After(reindexSweepPace):
		}
		// Loading is the rebuild: loadObject wipes and replays whatever
		// the version compare found stale.
		if _, err := s.Get(ctx, id); err != nil {
			storeLog.Warn("reindex sweep: load object",
				zap.String("objectId", id), zap.Error(err))
			continue
		}
		done++
	}
	storeLog.Info("reindex sweep: done",
		zap.String("spaceId", s.spaceId), zap.Int("rebuilt", done), zap.Int("total", len(ids)))
}
