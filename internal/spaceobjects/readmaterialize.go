package spaceobjects

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/readstate"
)

// readMaterializer projects read state into queryable local-scope
// fields: per-tag unread counters onto the object's row in the shared
// `objects` dataset (ReadTracking.CounterFields) and per-record unread
// booleans onto the tracked dataset's records (ReadTracking.
// RecordFlags — a flag is true iff at least one unread entry with that
// tag references the record).
//
// Driven by the engine's state pings, debounced and coalesced per
// object on one worker. Each pass recomputes the desired state from
// the unread entries and diffs it against the stored state, so a
// dropped ping self-heals on the next one. Writes go through the
// normal LocalSet route (device-local versions, regular subscribe
// events); Local changes don't ping, so no feedback loop.
const readMaterializeDebounce = 50 * time.Millisecond

type readMaterializer struct {
	store  *Store
	cancel func()

	mu    sync.Mutex
	dirty map[string]struct{}

	kick chan struct{}
	stop chan struct{}
	done chan struct{}
}

// needsReadMaterializer reports whether any tracked dataset declared a
// materialization target.
func needsReadMaterializer(tracking map[string]*crdt.ReadTracking) bool {
	for _, rt := range tracking {
		if len(rt.CounterFields) > 0 || len(rt.RecordFlags) > 0 {
			return true
		}
	}
	return false
}

func newReadMaterializer(s *Store) *readMaterializer {
	m := &readMaterializer{
		store: s,
		dirty: map[string]struct{}{},
		kick:  make(chan struct{}, 1),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	m.cancel = s.readState.SubscribeState(func(objectId string, _ uint64) {
		m.mu.Lock()
		m.dirty[objectId] = struct{}{}
		m.mu.Unlock()
		select {
		case m.kick <- struct{}{}:
		default:
		}
	})
	go m.run()
	return m
}

func (m *readMaterializer) close() {
	m.cancel()
	close(m.stop)
	<-m.done
}

func (m *readMaterializer) run() {
	defer close(m.done)
	for {
		select {
		case <-m.stop:
			return
		case <-m.kick:
		}
		timer := time.NewTimer(readMaterializeDebounce)
		select {
		case <-m.stop:
			timer.Stop()
			return
		case <-timer.C:
		}
		m.mu.Lock()
		batch := m.dirty
		m.dirty = map[string]struct{}{}
		m.mu.Unlock()
		for objectId := range batch {
			if err := m.processObject(context.Background(), objectId); err != nil {
				storeLog.Warn("materialize read state", zap.String("objectId", objectId), zap.Error(err))
			}
		}
	}
}

func (m *readMaterializer) processObject(ctx context.Context, objectId string) error {
	s := m.store
	entries, _, err := s.readState.UnreadEntries(ctx, objectId)
	if err != nil {
		return err
	}
	obj, err := s.Get(ctx, objectId)
	if err != nil {
		return err
	}
	ctrl := obj.Controller()

	// Per-record flags, per tracked dataset that declared them.
	for dataset, rt := range s.readTracking {
		if len(rt.RecordFlags) == 0 {
			continue
		}
		desired := desiredFlagRecords(entries, dataset, rt.RecordFlags)
		for field, want := range desired {
			current, err := recordsWithFlag(ctx, ctrl, dataset, field)
			if err != nil {
				return err
			}
			recs := flagFlipRecords(field, want, current)
			if len(recs) == 0 {
				continue
			}
			if _, err = obj.LocalSet(ctx, crdt.Change{
				Dataset:     dataset,
				DataVersion: s.dataVersions[dataset],
				Records:     recs,
			}); err != nil {
				return err
			}
		}
	}

	// Object-row counters. Raw stores (tech space) have no objects row.
	if s.datasetOwners == nil {
		return nil
	}
	counts, err := s.readState.Counts(ctx, objectId)
	if err != nil {
		return err
	}
	arena := &anyenc.Arena{}
	row := ctrl.Get(ctx, properties.Dataset, objectId)
	var ops []crdt.Op
	for dataset, rt := range s.readTracking {
		typeId := s.datasetOwners[dataset]
		if typeId == "" || len(rt.CounterFields) == 0 {
			continue
		}
		for tag, propId := range rt.CounterFields {
			want := counts[tag]
			if row != nil && row.GetInt(typeId, propId) == want {
				continue
			}
			ops = append(ops, crdt.Op{
				Type:    crdt.OpSet,
				Path:    []string{typeId, propId},
				Payload: arena.NewNumberInt(want),
			})
		}
	}
	if len(ops) == 0 {
		return nil
	}
	_, err = obj.LocalSet(ctx, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: properties.HandlerVersion,
		Records:     []crdt.RecordChange{{Id: objectId, Ops: ops}},
	})
	return err
}

// desiredFlagRecords computes, per flag field, the records that must
// carry it: those referenced by at least one unread entry of the
// field's tag. Every declared field gets a map entry (possibly empty)
// so vanished tags still clear their flags.
func desiredFlagRecords(entries []readstate.Entry, dataset string, flags map[string]string) map[string]map[string]struct{} {
	out := make(map[string]map[string]struct{}, len(flags))
	for _, field := range flags {
		out[field] = map[string]struct{}{}
	}
	for _, en := range entries {
		if en.Dataset != dataset {
			continue
		}
		for _, tag := range en.Tags {
			field := flags[tag]
			if field == "" {
				continue
			}
			for _, rid := range en.RecordIds {
				out[field][rid] = struct{}{}
			}
		}
	}
	return out
}

// flagFlipRecords diffs desired against current and returns the
// LocalSet records: $set field=true for newly-unread, $unset for
// no-longer-unread. Deterministic order for tests.
func flagFlipRecords(field string, desired, current map[string]struct{}) []crdt.RecordChange {
	var ids []string
	for id := range desired {
		if _, ok := current[id]; !ok {
			ids = append(ids, "+"+id)
		}
	}
	for id := range current {
		if _, ok := desired[id]; !ok {
			ids = append(ids, "-"+id)
		}
	}
	sort.Strings(ids)
	arena := &anyenc.Arena{}
	out := make([]crdt.RecordChange, 0, len(ids))
	for _, signed := range ids {
		id := signed[1:]
		op := crdt.Op{Type: crdt.OpUnset, Path: []string{field}}
		if signed[0] == '+' {
			op = crdt.Op{Type: crdt.OpSet, Path: []string{field}, Payload: arena.NewTrue()}
		}
		out = append(out, crdt.RecordChange{Id: id, Ops: []crdt.Op{op}})
	}
	return out
}

// recordsWithFlag returns the ids of records currently carrying the
// flag (indexed when the handler declared an index on the field).
func recordsWithFlag(ctx context.Context, ctrl *crdt.Controller, dataset, field string) (map[string]struct{}, error) {
	coll := ctrl.Collection(ctx, dataset)
	if coll == nil {
		return map[string]struct{}{}, nil
	}
	filter := query.Key{Path: []string{field}, Filter: query.NewComp(query.CompOpEq, true)}
	it, err := coll.Find(filter).Iter(ctx)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := map[string]struct{}{}
	for it.Next() {
		doc, err := it.Doc()
		if err != nil {
			return nil, err
		}
		out[string(doc.Value().GetStringBytes("id"))] = struct{}{}
	}
	return out, nil
}
