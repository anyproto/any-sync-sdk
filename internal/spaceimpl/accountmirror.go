package spaceimpl

import (
	"context"
	"errors"
	"sync"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/anyenc/anyencutil"

	"github.com/anyproto/any-sync-sdk/internal/accountvalues"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/subscribe"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/internal/types"
)

// accountMirror applies the tech-space carrier's converged
// account-scoped values into this space's objects rows. One mirror per
// loaded spaceId. See docs/scoped-properties-proposal.md § "Account
// transport".
//
// Three triggers, all funnelling into the same state-based reconcile
// (accountvalues.Diff is idempotent, so overlap is free):
//
//   - start: full reconcile — the state-based re-mirror that replays
//     anything that converged in tech space while this space was
//     unloaded, including unsets via retained `_ver` entries;
//   - carrier events: an unbounded sub on the carrier's dataset via
//     the TECH store's engine; events coalesce into full reconciles
//     (same pattern as spaceIndexWatcher). Sub close (overflow /
//     shutdown) ends the loop — the next space load re-mirrors;
//   - target row events: row created ⇒ replay that object's pending
//     carrier record; row tombstoned ⇒ GC its carrier records.
//
// Mirror applies are InjectedSet batches grouped by carrier version —
// never a single max-version claim — and always go through the
// object's CACHED controller (store.Get), keeping the _meta watermark
// mirrors consistent.
type accountMirror struct {
	store   *spaceobjects.Store
	tsp     *techspace.Service
	spaceId string

	sub           *subscribe.Sub
	cancelRowEvts func()
	rowEvts       chan spaceobjects.RowEvent

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// newAccountMirror derives the carrier (idempotent), subscribes to its
// dataset, runs the initial re-mirror, and starts the loops. Returns
// nil when the carrier can't be derived (tech space closed) — the
// mirror is best-effort and the next space load retries.
func newAccountMirror(ctx context.Context, store *spaceobjects.Store, tsp *techspace.Service, spaceId string) *accountMirror {
	carrier, err := tsp.AccountValuesObject(ctx, spaceId)
	if err != nil {
		return nil
	}
	sub, _ := tsp.Store().SubEngine().Subscribe(subscribe.SubConfig{
		Scope: subscribe.Scope{
			ObjectId: carrier.Id(),
			Dataset:  accountvalues.Dataset,
		},
	}, func(yield func(id string, doc *anyenc.Value)) error { return nil })

	m := &accountMirror{
		store:   store,
		tsp:     tsp,
		spaceId: spaceId,
		sub:     sub,
		rowEvts: make(chan spaceobjects.RowEvent, 64),
		stopCh:  make(chan struct{}),
	}
	// Row events arrive synchronously on the apply path — hand them to
	// the loop through a buffered channel; a full channel degrades to a
	// queued full reconcile (the kick below) instead of blocking apply.
	m.cancelRowEvts = store.SubscribeRowEvents(func(ev spaceobjects.RowEvent) {
		select {
		case m.rowEvts <- ev:
		default:
		}
	})

	m.reconcileAll(ctx)
	m.wg.Add(2)
	go m.carrierLoop()
	go m.rowLoop()
	return m
}

func (m *accountMirror) stop() {
	m.stopOnce.Do(func() {
		if m.cancelRowEvts != nil {
			m.cancelRowEvts()
		}
		if m.sub != nil {
			_ = m.sub.Close()
		}
		close(m.stopCh)
	})
	m.wg.Wait()
}

// carrierLoop coalesces carrier-change events into full reconciles.
func (m *accountMirror) carrierLoop() {
	defer m.wg.Done()
	if m.sub == nil {
		return
	}
	mb := m.sub.Events()
	for {
		select {
		case <-m.stopCh:
			return
		default:
		}
		if _, err := mb.Wait(context.Background()); err != nil {
			return // closed on stop / overflow — next space load re-mirrors
		}
		m.reconcileAll(context.Background())
	}
}

// rowLoop serves target-row lifecycle events: created rows replay
// their pending carrier record, tombstoned rows GC their carrier
// records in tech space.
func (m *accountMirror) rowLoop() {
	defer m.wg.Done()
	for {
		select {
		case <-m.stopCh:
			return
		case ev := <-m.rowEvts:
			ctx := context.Background()
			if ev.Deleted {
				_ = m.tsp.DeleteAccountValuesForObject(ctx, m.spaceId, ev.ObjectId)
				continue
			}
			m.reconcileObject(ctx, ev.ObjectId)
		}
	}
}

// reconcileAll is the state-based re-mirror: walk every carrier
// record, diff against its target row, inject what's missing. Carrier
// GC for tombstoned targets is collected during the walk and executed
// AFTER it — DeleteAccountValuesForObject writes the same collection
// the walk iterates.
func (m *accountMirror) reconcileAll(ctx context.Context) {
	resolve := m.newResolver()
	var gc []string
	_ = m.tsp.IterAccountValues(ctx, m.spaceId, func(rec *anyenc.Value) error {
		objectId, dataset, _, ok := accountvalues.ParseKey(rec.GetString(crdt.IdField))
		if !ok || dataset != properties.Dataset {
			return nil // v1 mirrors the objects rows only
		}
		switch m.mirrorRecord(ctx, objectId, rec, resolve) {
		case targetTombstoned:
			gc = append(gc, objectId)
		}
		return nil
	})
	for _, objectId := range gc {
		_ = m.tsp.DeleteAccountValuesForObject(ctx, m.spaceId, objectId)
	}
}

// reconcileObject replays one object's pending carrier record (the
// row-created trigger).
func (m *accountMirror) reconcileObject(ctx context.Context, objectId string) {
	rec, err := m.tsp.GetAccountValues(ctx, m.spaceId, accountvalues.Key(objectId, properties.Dataset, objectId))
	if err != nil || rec == nil {
		return
	}
	if m.mirrorRecord(ctx, objectId, rec, m.newResolver()) == targetTombstoned {
		_ = m.tsp.DeleteAccountValuesForObject(ctx, m.spaceId, objectId)
	}
}

type mirrorOutcome int

const (
	mirrorDone mirrorOutcome = iota
	targetAbsent
	targetTombstoned
)

// mirrorRecord diffs one carrier record against its target row and
// injects the resulting batches. Absent rows are skipped (the carrier
// record is the replay log; the row-created trigger replays it);
// tombstoned rows are reported for carrier GC.
func (m *accountMirror) mirrorRecord(ctx context.Context, objectId string, carrierRec *anyenc.Value, resolve accountvalues.ScopeResolver) mirrorOutcome {
	target, state := m.readTargetRow(ctx, objectId)
	switch state {
	case targetAbsent:
		return targetAbsent
	case targetTombstoned:
		return targetTombstoned
	}
	arena := &anyenc.Arena{}
	batches := accountvalues.Diff(arena, carrierRec, target, resolve)
	if len(batches) == 0 {
		return mirrorDone
	}
	obj, err := m.store.Get(ctx, objectId)
	if err != nil {
		return mirrorDone // tree unavailable — next reconcile retries
	}
	for _, b := range batches {
		_, _ = obj.InjectedSet(ctx, crdt.Change{
			Dataset:     properties.Dataset,
			DataVersion: properties.HandlerVersion,
			VersionId:   b.VersionId,
			Records: []crdt.RecordChange{{
				Id:  objectId,
				Ops: b.Ops,
			}},
		})
	}
	return mirrorDone
}

// readTargetRow loads the object's row from the shared objects
// collection. Returned value is cloned (safe to retain through the
// Diff).
func (m *accountMirror) readTargetRow(ctx context.Context, objectId string) (*anyenc.Value, mirrorOutcome) {
	coll, err := m.store.SharedObjects(ctx)
	if err != nil {
		return nil, targetAbsent
	}
	doc, err := coll.FindId(ctx, objectId)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return nil, targetAbsent
		}
		return nil, targetAbsent
	}
	v := doc.Value()
	if v == nil {
		return nil, targetAbsent
	}
	if v.Get(crdt.DeletedAtField) != nil {
		return nil, targetTombstoned
	}
	var cloned anyencutil.Value
	cloned.FillCopy(v)
	return cloned.Value, mirrorDone
}

// newResolver snapshots the registry into a memoizing ScopeResolver
// for one reconcile pass.
func (m *accountMirror) newResolver() accountvalues.ScopeResolver {
	reg := m.store.Registry()
	memo := make(map[string]map[string]types.PropInfo)
	return func(typeId, propId string) (schema.Scope, bool) {
		byId, ok := memo[typeId]
		if !ok {
			props, known := reg.PropsOf(typeId)
			if !known {
				memo[typeId] = nil
				return 0, false
			}
			byId = make(map[string]types.PropInfo, len(props))
			for _, pi := range props {
				byId[pi.Id] = pi
			}
			memo[typeId] = byId
		}
		if byId == nil {
			return 0, false
		}
		pi, found := byId[propId]
		if !found {
			return 0, false
		}
		return pi.EffectiveScope(), true
	}
}

// Compile-time check: accountMirror satisfies stopper for the
// watcherRegistry.
var _ stopper = (*accountMirror)(nil)
