package spaceobjects

import (
	"context"
	"fmt"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/subscribe"
	"github.com/anyproto/any-sync-sdk/internal/types"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
)

// gateFor returns the apply-time DataVersion gate for objectId. The
// gate parses the change's DataVersion, asks the registry whether
// each (typeId, shortId) is known, and parks the change when any is
// missing. The legacy hardcoded handler-version strings (e.g.
// "systemPropertyHandler-v1") parse-fail and are treated as
// unconstrained — the gate lets them through.
func (s *Store) gateFor(objectId string) object.ApplyGate {
	return func(ctx context.Context, ch *crdt.Change, raw []byte) (bool, error) {
		pairs, err := types.ParseDataVersion(ch.DataVersion)
		if err != nil {
			// Legacy / unknown DataVersion shape — pass through.
			return true, nil
		}
		if len(pairs) == 0 {
			return true, nil
		}
		var missing []types.DataVersionPair
		for _, p := range pairs {
			known, kerr := s.reg.KnownShortId(ctx, p.TypeId, p.ShortId)
			if kerr != nil {
				return false, fmt.Errorf("gate: KnownShortId %s/%s: %w", p.TypeId, p.ShortId, kerr)
			}
			if !known {
				missing = append(missing, p)
			}
		}
		if len(missing) == 0 {
			return true, nil
		}
		row := DetachedRow{
			ChangeId:  ch.ChangeId,
			SpaceId:   ch.SpaceId,
			ObjectId:  objectId,
			AddSeq:    ch.AddSeq,
			OrderId:   string(ch.VersionId),
			Timestamp: ch.Timestamp,
			Payload:   append([]byte(nil), raw...),
			Pending:   missing,
		}
		if perr := s.Park(ctx, row); perr != nil {
			return false, fmt.Errorf("gate: park: %w", perr)
		}
		return false, nil
	}
}

// afterApplyFor is the post-apply hook. Two independent fan-outs
// run off every successful change:
//
//  1. engine.OnApply — gated by HasSubscribers (single atomic load),
//     so the cold-restore path stays free when nobody is listening.
//     Routes to windowed Query.Subscribe consumers.
//
//  2. drainer.Notify — only fires for typetype.PropertyHandler
//     writes (those land shortIds and may unblock parked changes).
//     Decoupled from the apply lock to avoid the o.mu re-entry
//     deadlock that synchronous Drain previously hit.
//
// applySeqOf extracts the apply sequence the controller allocated for
// this change. ApplyChangeWithResult takes the Change by value, so the
// allocation only travels back through the result.
func applySeqOf(res *crdt.ApplyResult) uint64 {
	if res == nil {
		return 0
	}
	return res.ApplySeq
}

func (s *Store) afterApplyFor() object.AfterApply {
	return func(ctx context.Context, obj *object.Object, ch *crdt.Change, res *crdt.ApplyResult) {
		if ch == nil {
			return
		}
		// Resolve record ids once — both the event build (for the
		// $set/$unset projection) and the drainer (for shortId-keyed
		// wakeups) want them. Resolution can fail on malformed input;
		// when it does we still feed the drainer a generic wakeup to
		// avoid stuck parked changes.
		ids, idsErr := crdt.ResolveRecordIds(*ch)

		if s.engine != nil && s.engine.HasSubscribers() {
			rowIds, postValue := s.postValueFor(ctx, obj, ch, ids)
			var derivedOps [][]crdt.Op
			if res != nil {
				derivedOps = res.DerivedOps
			}
			ev := subscribe.BuildEvent(ch, rowIds, derivedOps, postValue)
			s.engine.OnApply(ev, postValue)
		}

		// Change-index feed: one notification per applied change across
		// every dataset (base content, properties, type defs). Gated on
		// hasSubscribers so the cold-restore / catch-up path stays free
		// when no indexer is attached.
		if s.changeSubs.HasSubscribers() {
			s.changeSubs.Dispatch(ObjectChange{ObjectId: ch.ObjectId, ApplySeq: applySeqOf(res)})
		}

		// Read-state ping: the apply hook already recorded any unread
		// entry inside the committed tx; this fires the best-effort
		// subscriber notification. Over-notifies (an apply whose
		// classification changed nothing still pings) — consumers pull
		// TransitionsSince and see an empty diff.
		if s.readState != nil && s.readTracking[ch.Dataset] != nil && !ch.Local && !ch.Injected {
			s.readState.NotifyState(ch.ObjectId, applySeqOf(res))
		}

		// Row lifecycle events for the objects collection: creation is
		// detected via the synthetic _ver.id derived op the apply path
		// emits exactly once per record; deletion via a delete op in the
		// change. Both key the account mirror's replay/GC.
		if ch.Dataset == properties.Dataset && s.rowEvents.HasSubscribers() {
			for i, rc := range ch.Records {
				rid := ch.ObjectId // shared collection: row id = objectId
				if hasDeleteOp(rc.Ops) {
					s.rowEvents.Dispatch(RowEvent{ObjectId: rid, Deleted: true})
					continue
				}
				if res != nil && i < len(res.DerivedOps) && hasCreationMarker(res.DerivedOps[i]) {
					s.rowEvents.Dispatch(RowEvent{ObjectId: rid, Deleted: false})
				}
			}
		}

		if ch.Dataset != typetype.DatasetPropertyDefs {
			return
		}
		if idsErr != nil {
			s.drainer.Notify(types.DataVersionPair{TypeId: ch.ObjectId})
			return
		}
		for _, id := range ids {
			s.drainer.Notify(types.DataVersionPair{TypeId: ch.ObjectId, ShortId: id})
		}
	}
}

// postValueFor returns the row-id slice and the per-record post-apply
// lookup the dispatcher needs to project $inc / $addToSet / etc. ops
// down to a $set against the merged value.
//
// Shared datasets (per-space `objects`): the controller stores all
// rows under ch.ObjectId regardless of the change's RecordChange.Id;
// we mirror that here so the wire's EventRecord.Id matches what a
// follow-up Query on the same dataset returns. Per-object datasets
// keep the resolved RecordChange ids untouched.
func (s *Store) postValueFor(ctx context.Context, obj *object.Object, ch *crdt.Change, ids []string) ([]string, subscribe.PostValueFn) {
	// The Object is handed in by afterApply directly — DO NOT do a
	// cache lookup here. afterApply runs from inside the LoadFunc on
	// a fresh joiner (synctree's afterBuild → Rebuild → replayLocked
	// → applyDecodedLocked); any cache.Pick / cache.Get on the same
	// id would block on the load channel that hasn't closed yet,
	// producing a self-recursive deadlock that any-sync surfaces as
	// `panic: app.Close timeout`.
	if obj == nil {
		return ids, nil
	}
	ctrl := obj.Controller()
	if ctrl == nil {
		return ids, nil
	}
	rowIds := ids
	if ctrl.IsShared(ch.Dataset) {
		rowIds = make([]string, len(ids))
		for i := range rowIds {
			rowIds[i] = ch.ObjectId
		}
	}
	return rowIds, func(i int) *anyenc.Value {
		if i < 0 || i >= len(rowIds) {
			return nil
		}
		rowId := rowIds[i]
		if rowId == "" {
			return nil
		}
		return ctrl.Get(ctx, ch.Dataset, rowId)
	}
}

// Drain scans the detached collection, re-checks each parked
// change's pending list, and replays any that are now satisfied.
// Idempotent — call as often as you like: passes are serialized, so
// two callers can't both collect a row before either unparks it and
// double-replay it (a replayed change re-tracks as unread if the user
// read it between the deliveries).
func (s *Store) Drain(ctx context.Context) error {
	s.drainMu.Lock()
	defer s.drainMu.Unlock()
	var ready []DetachedRow
	if err := s.IterDetached(ctx, func(row DetachedRow) bool {
		all, err := s.allPendingKnown(ctx, row.Pending)
		if err != nil {
			return true
		}
		if all {
			ready = append(ready, row)
		}
		return true
	}); err != nil {
		return err
	}
	for _, row := range ready {
		if err := s.replayParked(ctx, row); err != nil {
			// Don't surface — keep draining the rest. A stuck row
			// will be retried on the next drain pass.
			continue
		}
		_ = s.Unpark(ctx, row.ChangeId)
	}
	return nil
}

// allPendingKnown reports whether every (typeId, shortId) in pending
// is now in the registry. Empty pending → trivially true.
func (s *Store) allPendingKnown(ctx context.Context, pending []types.DataVersionPair) (bool, error) {
	for _, p := range pending {
		ok, err := s.reg.KnownShortId(ctx, p.TypeId, p.ShortId)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

// replayParked decodes the parked payload, fills the envelope from
// the row, and applies through the source object — bypassing the
// gate so we don't immediately re-park.
func (s *Store) replayParked(ctx context.Context, row DetachedRow) error {
	codec := object.NewCodec()
	decoded, err := codec.Decode(row.Payload)
	if err != nil {
		return fmt.Errorf("drain: decode %s: %w", row.ChangeId, err)
	}
	decoded.SpaceId = row.SpaceId
	decoded.ObjectId = row.ObjectId
	decoded.ChangeId = row.ChangeId
	decoded.AddSeq = row.AddSeq
	decoded.Timestamp = row.Timestamp
	decoded.VersionId = crdt.VersionId(row.OrderId)

	obj, err := s.Get(ctx, row.ObjectId)
	if err != nil {
		return fmt.Errorf("drain: get %s: %w", row.ObjectId, err)
	}
	return obj.ApplyDecoded(ctx, decoded)
}

// hasDeleteOp reports whether ops contains a record-level delete.
func hasDeleteOp(ops []crdt.Op) bool {
	for _, op := range ops {
		if op.Type == crdt.OpDelete {
			return true
		}
	}
	return false
}

// hasCreationMarker reports whether the apply emitted the one-shot
// synthetic `_ver.id` stamp for this record — the unambiguous "this
// change created the row" signal (see subscribe.projectRecords).
func hasCreationMarker(derived []crdt.Op) bool {
	for _, op := range derived {
		if op.Type == crdt.OpSet && len(op.Path) == 2 &&
			op.Path[0] == crdt.VersionsKey && op.Path[1] == crdt.IdField {
			return true
		}
	}
	return false
}
