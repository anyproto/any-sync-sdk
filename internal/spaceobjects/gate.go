package spaceobjects

import (
	"context"
	"fmt"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
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

// afterApplyFor is the post-apply hook. Notifies the per-Store
// drainer that something landed which may unblock parked changes;
// the drainer goroutine consumes pairs off its mb queue and runs
// Store.Drain off this apply path's locks.
//
// Decoupled from the apply lock for two reasons:
//  1. afterApply runs under o.mu (called from applyDecodedLocked).
//     Drain ultimately calls obj.ApplyDecoded, which retakes o.mu —
//     synchronous Drain re-entered the same Object's lock and
//     deadlocked when a parked row was for the same Object.
//  2. LocalWrite latency no longer absorbs unrelated parked replays.
//
// We only push when the change is a typetype.PropertyHandler write
// (its sibling Project lands shortIds), since that's currently the
// only path that can produce new (typeId, shortId) pairs the gate
// is waiting on. Other dataset writes don't notify — the drainer
// would just scan _detached and find nothing changed.
func (s *Store) afterApplyFor() object.AfterApply {
	return func(_ context.Context, ch *crdt.Change) {
		if ch == nil || ch.Dataset != typetype.DatasetProperties {
			return
		}
		ids, err := crdt.ResolveRecordIds(*ch)
		if err != nil {
			// Resolution failed — fall back to a generic wakeup so
			// the drainer doesn't miss the signal entirely.
			s.drainer.Notify(types.DataVersionPair{TypeId: ch.ObjectId})
			return
		}
		for _, id := range ids {
			s.drainer.Notify(types.DataVersionPair{TypeId: ch.ObjectId, ShortId: id})
		}
	}
}

// Drain scans the detached collection, re-checks each parked
// change's pending list, and replays any that are now satisfied.
// Idempotent — call as often as you like.
func (s *Store) Drain(ctx context.Context) error {
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
