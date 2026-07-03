package spaceobjects

import (
	"context"

	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/readstate"
)

// buildReadTracking collects the read-tracking registrations by
// dataset from the type catalog (regular mode) or the raw handler
// regs (tech space). Empty map = nothing tracked in this space.
func buildReadTracking(extTypes []handler.Type, raw []crdt.HandlerReg) map[string]*crdt.ReadTracking {
	out := map[string]*crdt.ReadTracking{}
	for _, t := range extTypes {
		for _, d := range t.Datasets {
			if d.ReadTracking != nil {
				out[d.Name] = d.ReadTracking
			}
		}
	}
	for _, reg := range raw {
		if reg.ReadTracking != nil {
			out[reg.Name] = reg.ReadTracking
		}
	}
	return out
}

// ReadState returns the per-space read/unread engine, nil when no
// dataset in this space opted into tracking.
func (s *Store) ReadState() *readstate.Engine { return s.readState }

// readResolver backs the engine's gap traversal with any-sync's
// persisted change storage — a point lookup, never a tree load.
func (s *Store) readResolver() readstate.Resolver {
	return func(ctx context.Context, objectId, changeId string) ([]string, string, bool, error) {
		handle, err := s.app.GetSpace(ctx, s.spaceId)
		if err != nil {
			return nil, "", false, err
		}
		st := handle.Inner().Storage()
		if st == nil {
			return nil, "", false, nil
		}
		ts, err := st.TreeStorage(ctx, objectId)
		if err != nil {
			// Unknown tree — nothing synced yet on this device.
			return nil, "", false, nil
		}
		sc, err := ts.Get(ctx, changeId)
		if err != nil {
			// Not arrived; the engine parks it as a pending head.
			return nil, "", false, nil
		}
		return sc.PrevIds, sc.OrderId, true, nil
	}
}

// readApplyHook is the in-tx classification seam installed on every
// controller (crdt.ApplyHook). Nil when the space tracks nothing, so
// untracked spaces pay a single nil check per apply.
func (s *Store) readApplyHook() crdt.ApplyHook {
	if len(s.readTracking) == 0 || s.readState == nil {
		return nil
	}
	return func(txCtx context.Context, ch *crdt.Change, recordIds []string, _ *crdt.ApplyResult) error {
		rt := s.readTracking[ch.Dataset]
		if rt == nil || rt.Classify == nil || ch.Local || ch.Injected || ch.ChangeId == "" {
			return nil
		}
		if _, restoring := s.readSeedPending.Load(ch.ObjectId); restoring {
			// First restore of a first-sight object: everything present
			// gets seeded read right after, so tracking each historical
			// change (insert + transition, then delete + transition)
			// would be pure write amplification. The seed's frontier
			// covers this change instead.
			return nil
		}
		var (
			tags       []string
			trackedIds []string
			deletedIds []string
			key        string
			tracked    bool
		)
		cctx := &crdt.ChangeCtx{Change: ch}
		for i := range ch.Records {
			rec := &ch.Records[i]
			for _, op := range rec.Ops {
				if op.Type == crdt.OpDelete {
					deletedIds = append(deletedIds, recordIds[i])
					break
				}
			}
			cl := rt.Classify(cctx, rec)
			// One supersede key per change: chat-shaped datasets write
			// one user action per change, so the first key wins.
			if key == "" && cl.Key != "" {
				key = cl.Key
			}
			if !cl.Track {
				continue
			}
			tracked = true
			trackedIds = append(trackedIds, recordIds[i])
			for _, tag := range cl.Tags {
				if !containsString(tags, tag) {
					tags = append(tags, tag)
				}
			}
		}
		if len(deletedIds) > 0 {
			if err := s.readState.ClearRecords(txCtx, ch.ObjectId, deletedIds, ch.ApplySeq); err != nil {
				return err
			}
		}
		// Untracked keyless changes still go through TrackChange: it
		// no-ops cheaply, but must see the change to resolve a pending
		// head another device marked before this change synced here.
		return s.readState.TrackChange(txCtx, readstate.Track{
			ObjectId:     ch.ObjectId,
			Dataset:      ch.Dataset,
			ChangeId:     ch.ChangeId,
			VersionId:    string(ch.VersionId),
			AddSeq:       ch.AddSeq,
			ApplySeq:     ch.ApplySeq,
			RecordIds:    trackedIds,
			Tags:         tags,
			Key:          key,
			PrevIds:      ch.PrevIds,
			Tracked:      tracked,
			SelfAuthored: ch.Creator != "" && ch.Creator == s.selfIdentity,
		})
	}
}

func containsString(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

// readSeedable reports whether first-sight seeding applies in this
// space (every tracked dataset opted for ReadSeedAtFirstSight).
func (s *Store) readSeedable() bool {
	if s.readState == nil || len(s.readTracking) == 0 {
		return false
	}
	for _, rt := range s.readTracking {
		if rt.Seed != crdt.ReadSeedAtFirstSight {
			return false
		}
	}
	return true
}

// seedReadState implements ReadSeedAtFirstSight: on an object's first
// tracked load, everything present becomes read — the frontier is set
// to the tree heads captured after the restore, and the seed is
// recorded durably (engine seeded bit), so a crash on either side of
// the restore re-seeds on the next load instead of skipping forever.
// Idempotent via the seeded bit; pending remote heads survive.
func (s *Store) seedReadState(ctx context.Context, obj *object.Object, objectId string) {
	if !s.readSeedable() {
		return
	}
	tree := obj.Tree()
	if tree == nil {
		return
	}
	tree.Lock()
	heads := append([]string(nil), tree.Heads()...)
	tree.Unlock()
	err := s.readState.WriteTx(ctx, func(txCtx context.Context) error {
		_, seedErr := s.readState.SeedFrontier(txCtx, objectId, heads)
		return seedErr
	})
	if err != nil {
		storeLog.Warn("seed read state", zap.String("objectId", objectId), zap.Error(err))
	}
}
