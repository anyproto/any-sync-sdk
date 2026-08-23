package spaceobjects

import (
	"context"
	"slices"

	"github.com/anyproto/any-store/v2/query"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/readstate"
)

// buildReadTracking collects the read-tracking registrations by
// dataset from the type catalog and the store's system regs. Empty
// map = nothing tracked in this space.
func buildReadTracking(extTypes []handler.Type, system []crdt.HandlerReg) map[string]*crdt.ReadTracking {
	out := map[string]*crdt.ReadTracking{}
	for _, t := range extTypes {
		for _, d := range t.Datasets {
			if d.ReadTracking != nil {
				out[d.Name] = d.ReadTracking
			}
		}
	}
	for _, reg := range system {
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
// untracked spaces pay a single nil check per apply. The controller is
// captured at install time so audience-restricted verdicts
// (ReadClassification.Audience) can point-read the target record
// inside the same tx.
func (s *Store) readApplyHook(ctrl *crdt.Controller) crdt.ApplyHook {
	if len(s.readTracking) == 0 || s.readState == nil {
		return nil
	}
	return func(txCtx context.Context, ch *crdt.Change, recordIds []string, res *crdt.ApplyResult) error {
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
		rejected := rejectedOps(res)
		var (
			tags       []string
			trackedIds []string
			deletedIds []string
			key        string
			tracked    bool
		)
		cctx := &crdt.ChangeCtx{Change: ch, SelfIdentity: s.selfIdentity,
			// Classifiers may point-read the POST-apply record (the
			// hook runs after the record loop, same tx) — e.g. a
			// derived field a handler just stamped. Same read
			// recordMatchesAudience does; verdicts stay device-local,
			// so self-relative use of post-apply state is sound.
			Get: ctrl.RecordGetter(txCtx)}
		for i := range ch.Records {
			// Classify only what actually LANDED: a handler-rejected op
			// never mutated the record, so it must not create, clear, or
			// supersede unread state — otherwise a rejected delete wipes
			// the victim's unread entries and a rejected edit forges or
			// clears mention entries. Whole-record rejections skip the
			// record outright.
			rec, ok := survivingRecord(&ch.Records[i], rejected[i])
			if !ok {
				continue
			}
			cctx.RecordId = recordIds[i]
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
			if cl.Audience != nil && !recordMatchesAudience(txCtx, ctrl, ch.Dataset, recordIds[i], cl.Audience) {
				// Audience-restricted entry whose target record doesn't
				// match on this replica: applies untracked here — the
				// key above still supersedes/clears.
				continue
			}
			tracked = true
			trackedIds = append(trackedIds, recordIds[i])
			for _, tag := range cl.Tags {
				if !slices.Contains(tags, tag) {
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

// rejectedOps indexes an ApplyResult's rejections by record index →
// op-index set (OpIndex -1 = the whole record was dropped). nil map
// (and nil inner sets) on the common everything-landed path.
func rejectedOps(res *crdt.ApplyResult) map[int]map[int]struct{} {
	if res == nil || len(res.Rejections) == 0 {
		return nil
	}
	out := make(map[int]map[int]struct{}, len(res.Rejections))
	for _, rj := range res.Rejections {
		m := out[rj.RecordIndex]
		if m == nil {
			m = make(map[int]struct{}, 1)
			out[rj.RecordIndex] = m
		}
		m[rj.OpIndex] = struct{}{}
	}
	return out
}

// survivingRecord narrows a RecordChange to the ops the apply step
// actually landed. ok=false when the whole record was rejected
// (OpIndex -1) or every op was. The common no-rejection path returns
// the original pointer, copy-free. A partially-salvaged multi-field
// op (some keys landed, some shed — same OpIndex) is dropped whole:
// deliberately conservative, a rejection can only ever suppress
// tracking, never forge or clear it.
func survivingRecord(rec *crdt.RecordChange, rejected map[int]struct{}) (*crdt.RecordChange, bool) {
	if len(rejected) == 0 {
		return rec, true
	}
	if _, whole := rejected[-1]; whole {
		return nil, false
	}
	ops := make([]crdt.Op, 0, len(rec.Ops))
	for i := range rec.Ops {
		if _, drop := rejected[i]; drop {
			continue
		}
		ops = append(ops, rec.Ops[i])
	}
	if len(ops) == 0 {
		return nil, false
	}
	out := *rec
	out.Ops = ops
	return &out, true
}

// recordMatchesAudience resolves an audience-restricted verdict: one
// in-tx point read of the (already-applied) target record, matched
// in-memory against the verdict's typed filter. Missing record ⇒
// nobody's audience. Deterministic across replays because changes
// apply in DAG causal order (the record's create precedes anything
// that targets it) and audience filters consult immutable fields by
// contract.
func recordMatchesAudience(ctx context.Context, ctrl *crdt.Controller, dataset, recordId string, f query.Filter) bool {
	if ctrl == nil {
		return false
	}
	doc := ctrl.Get(ctx, dataset, recordId)
	if doc == nil {
		return false
	}
	return f.Ok(doc, nil)
}

// SeedHeadsProvider returns the account's published read frontiers
// for an object (one set per device row, own rows included), nil when
// none. Injected by the space layer from the read-sync service —
// consulted before first-sight seeding so a fresh device lands on the
// account's REAL read state whenever it already synced (the tech
// space usually syncs before chat trees do).
type SeedHeadsProvider func(ctx context.Context, objectId string) ([][]string, error)

// SetSeedHeadsProvider wires the provider. Call before objects load.
func (s *Store) SetSeedHeadsProvider(p SeedHeadsProvider) { s.seedHeads = p }

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

// publishedSeedHeads consults the account's published frontiers for
// the object. nil = none synced yet (or no provider) → first-sight
// seeding applies.
func (s *Store) publishedSeedHeads(ctx context.Context, objectId string) [][]string {
	if s.seedHeads == nil {
		return nil
	}
	sets, err := s.seedHeads(ctx, objectId)
	if err != nil {
		storeLog.Warn("seed: published frontiers", zap.String("objectId", objectId), zap.Error(err))
		return nil
	}
	return sets
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
	heads := slices.Clone(tree.Heads())
	tree.Unlock()
	err := s.readState.WriteTx(ctx, func(txCtx context.Context) error {
		_, seedErr := s.readState.SeedFrontier(txCtx, objectId, heads)
		return seedErr
	})
	if err != nil {
		storeLog.Warn("seed read state", zap.String("objectId", objectId), zap.Error(err))
	}
}

// seedFromPublished is the consult-KV seed path: the restore tracked
// the object's history as unread (no skip), and the account's
// published frontiers now flip everything the account already read —
// the device lands exactly on the account's read state; the remainder
// stays genuinely unread. Costs insert+cover for the covered history,
// paid once per object on a fresh device, in exchange for accurate
// per-message unread instead of the everything-read approximation.
func (s *Store) seedFromPublished(ctx context.Context, objectId string, sets [][]string) {
	var last uint64
	err := s.readState.WriteTx(ctx, func(txCtx context.Context) error {
		if err := s.readState.MarkSeeded(txCtx, objectId); err != nil {
			return err
		}
		for _, heads := range sets {
			res, err := s.readState.MergeHeads(txCtx, objectId, heads)
			if err != nil {
				return err
			}
			if res.StateSeq != 0 {
				last = res.StateSeq
			}
		}
		return nil
	})
	if err != nil {
		storeLog.Warn("seed from published frontiers", zap.String("objectId", objectId), zap.Error(err))
		return
	}
	if last != 0 {
		s.readState.NotifyState(objectId, last)
	}
}
