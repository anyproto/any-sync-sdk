package spaceobjects

import (
	"context"
	"errors"
	"fmt"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/types"
)

// DetachedCollection is the on-disk collection that holds parked
// changes — those whose DataVersion references a (typeId, shortId)
// the apply path didn't know yet. Schema:
//
//	{
//	  id:        "<changeId>",
//	  spaceId:   string,
//	  objectId:  string,
//	  addSeq:    number,
//	  changeId:  string,
//	  timestamp: number,
//	  payload:   binary,           // wire-format bytes from the codec
//	  pending:   ["typeId:shortId", ...]   // unsatisfied pairs
//	}
//
// Indexed implicitly by id (the changeId). Drains scan all rows;
// for MVP scale this is fine — a full multi-peer cold-restore at
// scale needs an index on `pending` later.
const DetachedCollection = "_detached"

// DetachedRow is the stable view of one parked change. Fields map
// 1:1 to the on-disk shape.
//
// OrderId carries any-sync's per-tree lexid for the change; the
// drain path stamps it back onto crdt.Change.VersionId before apply
// so LWW gating uses any-sync's ordering, not a fresh local one.
type DetachedRow struct {
	ChangeId  string
	SpaceId   string
	ObjectId  string
	AddSeq    uint64
	OrderId   string
	Timestamp int64
	Payload   []byte
	Pending   []types.DataVersionPair
}

// Detached returns the per-space `_detached` collection, opening it
// on first call. Concurrency-safe.
func (s *Store) Detached(ctx context.Context) (anystore.Collection, error) {
	s.mu.Lock()
	if s.detached != nil {
		coll := s.detached
		s.mu.Unlock()
		return coll, nil
	}
	s.mu.Unlock()
	collName := s.spaceId + "/" + DetachedCollection
	coll, err := s.db.Collection(ctx, collName)
	if err != nil {
		return nil, fmt.Errorf("spaceobjects: open %s: %w", collName, err)
	}
	s.mu.Lock()
	if s.detached == nil {
		s.detached = coll
	} else {
		coll = s.detached
	}
	s.mu.Unlock()
	return coll, nil
}

// Park inserts (or replaces) a parked change. The pending list is
// the (typeId, shortId) pairs the change still needs before it can
// land. Returns nil even on idempotent re-park (already parked).
func (s *Store) Park(ctx context.Context, row DetachedRow) error {
	coll, err := s.Detached(ctx)
	if err != nil {
		return err
	}
	a := &anyenc.Arena{}
	doc := a.NewObject()
	doc.Set("id", a.NewString(row.ChangeId))
	doc.Set("spaceId", a.NewString(row.SpaceId))
	doc.Set("objectId", a.NewString(row.ObjectId))
	doc.Set("addSeq", a.NewNumberInt(int(row.AddSeq)))
	doc.Set("orderId", a.NewString(row.OrderId))
	doc.Set("timestamp", a.NewNumberInt(int(row.Timestamp)))
	doc.Set("payload", a.NewBinary(row.Payload))
	pendingArr := a.NewArray()
	for i, p := range row.Pending {
		pendingArr.SetArrayItem(i, a.NewString(p.TypeId+":"+p.ShortId))
	}
	doc.Set("pending", pendingArr)
	return coll.UpsertOne(ctx, doc)
}

// Unpark removes the parked change with the given id.
func (s *Store) Unpark(ctx context.Context, changeId string) error {
	coll, err := s.Detached(ctx)
	if err != nil {
		return err
	}
	if err := coll.DeleteId(ctx, changeId); err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return nil
		}
		return err
	}
	return nil
}

// IterDetached calls fn for every parked change. The visitor reads
// each row out before advancing — safe against the iter-buffer-
// reuse issue, since we copy fields out of the value as we go.
//
// Iteration is best-effort (errors decoding individual rows are
// skipped with the rest still visited). Stops early on fn returning
// false.
func (s *Store) IterDetached(ctx context.Context, fn func(row DetachedRow) bool) error {
	coll, err := s.Detached(ctx)
	if err != nil {
		return err
	}
	iter, err := coll.Find(nil).Iter(ctx)
	if err != nil {
		return fmt.Errorf("spaceobjects: detached iter: %w", err)
	}
	defer iter.Close()
	for iter.Next() {
		doc, err := iter.Doc()
		if err != nil {
			continue
		}
		row := decodeDetachedRow(doc.Value())
		if row.ChangeId == "" {
			continue
		}
		if !fn(row) {
			return nil
		}
	}
	return iter.Err()
}

// decodeDetachedRow extracts a DetachedRow from the on-disk anyenc
// value. Returns the zero value if the row is malformed.
func decodeDetachedRow(v *anyenc.Value) DetachedRow {
	if v == nil {
		return DetachedRow{}
	}
	row := DetachedRow{
		ChangeId:  v.GetString("id"),
		SpaceId:   v.GetString("spaceId"),
		ObjectId:  v.GetString("objectId"),
		AddSeq:    uint64(v.GetInt("addSeq")),
		OrderId:   v.GetString("orderId"),
		Timestamp: int64(v.GetInt("timestamp")),
		Payload:   append([]byte(nil), v.GetBytes("payload")...),
	}
	if pending := v.GetArray("pending"); len(pending) > 0 {
		row.Pending = make([]types.DataVersionPair, 0, len(pending))
		for _, p := range pending {
			s := string(p.GetStringBytes())
			parsed, err := types.ParseDataVersion(s)
			if err != nil || len(parsed) != 1 {
				continue
			}
			row.Pending = append(row.Pending, parsed[0])
		}
	}
	return row
}
