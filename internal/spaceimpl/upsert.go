package spaceimpl

// Space.Upsert — generic schema-driven batch ingest (SYN-147). One $in
// query per page for current values (never FindId-per-record), in-memory
// structural diff (anyencutil.Equal — no re-encoding of stored values),
// one Modify change per page.

import (
	"context"
	"errors"
	"fmt"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/anyenc/anyencutil"
	"github.com/anyproto/any-store/v2/query"

	"github.com/anyproto/any-sync-sdk/internal/anyencx"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/space"
)

const defaultUpsertPageSize = 500

func (s *spaceImpl) Upsert(ctx context.Context, batch space.UpsertBatch) (space.UpsertResult, error) {
	var res space.UpsertResult
	if batch.ObjectId == "" {
		return res, errors.New("spaceimpl: Upsert: ObjectId required")
	}
	if batch.Dataset == "" {
		return res, errors.New("spaceimpl: Upsert: Dataset required")
	}
	if err := checkPublicDataset(batch.Dataset); err != nil {
		return res, err
	}
	decl, ok := s.store.DatasetDecl(batch.Dataset)
	if !ok {
		return res, fmt.Errorf("spaceimpl: Upsert: unknown dataset %q", batch.Dataset)
	}
	decl = decl.Normalized()
	if decl.IdRule != schema.IdUser {
		return res, fmt.Errorf("%w: dataset %q", space.ErrUpsertRequiresUserIds, batch.Dataset)
	}
	if len(batch.Records) == 0 {
		return res, nil
	}
	pageSize := batch.PageSize
	if pageSize <= 0 {
		pageSize = defaultUpsertPageSize
	}

	up := &upserter{
		decl:     &decl,
		rules:    make(map[string]*schema.Field, len(decl.Fields)),
		identity: s.store.SelfIdentity(),
	}
	for i := range decl.Fields {
		f := &decl.Fields[i]
		up.rules[f.Id] = f
		if f.Stamp == schema.StampCreator {
			up.creatorField = f.Id
		}
	}

	for start := 0; start < len(batch.Records); start += pageSize {
		end := min(start+pageSize, len(batch.Records))
		if err := s.upsertPage(ctx, &batch, up, start, end, &res); err != nil {
			return res, err
		}
	}
	return res, nil
}

// upserter carries the per-call compiled declaration bits.
type upserter struct {
	decl         *schema.Dataset
	rules        map[string]*schema.Field
	creatorField string
	identity     string
}

// upsertPage diffs records[start:end] against stored values and emits
// at most one Modify change.
func (s *spaceImpl) upsertPage(ctx context.Context, batch *space.UpsertBatch, up *upserter, start, end int, res *space.UpsertResult) error {
	page := batch.Records[start:end]
	stored, err := s.upsertReadPage(ctx, batch.ObjectId, batch.Dataset, page)
	if err != nil {
		return err
	}

	arena := &anyenc.Arena{}
	var recs []space.RecordModify
	seen := make(map[string]struct{}, len(page))
	for i := range page {
		rec := &page[i]
		idx := start + i
		if rec.Id == "" {
			res.Rejections = append(res.Rejections, space.UpsertRejection{Index: idx, Id: rec.Id,
				Err: errors.New("upsert: record id required")})
			continue
		}
		if _, dup := seen[rec.Id]; dup {
			res.Rejections = append(res.Rejections, space.UpsertRejection{Index: idx, Id: rec.Id,
				Err: fmt.Errorf("upsert: duplicate id %q in page", rec.Id)})
			continue
		}
		seen[rec.Id] = struct{}{}

		desired, cerr := upsertFields(arena, rec.Fields)
		if cerr != nil {
			res.Rejections = append(res.Rejections, space.UpsertRejection{Index: idx, Id: rec.Id, Err: cerr})
			continue
		}
		before := stored[rec.Id]
		switch {
		case before == nil:
			recs = append(recs, upsertCreate(arena, rec.Id, desired))
			res.Created++
		default:
			if before.Get(crdt.DeletedAtField) != nil {
				res.Rejections = append(res.Rejections, space.UpsertRejection{Index: idx, Id: rec.Id, Err: space.ErrRecordDeleted})
				continue
			}
			mod, derr := up.diffRecord(rec.Id, desired, before)
			if derr != nil {
				res.Rejections = append(res.Rejections, space.UpsertRejection{Index: idx, Id: rec.Id, Err: derr})
				continue
			}
			if mod == nil {
				res.Skipped++
				continue
			}
			recs = append(recs, *mod)
			res.Updated++
		}
	}
	if len(recs) == 0 {
		return nil
	}
	mres, err := s.Modify(ctx, space.ModifyBatch{
		ObjectId: batch.ObjectId,
		Dataset:  batch.Dataset,
		Records:  recs,
		TraceIds: batch.TraceIds,
	})
	if err != nil {
		return fmt.Errorf("spaceimpl: Upsert: page write: %w", err)
	}
	res.Pages = append(res.Pages, mres)
	return nil
}

// upsertReadPage fetches the page's current values with one $in query.
// Missing collection (nothing written yet) → all creates. Values are
// cloned off the iterator buffer — they outlive the iteration.
func (s *spaceImpl) upsertReadPage(ctx context.Context, objectId, dataset string, page []space.UpsertRecord) (map[string]*anyenc.Value, error) {
	s.store.EnsureDatasetRegistered(ctx, objectId, dataset)
	obj, err := s.store.Get(ctx, objectId)
	if err != nil {
		return nil, err
	}
	coll := obj.Controller().Collection(ctx, dataset)
	if coll == nil {
		return nil, nil
	}
	arena := &anyenc.Arena{}
	vals := make([]*anyenc.Value, 0, len(page))
	for i := range page {
		if page[i].Id != "" {
			vals = append(vals, arena.NewString(page[i].Id))
		}
	}
	if len(vals) == 0 {
		return nil, nil
	}
	iter, err := coll.Find(query.Key{Path: []string{crdt.IdField}, Filter: query.NewInValue(vals...)}).Iter(ctx)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: Upsert: read page: %w", err)
	}
	defer iter.Close()
	out := make(map[string]*anyenc.Value, len(vals))
	for iter.Next() {
		doc, derr := iter.Doc()
		if derr != nil {
			return nil, derr
		}
		if v := doc.Value(); v != nil {
			out[v.GetString(crdt.IdField)] = anyencx.Clone(v)
		}
	}
	return out, iter.Err()
}

// upsertFields converts the caller field map, rejecting reserved keys
// and dotted paths (upsert works on whole top-level fields).
func upsertFields(arena *anyenc.Arena, fields map[string]any) (map[string]*anyenc.Value, error) {
	out := make(map[string]*anyenc.Value, len(fields))
	for k, val := range fields {
		if k == "" || k == crdt.IdField || k[0] == '_' {
			return nil, fmt.Errorf("upsert: reserved field %q", k)
		}
		for _, c := range k {
			if c == '.' {
				return nil, fmt.Errorf("upsert: field %q: dotted paths not supported (whole top-level fields only)", k)
			}
		}
		v, err := goToAnyenc(arena, val)
		if err != nil {
			return nil, fmt.Errorf("upsert: field %q: %w", k, err)
		}
		out[k] = v
	}
	return out, nil
}

// upsertCreate builds the create record: one multi-field $set with all
// fields, Upsert on.
func upsertCreate(arena *anyenc.Arena, id string, desired map[string]*anyenc.Value) space.RecordModify {
	payload := arena.NewObject()
	for k, v := range desired {
		payload.Set(k, v)
	}
	return space.RecordModify{
		Id:     id,
		Upsert: true,
		Ops:    []space.Op{{Type: space.OpSet, Value: payload}},
	}
}

// diffRecord compares desired fields to the stored record. Returns nil
// when nothing changed; an error when the change is undeliverable
// (immutable field differs / not the author); otherwise a RecordModify
// with one single-path $set per changed declared-mutable field.
func (u *upserter) diffRecord(id string, desired map[string]*anyenc.Value, before *anyenc.Value) (*space.RecordModify, error) {
	var ops []space.Op
	for k, v := range desired {
		stored := before.Get(k)
		if anyencutil.Equal(stored, v) {
			continue
		}
		rule, declared := u.rules[k]
		if !declared {
			if !u.decl.Dynamic {
				return nil, fmt.Errorf("upsert: field %q is not declared on the dataset", k)
			}
			// Free-keyspace field: no mutability semantics, plain set.
			ops = append(ops, space.Op{Type: space.OpSet, Path: k, Value: v})
			continue
		}
		switch {
		case rule.Stamp != schema.StampNone:
			return nil, fmt.Errorf("upsert: field %q is derived (stamped) and not writable", k)
		case rule.MutableBy == schema.MutableNever:
			return nil, fmt.Errorf("%w: %q on record %q", space.ErrImmutableFieldChanged, k, id)
		case rule.MutableBy == schema.MutableByAuthor:
			if u.creatorField == "" || u.identity == "" ||
				string(before.GetStringBytes(u.creatorField)) != u.identity {
				return nil, fmt.Errorf("%w: %q on record %q", space.ErrUpsertNotAuthor, k, id)
			}
		}
		ops = append(ops, space.Op{Type: space.OpSet, Path: k, Value: v})
	}
	if len(ops) == 0 {
		return nil, nil
	}
	return &space.RecordModify{Id: id, Ops: ops}, nil
}
