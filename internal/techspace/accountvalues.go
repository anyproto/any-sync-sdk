// Account-values carrier — the tech-space transport for account-scoped
// values. One derived carrier object per TARGET space, dataset
// `account_values`, one record per (objectId, dataset, recordId) of
// the target space. See internal/accountvalues (key codec + mirror
// diff) and docs/scoped-properties-proposal.md § "Account transport".

package techspace

import (
	"context"
	"errors"
	"fmt"
	"strings"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/accountvalues"
	"github.com/anyproto/any-sync-sdk/internal/anyencx"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
)

// AccountValuesObject derives (idempotently) and returns the carrier
// object for targetSpaceId, keeping it resident in the tech store's
// ocache. Every device of the account mints the same object id for the
// same target space. The id is cached after the first call.
func (s *Service) AccountValuesObject(ctx context.Context, targetSpaceId string) (*object.Object, error) {
	if !s.open.Load() {
		return nil, errors.New("techspace: service not open")
	}
	if targetSpaceId == "" {
		return nil, errors.New("techspace: targetSpaceId required")
	}
	s.avMu.Lock()
	id, cached := s.avIds[targetSpaceId]
	s.avMu.Unlock()
	if cached {
		return s.store.Get(ctx, id)
	}
	obj, err := s.store.Derive(ctx, spaceobjects.DeriveOpts{ChangePayload: []byte(accountvalues.DeriveSeed(targetSpaceId))})
	if err != nil {
		return nil, fmt.Errorf("techspace: derive account-values carrier for %s: %w", targetSpaceId, err)
	}
	s.avMu.Lock()
	if s.avIds == nil {
		s.avIds = make(map[string]string)
	}
	s.avIds[targetSpaceId] = obj.Id()
	s.avMu.Unlock()
	return obj, nil
}

// WriteAccountValues applies one RecordChange to the carrier of
// targetSpaceId — the account route's tech-space write. The returned
// WriteResult.VersionId is the carrier tree's orderId: the version the
// mirror stamps onto target rows, and what Properties.Set reports.
func (s *Service) WriteAccountValues(ctx context.Context, targetSpaceId string, rec crdt.RecordChange) (object.WriteResult, error) {
	obj, err := s.AccountValuesObject(ctx, targetSpaceId)
	if err != nil {
		return object.WriteResult{}, err
	}
	return obj.LocalWrite(ctx, crdt.Change{
		Dataset:     accountvalues.Dataset,
		DataVersion: accountvalues.HandlerVersion,
		Records:     []crdt.RecordChange{rec},
	})
}

// GetAccountValues returns one carrier record (cloned), or nil when it
// doesn't exist. Pure any-store read.
func (s *Service) GetAccountValues(ctx context.Context, targetSpaceId, recordKey string) (*anyenc.Value, error) {
	coll, err := s.accountValuesCollection(ctx, targetSpaceId)
	if err != nil || coll == nil {
		return nil, err
	}
	doc, err := coll.FindId(ctx, recordKey)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return nil, nil
		}
		return nil, err
	}
	v := doc.Value()
	if v == nil || v.Get(crdt.DeletedAtField) != nil {
		return nil, nil
	}
	return anyencx.Clone(v), nil
}

// IterAccountValues streams every live carrier record of
// targetSpaceId to fn (rows are valid only during the callback —
// clone to retain). Backs the mirror's state re-mirror. A space with
// no carrier collection yet yields nothing.
func (s *Service) IterAccountValues(ctx context.Context, targetSpaceId string, fn func(rec *anyenc.Value) error) error {
	coll, err := s.accountValuesCollection(ctx, targetSpaceId)
	if err != nil || coll == nil {
		return err
	}
	iter, err := coll.Find(nil).Iter(ctx)
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.Next() {
		doc, derr := iter.Doc()
		if derr != nil {
			return derr
		}
		v := doc.Value()
		if v == nil || v.Get(crdt.DeletedAtField) != nil {
			continue
		}
		if err := fn(v); err != nil {
			return err
		}
	}
	return iter.Err()
}

// DeleteAccountValuesForObject tombstones every carrier record of one
// target object (any dataset/recordId) — the object-delete GC. Every
// device may issue the same deletes: CRDT deletes are idempotent and
// tombstones sticky, so a racing account write loses cleanly. No-op
// when the object has no carrier records.
func (s *Service) DeleteAccountValuesForObject(ctx context.Context, targetSpaceId, targetObjectId string) error {
	coll, err := s.accountValuesCollection(ctx, targetSpaceId)
	if err != nil || coll == nil {
		return err
	}
	prefix := accountvalues.KeyPrefixForObject(targetObjectId)
	var ids []string
	iter, err := coll.Find(nil).Iter(ctx)
	if err != nil {
		return err
	}
	for iter.Next() {
		doc, derr := iter.Doc()
		if derr != nil {
			_ = iter.Close()
			return derr
		}
		v := doc.Value()
		if v == nil || v.Get(crdt.DeletedAtField) != nil {
			continue
		}
		if id := v.GetString(crdt.IdField); strings.HasPrefix(id, prefix) {
			ids = append(ids, id)
		}
	}
	if err := iter.Err(); err != nil {
		_ = iter.Close()
		return err
	}
	_ = iter.Close()
	if len(ids) == 0 {
		return nil
	}
	obj, err := s.AccountValuesObject(ctx, targetSpaceId)
	if err != nil {
		return err
	}
	recs := make([]crdt.RecordChange, len(ids))
	for i, id := range ids {
		recs[i] = crdt.RecordChange{Id: id, Ops: []crdt.Op{{Type: crdt.OpDelete}}}
	}
	_, err = obj.LocalWrite(ctx, crdt.Change{
		Dataset:     accountvalues.Dataset,
		DataVersion: accountvalues.HandlerVersion,
		Records:     recs,
	})
	return err
}

// DropAccountValues deletes the whole carrier object for
// targetSpaceId — the space-leave/delete GC.
//
// KNOWN PROTOCOL VIOLATION, to be replaced (see the proposal's
// follow-up ledger): any-sync forbids deleting DERIVED trees —
// deterministic ids mean delete + re-derive = the same identity with
// fresh history (history replacement). This must become record-level
// GC (tombstone every carrier record, keep the empty derived tree).
// Kept for now because the target space is being deleted account-wide
// anyway and the call is best-effort.
func (s *Service) DropAccountValues(ctx context.Context, targetSpaceId string) error {
	obj, err := s.AccountValuesObject(ctx, targetSpaceId)
	if err != nil {
		return err
	}
	if err := s.store.DeleteTree(ctx, obj.Id()); err != nil {
		return err
	}
	s.avMu.Lock()
	delete(s.avIds, targetSpaceId)
	s.avMu.Unlock()
	return nil
}

// accountValuesCollection opens the carrier's dataset collection for
// reads without binding the tree. (nil, nil) when no writer has
// materialised it yet — callers treat that as "no records".
func (s *Service) accountValuesCollection(ctx context.Context, targetSpaceId string) (anystore.Collection, error) {
	obj, err := s.AccountValuesObject(ctx, targetSpaceId)
	if err != nil {
		return nil, err
	}
	coll, err := s.store.OpenObjectCollection(ctx, obj.Id(), accountvalues.Dataset)
	if err != nil {
		if errors.Is(err, anystore.ErrCollectionNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return coll, nil
}
