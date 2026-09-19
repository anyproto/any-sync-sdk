package spaceimpl

import (
	"context"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/util/crypto"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
)

func (s *spaceImpl) sharedInviteKey(ctx context.Context, pub crypto.PubKey) crypto.PrivKey {
	id, err := s.indexObjectId(ctx)
	if err != nil || pub == nil {
		return nil
	}
	obj, err := s.store.Get(ctx, id)
	if err != nil {
		return nil
	}
	row := obj.Controller().Get(ctx, spaceindex.InviteKeysDataset, pub.Account())
	if row == nil || row.Get(crdt.DeletedAtField) != nil {
		return nil
	}
	key, err := crypto.DecodeKeyFromString(row.GetString(spaceindex.FieldInviteKey), crypto.UnmarshalEd25519PrivateKey, nil)
	if err != nil || !key.GetPublic().Equals(pub) {
		return nil
	}
	return key
}

func (s *spaceImpl) publishInviteKey(ctx context.Context, key crypto.PrivKey) error {
	if s.sharedInviteKey(ctx, key.GetPublic()) != nil {
		return nil
	}
	encoded, err := crypto.EncodeKeyToString(key)
	if err != nil {
		return err
	}
	obj, err := s.bundles.indexObj(ctx)
	if err != nil {
		return err
	}
	version, err := s.store.DataVersion(spaceindex.InviteKeysDataset)
	if err != nil {
		return err
	}
	arena := &anyenc.Arena{}
	_, err = obj.LocalWrite(ctx, crdt.Change{
		Dataset: spaceindex.InviteKeysDataset, DataVersion: version,
		Records: []crdt.RecordChange{{
			Id: key.GetPublic().Account(), Upsert: true,
			Ops: []crdt.Op{{Type: crdt.OpSet, Path: []string{spaceindex.FieldInviteKey}, Payload: arena.NewString(encoded)}},
		}},
	})
	return err
}

// Existing issuers backfill their active invite on space load. Failed
// writes retry on the next load; revoked or lost keys are never published.
func (s *spaceImpl) publishIssuedInvite(ctx context.Context) {
	if s.techIndexId != "" || !s.canWrite(ctx) {
		return
	}
	invites, err := s.members.Invites(ctx)
	if err != nil {
		return
	}
	for _, inv := range invites {
		if inv.Key != nil {
			_ = s.publishInviteKey(ctx, inv.Key)
		}
	}
}
