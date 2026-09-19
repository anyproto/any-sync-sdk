package spaceindex

import (
	"bytes"
	"crypto/ed25519"
	"fmt"

	"github.com/anyproto/any-sync/util/crypto"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// InviteKeysDataset shares request-to-join keys through the encrypted
// spaceIndex tree. Rows are keyed by the invite's public-key account ID.
// The ACL remains authoritative: possession only permits a join request.
const InviteKeysDataset = "inviteKeys"
const InviteKeysHandlerVersion = "inviteKeys-v1"
const FieldInviteKey = "key"

func InviteKeysSchema() schema.Dataset {
	return schema.Dataset{Fields: []schema.Field{{
		Id: FieldInviteKey, Name: "Invite key", Schema: schema.Leaf(schema.KindString), Scope: schema.ScopeSynced,
		Description: "Request-to-join proof key; usable only while its public key is active in the ACL.",
	}}}
}

// Every accepted write contains the same key for a given row ID. A
// writer cannot replace another invite's key or tombstone its custody.
type InviteKeysHandler struct{ crdt.DefaultHandler }

func (InviteKeysHandler) BeforeCreate(_ *crdt.ChangeCtx, rec *crdt.RecordChange, _ *crdt.Sink) error {
	for i := range rec.Ops {
		if err := validateInviteKeyOp(rec.Id, &rec.Ops[i]); err != nil {
			return err
		}
	}
	return nil
}

func (InviteKeysHandler) BeforeModify(_ *crdt.ChangeCtx, rec *crdt.RecordChange, op *crdt.Op, _ *crdt.Sink) error {
	return validateInviteKeyOp(rec.Id, op)
}

func (InviteKeysHandler) BeforeDelete(_ *crdt.ChangeCtx, _ *crdt.RecordChange, _ *crdt.Sink) error {
	return fmt.Errorf("%w: invite keys are revoked through the ACL", crdt.ErrValidation)
}

func validateInviteKeyOp(id string, op *crdt.Op) error {
	if op.Type != crdt.OpSet || len(op.Path) != 1 || op.Path[0] != FieldInviteKey || !nonEmptyStringPayload(op.Payload) {
		return fmt.Errorf("%w: invite key requires $set key", crdt.ErrValidation)
	}
	key, err := crypto.DecodeKeyFromString(op.Payload.GetString(), crypto.UnmarshalEd25519PrivateKey, nil)
	if err != nil || key.GetPublic().Account() != id {
		return fmt.Errorf("%w: invite key does not match row ID", crdt.ErrValidation)
	}
	// Ed25519's wire form includes a cached public key. Verify the seed
	// actually derives it, rather than trusting attacker-supplied bytes.
	raw, err := key.Raw()
	if err != nil || !bytes.Equal(raw, ed25519.NewKeyFromSeed(raw[:ed25519.SeedSize])) {
		return fmt.Errorf("%w: inconsistent invite key", crdt.ErrValidation)
	}
	return nil
}
