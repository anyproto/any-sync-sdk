package payloads

import (
	"context"
	"errors"

	"github.com/anyproto/any-store/v2/anyenc"
)

// Row is the typed view of one payloads record. The cleartext fields
// are always populated from the materialized row; Enc is populated
// only after a successful Unseal (Sealed reports whether the secrets
// are still closed).
type Row struct {
	// Id is the fileId — derived from the creating change
	// (crdt.DeriveRecordId of its ChangeId), deterministic and never
	// reused.
	Id string
	// RootCid is the UnixFS root of the encrypted file. Empty for
	// inline rows.
	RootCid string
	// Size is the plaintext byte size (cleartext hint; quota uses the
	// node's own measurement).
	Size int64
	// NetworkSign is the node's durable-custody receipt. Empty until
	// the file is durable; always empty for inline rows.
	NetworkSign string
	// Author is the account that registered the file (derived from the
	// creating change's signer).
	Author string
	// EncKid is the ACL key-record id the `enc` blob was sealed under
	// (cleartext — needed to pick the unseal key after ACL rotation).
	EncKid string
	// encCt is the sealed blob.
	encCt []byte

	// Enc holds the unsealed member-only secrets after Unseal.
	Enc EncPayload
	// Sealed is true until Unseal succeeds. A keyless reader keeps
	// Sealed rows — that's the broker view, not an error.
	Sealed bool
}

// Inline reports whether the row is an inline-tier file (no rootCid,
// bytes inside enc).
func (r *Row) Inline() bool { return r.RootCid == "" }

// RowFromValue decodes the typed Row from a materialized payloads
// record. Byte slices are copied out, so the Row does not retain the
// caller's buffer.
func RowFromValue(v *anyenc.Value) (Row, error) {
	if v == nil {
		return Row{}, errors.New("payloads: nil record value")
	}
	r := Row{
		Id:          v.GetString("id"),
		RootCid:     v.GetString(FieldRootCid),
		Size:        int64(v.GetFloat64(FieldSize)),
		NetworkSign: v.GetString(FieldNetworkSign),
		Author:      v.GetString(FieldAuthor),
		EncKid:      v.GetString(FieldEnc, EncKeyId),
		Sealed:      true,
	}
	if ct := v.GetBytes(FieldEnc, EncKeyCiphertext); len(ct) > 0 {
		r.encCt = append([]byte(nil), ct...)
	}
	return r, nil
}

// Unseal opens the row's enc blob via the provider. On ErrNoKey the
// row simply stays Sealed (the keyless-reader view) and no error is
// returned; any other failure (corrupt blob, wrong key) propagates.
func (r *Row) Unseal(ctx context.Context, kp KeyProvider) error {
	if !r.Sealed || kp == nil {
		return nil
	}
	if r.EncKid == "" || len(r.encCt) == 0 {
		return errors.New("payloads: row has no sealed enc")
	}
	key, err := kp.KeyById(ctx, r.EncKid)
	if err != nil {
		if errors.Is(err, ErrNoKey) {
			return nil
		}
		return err
	}
	enc, err := UnsealEnc(key, r.encCt)
	if err != nil {
		return err
	}
	r.Enc = enc
	r.Sealed = false
	return nil
}
