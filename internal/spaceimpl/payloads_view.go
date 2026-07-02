package spaceimpl

import (
	"context"
	"errors"

	"github.com/anyproto/any-sync-sdk/internal/payloads"
	"github.com/anyproto/any-sync-sdk/space"
)

// payloadsView implements space.PayloadsView — the public read-only
// surface over the space's payloads objects, exposing the cleartext row
// fields only. This is what the filenode-v2 broker indexes from; it
// works identically with and without the space key (Sealed reflects
// which case the reader is in).
type payloadsView struct {
	s    *spaceImpl
	keys *aclKeyProvider
}

// Payloads returns the read-only payloads view. See space.PayloadsView.
func (s *spaceImpl) Payloads() space.PayloadsView {
	return &payloadsView{s: s, keys: &aclKeyProvider{s: s}}
}

// ListObjects enumerates the materialized payloads objects by their
// tree root's changeType — the signed, content-addressed class marker,
// so the set can't be spoofed and needs no collection-name parsing.
func (v *payloadsView) ListObjects(ctx context.Context) ([]string, error) {
	return v.s.store.TreeIdsByChangeType(ctx, payloads.ChangeType)
}

// ListRows reads every live row of one payloads object and maps it to
// the public cleartext shape. Unsealing is attempted best-effort purely
// to report Sealed truthfully (a keyless reader stays Sealed — the
// broker view); the opened secrets are never surfaced.
func (v *payloadsView) ListRows(ctx context.Context, payloadsObjectId string) ([]space.PayloadRow, error) {
	if payloadsObjectId == "" {
		return nil, errors.New("spaceimpl: payloads view: payloadsObjectId required")
	}
	vals, err := newQuery(v.s.store, payloadsObjectId, payloads.Dataset).All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]space.PayloadRow, 0, len(vals))
	for _, val := range vals {
		row, err := payloads.RowFromValue(val)
		if err != nil {
			return nil, err
		}
		if err := row.Unseal(ctx, v.keys); err != nil {
			return nil, err
		}
		out = append(out, space.PayloadRow{
			FileId:      row.Id,
			RootCid:     row.RootCid,
			Size:        row.Size,
			NetworkSign: row.NetworkSign,
			Author:      row.Author,
			Sealed:      row.Sealed,
		})
	}
	return out, nil
}
