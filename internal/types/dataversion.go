package types

import (
	"errors"
	"strings"
)

// DataVersion encodes the schema state a writer was aware of when
// emitting a Change to the per-space `objects` collection.
//
// Wire format: one or more `typeId:shortId` pairs separated by `;`.
// Both halves are base58-derived (CIDs / shortIds) — neither carries
// a `:` or `;` so the encoding is unambiguous. Empty input means no
// type/schema gating (legacy hardcoded handler-version writes still
// pass without a parse step).
//
// Examples:
//   "":                                  no schema gate
//   "<typeId>:<shortId>":                one type
//   "<typeIdA>:<sA>;<typeIdB>:<sB>":     multi-type write
//
// docs/types-properties-proposal.md § "Change-level DataVersion".

// DataVersionPair is one (typeId, shortId) entry inside a DataVersion.
type DataVersionPair struct {
	TypeId  string
	ShortId string
}

// ErrInvalidDataVersion is returned by ParseDataVersion when the
// input doesn't match `typeId:shortId[;...]`.
var ErrInvalidDataVersion = errors.New("types: invalid DataVersion")

// EncodeDataVersion joins the pairs into wire form. Pairs whose
// ShortId is empty are skipped — a missing shortId means the type
// has had no important changes yet, so there's nothing to gate on.
// Returns "" when every pair was empty.
func EncodeDataVersion(pairs []DataVersionPair) string {
	if len(pairs) == 0 {
		return ""
	}
	var b strings.Builder
	for i, p := range pairs {
		if p.TypeId == "" || p.ShortId == "" {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte(';')
		}
		b.WriteString(p.TypeId)
		b.WriteByte(':')
		b.WriteString(p.ShortId)
		_ = i
	}
	return b.String()
}

// ParseDataVersion splits a wire-format DataVersion into its pairs.
// Empty input yields nil, no error — the gate treats that as "no
// schema constraints attached to this change". Returns
// ErrInvalidDataVersion on malformed input.
func ParseDataVersion(s string) ([]DataVersionPair, error) {
	if s == "" {
		return nil, nil
	}
	parts := strings.Split(s, ";")
	out := make([]DataVersionPair, 0, len(parts))
	for _, p := range parts {
		colon := strings.IndexByte(p, ':')
		if colon <= 0 || colon == len(p)-1 {
			return nil, ErrInvalidDataVersion
		}
		out = append(out, DataVersionPair{
			TypeId:  p[:colon],
			ShortId: p[colon+1:],
		})
	}
	return out, nil
}
