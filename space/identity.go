package space

// IdentityProfileKind is the identityRepo `Kind` string the SDK uses
// for the account profile record. Namespaced separately from
// anytype-heart's "profile" because the binary format differs:
//
//   - SDK   ("anysync-sdk-profile"): NUL-separated layout
//                                    (name\x00description\x00iconCID),
//                                    signed by account key, no
//                                    symmetric encryption.
//   - heart ("profile"):             encrypted protobuf with the
//                                    AccountMetadataSymKey.
//
// Mixing the two would require explicit format negotiation; this is
// out of scope for now.
//
// The kind string MUST NOT contain a dot — the coordinator stores
// records as MongoDB sub-documents keyed by Kind (`data.<kind>`), so
// a dot is interpreted as a nested-path separator and silently
// re-shapes the record on disk.
const IdentityProfileKind = "anysync-sdk-profile"

// EncodeAccountMetadata serialises an AccountMetadata into the
// canonical bytes the SDK pushes to identityRepo. Same NUL-separated
// layout as the join-time metadata blob (see internal/spaceimpl
// encodeMetadata) so the same decoder works on both sides — keeps
// the parsing surface small.
//
// Returns nil for an empty input; caller is expected to refuse-to-push
// on nil to avoid clobbering an existing profile with empty values.
func EncodeAccountMetadata(m AccountMetadata) []byte {
	if m.Name == "" && m.Description == "" && m.IconCID == "" {
		return nil
	}
	out := make([]byte, 0, len(m.Name)+len(m.Description)+len(m.IconCID)+2)
	out = append(out, m.Name...)
	out = append(out, 0)
	out = append(out, m.Description...)
	out = append(out, 0)
	out = append(out, m.IconCID...)
	return out
}

// DecodeAccountMetadata is the inverse — used by the per-space
// fetcher to interpret the bytes pulled from identityRepo. Returns
// the zero value on a malformed input rather than an error: receivers
// fall through to the join-time metadata snapshot in that case.
func DecodeAccountMetadata(b []byte) AccountMetadata {
	if len(b) == 0 {
		return AccountMetadata{}
	}
	name, rest, ok := splitNul(b)
	if !ok {
		return AccountMetadata{}
	}
	desc, icon, ok := splitNul(rest)
	if !ok {
		return AccountMetadata{Name: name}
	}
	return AccountMetadata{
		Name:        name,
		Description: desc,
		IconCID:     string(icon),
	}
}

// splitNul splits on the first 0x00 byte. Returns (head, tail, true)
// on success; (whole, nil, false) if no separator.
func splitNul(b []byte) (string, []byte, bool) {
	for i, c := range b {
		if c == 0 {
			return string(b[:i]), b[i+1:], true
		}
	}
	return string(b), nil, false
}
