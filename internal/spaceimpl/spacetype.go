package spaceimpl

import (
	"context"
	"encoding/json"

	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
)

// derivePayloadV1 is the structured SpaceHeaderPayload the SDK writes for
// derived spaces. It carries:
//   - Seed: the consumer's derivation seed, so per-seed id uniqueness is
//     preserved (the payload is hashed into the derived spaceId);
//   - Type: the app-level SpaceType tag, so the type is intrinsic to the
//     space header and recoverable on cold restore without the
//     tech-space index or the in-space spaceIndex object.
//
// JSON is used for a deterministic, self-describing encoding (Go marshals
// struct fields in declaration order, so the bytes are stable for a given
// value — required because they feed the content-addressed id).
type derivePayloadV1 struct {
	V    int    `json:"v"`
	Seed []byte `json:"seed,omitempty"`
	Type string `json:"type"`
}

// derivePayloadVersion is the only supported derivePayloadV1.V value.
const derivePayloadVersion = 1

// encodeDerivePayload packs the seed + app SpaceType tag for the derive
// header payload.
func encodeDerivePayload(seed []byte, spaceType string) []byte {
	b, err := json.Marshal(derivePayloadV1{V: derivePayloadVersion, Seed: seed, Type: spaceType})
	if err != nil {
		// json.Marshal of this concrete struct cannot fail; fall back to
		// the raw seed so derivation still works (type unrecoverable).
		return seed
	}
	return b
}

// decodeDerivePayload extracts the app SpaceType tag from a header
// payload written by encodeDerivePayload. ok is false for payloads that
// are not in this format (e.g. raw-seed legacy or onetoone info).
func decodeDerivePayload(b []byte) (spaceType string, ok bool) {
	if len(b) == 0 {
		return "", false
	}
	var p derivePayloadV1
	if err := json.Unmarshal(b, &p); err != nil || p.V != derivePayloadVersion {
		return "", false
	}
	return p.Type, true
}

// spaceTypeFromHeader resolves a space's app-level SpaceType straight from
// its header — the authoritative, always-present source that survives
// cold restore (the header is the root of the space, stored locally once
// the space exists). For SDK-derived spaces it reads the structured
// SpaceHeaderPayload; for created spaces (no structured payload) it falls
// back to the on-wire header SpaceType, which IS their type.
//
// Best-effort: returns "" on any parse failure or when the space is not
// already resident. Reading the header needs the space loaded, but this
// runs on the cheap List/Info path, so it only peeks at an in-memory
// space (PickSpace) and never triggers a load — callers should prefer
// the cached tech-space record value and only fall back here.
func (s *Service) spaceTypeFromHeader(ctx context.Context, spaceId string) string {
	handle, ok := s.app.PickSpace(ctx, spaceId)
	if !ok {
		return ""
	}
	desc, err := handle.Inner().Description(ctx)
	if err != nil || desc.SpaceHeader == nil {
		return ""
	}
	var raw spacesyncproto.RawSpaceHeader
	if err := raw.UnmarshalVT(desc.SpaceHeader.RawHeader); err != nil {
		return ""
	}
	var hdr spacesyncproto.SpaceHeader
	if err := hdr.UnmarshalVT(raw.SpaceHeader); err != nil {
		return ""
	}
	if t, ok := decodeDerivePayload(hdr.SpaceHeaderPayload); ok && t != "" {
		return t
	}
	return hdr.SpaceType
}

// headerTypeFromHeader reads a space's raw on-wire header SpaceType —
// the coordinator-gated string (any.space / anytype.space / …), NOT
// the app-level tag spaceTypeFromHeader prefers. Best-effort like the
// other header readers: "" when the space is not resident or the
// header does not parse.
func (s *Service) headerTypeFromHeader(ctx context.Context, spaceId string) string {
	handle, ok := s.app.PickSpace(ctx, spaceId)
	if !ok {
		return ""
	}
	desc, err := handle.Inner().Description(ctx)
	if err != nil || desc.SpaceHeader == nil {
		return ""
	}
	var raw spacesyncproto.RawSpaceHeader
	if err := raw.UnmarshalVT(desc.SpaceHeader.RawHeader); err != nil {
		return ""
	}
	var hdr spacesyncproto.SpaceHeader
	if err := hdr.UnmarshalVT(raw.SpaceHeader); err != nil {
		return ""
	}
	return hdr.SpaceType
}

// resolveSpaceType returns the cached tech-space SpaceType when present,
// otherwise falls back to the authoritative header value. Keeps List/Info
// cheap on the common path while guaranteeing correctness on cold restore
// or for spaces discovered via sync whose cache is not yet populated.
func (s *Service) resolveSpaceType(ctx context.Context, spaceId, cached string) string {
	if cached != "" {
		return cached
	}
	return s.spaceTypeFromHeader(ctx, spaceId)
}

// resolveAuthor returns the space owner's account identity from the ACL.
// Best-effort: empty when the space is not already resident. This runs
// on the cheap List/Info path, so it only peeks at an in-memory space
// (PickSpace) and never triggers a load — a per-row GetSpace here would
// hang List whenever a space's Init blocks on an unreachable sync-node.
// Surfaced as space.SpaceInfo.Author.
func (s *Service) resolveAuthor(ctx context.Context, spaceId string) string {
	handle, ok := s.app.PickSpace(ctx, spaceId)
	if !ok {
		return ""
	}
	acl := handle.Inner().Acl()
	if acl == nil {
		return ""
	}
	acl.RLock()
	defer acl.RUnlock()
	for _, acc := range acl.AclState().CurrentAccounts() {
		if acc.Permissions == list.AclPermissionsOwner {
			return acc.PubKey.Account()
		}
	}
	return ""
}
