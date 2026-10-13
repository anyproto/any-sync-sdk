package spaceimpl

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

// Contact avatars. A resolved profile names the avatar's ciphertext by
// (space, rootCid) and carries its file key; the bytes are fetched
// through the same ladder as any file — local cache, LAN peer, public
// read — under a ref named after the identity, so the cached CAR
// outlives any single read and is released only when the profile moves
// to another avatar (or none). The owner's space is never joined.

// identityIconRef is the local-store ref an identity's avatar is held
// under. Not a fileId: the safety sweep retains rows of a space it
// cannot resolve, so the release below is the one reclaim path.
func identityIconRef(identity string) string { return "identity/" + identity }

// setIdentityProfile writes a contact's resolved profile through to the
// directory and releases the cached bytes of an avatar it no longer
// names.
func (s *Service) setIdentityProfile(ctx context.Context, identity string, p space.Profile) {
	prev, had := s.tsp.GetIdentity(ctx, identity)
	next := techspace.IdentityProfile{
		Name:        p.Name,
		Description: p.Description,
		IconCID:     p.IconCID,
		IconSpaceId: p.IconSpaceId,
		IconKey:     p.IconKey,
	}
	if err := s.tsp.SetIdentityProfile(ctx, identity, next); err != nil {
		return
	}
	if had && prev.IconCID != "" && prev.IconSpaceId != "" &&
		(prev.IconCID != next.IconCID || prev.IconSpaceId != next.IconSpaceId) {
		s.releaseIdentityIcon(ctx, identity, prev.IconSpaceId, prev.IconCID)
	}
}

// openIdentityIcon opens the avatar a cached profile names. The
// reference was chosen by another account: the space id is checked
// before it reaches a path or URL, the content address must parse,
// and an object larger than MaxIconSize is refused once its size is
// known (the DAG root), before its bytes are read.
func (s *Service) openIdentityIcon(ctx context.Context, identity string, p techspace.IdentityProfile) (space.FileReader, error) {
	if s.fetch == nil {
		return nil, errors.New("identities: files not configured")
	}
	if p.IconCID == "" || p.IconSpaceId == "" || p.IconKey == "" {
		return nil, fmt.Errorf("identities: %s names no avatar: %w", identity, space.ErrNotFound)
	}
	if !safeSpaceId(p.IconSpaceId) {
		return nil, fmt.Errorf("identities: %s: malformed avatar space: %w", identity, space.ErrNotFound)
	}
	key, ok := space.Profile{IconKey: p.IconKey}.IconKeyBytes()
	if !ok {
		return nil, fmt.Errorf("identities: %s: malformed avatar key: %w", identity, space.ErrNotFound)
	}
	root, err := cid.Decode(p.IconCID)
	if err != nil {
		return nil, fmt.Errorf("identities: %s: avatar cid: %w", identity, space.ErrNotFound)
	}
	// An avatar is published only once durable, so the public read
	// path is always eligible. Whatever stops the fetch — no public
	// read base, an object the store no longer serves, a transport
	// failure — is the same retry-later state to the caller, and the
	// underlying error (which names the owner's space) stays in the log.
	f, err := s.fetch.Open(ctx, p.IconSpaceId, root, key, true, identityIconRef(identity))
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		filesLog.Debug("identity icon fetch", zap.String("identity", identity), zap.Error(err))
		return nil, fmt.Errorf("identities: %s: avatar not fetchable: %w", identity, space.ErrFileNotAvailable)
	}
	if f.Size() > space.MaxIconSize {
		_ = f.Close()
		return nil, fmt.Errorf("identities: %s: avatar of %d bytes: %w", identity, f.Size(), space.ErrIconTooLarge)
	}
	return f, nil
}

// safeSpaceId reports whether id has the shape of an any-sync space id
// — letters and digits around one dot — so a profile-supplied value
// never carries a path segment or URL syntax into the file store.
func safeSpaceId(id string) bool {
	if id == "" || len(id) > 256 || id[0] == '.' || id[len(id)-1] == '.' {
		return false
	}
	dots := 0
	for i := 0; i < len(id); i++ {
		switch c := id[i]; {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.':
			dots++
		default:
			return false
		}
	}
	return dots == 1
}

// releaseIdentityIcon drops the identity's ref on a cached avatar and
// deletes the CAR when nothing else holds it. Best-effort.
func (s *Service) releaseIdentityIcon(ctx context.Context, identity, spaceId, rootCid string) {
	st := s.fstore
	if st == nil || !safeSpaceId(spaceId) {
		return
	}
	root, err := cid.Decode(rootCid)
	if err != nil {
		return
	}
	remaining, err := st.RemoveRef(ctx, spaceId, root, identityIconRef(identity))
	if err != nil || remaining > 0 {
		return
	}
	if err := st.Delete(ctx, spaceId, root); err != nil {
		filesLog.Warn("release identity icon", zap.String("identity", identity), zap.Error(err))
	}
}
