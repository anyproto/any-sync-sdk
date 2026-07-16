// Push-notification key provider (SYN-47) — resolves a space's push
// keys from its ACL for internal/pushclient. Same ACL access pattern
// as payloads.go's aclKeyProvider.

package spaceimpl

import (
	"context"
	"fmt"
	"sync"

	"github.com/anyproto/any-sync/util/crypto"

	"github.com/anyproto/any-sync-sdk/internal/pushclient"
)

// pushKeyCache holds the per-space derived push keys. The space
// signing key derives from the ACL's FIRST metadata key — fixed for
// the space's life, so it caches per spaceId; the payload enc key
// derives from the CURRENT read key, which rotates, so it caches per
// (spaceId, readKeyId). Derivation is deterministic — a stale entry is
// impossible, only a missed hit.
type pushKeyCache struct {
	mu        sync.Mutex
	spaceKeys map[string]crypto.PrivKey
	encKeys   map[string]crypto.SymKey
}

// PushKeys implements pushclient.KeysProvider: loads the space (via
// the app cache), reads FirstMetadataKey + CurrentReadKey off its ACL
// state under RLock, and returns the derived push signing key and
// payload encryption key. Requires read access — a keyless
// reader/tracker gets an error, matching the server's members-only
// trust model.
func (s *Service) PushKeys(ctx context.Context, spaceId string) (spaceKey crypto.PrivKey, encKey crypto.SymKey, err error) {
	handle, err := s.app.GetSpace(ctx, spaceId)
	if err != nil {
		return nil, nil, fmt.Errorf("push: load space %q: %w", spaceId, err)
	}
	acl := handle.Inner().Acl()
	if acl == nil {
		return nil, nil, fmt.Errorf("push: space %q has no acl", spaceId)
	}
	acl.RLock()
	state := acl.AclState()
	firstMeta, fmErr := state.FirstMetadataKey()
	kid := state.CurrentReadKeyId()
	readKey, rkErr := state.CurrentReadKey()
	acl.RUnlock()
	if fmErr != nil {
		return nil, nil, fmt.Errorf("push: first metadata key for %q: %w", spaceId, fmErr)
	}
	if rkErr != nil || readKey == nil {
		return nil, nil, fmt.Errorf("push: current read key for %q: %w", spaceId, rkErr)
	}

	c := &s.pushKeys
	c.mu.Lock()
	defer c.mu.Unlock()
	spaceKey, ok := c.spaceKeys[spaceId]
	if !ok {
		if spaceKey, err = pushclient.DeriveSpaceKey(firstMeta); err != nil {
			return nil, nil, fmt.Errorf("push: derive space key for %q: %w", spaceId, err)
		}
		if c.spaceKeys == nil {
			c.spaceKeys = make(map[string]crypto.PrivKey)
		}
		c.spaceKeys[spaceId] = spaceKey
	}
	encCacheKey := spaceId + "/" + kid
	encKey, ok = c.encKeys[encCacheKey]
	if !ok {
		if encKey, err = pushclient.DeriveEncKey(readKey); err != nil {
			return nil, nil, fmt.Errorf("push: derive enc key for %q: %w", spaceId, err)
		}
		if c.encKeys == nil {
			c.encKeys = make(map[string]crypto.SymKey)
		}
		c.encKeys[encCacheKey] = encKey
	}
	return spaceKey, encKey, nil
}
