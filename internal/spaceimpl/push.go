// Push-notification key provider (SYN-47) — resolves a space's push
// keys from its ACL for internal/pushclient. Same ACL access pattern
// as payloads.go's aclKeyProvider.

package spaceimpl

import (
	"context"
	"fmt"
	"sync"

	"github.com/anyproto/any-sync/commonspace/object/acl/aclrecordproto"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/util/crypto"

	"github.com/anyproto/any-sync-sdk/internal/pushclient"
	"github.com/anyproto/any-sync-sdk/space"
)

// pushKeyCache holds the per-space derived push keys. The space
// signing key derives from the ACL's FIRST metadata key (a derived
// space: one derived from account key + space id) — fixed for the
// space's life, so it
// caches per spaceId; the payload enc key derives from the CURRENT
// read key, which rotates, so it caches per (spaceId, readKeyId).
// Derivation is deterministic — a stale entry is impossible, only a
// missed hit.
type pushKeyCache struct {
	mu        sync.Mutex
	spaceKeys map[string]crypto.PrivKey
	encKeys   map[string]crypto.SymKey
}

// PushKeys implements pushclient.KeysProvider: loads the space (via
// the app cache), reads its push signing key (pushSpaceKey, on a cache
// miss) and CurrentReadKey off its ACL state under RLock, and returns
// the push signing key and the derived payload encryption key.
// Requires read access — a keyless reader/tracker gets an error,
// matching the server's members-only trust model.
func (s *Service) PushKeys(ctx context.Context, spaceId string) (spaceKey crypto.PrivKey, encKey crypto.SymKey, err error) {
	// The tech space has a derived root too, but never pushes.
	if s.tsp != nil && spaceId != "" && spaceId == s.tsp.SpaceId() {
		return nil, nil, fmt.Errorf("push: %w", space.ErrIsTechSpace)
	}
	handle, err := s.app.GetSpace(ctx, spaceId)
	if err != nil {
		return nil, nil, fmt.Errorf("push: load space %q: %w", spaceId, err)
	}
	acl := handle.Inner().Acl()
	if acl == nil {
		return nil, nil, fmt.Errorf("push: space %q has no acl", spaceId)
	}
	c := &s.pushKeys
	c.mu.Lock()
	spaceKey, cached := c.spaceKeys[spaceId]
	c.mu.Unlock()

	acl.RLock()
	if !cached {
		spaceKey, err = pushSpaceKey(acl, spaceId)
	}
	state := acl.AclState()
	kid := state.CurrentReadKeyId()
	readKey, rkErr := state.CurrentReadKey()
	acl.RUnlock()
	if err != nil {
		return nil, nil, err
	}
	if rkErr == nil && readKey == nil {
		rkErr = list.ErrNoReadKey
	}
	if rkErr != nil {
		return nil, nil, fmt.Errorf("push: current read key for %q: %w", spaceId, rkErr)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if !cached {
		if c.spaceKeys == nil {
			c.spaceKeys = make(map[string]crypto.PrivKey)
		}
		c.spaceKeys[spaceId] = spaceKey
	}
	encCacheKey := spaceId + "/" + kid
	encKey, ok := c.encKeys[encCacheKey]
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

// pushSpaceKey derives the space's push signing key from its push
// metadata key; the caller holds the ACL read lock.
func pushSpaceKey(acl list.AclList, spaceId string) (crypto.PrivKey, error) {
	meta, err := pushMetadataKey(acl, spaceId)
	if err != nil {
		return nil, err
	}
	key, err := pushclient.DeriveSpaceKey(meta)
	if err != nil {
		return nil, fmt.Errorf("push: derive space key for %q: %w", spaceId, err)
	}
	return key, nil
}

// pushMetadataKey is the ACL's first metadata key — for a 1-1 space the
// one any-sync derives from the two accounts' keys. A derived ACL (the
// root record carries no read key, the test any-sync's applyRoot uses)
// has none, so the SDK derives one from the deriving account's key and
// the space id; another account (a tracker) gets none.
func pushMetadataKey(acl list.AclList, spaceId string) (crypto.PrivKey, error) {
	recs := acl.Records()
	if len(recs) == 0 {
		return nil, fmt.Errorf("push: space %q: empty acl", spaceId)
	}
	state := acl.AclState()
	if root, ok := recs[0].Model.(*aclrecordproto.AclRoot); ok &&
		root.EncryptedReadKey == nil && root.OneToOneInfo == nil {
		if state.AccountKey() == nil || !recs[0].Identity.Equals(state.Identity()) {
			return nil, fmt.Errorf("push: derived space %q belongs to another account", spaceId)
		}
		meta, err := pushclient.DeriveDerivedSpaceMetadataKey(state.AccountKey(), spaceId)
		if err != nil {
			return nil, fmt.Errorf("push: derived space %q metadata key: %w", spaceId, err)
		}
		return meta, nil
	}
	meta, err := state.FirstMetadataKey()
	if err != nil {
		return nil, fmt.Errorf("push: first metadata key for %q: %w", spaceId, err)
	}
	return meta, nil
}
