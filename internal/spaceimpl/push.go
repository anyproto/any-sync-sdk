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
)

// pushKeyCache holds the per-space derived push keys. The space
// signing key derives from the ACL's FIRST metadata key (a derived
// space: root read key + space id) — fixed for the space's life, so it
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
// the app cache), reads the push-key source (pushSpaceKeySource) +
// CurrentReadKey off its ACL state under RLock, and
// returns the derived push signing key and payload encryption key.
// Requires read access — a keyless reader/tracker gets an error,
// matching the server's members-only trust model.
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
	deriveSpaceKey, skErr := pushSpaceKeySource(acl, spaceId)
	state := acl.AclState()
	kid := state.CurrentReadKeyId()
	readKey, rkErr := state.CurrentReadKey()
	acl.RUnlock()
	if skErr != nil {
		return nil, nil, skErr
	}
	if rkErr != nil || readKey == nil {
		return nil, nil, fmt.Errorf("push: current read key for %q: %w", spaceId, rkErr)
	}

	c := &s.pushKeys
	c.mu.Lock()
	defer c.mu.Unlock()
	spaceKey, ok := c.spaceKeys[spaceId]
	if !ok {
		if spaceKey, err = deriveSpaceKey(); err != nil {
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

// pushSpaceKeySource reads the input of the space's push signing key
// off its ACL (caller holds the read lock) and returns the derivation
// to run outside it. A derived ACL — the root record carries no read
// key, the test any-sync's applyRoot uses — has no metadata key, so its
// key comes from the root read key and the space id.
func pushSpaceKeySource(acl list.AclList, spaceId string) (func() (crypto.PrivKey, error), error) {
	recs := acl.Records()
	if len(recs) == 0 {
		return nil, fmt.Errorf("push: space %q: empty acl", spaceId)
	}
	if root, ok := recs[0].Model.(*aclrecordproto.AclRoot); ok &&
		root.EncryptedReadKey == nil && root.OneToOneInfo == nil {
		rootReadKey := acl.AclState().Keys()[acl.Id()].ReadKey
		if rootReadKey == nil {
			return nil, fmt.Errorf("push: derived space %q: no root read key", spaceId)
		}
		return func() (crypto.PrivKey, error) {
			return pushclient.DeriveDerivedSpaceKey(rootReadKey, spaceId)
		}, nil
	}
	firstMeta, err := acl.AclState().FirstMetadataKey()
	if err != nil {
		return nil, fmt.Errorf("push: first metadata key for %q: %w", spaceId, err)
	}
	return func() (crypto.PrivKey, error) {
		return pushclient.DeriveSpaceKey(firstMeta)
	}, nil
}
