package anysyncx

import (
	"context"
	"sync"
	"time"

	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/clientspaceproto"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/recordverifier"
	"go.uber.org/zap"
)

var dkLog = logger.NewNamed("anysyncx.discoverykeys")

// negativeRetryAfter is how long a failed derivation (no ACL yet, no
// read key for us yet) is remembered before retrying. Keys appear when
// our join is accepted or the ACL syncs in, so retry on a short leash;
// successful derivations are cached forever (the first read key never
// rotates).
const negativeRetryAfter = time.Minute

// discoveryKeySource derives and caches the per-space LAN discovery
// keys the SpaceExchangeV2 tokens are built from. The key is
// HKDF(firstAclReadKey, spaceId) — see clientspaceproto.DeriveDiscoveryKey —
// so deriving requires the space's ACL state: spaces whose ACL we can't
// read yet (pull in progress, join not accepted) are skipped and the
// exchange simply doesn't cover them until the key becomes available.
type discoveryKeySource struct {
	storage *storageProvider
	keys    *accountdata.AccountKeys

	mu       sync.Mutex
	derived  map[string][]byte    // spaceId → discovery key, stable for the space's lifetime
	failedAt map[string]time.Time // spaceId → last failed attempt
}

func newDiscoveryKeySource(storage *storageProvider, keys *accountdata.AccountKeys) *discoveryKeySource {
	return &discoveryKeySource{
		storage:  storage,
		keys:     keys,
		derived:  map[string][]byte{},
		failedAt: map[string]time.Time{},
	}
}

// DiscoveryKeys returns the discovery keys for the given spaces, keyed
// by spaceId. Spaces without a derivable key are absent from the result.
func (d *discoveryKeySource) DiscoveryKeys(ctx context.Context, spaceIds []string) map[string][]byte {
	out := make(map[string][]byte, len(spaceIds))
	for _, id := range spaceIds {
		if key := d.get(ctx, id); key != nil {
			out[id] = key
		}
	}
	return out
}

func (d *discoveryKeySource) get(ctx context.Context, spaceId string) []byte {
	d.mu.Lock()
	if key, ok := d.derived[spaceId]; ok {
		d.mu.Unlock()
		return key
	}
	if at, ok := d.failedAt[spaceId]; ok && time.Since(at) < negativeRetryAfter {
		d.mu.Unlock()
		return nil
	}
	d.mu.Unlock()

	key, err := d.derive(ctx, spaceId)

	d.mu.Lock()
	defer d.mu.Unlock()
	if err != nil {
		dkLog.Debug("discovery key unavailable", zap.String("spaceId", spaceId), zap.Error(err))
		d.failedAt[spaceId] = time.Now()
		return nil
	}
	delete(d.failedAt, spaceId)
	d.derived[spaceId] = key
	return key
}

// derive reads the space's ACL from storage and extracts the first read
// key (the one created at space creation — st.keys[aclRootId] in
// any-sync's AclState, available to every member and never rotated).
func (d *discoveryKeySource) derive(ctx context.Context, spaceId string) ([]byte, error) {
	st, err := d.storage.WaitSpaceStorage(ctx, spaceId)
	if err != nil {
		return nil, err
	}
	aclStorage, err := st.AclStorage()
	if err != nil {
		return nil, err
	}
	aclList, err := list.BuildAclListWithIdentity(d.keys, aclStorage, recordverifier.NewValidateFull())
	if err != nil {
		return nil, err
	}
	firstKeys, ok := aclList.AclState().Keys()[aclList.Id()]
	if !ok || firstKeys.ReadKey == nil {
		return nil, list.ErrNoReadKey
	}
	return clientspaceproto.DeriveDiscoveryKey(firstKeys.ReadKey, spaceId)
}
