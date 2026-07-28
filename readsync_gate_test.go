package anysyncsdk

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/commonspace/object/keyvalue/keyvaluestorage"
	"github.com/anyproto/any-sync/commonspace/object/keyvalue/keyvaluestorage/innerstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/readstate"
	"github.com/anyproto/any-sync-sdk/internal/readsync"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/internal/types"
	"github.com/anyproto/any-sync-sdk/space"
)

// rowsFor adapts a fixed record set to the gate's tech-space lookup.
func rowsFor(rows map[string]techspace.SpaceIndexRecord) func(string) (techspace.SpaceIndexRecord, bool) {
	return func(spaceId string) (techspace.SpaceIndexRecord, bool) {
		rec, ok := rows[spaceId]
		return rec, ok
	}
}

// TestReadSyncEngineFor_Gate pins the liveness decision: only an
// active tech-space row with local storage reaches the engine lookup;
// absent / deleted / pending / storage-less rows short-circuit to nil
// without touching StoreFor (no Store construction for dead spaces).
func TestReadSyncEngineFor_Gate(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "gate.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	eng := readstate.New(db, "sp-live", func(context.Context) (uint64, error) { return 1, nil }, nil)

	rows := map[string]techspace.SpaceIndexRecord{
		"sp-live":      {Id: "sp-live"},
		"sp-nostorage": {Id: "sp-nostorage"},
		"sp-deleted":   {Id: "sp-deleted", RemoteStatus: techspace.StatusDeleted},
		"sp-local-del": {Id: "sp-local-del", LocalStatus: techspace.StatusDeleted},
		"sp-1to1-del":  {Id: "sp-1to1-del", RemoteStatus: techspace.OneToOneDeletedStatus},
		// MaterializeBlock arms: pending join, direct-add invite
		// pending/declined, incoming/declined 1-1. The local-status
		// literals are spaceimpl's unexported constants.
		"sp-joining":      {Id: "sp-joining", LocalStatus: "joining"},
		"sp-invpending":   {Id: "sp-invpending", RemoteStatus: techspace.InvitePendingRemoteStatus},
		"sp-invdeclined":  {Id: "sp-invdeclined", RemoteStatus: techspace.InviteDeclinedRemoteStatus},
		"sp-1to1-pending": {Id: "sp-1to1-pending", Type: space.SpaceTypeOneToOne, LocalStatus: "oneToOnePending"},
		"sp-1to1-decl":    {Id: "sp-1to1-decl", Type: space.SpaceTypeOneToOne, RemoteStatus: "oneToOneDeclined"},
	}
	calls := map[string]int{}
	gate := readSyncEngineFor(
		rowsFor(rows),
		func(spaceId string) bool { return spaceId != "sp-nostorage" },
		func(spaceId string) *readstate.Engine {
			calls[spaceId]++
			return eng
		},
	)

	for _, spaceId := range []string{
		"sp-unknown", "sp-deleted", "sp-local-del", "sp-1to1-del",
		"sp-joining", "sp-invpending", "sp-invdeclined", "sp-1to1-pending", "sp-1to1-decl",
		"sp-nostorage",
	} {
		assert.Nil(t, gate(spaceId), spaceId)
	}
	assert.Empty(t, calls, "gated spaces must not reach the engine lookup")

	// A live space that hasn't loaded yet passes: the gate consults only
	// the row and storage existence, and the store builds lazily behind it.
	assert.Same(t, eng, gate("sp-live"))
	assert.Equal(t, map[string]int{"sp-live": 1}, calls)
}

// gateKV is a minimal tech-space KV fake: Iterate serves seeded rows,
// Set records publishes. Mirrors the readsync package's test fake.
type gateKV struct {
	keyvaluestorage.Storage
	mu   sync.Mutex
	sets map[string][]byte
	rows map[string][]innerstorage.KeyValue
}

func plainDecrypt(kv innerstorage.KeyValue) ([]byte, error) { return kv.Value.Value, nil }

func (f *gateKV) Set(_ context.Context, key string, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sets[key] = append([]byte(nil), value...)
	return nil
}

func (f *gateKV) Iterate(_ context.Context, fn func(decryptor keyvaluestorage.Decryptor, key string, values []innerstorage.KeyValue) (bool, error)) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for key, values := range f.rows {
		cont, err := fn(plainDecrypt, key, values)
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func (f *gateKV) GetAll(_ context.Context, key string, get func(decryptor keyvaluestorage.Decryptor, values []innerstorage.KeyValue) error) error {
	f.mu.Lock()
	values := f.rows[key]
	f.mu.Unlock()
	return get(plainDecrypt, values)
}

func frontierKV(key, peerId string, heads []string) innerstorage.KeyValue {
	raw, _ := json.Marshal(struct {
		H []string `json:"h"`
	}{H: heads})
	return innerstorage.KeyValue{Key: key, PeerId: peerId, Value: innerstorage.Value{Value: raw}}
}

// TestReadSyncGate_ReconcileAllDeadSpace runs the gated EngineFor
// through a real readsync.ReconcileAll over frontier keys of a dead
// and a live space, with the engine lookup building REAL
// spaceobjects.Stores (drain kick included, like spaceimpl.storeFor).
// The dead space's keys must not construct a Store — and therefore
// must not re-create its `<spaceId>__detached` collection — while the
// live space's frontier still merges.
func TestReadSyncGate_ReconcileAllDeadSpace(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "sdk.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	const (
		liveSpace = "sp-live"
		deadSpace = "sp-dead"
	)

	var (
		storesMu sync.Mutex
		stores   = map[string]*spaceobjects.Store{}
	)
	t.Cleanup(func() {
		storesMu.Lock()
		defer storesMu.Unlock()
		for _, st := range stores {
			_ = st.Close()
		}
	})
	buildStore := func(spaceId string) *spaceobjects.Store {
		st := spaceobjects.NewStoreWithConfig(spaceobjects.StoreConfig{
			DB:      db,
			SpaceId: spaceId,
			// Raw mode with one read-tracked dataset so ReadState() is live.
			Handlers: []crdt.HandlerReg{{
				Name: "msgs",
				ReadTracking: &crdt.ReadTracking{
					Classify: func(*crdt.ChangeCtx, *crdt.RecordChange) crdt.ReadClassification {
						return crdt.ReadClassification{}
					},
				},
			}},
		})
		// First-touch drain kick, same as spaceimpl.storeFor — the side
		// effect that open-or-creates `<spaceId>__detached`.
		st.NotifyDrainer(types.DataVersionPair{})
		return st
	}
	engineOf := func(spaceId string) *readstate.Engine {
		storesMu.Lock()
		defer storesMu.Unlock()
		st, ok := stores[spaceId]
		if !ok {
			st = buildStore(spaceId)
			stores[spaceId] = st
		}
		return st.ReadState()
	}

	rows := map[string]techspace.SpaceIndexRecord{
		liveSpace: {Id: liveSpace},
		deadSpace: {Id: deadSpace, RemoteStatus: techspace.StatusDeleted},
	}
	gate := readSyncEngineFor(rowsFor(rows), func(string) bool { return true }, engineOf)

	// Live space: one tracked unread change another device has read.
	eng := gate(liveSpace)
	require.NotNil(t, eng)
	require.NoError(t, eng.WriteTx(ctx, func(txCtx context.Context) error {
		return eng.TrackChange(txCtx, readstate.Track{
			ObjectId: "obj1", ChangeId: "c1", VersionId: "v01",
			ApplySeq: 1, RecordIds: []string{"rec-c1"},
			Tags: []string{"message"}, Tracked: true,
		})
	}))

	kv := &gateKV{sets: map[string][]byte{}, rows: map[string][]innerstorage.KeyValue{
		"read/" + liveSpace + "/obj1": {frontierKV("read/"+liveSpace+"/obj1", "peer-other", []string{"c1"})},
		"read/" + deadSpace + "/objX": {frontierKV("read/"+deadSpace+"/objX", "peer-other", []string{"zz"})},
		"read/" + deadSpace + "/objY": {frontierKV("read/"+deadSpace+"/objY", "peer-other", []string{"zz2"})},
	}}
	svc := readsync.New(gate,
		func(context.Context) (keyvaluestorage.Storage, error) { return kv, nil },
		"peer-self")
	t.Cleanup(svc.Close)

	require.NoError(t, svc.ReconcileAll(ctx))

	// Control: the live space's frontier merged.
	entries, _, err := eng.UnreadEntries(ctx, "obj1")
	require.NoError(t, err)
	assert.Empty(t, entries, "live space frontier must still merge")

	// The dead space never got a Store...
	storesMu.Lock()
	_, deadBuilt := stores[deadSpace]
	storesMu.Unlock()
	assert.False(t, deadBuilt, "dead space must not construct a Store")

	// ...and therefore no `<deadSpaceId>__detached` collection, while the
	// live space's drain kick does create one (drain is async).
	require.Eventually(t, func() bool {
		names, err := db.GetCollectionNames(ctx)
		require.NoError(t, err)
		for _, n := range names {
			if n == liveSpace+"__detached" {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "live space drain creates its _detached collection")
	names, err := db.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.NotContains(t, names, deadSpace+"__detached", "dead space _detached must not be re-created")
}
