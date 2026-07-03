package readsync

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

	"github.com/anyproto/any-sync-sdk/internal/readstate"
)

var ctx = context.Background()

const (
	testSpace = "space1"
	testObj   = "obj1"
	selfPeer  = "peer-self"
)

// fakeKV records Set calls and serves Iterate from its rows.
type fakeKV struct {
	keyvaluestorage.Storage
	mu   sync.Mutex
	sets map[string][]byte
	rows map[string][]innerstorage.KeyValue
}

func newFakeKV() *fakeKV {
	return &fakeKV{sets: map[string][]byte{}, rows: map[string][]innerstorage.KeyValue{}}
}

func (f *fakeKV) Set(_ context.Context, key string, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sets[key] = append([]byte(nil), value...)
	return nil
}

func (f *fakeKV) Iterate(_ context.Context, fn func(decryptor keyvaluestorage.Decryptor, key string, values []innerstorage.KeyValue) (bool, error)) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for key, values := range f.rows {
		cont, err := fn(plainDecryptor, key, values)
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func plainDecryptor(kv innerstorage.KeyValue) ([]byte, error) { return kv.Value.Value, nil }

func frontierKV(key, peerId string, heads []string) innerstorage.KeyValue {
	raw, _ := json.Marshal(frontierValue{Heads: heads})
	return innerstorage.KeyValue{Key: key, PeerId: peerId, Value: innerstorage.Value{Value: raw}}
}

type fixture struct {
	*Service
	eng *readstate.Engine
	kv  *fakeKV
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "readsync.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	seq := uint64(1000)
	eng := readstate.New(db, testSpace, func(context.Context) (uint64, error) {
		seq++
		return seq, nil
	}, nil)
	kv := newFakeKV()
	svc := New(
		func(spaceId string) *readstate.Engine {
			if spaceId == testSpace {
				return eng
			}
			return nil
		},
		func(context.Context) (keyvaluestorage.Storage, error) { return kv, nil },
		selfPeer,
	)
	t.Cleanup(svc.Close)
	return &fixture{Service: svc, eng: eng, kv: kv}
}

func (f *fixture) trackUnread(t *testing.T, changeId, versionId string, prev []string) {
	t.Helper()
	require.NoError(t, f.eng.WriteTx(ctx, func(txCtx context.Context) error {
		return f.eng.TrackChange(txCtx, readstate.Track{
			ObjectId: testObj, ChangeId: changeId, VersionId: versionId,
			PrevIds: prev, ApplySeq: 1, RecordIds: []string{"rec-" + changeId},
			Tags: []string{"message"}, Tracked: true,
		})
	}))
}

func TestKeyCodec(t *testing.T) {
	sp, ob, ok := parseKey(kvKey("sp1", "ob1"))
	require.True(t, ok)
	assert.Equal(t, "sp1", sp)
	assert.Equal(t, "ob1", ob)

	_, _, ok = parseKey("other/sp1/ob1")
	assert.False(t, ok)
	_, _, ok = parseKey("read/justspace")
	assert.False(t, ok)
}

func TestMarkPublishesFrontier(t *testing.T) {
	f := newFixture(t)
	f.trackUnread(t, "c1", "v01", nil)

	res, err := f.MarkReadUpTo(ctx, testSpace, testObj, "")
	require.NoError(t, err)
	assert.Equal(t, []string{"c1"}, res.Frontier)

	f.kv.mu.Lock()
	raw := f.kv.sets[kvKey(testSpace, testObj)]
	f.kv.mu.Unlock()
	require.NotNil(t, raw)
	var val frontierValue
	require.NoError(t, json.Unmarshal(raw, &val))
	assert.Equal(t, []string{"c1"}, val.Heads)
}

func TestOnKeyValues_MergesRemoteFrontier(t *testing.T) {
	f := newFixture(t)
	f.trackUnread(t, "c1", "v01", nil)
	f.trackUnread(t, "c2", "v02", []string{"c1"})

	// Own row is ignored; another device's row covers both changes.
	f.OnKeyValues(plainDecryptor, []innerstorage.KeyValue{
		frontierKV(kvKey(testSpace, testObj), selfPeer, []string{"bogus"}),
		frontierKV(kvKey(testSpace, testObj), "peer-other", []string{"c2"}),
	})

	require.Eventually(t, func() bool {
		entries, _, err := f.eng.UnreadEntries(ctx, testObj)
		return err == nil && len(entries) == 0
	}, 2*time.Second, 10*time.Millisecond)

	heads, _, err := f.eng.Frontier(ctx, testObj)
	require.NoError(t, err)
	assert.Equal(t, []string{"c2"}, heads)
}

func TestReconcile_ReplaysPublishedFrontiers(t *testing.T) {
	f := newFixture(t)
	f.trackUnread(t, "c1", "v01", nil)

	f.kv.rows[kvKey(testSpace, testObj)] = []innerstorage.KeyValue{
		frontierKV(kvKey(testSpace, testObj), "peer-other", []string{"c1"}),
	}
	// A foreign-space key must be skipped, not break iteration.
	f.kv.rows[kvKey("space-other", "objX")] = []innerstorage.KeyValue{
		frontierKV(kvKey("space-other", "objX"), "peer-other", []string{"zz"}),
	}

	require.NoError(t, f.Reconcile(ctx, testSpace))

	entries, _, err := f.eng.UnreadEntries(ctx, testObj)
	require.NoError(t, err)
	assert.Empty(t, entries)

	// Second run is the idempotent fast path — still clean.
	require.NoError(t, f.Reconcile(ctx, testSpace))
}

func TestReconcile_MergesOwnRowAfterRebuild(t *testing.T) {
	f := newFixture(t)
	// Fresh DB (rebuild): the unread entry exists, and the only KV row
	// is OUR OWN previously-published frontier. It must count.
	f.trackUnread(t, "c1", "v01", nil)
	f.kv.rows[kvKey(testSpace, testObj)] = []innerstorage.KeyValue{
		frontierKV(kvKey(testSpace, testObj), selfPeer, []string{"c1"}),
	}
	require.NoError(t, f.Reconcile(ctx, testSpace))

	entries, _, err := f.eng.UnreadEntries(ctx, testObj)
	require.NoError(t, err)
	assert.Empty(t, entries, "own published frontier restores read state after a rebuild")
}

func TestReconcile_RepublishesDivergentFrontier(t *testing.T) {
	f := newFixture(t)
	// Local frontier advanced (a mark whose publish was lost): KV has
	// a stale own row.
	f.trackUnread(t, "c1", "v01", nil)
	f.trackUnread(t, "c2", "v02", []string{"c1"})
	require.NoError(t, f.eng.WriteTx(ctx, func(txCtx context.Context) error {
		_, err := f.eng.MarkReadUpTo(txCtx, testObj, "")
		return err
	}))
	f.kv.rows[kvKey(testSpace, testObj)] = []innerstorage.KeyValue{
		frontierKV(kvKey(testSpace, testObj), selfPeer, []string{"c1"}), // stale
	}

	require.NoError(t, f.Reconcile(ctx, testSpace))

	f.kv.mu.Lock()
	raw := f.kv.sets[kvKey(testSpace, testObj)]
	f.kv.mu.Unlock()
	require.NotNil(t, raw, "divergent frontier republished at reconcile")
	var val frontierValue
	require.NoError(t, json.Unmarshal(raw, &val))
	assert.Equal(t, []string{"c2"}, val.Heads)
}
