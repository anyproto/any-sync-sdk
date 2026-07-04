package gc

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	mrand "math/rand"
	"path/filepath"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/commonfile/fileservice"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/files/carfile"
	"github.com/anyproto/any-sync-sdk/internal/files/crypt"
	"github.com/anyproto/any-sync-sdk/internal/files/store"
)

const spaceId = "space.test"

type rowFact struct{ durable, exists bool }

type fakeRows struct {
	m       map[string]rowFact
	intents map[string]string // ownerId/rootCid → resolvable fileId
}

func (f *fakeRows) SpaceFiles(_ context.Context, _ string) (map[string]bool, error) {
	out := map[string]bool{}
	for id, e := range f.m {
		if e.exists {
			out[id] = e.durable
		}
	}
	return out, nil
}

func (f *fakeRows) ResolveIntent(_ context.Context, _ string, ownerId string, root cid.Cid) (string, bool, error) {
	fileId, ok := f.intents[ownerId+"/"+root.String()]
	return fileId, ok, nil
}

func newStore(t *testing.T) *store.Store {
	t.Helper()
	dir := t.TempDir()
	db, err := anystore.Open(context.Background(), filepath.Join(dir, "meta.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	st, err := store.New(context.Background(), filepath.Join(dir, "files"), db)
	require.NoError(t, err)
	return st
}

// addFile lands one complete CAR referenced by ref.
func addFile(t *testing.T, st *store.Store, size int, ref string) store.Info {
	t.Helper()
	ctx := context.Background()
	plain := make([]byte, size)
	mrand.New(mrand.NewSource(int64(size))).Read(plain)
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	b, err := st.NewBuild(ctx, spaceId)
	require.NoError(t, err)
	ct, err := crypt.NewEncryptReader(key, bytes.NewReader(plain))
	require.NoError(t, err)
	node, err := fileservice.NewFileHandler(b.Blockstore()).AddFile(ctx, ct)
	require.NoError(t, err)
	info, err := b.Finalize(ctx, node.Cid(), ref)
	require.NoError(t, err)
	return info
}

// addPartial lands a sparse skeleton (no data sections) for a file
// built in a scratch store.
func addPartial(t *testing.T, st *store.Store, size int, ref string) store.Info {
	t.Helper()
	ctx := context.Background()
	scratch := newStore(t)
	src := addFile(t, scratch, size, ref)
	h, err := scratch.Open(ctx, spaceId, src.Root)
	require.NoError(t, err)
	car, err := io.ReadAll(io.NewSectionReader(h, 0, src.Size))
	require.NoError(t, err)
	require.NoError(t, h.Close())

	hdr, err := carfile.ParseHeader(car[:4096])
	require.NoError(t, err)
	info, err := st.CreateSparse(ctx, spaceId, car[:4096], car[hdr.IndexOffset:], ref)
	require.NoError(t, err)
	require.Equal(t, store.StatePartial, info.State)
	return info
}

func setClocks(t *testing.T, grace, ttl time.Duration) {
	t.Helper()
	og, ot := UnreferencedGrace, StalePartialTTL
	UnreferencedGrace, StalePartialTTL = grace, ttl
	t.Cleanup(func() { UnreferencedGrace, StalePartialTTL = og, ot })
}

func stateOf(t *testing.T, st *store.Store, root cid.Cid) string {
	t.Helper()
	info, err := st.Info(context.Background(), spaceId, root)
	if err != nil {
		return "gone"
	}
	return info.State
}

func TestCacheSize(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	a := addFile(t, st, 100_000, "fa")
	b := addFile(t, st, 200_000, "fb")
	svc := New(st, &fakeRows{m: map[string]rowFact{}}, nil)

	total, err := svc.CacheSize(ctx)
	require.NoError(t, err)
	assert.Equal(t, a.Size+b.Size, total)

	require.NoError(t, st.Offload(ctx, spaceId, a.Root))
	total, err = svc.CacheSize(ctx)
	require.NoError(t, err)
	assert.Equal(t, b.Size, total, "offloaded bytes leave the accounting")
}

func TestFreeUpLRUOrder(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	old := addFile(t, st, 120_000, "fold")
	time.Sleep(1100 * time.Millisecond) // last-access granularity is seconds
	fresh := addFile(t, st, 120_000, "ffresh")
	rows := &fakeRows{m: map[string]rowFact{
		"fold":   {durable: true, exists: true},
		"ffresh": {durable: true, exists: true},
	}}
	svc := New(st, rows, nil)

	freed, err := svc.FreeUp(ctx, 1) // any positive budget evicts exactly the LRU head
	require.NoError(t, err)
	assert.Equal(t, old.Size, freed)
	assert.Equal(t, store.StateOffload, stateOf(t, st, old.Root), "oldest goes first")
	assert.Equal(t, store.StateComplete, stateOf(t, st, fresh.Root))
}

func TestFreeUpRetainsNotDurable(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	info := addFile(t, st, 90_000, "f1")
	svc := New(st, &fakeRows{m: map[string]rowFact{"f1": {durable: false, exists: true}}}, nil)

	freed, err := svc.FreeUp(ctx, 1<<30)
	require.NoError(t, err)
	assert.Zero(t, freed, "the only copy of a not-backed-up file is never dropped")
	assert.Equal(t, store.StateComplete, stateOf(t, st, info.Root))
}

func TestFreeUpUnreferenced(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	info := addFile(t, st, 80_000, "fgone")
	rows := &fakeRows{m: map[string]rowFact{}} // row deleted

	// Fresh unreferenced CARs survive (an Attach may be mid-flight).
	setClocks(t, time.Hour, StalePartialTTL)
	svc := New(st, rows, nil)
	freed, err := svc.FreeUp(ctx, 1<<30)
	require.NoError(t, err)
	assert.Zero(t, freed)

	// Past grace they are deleted outright (row and bytes).
	setClocks(t, -time.Second, StalePartialTTL)
	freed, err = svc.FreeUp(ctx, 1<<30)
	require.NoError(t, err)
	assert.Equal(t, info.Size, freed)
	assert.Equal(t, "gone", stateOf(t, st, info.Root))
}

// addOrphanCar lands a complete CAR with NO refs — the state a crash
// between Finalize and RegisterFile/AddRefs leaves behind.
func addOrphanCar(t *testing.T, st *store.Store, size int) store.Info {
	t.Helper()
	ctx := context.Background()
	info := addFile(t, st, size, "tmp-ref")
	_, err := st.RemoveRef(ctx, spaceId, info.Root, "tmp-ref")
	require.NoError(t, err)
	out, err := st.Info(ctx, spaceId, info.Root)
	require.NoError(t, err)
	require.Empty(t, out.Refs)
	return out
}

func TestSweepIntentHealsCrashOrphan(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	info := addOrphanCar(t, st, 90_000)
	// The attach died between RegisterFile and AddRefs/Enqueue: intent
	// marker present, row exists but unreferenced and unqueued.
	require.NoError(t, st.SetKV(ctx, store.IntentKey(spaceId, info.Root), "owner1"))
	rows := &fakeRows{
		m:       map[string]rowFact{"fhealed": {durable: false, exists: true}},
		intents: map[string]string{"owner1/" + info.Root.String(): "fhealed"},
	}
	var enqueued []string
	setClocks(t, -time.Second, StalePartialTTL)
	svc := New(st, rows, func(_ context.Context, _ string, fileId string) error {
		enqueued = append(enqueued, fileId)
		return nil
	})
	require.NoError(t, svc.Sweep(ctx))

	healed, err := st.Info(ctx, spaceId, info.Root)
	require.NoError(t, err, "the CAR must survive — it is the only copy")
	assert.Equal(t, []string{"fhealed"}, healed.Refs, "row re-linked")
	assert.Equal(t, []string{"fhealed"}, enqueued, "backup re-queued")
	_, marked, err := st.GetKV(ctx, store.IntentKey(spaceId, info.Root))
	require.NoError(t, err)
	assert.False(t, marked, "marker cleared after heal")

	// Idempotent: a second sweep leaves the healed state alone.
	require.NoError(t, svc.Sweep(ctx))
	healed, err = st.Info(ctx, spaceId, info.Root)
	require.NoError(t, err)
	assert.Equal(t, []string{"fhealed"}, healed.Refs)
}

func TestSweepIntentWithoutRowDeletes(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	info := addOrphanCar(t, st, 90_000)
	// The crash preceded the row write: marker with nothing to heal.
	require.NoError(t, st.SetKV(ctx, store.IntentKey(spaceId, info.Root), "owner1"))
	setClocks(t, -time.Second, StalePartialTTL)
	svc := New(st, &fakeRows{m: map[string]rowFact{}}, nil)
	require.NoError(t, svc.Sweep(ctx))

	assert.Equal(t, "gone", stateOf(t, st, info.Root))
	_, marked, err := st.GetKV(ctx, store.IntentKey(spaceId, info.Root))
	require.NoError(t, err)
	assert.False(t, marked)
}

func TestFreeUpRetainsIntentMarked(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	info := addOrphanCar(t, st, 90_000)
	require.NoError(t, st.SetKV(ctx, store.IntentKey(spaceId, info.Root), "owner1"))
	setClocks(t, -time.Second, StalePartialTTL) // even past grace
	svc := New(st, &fakeRows{m: map[string]rowFact{}}, nil)

	freed, err := svc.FreeUp(ctx, 1<<30)
	require.NoError(t, err)
	assert.Zero(t, freed, "an intent-marked root is pinned; Sweep owns healing it")
	assert.Equal(t, store.StateComplete, stateOf(t, st, info.Root))
}

func TestSweepStalePartialRequiresAllRefsDurable(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	part := addPartial(t, st, 130_000, "fsigned")
	require.NoError(t, st.AddRefs(ctx, spaceId, part.Root, "funsigned"))
	rows := &fakeRows{m: map[string]rowFact{
		"fsigned":   {durable: true, exists: true},
		"funsigned": {durable: false, exists: true}, // e.g. a BIND row not yet driven
	}}
	setClocks(t, UnreferencedGrace, -time.Second) // partial is "stale"
	svc := New(st, rows, nil)
	require.NoError(t, svc.Sweep(ctx))
	assert.Equal(t, store.StatePartial, stateOf(t, st, part.Root),
		"one unsigned live ref must pin the bytes")
}

func TestSweep(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	// shared: one dead ref to prune, one live durable ref keeps it.
	shared := addFile(t, st, 60_000, "fdead")
	require.NoError(t, st.AddRefs(ctx, spaceId, shared.Root, "flive"))
	// orphan: row gone, past grace → delete.
	orphan := addFile(t, st, 70_000, "forphan")
	// stalePart: durable partial past TTL → offload.
	stalePart := addPartial(t, st, 130_000, "fstale")
	// pinnedPart: NOT durable partial — retained whatever its age.
	pinnedPart := addPartial(t, st, 140_000, "fpinned")

	rows := &fakeRows{m: map[string]rowFact{
		"flive":   {durable: true, exists: true},
		"fstale":  {durable: true, exists: true},
		"fpinned": {durable: false, exists: true},
	}}
	setClocks(t, -time.Second, -time.Second) // everything is "old"
	svc := New(st, rows, nil)
	require.NoError(t, svc.Sweep(ctx))

	sharedInfo, err := st.Info(ctx, spaceId, shared.Root)
	require.NoError(t, err)
	assert.Equal(t, []string{"flive"}, sharedInfo.Refs, "dead ref pruned, live ref kept")
	assert.Equal(t, store.StateComplete, sharedInfo.State)

	assert.Equal(t, "gone", stateOf(t, st, orphan.Root), "unreferenced past grace deleted")
	assert.Equal(t, store.StateOffload, stateOf(t, st, stalePart.Root), "stale durable partial swept")
	assert.Equal(t, store.StatePartial, stateOf(t, st, pinnedPart.Root), "not-durable bytes retained")

	// Idempotent second pass.
	require.NoError(t, svc.Sweep(ctx))
	assert.Equal(t, store.StatePartial, stateOf(t, st, pinnedPart.Root))
}
