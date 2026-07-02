package fetch

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	mrand "math/rand"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/commonfile/fileservice"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/files/crypt"
	"github.com/anyproto/any-sync-sdk/internal/files/store"
)

const spaceId = "space.test"

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

// buildSource runs the real upload pipeline in a throwaway store and
// returns the resulting CAR bytes (== the S3 object), root, key and
// plaintext.
func buildSource(t *testing.T, plainSize int) (car []byte, root cid.Cid, key, plain []byte) {
	t.Helper()
	ctx := context.Background()
	src := newStore(t)
	plain = make([]byte, plainSize)
	mrand.New(mrand.NewSource(int64(plainSize))).Read(plain)
	key = make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)

	b, err := src.NewBuild(ctx, spaceId)
	require.NoError(t, err)
	ct, err := crypt.NewEncryptReader(key, bytes.NewReader(plain))
	require.NoError(t, err)
	node, err := fileservice.NewFileHandler(b.Blockstore()).AddFile(ctx, ct)
	require.NoError(t, err)
	info, err := b.Finalize(ctx, node.Cid())
	require.NoError(t, err)

	h, err := src.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
	defer h.Close()
	car, err = io.ReadAll(io.NewSectionReader(h, 0, info.Size))
	require.NoError(t, err)
	return car, info.Root, key, plain
}

// serveCar is the synthetic CDN: Range-capable, counting requests.
func serveCar(t *testing.T, root cid.Cid, car []byte) (baseURL string, requests *atomic.Int64) {
	t.Helper()
	requests = &atomic.Int64{}
	want := "/blob/" + spaceId + "/" + root.String()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != want {
			http.NotFound(w, r)
			return
		}
		requests.Add(1)
		http.ServeContent(w, r, "", time.Unix(0, 0), bytes.NewReader(car))
	}))
	t.Cleanup(srv.Close)
	return srv.URL, requests
}

func staticBase(url string) BaseURL {
	return func(context.Context) (string, error) { return url, nil }
}

func TestOpenRemoteReadAll(t *testing.T) {
	ctx := context.Background()
	car, root, key, plain := buildSource(t, 3_200_000) // 4 leaves
	base, requests := serveCar(t, root, car)

	st := newStore(t)
	svc := New(st, staticBase(base))
	f, err := svc.Open(ctx, spaceId, root, key, true, "file1")
	require.NoError(t, err)
	require.EqualValues(t, len(plain), f.Size())
	got, err := io.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.True(t, bytes.Equal(plain, got), "plaintext round-trip")
	t.Logf("full read: %d range requests", requests.Load())

	// Everything persisted: byte-identical local CAR, readable offline.
	require.NoError(t, svc.Fetch(ctx, spaceId, root, true, "file1"))
	info, err := st.Info(ctx, spaceId, root)
	require.NoError(t, err)
	require.Equal(t, store.StateComplete, info.State)
	h, err := st.Open(ctx, spaceId, root)
	require.NoError(t, err)
	local, err := io.ReadAll(io.NewSectionReader(h, 0, info.Size))
	require.NoError(t, err)
	require.NoError(t, h.Close())
	require.True(t, bytes.Equal(car, local), "local CAR must equal the remote object byte-for-byte")

	offline := New(st, staticBase("")) // no remote rung
	f2, err := offline.Open(ctx, spaceId, root, key, true, "file1")
	require.NoError(t, err)
	got2, err := io.ReadAll(f2)
	require.NoError(t, err)
	require.NoError(t, f2.Close())
	require.True(t, bytes.Equal(plain, got2), "offline read after complete")
}

func TestOpenSeekTargeted(t *testing.T) {
	ctx := context.Background()
	car, root, key, plain := buildSource(t, 8_000_000) // 8 leaves
	base, requests := serveCar(t, root, car)

	st := newStore(t)
	svc := New(st, staticBase(base))
	f, err := svc.Open(ctx, spaceId, root, key, true, "file1")
	require.NoError(t, err)
	defer f.Close()

	// Seek deep into the file and read 64 KiB.
	off := int64(6_500_000)
	_, err = f.Seek(off, io.SeekStart)
	require.NoError(t, err)
	buf := make([]byte, 64<<10)
	_, err = io.ReadFull(f, buf)
	require.NoError(t, err)
	require.True(t, bytes.Equal(plain[off:off+int64(len(buf))], buf), "seek read")

	// No preload: probe + index + a handful of section ranges, never
	// the whole object.
	assert.LessOrEqual(t, requests.Load(), int64(6), "targeted seek must not stream the file")

	// SeekEnd works and EOF is clean.
	_, err = f.Seek(-10, io.SeekEnd)
	require.NoError(t, err)
	tail, err := io.ReadAll(f)
	require.NoError(t, err)
	require.True(t, bytes.Equal(plain[len(plain)-10:], tail))
}

func TestResumeAcrossReopen(t *testing.T) {
	ctx := context.Background()
	car, root, key, plain := buildSource(t, 5_000_000)
	base, _ := serveCar(t, root, car)

	st := newStore(t)
	svc := New(st, staticBase(base))
	f, err := svc.Open(ctx, spaceId, root, key, true, "file1")
	require.NoError(t, err)
	head := make([]byte, 1_000_000)
	_, err = io.ReadFull(f, head)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.True(t, bytes.Equal(plain[:len(head)], head))

	// The partial survives: a fresh open resumes from the bitmap and
	// the interrupted download completes.
	info, err := st.Info(ctx, spaceId, root)
	require.NoError(t, err)
	require.Equal(t, store.StatePartial, info.State)
	require.Positive(t, info.Present)

	require.NoError(t, svc.Fetch(ctx, spaceId, root, true, "file1"))
	info, err = st.Info(ctx, spaceId, root)
	require.NoError(t, err)
	require.Equal(t, store.StateComplete, info.State)
}

func TestOfflineOffloadedThenRefetch(t *testing.T) {
	ctx := context.Background()
	car, root, key, plain := buildSource(t, 2_000_000)
	base, _ := serveCar(t, root, car)

	st := newStore(t)
	svc := New(st, staticBase(base))
	require.NoError(t, svc.Fetch(ctx, spaceId, root, true, "file1"))
	require.NoError(t, st.Offload(ctx, spaceId, root))

	// Reopen refetches transparently through the public URL.
	f, err := svc.Open(ctx, spaceId, root, key, true, "file1")
	require.NoError(t, err)
	got, err := io.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.True(t, bytes.Equal(plain, got))
}

func TestNotDurableNotLocal(t *testing.T) {
	ctx := context.Background()
	_, root, key, _ := buildSource(t, 100_000)
	st := newStore(t)
	svc := New(st, staticBase("http://unused.test"))
	_, err := svc.Open(ctx, spaceId, root, key, false, "file1")
	require.ErrorIs(t, err, ErrNotAvailable)
}

func TestNoPublicBase(t *testing.T) {
	ctx := context.Background()
	_, root, key, _ := buildSource(t, 100_000)
	st := newStore(t)
	svc := New(st, staticBase(""))
	_, err := svc.Open(ctx, spaceId, root, key, true, "file1")
	require.ErrorIs(t, err, ErrNotAvailable)
}

func TestWrongObjectAtURL(t *testing.T) {
	ctx := context.Background()
	_, wantRoot, key, _ := buildSource(t, 150_000)
	otherCar, otherRoot, _, _ := buildSource(t, 160_000)
	// Serve the WRONG object at wantRoot's path.
	requests := &atomic.Int64{}
	_ = otherRoot
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.ServeContent(w, r, "", time.Unix(0, 0), bytes.NewReader(otherCar))
	}))
	t.Cleanup(srv.Close)

	st := newStore(t)
	svc := New(st, staticBase(srv.URL))
	_, err := svc.Open(ctx, spaceId, wantRoot, key, true, "file1")
	require.ErrorContains(t, err, "wanted "+wantRoot.String())
	// The rejected skeleton must not linger.
	_, err = st.Info(ctx, spaceId, otherRoot)
	require.ErrorIs(t, err, store.ErrNotFound)
}

func TestPromotionWindow404(t *testing.T) {
	restore := promotionDelay
	promotionDelay = 10 * time.Millisecond
	defer func() { promotionDelay = restore }()

	ctx := context.Background()
	car, root, key, plain := buildSource(t, 120_000)
	var after atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if after.Add(1) <= 2 {
			http.NotFound(w, r) // still promoting staging→blob
			return
		}
		http.ServeContent(w, r, "", time.Unix(0, 0), bytes.NewReader(car))
	}))
	t.Cleanup(srv.Close)

	st := newStore(t)
	svc := New(st, staticBase(srv.URL))
	f, err := svc.Open(ctx, spaceId, root, key, true, "file1")
	require.NoError(t, err, "must retry through the 404 window")
	got, err := io.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.True(t, bytes.Equal(plain, got))
}
