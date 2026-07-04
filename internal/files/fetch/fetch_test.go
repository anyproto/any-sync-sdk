package fetch

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
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

	"github.com/anyproto/any-sync-sdk/internal/files/carfile"
	"github.com/anyproto/any-sync-sdk/internal/files/crypt"
	"github.com/anyproto/any-sync-sdk/internal/files/store"
	"github.com/anyproto/any-sync-sdk/space"
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

// TestPartialReadableOffline pins the offline-first contract: the
// locally-present ranges of a partial file serve with no network; only
// a read that hits a hole surfaces the unavailability.
func TestPartialReadableOffline(t *testing.T) {
	ctx := context.Background()
	car, root, key, plain := buildSource(t, 5_000_000)
	base, _ := serveCar(t, root, car)

	st := newStore(t)
	online := New(st, staticBase(base))
	f, err := online.Open(ctx, spaceId, root, key, true, "file1")
	require.NoError(t, err)
	head := make([]byte, 1_500_000)
	_, err = io.ReadFull(f, head)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	offline := New(st, staticBase("")) // network gone
	f2, err := offline.Open(ctx, spaceId, root, key, true, "file1")
	require.NoError(t, err, "a partial file must open offline")
	got := make([]byte, 1_000_000)
	_, err = io.ReadFull(f2, got)
	require.NoError(t, err, "locally-present range must read offline")
	require.True(t, bytes.Equal(plain[:len(got)], got))

	// A hole surfaces the unavailability lazily — at the seek (the CFB
	// decryptor recovers the IV from the preceding ciphertext block) or
	// at the read, never at Open.
	_, holeErr := f2.Seek(4_000_000, io.SeekStart)
	if holeErr == nil {
		_, holeErr = io.ReadFull(f2, got[:4096])
	}
	require.ErrorIs(t, holeErr, ErrNotAvailable)
	require.NoError(t, f2.Close())
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
	require.ErrorIs(t, err, space.ErrFileNotAvailable, "consumers match the public sentinel")
}

func TestNoPublicBase(t *testing.T) {
	ctx := context.Background()
	_, root, key, _ := buildSource(t, 100_000)
	st := newStore(t)
	svc := New(st, staticBase(""))
	_, err := svc.Open(ctx, spaceId, root, key, true, "file1")
	require.ErrorIs(t, err, ErrNotAvailable)
	require.ErrorIs(t, err, space.ErrFileNotAvailable)
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

// TestWrongObjectDoesNotTouchExistingFile pins that a wrong object at
// a file's public URL is rejected before ANYTHING is written — in
// particular it must not merge into (or delete) another local file
// that happens to be the served object.
func TestWrongObjectDoesNotTouchExistingFile(t *testing.T) {
	ctx := context.Background()
	_, wantRoot, key, _ := buildSource(t, 150_000)
	otherCar, otherRoot, _, _ := buildSource(t, 160_000)

	st := newStore(t)
	// File B is a legitimate complete local file.
	baseB, _ := serveCar(t, otherRoot, otherCar)
	require.NoError(t, New(st, staticBase(baseB)).Fetch(ctx, spaceId, otherRoot, true, "fileB"))

	// A misconfigured CDN serves B's bytes at A's URL.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "", time.Unix(0, 0), bytes.NewReader(otherCar))
	}))
	t.Cleanup(srv.Close)
	_, err := New(st, staticBase(srv.URL)).Open(ctx, spaceId, wantRoot, key, true, "fileA")
	require.ErrorContains(t, err, "wanted "+wantRoot.String())

	// B is untouched: complete, refs intact.
	info, err := st.Info(ctx, spaceId, otherRoot)
	require.NoError(t, err)
	require.Equal(t, store.StateComplete, info.State)
	require.Equal(t, []string{"fileB"}, info.Refs)
}

// Test206WithoutContentRange pins that a proxy answering 206 without a
// parseable Content-Range yields a clean error, never a slice-bounds
// panic from treating a truncated probe as the whole object.
func Test206WithoutContentRange(t *testing.T) {
	ctx := context.Background()
	car, root, key, _ := buildSource(t, 3_000_000) // index offset far past the probe
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Serve the requested range but strip the Content-Range header.
		rec := httptest.NewRecorder()
		http.ServeContent(rec, r, "", time.Unix(0, 0), bytes.NewReader(car))
		for k, vs := range rec.Header() {
			if k == "Content-Range" {
				continue
			}
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	}))
	t.Cleanup(srv.Close)

	st := newStore(t)
	_, err := New(st, staticBase(srv.URL)).Open(ctx, spaceId, root, key, true, "file1")
	require.ErrorContains(t, err, "Content-Range")
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

// --- P2P peer-source ladder tests (SYN-48) --------------------------------

// fakeCar serves ranges of a CAR object, optionally lying or failing, so
// we can exercise the peer→HTTP validation ladder.
type fakeCar struct {
	car      []byte
	fail     bool  // every read errors (transient/transport failure)
	lieBelow int64 // corrupt ReadRange whose offset is < this (0 = never)
	lieProbe bool  // corrupt bytes returned by ReadProbe
}

func (f *fakeCar) ReadRange(_ context.Context, off, length int64) ([]byte, error) {
	if f.fail {
		return nil, errors.New("peer down")
	}
	end := off + length
	if end > int64(len(f.car)) {
		end = int64(len(f.car))
	}
	out := append([]byte(nil), f.car[off:end]...)
	if f.lieBelow > 0 && off < f.lieBelow {
		for i := range out {
			out[i] ^= 0xff
		}
	}
	return out, nil
}

func (f *fakeCar) ReadProbe(_ context.Context) ([]byte, int64, error) {
	if f.fail {
		return nil, 0, errors.New("peer down")
	}
	n := int64(4096)
	if n > int64(len(f.car)) {
		n = int64(len(f.car))
	}
	head := append([]byte(nil), f.car[:n]...)
	if f.lieProbe {
		for i := range head {
			head[i] ^= 0xff
		}
	}
	return head, int64(len(f.car)), nil
}

type fakePeerSource struct {
	car    *fakeCar
	banned *bool
	absent bool
}

func (p *fakePeerSource) SourceFor(context.Context, string, cid.Cid) (CarSource, func(), bool) {
	if p.absent {
		return nil, nil, false
	}
	return p.car, func() {
		if p.banned != nil {
			*p.banned = true
		}
	}, true
}

// A healthy peer serves the whole file; HTTP egress must be zero.
func TestPeerPreferredNoHTTPEgress(t *testing.T) {
	ctx := context.Background()
	car, root, key, plain := buildSource(t, 3_200_000)
	base, requests := serveCar(t, root, car)

	st := newStore(t)
	svc := New(st, staticBase(base))
	svc.SetPeer(&fakePeerSource{car: &fakeCar{car: car}})

	f, err := svc.Open(ctx, spaceId, root, key, true, "file1")
	require.NoError(t, err)
	got, err := io.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.True(t, bytes.Equal(plain, got))
	require.Zero(t, requests.Load(), "a healthy peer serves the whole file with no HTTP egress")
}

// A peer that lies on DATA ranges (honest probe) is caught by the wanted-
// block cid check, banned, and the fetch completes over HTTP — the store
// is never corrupted and the file stays readable.
func TestLyingPeerFallsBackToHTTPAndBans(t *testing.T) {
	ctx := context.Background()
	car, root, key, plain := buildSource(t, 3_200_000)
	base, requests := serveCar(t, root, car)

	// Corrupt only the block region (offsets below the index) — the peer
	// serves an honest CARv2 structure (head + index) so seeding works,
	// then lies on block bytes, which the wanted-block cid check catches.
	hdr, err := carfile.ParseHeader(car)
	require.NoError(t, err)

	st := newStore(t)
	svc := New(st, staticBase(base))
	banned := false
	svc.SetPeer(&fakePeerSource{car: &fakeCar{car: car, lieBelow: int64(hdr.IndexOffset)}, banned: &banned})

	f, err := svc.Open(ctx, spaceId, root, key, true, "file1")
	require.NoError(t, err, "a lying peer must not fail the fetch — HTTP serves it")
	got, err := io.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.True(t, bytes.Equal(plain, got), "content is correct despite the lying peer")
	require.True(t, banned, "a peer serving invalid bytes must be banned")
	require.Greater(t, requests.Load(), int64(0), "HTTP fallback was used")

	// The store is intact: a full fetch completes and the local CAR
	// equals the real object byte-for-byte (no corruption landed).
	require.NoError(t, svc.Fetch(ctx, spaceId, root, true, "file1"))
	h, err := st.Open(ctx, spaceId, root)
	require.NoError(t, err)
	info, _ := st.Info(ctx, spaceId, root)
	local, err := io.ReadAll(io.NewSectionReader(h, 0, info.Size))
	require.NoError(t, err)
	require.NoError(t, h.Close())
	require.True(t, bytes.Equal(car, local), "no corrupt bytes persisted")
}

// A peer that lies on the PROBE (wrong root) is caught by seed's root
// check, banned, and seeding continues over HTTP.
func TestLyingPeerProbeFallsBack(t *testing.T) {
	ctx := context.Background()
	car, root, key, plain := buildSource(t, 800_000)
	base, _ := serveCar(t, root, car)

	st := newStore(t)
	svc := New(st, staticBase(base))
	banned := false
	svc.SetPeer(&fakePeerSource{car: &fakeCar{car: car, lieProbe: true}, banned: &banned})

	f, err := svc.Open(ctx, spaceId, root, key, true, "file1")
	require.NoError(t, err)
	got, err := io.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.True(t, bytes.Equal(plain, got))
	require.True(t, banned, "a peer lying on the probe must be banned")
}

// A NON-durable file (no HTTP object) whose only peer fails must return a
// clean error — never a nil-pointer panic (the typed-nil http regression).
func TestNonDurablePeerFailsGracefully(t *testing.T) {
	ctx := context.Background()
	car, root, key, _ := buildSource(t, 800_000)

	st := newStore(t)
	svc := New(st, staticBase("")) // no public base
	svc.SetPeer(&fakePeerSource{car: &fakeCar{car: car, fail: true}})

	// The regression: with a typed-nil HTTP source, the demote path
	// dereferenced nil and panicked. It must return a clean error.
	_, err := svc.Open(ctx, spaceId, root, key, false, "file1") // durable=false
	require.Error(t, err, "no usable source → error, not panic")
}

// A non-durable file with no peer at all also fails gracefully.
func TestNonDurableNoPeerGraceful(t *testing.T) {
	ctx := context.Background()
	_, root, key, _ := buildSource(t, 800_000)
	st := newStore(t)
	svc := New(st, staticBase(""))
	svc.SetPeer(&fakePeerSource{absent: true})
	_, err := svc.Open(ctx, spaceId, root, key, false, "file1")
	require.ErrorIs(t, err, space.ErrFileNotAvailable)
}
