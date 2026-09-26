package upload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	mrand "math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/commonfile/fileproto/fileprotov2"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/files/carfile"
	"github.com/anyproto/any-sync-sdk/internal/files/store"
	"github.com/anyproto/any-sync-sdk/internal/payloads"
	"github.com/anyproto/any-sync-sdk/space"
)

const spaceId = "space.test"

type fakeBroker struct {
	uploadCodes []fileprotov2.ErrCode // consumed one per Upload call; empty = Ok
	signCode    fileprotov2.ErrCode
	verifyErr   error

	uploads   int
	signs     int
	putBodies [][]byte
}

func (b *fakeBroker) Upload(_ context.Context, _ string, items []*fileprotov2.UploadRequestItem) (*fileprotov2.UploadResponse, error) {
	b.uploads++
	code := fileprotov2.ErrCode_Ok
	if len(b.uploadCodes) > 0 {
		code, b.uploadCodes = b.uploadCodes[0], b.uploadCodes[1:]
	}
	res := &fileprotov2.UploadResult{RootCid: items[0].RootCid, Code: code}
	if code == fileprotov2.ErrCode_Ok {
		res.Upload = &fileprotov2.PresignedUpload{Url: "http://presigned.test/put", MaxContentLength: 1 << 30}
	}
	return &fileprotov2.UploadResponse{Results: []*fileprotov2.UploadResult{res}}, nil
}

func (b *fakeBroker) Put(_ context.Context, _ *fileprotov2.PresignedUpload, body io.Reader, _ int64) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	b.putBodies = append(b.putBodies, data)
	return nil
}

func (b *fakeBroker) RequestSign(_ context.Context, _ string, rootCids [][]byte) (*fileprotov2.RequestSignResponse, error) {
	b.signs++
	res := &fileprotov2.RequestSignResult{RootCid: rootCids[0], Code: b.signCode}
	if b.signCode == fileprotov2.ErrCode_Ok {
		res.Receipt = &fileprotov2.NetworkSignReceipt{ReceiptPayload: []byte("payload"), Signature: []byte("sig")}
	}
	return &fileprotov2.RequestSignResponse{Results: []*fileprotov2.RequestSignResult{res}}, nil
}

func (b *fakeBroker) VerifyReceipt(_ *fileprotov2.NetworkSignReceipt, _ string, root cid.Cid, _ uint64) (string, error) {
	if b.verifyErr != nil {
		return "", b.verifyErr
	}
	return "fleetPeer/" + root.String(), nil
}

type fakeRegistrar struct {
	n           int
	rows        map[string]payloads.Row
	registerErr error  // returned by RegisterFile when set
	refusedRoot string // RootCid of the last refused registration
}

func newFakeRegistrar() *fakeRegistrar {
	return &fakeRegistrar{rows: map[string]payloads.Row{}}
}

func (r *fakeRegistrar) RegisterFile(_ context.Context, ownerId string, opts RegisterOpts) (string, error) {
	if r.registerErr != nil {
		r.refusedRoot = opts.RootCid
		return "", r.registerErr
	}
	r.n++
	id := fmt.Sprintf("file-%d", r.n)
	r.rows[ownerId+"/"+id] = payloads.Row{
		Id:          id,
		RootCid:     opts.RootCid,
		Size:        opts.Size,
		NetworkSign: opts.NetworkSign,
		Enc:         opts.Enc,
	}
	return id, nil
}

func (r *fakeRegistrar) SetNetworkSign(_ context.Context, ownerId, fileId, sign string) error {
	key := ownerId + "/" + fileId
	row, ok := r.rows[key]
	if !ok {
		return fmt.Errorf("no row %s", key)
	}
	row.NetworkSign = sign
	r.rows[key] = row
	return nil
}

func (r *fakeRegistrar) FindRow(_ context.Context, fileId string) (payloads.Row, error) {
	for _, row := range r.rows {
		if row.Id == fileId {
			return row, nil
		}
	}
	return payloads.Row{}, fmt.Errorf("no row %s", fileId)
}

func (r *fakeRegistrar) Row(_ context.Context, ownerId, fileId string) (payloads.Row, error) {
	row, ok := r.rows[ownerId+"/"+fileId]
	if !ok {
		return payloads.Row{}, fmt.Errorf("no row %s/%s", ownerId, fileId)
	}
	return row, nil
}

func newService(t *testing.T, br Broker) (*Service, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	db, err := anystore.Open(context.Background(), filepath.Join(dir, "meta.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	st, err := store.New(context.Background(), filepath.Join(dir, "files"), db)
	require.NoError(t, err)
	s := New(st, br)
	s.durableWait = 2 * time.Second
	s.retryDelay = time.Millisecond
	return s, st
}

func testContent(n int) []byte {
	buf := make([]byte, n)
	mrand.New(mrand.NewSource(int64(n))).Read(buf)
	return buf
}

func TestAddInline(t *testing.T) {
	br := &fakeBroker{}
	s, _ := newService(t, br)
	reg := newFakeRegistrar()
	content := testContent(100)

	res, err := s.Add(context.Background(), reg, spaceId, "owner1", bytes.NewReader(content), AddOpts{Name: "tiny.bin", Mime: "application/octet-stream"})
	require.NoError(t, err)
	require.True(t, res.Inline)
	require.True(t, res.Durable)
	require.Empty(t, res.RootCid)
	require.EqualValues(t, 100, res.Size)

	row := reg.rows["owner1/"+res.FileId]
	require.Equal(t, content, row.Enc.Inline)
	require.Empty(t, row.RootCid)
	require.Empty(t, row.Enc.Key)
	require.Equal(t, "tiny.bin", row.Enc.Name)
	require.Zero(t, br.uploads, "inline never touches the broker")
}

func TestAddFullDurable(t *testing.T) {
	br := &fakeBroker{}
	s, st := newService(t, br)
	reg := newFakeRegistrar()
	content := testContent(100_000)

	res, err := s.Add(context.Background(), reg, spaceId, "owner1", bytes.NewReader(content), AddOpts{Name: "big.bin"})
	require.NoError(t, err)
	require.False(t, res.Inline)
	require.False(t, res.Durable, "the durable phase belongs to the queue")
	require.NotEmpty(t, res.RootCid)
	require.Zero(t, br.uploads, "Add never touches the broker")

	row := reg.rows["owner1/"+res.FileId]
	require.Equal(t, res.RootCid, row.RootCid)
	require.Len(t, row.Enc.Key, 32)
	require.Empty(t, row.NetworkSign)

	require.NoError(t, s.DriveDurable(context.Background(), reg, spaceId, "owner1", res.FileId))
	require.NotEmpty(t, reg.rows["owner1/"+res.FileId].NetworkSign)

	// The PUT body is the local CARv2 verbatim.
	require.Len(t, br.putBodies, 1)
	cf, err := carfile.Open(bytes.NewReader(br.putBodies[0]))
	require.NoError(t, err)
	require.Equal(t, res.RootCid, cf.Root().String())

	root, err := cid.Decode(res.RootCid)
	require.NoError(t, err)
	info, err := st.Info(context.Background(), spaceId, root)
	require.NoError(t, err)
	require.Equal(t, store.StateComplete, info.State)
	require.EqualValues(t, len(br.putBodies[0]), info.Size)
	require.Equal(t, []string{res.FileId}, info.Refs)
}

func TestDriveDurableRetriesActivationWindow(t *testing.T) {
	br := &fakeBroker{uploadCodes: []fileprotov2.ErrCode{
		fileprotov2.ErrCode_ErrUnexpected, // broker still activating the space
		fileprotov2.ErrCode_Ok,
	}}
	s, _ := newService(t, br)
	reg := newFakeRegistrar()

	res, err := s.Add(context.Background(), reg, spaceId, "owner1", bytes.NewReader(testContent(20_000)), AddOpts{})
	require.NoError(t, err)
	require.NoError(t, s.DriveDurable(context.Background(), reg, spaceId, "owner1", res.FileId))
	require.Equal(t, 2, br.uploads)
	require.NotEmpty(t, reg.rows["owner1/"+res.FileId].NetworkSign)
}

// TestDriveDurableRetryBudget pins that the activation-window retries
// stop at the durable-wait budget and hand the job back to the queue.
func TestDriveDurableRetryBudget(t *testing.T) {
	br := &fakeBroker{signCode: fileprotov2.ErrCode_ErrCidNotFound}
	s, _ := newService(t, br)
	s.durableWait = 20 * time.Millisecond
	reg := newFakeRegistrar()

	res, err := s.Add(context.Background(), reg, spaceId, "owner1", bytes.NewReader(testContent(20_000)), AddOpts{})
	require.NoError(t, err)
	start := time.Now()
	require.Error(t, s.DriveDurable(context.Background(), reg, spaceId, "owner1", res.FileId))
	require.Less(t, time.Since(start), 2*time.Second)
	require.Greater(t, br.signs, 1, "retried inside the budget")
	require.Len(t, br.putBodies, 1, "a sign retry never moves the bytes again")
}

// TestDriveDurableSharesSiblingReceipt pins that rows bound to one root
// share one upload, whichever job runs first.
func TestDriveDurableSharesSiblingReceipt(t *testing.T) {
	ctx := context.Background()
	br := &fakeBroker{}
	s, _ := newService(t, br)
	reg := newFakeRegistrar()
	content := testContent(120_000)

	donor, err := s.Add(ctx, reg, spaceId, "owner1", bytes.NewReader(content), AddOpts{})
	require.NoError(t, err)
	bound, err := s.Add(ctx, reg, spaceId, "owner2", bytes.NewReader(content), AddOpts{})
	require.NoError(t, err)
	require.True(t, bound.Bound)

	// The bound row's job happens to run first.
	require.NoError(t, s.DriveDurable(ctx, reg, spaceId, "owner2", bound.FileId))
	require.NoError(t, s.DriveDurable(ctx, reg, spaceId, "owner1", donor.FileId))
	require.Len(t, br.putBodies, 1, "one upload for the shared root")
	require.Equal(t, 1, br.uploads)
	sign := reg.rows["owner2/"+bound.FileId].NetworkSign
	require.NotEmpty(t, sign)
	require.Equal(t, sign, reg.rows["owner1/"+donor.FileId].NetworkSign)
}

func TestAddBind(t *testing.T) {
	br := &fakeBroker{}
	s, st := newService(t, br)
	reg := newFakeRegistrar()
	content := testContent(60_000)

	first, err := s.Add(context.Background(), reg, spaceId, "owner1", bytes.NewReader(content), AddOpts{Name: "orig.bin"})
	require.NoError(t, err)
	require.NoError(t, s.DriveDurable(context.Background(), reg, spaceId, "owner1", first.FileId))
	uploadsAfterFirst := br.uploads

	second, err := s.Add(context.Background(), reg, spaceId, "owner2", bytes.NewReader(content), AddOpts{Name: "copy.bin"})
	require.NoError(t, err)
	require.True(t, second.Bound)
	require.True(t, second.Durable, "BIND reuses the donor receipt")
	require.Equal(t, first.RootCid, second.RootCid)
	require.NotEqual(t, first.FileId, second.FileId)
	require.Equal(t, uploadsAfterFirst, br.uploads, "no bytes move on BIND")

	donor := reg.rows["owner1/"+first.FileId]
	bound := reg.rows["owner2/"+second.FileId]
	require.Equal(t, donor.Enc.Key, bound.Enc.Key, "file key is shared")
	require.Equal(t, donor.NetworkSign, bound.NetworkSign)
	require.Equal(t, "copy.bin", bound.Enc.Name, "caller metadata is per-row")

	root, err := cid.Decode(first.RootCid)
	require.NoError(t, err)
	info, err := st.Info(context.Background(), spaceId, root)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{first.FileId, second.FileId}, info.Refs)
}

func TestAddBindDonorGoneFallsThrough(t *testing.T) {
	br := &fakeBroker{}
	s, _ := newService(t, br)
	reg := newFakeRegistrar()
	content := testContent(30_000)

	first, err := s.Add(context.Background(), reg, spaceId, "owner1", bytes.NewReader(content), AddOpts{})
	require.NoError(t, err)
	delete(reg.rows, "owner1/"+first.FileId)

	second, err := s.Add(context.Background(), reg, spaceId, "owner2", bytes.NewReader(content), AddOpts{})
	require.NoError(t, err)
	require.False(t, second.Bound)
	// Fresh random key ⇒ fresh ciphertext ⇒ a different root.
	require.NotEqual(t, first.RootCid, second.RootCid)
}

type fakeQueue struct {
	entries map[string]bool // "kind/space/file" → pending
	log     []string
}

func newFakeQueue() *fakeQueue { return &fakeQueue{entries: map[string]bool{}} }

func (q *fakeQueue) Enqueue(_ context.Context, kind, spaceId, fileId string) error {
	q.entries[kind+"/"+spaceId+"/"+fileId] = true
	q.log = append(q.log, "enqueue "+kind+"/"+fileId)
	return nil
}

func TestAddEnqueuesDurableJob(t *testing.T) {
	br := &fakeBroker{}
	s, _ := newService(t, br)
	q := newFakeQueue()
	s.SetQueue(q)
	reg := newFakeRegistrar()

	res, err := s.Add(context.Background(), reg, spaceId, "owner1", bytes.NewReader(testContent(20_000)), AddOpts{})
	require.NoError(t, err)
	require.Equal(t, []string{"enqueue durable/" + res.FileId}, q.log)
	require.True(t, q.entries["durable/"+spaceId+"/"+res.FileId])
}

// stallBroker never answers: every call parks until its ctx ends.
type stallBroker struct {
	fakeBroker
}

func (b *stallBroker) Upload(ctx context.Context, _ string, _ []*fileprotov2.UploadRequestItem) (*fileprotov2.UploadResponse, error) {
	b.uploads++
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestAddNeverWaitsOnTheNetwork pins the offline-first contract: Add
// registers the file, enqueues the backup and returns without a broker
// call, even when the network would stall forever.
func TestAddNeverWaitsOnTheNetwork(t *testing.T) {
	br := &stallBroker{}
	s, _ := newService(t, br)
	q := newFakeQueue()
	s.SetQueue(q)
	reg := newFakeRegistrar()

	res, err := s.Add(context.Background(), reg, spaceId, "owner1", bytesReaderOf(t, 20_000), AddOpts{})
	require.NoError(t, err)
	require.False(t, res.Durable)
	require.Zero(t, br.uploads, "no broker call inside Add")
	require.True(t, q.entries["durable/"+spaceId+"/"+res.FileId], "backup handed to the queue")

	row := reg.rows["owner1/"+res.FileId]
	require.NotEmpty(t, row.RootCid, "registration is local-first, network-independent")
}

type offlineBroker struct {
	fakeBroker
}

func (b *offlineBroker) Upload(ctx context.Context, spaceId string, items []*fileprotov2.UploadRequestItem) (*fileprotov2.UploadResponse, error) {
	b.uploads++
	return nil, errors.New("dial: network unreachable")
}

// TestDriveDurableOfflineReturnsFast pins that a transport failure goes
// straight back to the queue's backoff, never the activation retry loop.
func TestDriveDurableOfflineReturnsFast(t *testing.T) {
	br := &offlineBroker{}
	s, _ := newService(t, br)
	s.durableWait = time.Minute // must NOT be consumed
	reg := newFakeRegistrar()

	res, err := s.Add(context.Background(), reg, spaceId, "owner1", bytesReaderOf(t, 20_000), AddOpts{})
	require.NoError(t, err)
	start := time.Now()
	require.Error(t, s.DriveDurable(context.Background(), reg, spaceId, "owner1", res.FileId))
	require.Less(t, time.Since(start), 2*time.Second)
	require.Equal(t, 1, br.uploads, "exactly one transport attempt")
}

func TestDriveDurableLimited(t *testing.T) {
	br := &fakeBroker{uploadCodes: []fileprotov2.ErrCode{fileprotov2.ErrCode_ErrLimitExceeded}}
	s, st := newService(t, br)
	reg := newFakeRegistrar()
	res, err := s.Add(context.Background(), reg, spaceId, "owner1", bytesReaderOf(t, 20_000), AddOpts{})
	require.NoError(t, err)
	err = s.DriveDurable(context.Background(), reg, spaceId, "owner1", res.FileId)
	require.ErrorIs(t, err, ErrLimited, "the queue needs the typed refusal to park the job")
	require.Equal(t, 1, br.uploads, "limit is terminal, no retry")
	require.Empty(t, reg.rows["owner1/"+res.FileId].NetworkSign)

	root, err := cid.Decode(res.RootCid)
	require.NoError(t, err)
	info, err := st.Info(context.Background(), spaceId, root)
	require.NoError(t, err)
	require.Equal(t, store.StateComplete, info.State, "bytes stay local for the next attempt")

	// Headroom appears: the next drive signs the row, then is idempotent.
	require.NoError(t, s.DriveDurable(context.Background(), reg, spaceId, "owner1", res.FileId))
	require.NotEmpty(t, reg.rows["owner1/"+res.FileId].NetworkSign)
	require.NoError(t, s.DriveDurable(context.Background(), reg, spaceId, "owner1", res.FileId))
}

func bytesReaderOf(t *testing.T, n int) *bytes.Reader {
	t.Helper()
	return bytes.NewReader(testContent(n))
}

// TestAddIntentMarkerLifecycle pins the attach crash-window guard: the
// intent marker exists exactly while the CAR could be orphaned — set
// before the row write, cleared once ref + retry job landed.
func TestAddIntentMarkerLifecycle(t *testing.T) {
	ctx := context.Background()
	br := &fakeBroker{}
	s, st := newService(t, br)
	s.SetQueue(newFakeQueue())
	reg := newFakeRegistrar()

	// Success: no marker survives.
	res, err := s.Add(ctx, reg, spaceId, "owner1", bytesReaderOf(t, 20_000), AddOpts{})
	require.NoError(t, err)
	root, err := cid.Decode(res.RootCid)
	require.NoError(t, err)
	_, marked, err := st.GetKV(ctx, store.IntentKey(spaceId, root))
	require.NoError(t, err)
	require.False(t, marked, "marker cleared after ref+job landed")

	// Registration failure (standing in for a crash after the marker):
	// the finalized CAR stays marked so GC pins and heals it.
	reg.registerErr = errors.New("register failed")
	_, err = s.Add(ctx, reg, spaceId, "owner2", bytesReaderOf(t, 21_000), AddOpts{})
	require.Error(t, err)
	var orphanRoot cid.Cid
	require.NoError(t, st.IterateAll(ctx, func(info store.Info) (bool, error) {
		if len(info.Refs) == 0 {
			orphanRoot = info.Root
			return false, nil
		}
		return true, nil
	}))
	require.True(t, orphanRoot.Defined(), "the finalized CAR exists unreferenced")
	owner, marked, err := st.GetKV(ctx, store.IntentKey(spaceId, orphanRoot))
	require.NoError(t, err)
	require.True(t, marked, "the orphan stays intent-marked for the sweep to heal")
	require.Equal(t, "owner2", owner)
}

// TestAddBindUnsignedDonorEnqueues pins that a row bound to a
// not-yet-durable donor gets its own drive-toward-durable job (the
// donor's job signs only the donor's row).
func TestAddBindUnsignedDonorEnqueues(t *testing.T) {
	br := &fakeBroker{}
	s, _ := newService(t, br)
	q := newFakeQueue()
	s.SetQueue(q)
	reg := newFakeRegistrar()
	content := testContent(40_000)

	donor, err := s.Add(context.Background(), reg, spaceId, "owner1", bytes.NewReader(content), AddOpts{})
	require.NoError(t, err)
	require.False(t, donor.Durable)

	bound, err := s.Add(context.Background(), reg, spaceId, "owner2", bytes.NewReader(content), AddOpts{})
	require.NoError(t, err)
	require.True(t, bound.Bound)
	require.False(t, bound.Durable)
	require.True(t, q.entries["durable/"+spaceId+"/"+donor.FileId], "donor job pending")
	require.True(t, q.entries["durable/"+spaceId+"/"+bound.FileId], "bound row needs its own job")
}

func TestAddVariantTags(t *testing.T) {
	br := &fakeBroker{}
	s, _ := newService(t, br)
	reg := newFakeRegistrar()

	// Inline variant (a tiny thumbnail).
	res, err := s.Add(context.Background(), reg, spaceId, "owner1", bytesReaderOf(t, 500),
		AddOpts{Name: "thumb.jpg", Variant: "thumbnail", VariantOf: "origId"})
	require.NoError(t, err)
	row := reg.rows["owner1/"+res.FileId]
	require.Equal(t, "thumbnail", row.Enc.Variant)
	require.Equal(t, "origId", row.Enc.VariantOf)

	// Full-tier variant.
	res, err = s.Add(context.Background(), reg, spaceId, "owner1", bytesReaderOf(t, 30_000),
		AddOpts{Variant: "preview", VariantOf: "origId"})
	require.NoError(t, err)
	row = reg.rows["owner1/"+res.FileId]
	require.Equal(t, "preview", row.Enc.Variant)
	require.Equal(t, "origId", row.Enc.VariantOf)
}

func TestAddSpoolSpill(t *testing.T) {
	br := &fakeBroker{}
	s, st := newService(t, br)
	s.memSpool = 1024 // force the spill path
	reg := newFakeRegistrar()
	content := testContent(80_000)

	_, err := s.Add(context.Background(), reg, spaceId, "owner1", bytes.NewReader(content), AddOpts{})
	require.NoError(t, err)

	ents, err := os.ReadDir(st.TmpDir())
	require.NoError(t, err)
	require.Empty(t, ents, "spool temp must be removed")
}

// TestDriveDurableSkipsWhenPeerMadeItDurable is the P2P durability-takeover
// edge case: device A attaches a file and its backup has not run yet
// (local CAR built, NOT durable). Another device (B) then fetches it over
// the LAN and makes it durable, and B's receipt syncs into A's row. When
// A's still-queued durable job later fires, it must NOT re-upload — the
// NetworkSign re-read in DriveDurable makes it a no-op.
func TestDriveDurableSkipsWhenPeerMadeItDurable(t *testing.T) {
	ctx := context.Background()
	br := &fakeBroker{}
	s, _ := newService(t, br)
	reg := newFakeRegistrar()

	res, err := s.Add(ctx, reg, spaceId, "owner1", bytes.NewReader(testContent(80_000)), AddOpts{Name: "x.bin"})
	require.NoError(t, err)
	require.False(t, res.Durable)

	// Device B made it durable; its receipt lands in A's row via CRDT sync.
	require.NoError(t, reg.SetNetworkSign(ctx, "owner1", res.FileId, "peerB-receipt"))

	// A's queued durable job fires now — must be a no-op (no re-upload).
	require.NoError(t, s.DriveDurable(ctx, reg, spaceId, "owner1", res.FileId))
	require.Zero(t, br.uploads,
		"A must not re-upload a file a peer already made durable")
	require.Equal(t, "peerB-receipt", reg.rows["owner1/"+res.FileId].NetworkSign,
		"the peer's receipt must be preserved, not overwritten")
}

// A refused owner (deleted, gone, unresolvable) means no row was
// written: the finalized CAR and its intent marker are dropped instead
// of being pinned past every sweep.
func TestAddOwnerRefusedDropsCar(t *testing.T) {
	for _, refusal := range []error{space.ErrObjectDeleted, space.ErrObjectNotFound, payloads.ErrOwnerUnknown} {
		t.Run(refusal.Error(), func(t *testing.T) {
			ctx := context.Background()
			s, st := newService(t, &fakeBroker{})
			s.SetQueue(newFakeQueue())
			reg := newFakeRegistrar()
			reg.registerErr = fmt.Errorf("payloads: derive payloads object: %w", refusal)

			_, err := s.Add(ctx, reg, spaceId, "gone", bytesReaderOf(t, 21_000), AddOpts{})
			require.ErrorIs(t, err, refusal)
			root, err := cid.Decode(reg.refusedRoot)
			require.NoError(t, err)
			_, err = st.Info(ctx, spaceId, root)
			require.ErrorIs(t, err, store.ErrNotFound, "the refused CAR is dropped")
			_, marked, err := st.GetKV(ctx, store.IntentKey(spaceId, root))
			require.NoError(t, err)
			require.False(t, marked, "the refused CAR's marker is dropped")
		})
	}
}

// On the bind path a refused owner drops only its intent marker: the
// donor keeps its CAR and refs.
func TestAddBindOwnerRefusedKeepsDonor(t *testing.T) {
	ctx := context.Background()
	s, st := newService(t, &fakeBroker{})
	reg := newFakeRegistrar()
	content := testContent(60_000)

	first, err := s.Add(ctx, reg, spaceId, "owner1", bytes.NewReader(content), AddOpts{})
	require.NoError(t, err)
	require.NoError(t, s.DriveDurable(ctx, reg, spaceId, "owner1", first.FileId))

	reg.registerErr = fmt.Errorf("payloads: derive payloads object: %w", space.ErrObjectDeleted)
	_, err = s.Add(ctx, reg, spaceId, "gone", bytes.NewReader(content), AddOpts{})
	require.ErrorIs(t, err, space.ErrObjectDeleted)

	root, err := cid.Decode(first.RootCid)
	require.NoError(t, err)
	info, err := st.Info(ctx, spaceId, root)
	require.NoError(t, err)
	require.Equal(t, []string{first.FileId}, info.Refs, "the donor's CAR and ref stay")
	_, marked, err := st.GetKV(ctx, store.IntentKey(spaceId, root))
	require.NoError(t, err)
	require.False(t, marked, "no marker pins the donor's root")
}
