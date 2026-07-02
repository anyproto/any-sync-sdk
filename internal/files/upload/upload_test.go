package upload

import (
	"bytes"
	"context"
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
	n    int
	rows map[string]payloads.Row
}

func newFakeRegistrar() *fakeRegistrar {
	return &fakeRegistrar{rows: map[string]payloads.Row{}}
}

func (r *fakeRegistrar) RegisterFile(_ context.Context, ownerId string, opts RegisterOpts) (string, error) {
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
	require.True(t, res.Durable)
	require.NotEmpty(t, res.RootCid)

	row := reg.rows["owner1/"+res.FileId]
	require.Equal(t, res.RootCid, row.RootCid)
	require.Len(t, row.Enc.Key, 32)
	require.NotEmpty(t, row.NetworkSign)

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

func TestAddLimitExceeded(t *testing.T) {
	br := &fakeBroker{uploadCodes: []fileprotov2.ErrCode{fileprotov2.ErrCode_ErrLimitExceeded}}
	s, st := newService(t, br)
	reg := newFakeRegistrar()

	res, err := s.Add(context.Background(), reg, spaceId, "owner1", bytes.NewReader(testContent(50_000)), AddOpts{})
	require.NoError(t, err, "limit refusal must not fail the attach")
	require.False(t, res.Durable)
	require.NotEmpty(t, res.FileId)

	row := reg.rows["owner1/"+res.FileId]
	require.Empty(t, row.NetworkSign)
	root, err := cid.Decode(res.RootCid)
	require.NoError(t, err)
	info, err := st.Info(context.Background(), spaceId, root)
	require.NoError(t, err)
	require.Equal(t, store.StateComplete, info.State, "bytes stay local for the retry queue")
	require.Equal(t, 1, br.uploads, "limit is terminal, no retry")
}

func TestAddRetriesActivationWindow(t *testing.T) {
	br := &fakeBroker{uploadCodes: []fileprotov2.ErrCode{
		fileprotov2.ErrCode_ErrUnexpected, // broker still activating the space
		fileprotov2.ErrCode_Ok,
	}}
	s, _ := newService(t, br)
	reg := newFakeRegistrar()

	res, err := s.Add(context.Background(), reg, spaceId, "owner1", bytes.NewReader(testContent(20_000)), AddOpts{})
	require.NoError(t, err)
	require.True(t, res.Durable)
	require.Equal(t, 2, br.uploads)
}

func TestAddBind(t *testing.T) {
	br := &fakeBroker{}
	s, st := newService(t, br)
	reg := newFakeRegistrar()
	content := testContent(60_000)

	first, err := s.Add(context.Background(), reg, spaceId, "owner1", bytes.NewReader(content), AddOpts{Name: "orig.bin"})
	require.NoError(t, err)
	require.True(t, first.Durable)
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
	require.True(t, second.Durable)
	// Fresh random key ⇒ fresh ciphertext ⇒ a different root.
	require.NotEqual(t, first.RootCid, second.RootCid)
}

func TestAddSpoolSpill(t *testing.T) {
	br := &fakeBroker{}
	s, st := newService(t, br)
	s.memSpool = 1024 // force the spill path
	reg := newFakeRegistrar()
	content := testContent(80_000)

	res, err := s.Add(context.Background(), reg, spaceId, "owner1", bytes.NewReader(content), AddOpts{})
	require.NoError(t, err)
	require.True(t, res.Durable)

	ents, err := os.ReadDir(st.TmpDir())
	require.NoError(t, err)
	require.Empty(t, ents, "spool temp must be removed")
}
