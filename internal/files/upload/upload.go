// Package upload is the client upload path of the files subsystem
// (SYN-27): spool → tier decision (inline / BIND dedup / full) →
// encrypt → UnixFS DAG → local CARv2 → register the payloads row →
// enqueue the durable phase.
//
// Add never touches the network: the durable phase (presigned PUT +
// receipt-verified RequestSign against the fileV2 broker) belongs to
// the drive-toward-durable queue, which calls DriveDurable. A file
// whose durable phase fails (quota, offline) stays registered with
// local bytes and is retried by the queue.
package upload

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/anyproto/any-sync/commonfile/fileproto/fileprotov2"
	"github.com/anyproto/any-sync/commonfile/fileservice"
	"github.com/ipfs/go-cid"

	"github.com/anyproto/any-sync-sdk/internal/files/crypt"
	"github.com/anyproto/any-sync-sdk/internal/files/store"
	"github.com/anyproto/any-sync-sdk/internal/payloads"
)

// ErrLimited — the broker refused backup (storage limit). The file is
// registered and locally available; it is not durable.
var ErrLimited = errors.New("fileupload: storage limit exceeded")

const (
	// defaultMemSpool is the spool's in-memory threshold; larger inputs
	// spill to a temp file in the store scratch dir.
	defaultMemSpool = 8 << 20
	// defaultDurableWait bounds the retries of one durable drive over
	// the broker's lazy space-activation window; past it the queue's
	// backoff takes over.
	defaultDurableWait = 45 * time.Second
	// durableRetryDelay separates durable-phase attempts.
	durableRetryDelay = 2 * time.Second
	// fileKeySize is the per-file AES-256 key length.
	fileKeySize = 32
)

// Broker is the durable-phase seam (implemented by broker.Client;
// faked in tests).
type Broker interface {
	Upload(ctx context.Context, spaceId string, items []*fileprotov2.UploadRequestItem) (*fileprotov2.UploadResponse, error)
	Put(ctx context.Context, up *fileprotov2.PresignedUpload, body io.Reader, size int64) error
	RequestSign(ctx context.Context, spaceId string, rootCids [][]byte) (*fileprotov2.RequestSignResponse, error)
	VerifyReceipt(rcpt *fileprotov2.NetworkSignReceipt, spaceId string, root cid.Cid, objectSize uint64) (string, error)
}

// Registrar is the per-space payloads write surface (implemented over
// spaceimpl.PayloadsAPI; faked in tests).
type Registrar interface {
	RegisterFile(ctx context.Context, ownerId string, opts RegisterOpts) (fileId string, err error)
	SetNetworkSign(ctx context.Context, ownerId, fileId, sign string) error
	Row(ctx context.Context, ownerId, fileId string) (payloads.Row, error)
}

// Enqueuer is the persistent-queue seam: every unsigned row gets a
// durable job, and the queue's worker runs the durable phase. Nil = no
// queue (tests).
type Enqueuer interface {
	Enqueue(ctx context.Context, kind, spaceId, fileId string) error
}

// KindDurable is the drive-toward-durable job kind (mirrors the queue
// package constant; declared here so upload does not import it).
const KindDurable = "durable"

// RegisterOpts mirrors the payloads registration input.
type RegisterOpts struct {
	RootCid     string
	Size        int64
	NetworkSign string
	Enc         payloads.EncPayload
}

// AddOpts carries caller metadata for one file. All fields ride in
// the sealed (member-only) part of the row.
type AddOpts struct {
	Name string
	Mime string
	// Variant/VariantOf tag this file as an alternate representation
	// (e.g. a thumbnail) of an existing file. Validated by the caller
	// (spaceimpl) — this layer just seals them.
	Variant   string
	VariantOf string
}

// Result reports one completed Add.
type Result struct {
	FileId  string
	RootCid string // empty for inline
	Size    int64  // plaintext bytes
	Inline  bool
	Durable bool
	// Bound: the content matched an already-registered file (per-space
	// dedup); the new row reuses its rootCid, key and receipt.
	Bound bool
}

// Service is the upload orchestrator. One per SDK.
type Service struct {
	store  *store.Store
	broker Broker
	queue  Enqueuer // nil until SetQueue

	// test knobs; production uses the defaults above
	memSpool    int64
	durableWait time.Duration
	retryDelay  time.Duration
}

// SetQueue wires the persistent retry queue (sdk.Open, once).
func (s *Service) SetQueue(q Enqueuer) { s.queue = q }

// New builds the Service over the local store and the broker client.
func New(st *store.Store, br Broker) *Service {
	return &Service{
		store:       st,
		broker:      br,
		memSpool:    defaultMemSpool,
		durableWait: defaultDurableWait,
		retryDelay:  durableRetryDelay,
	}
}

// Add ingests r as a file bound to ownerId in spaceId: registers the
// payloads row and, for the S3 tier, lands the CARv2 locally and
// enqueues the durable phase. Add is local-only and never waits on the
// network; Result.Durable is true only for an inline file or one bound
// to an already durable donor.
func (s *Service) Add(ctx context.Context, reg Registrar, spaceId, ownerId string, r io.Reader, opts AddOpts) (Result, error) {
	sp, err := fillSpool(s.store.TmpDir(), s.memSpool, r)
	if err != nil {
		return Result{}, fmt.Errorf("fileupload: spool: %w", err)
	}
	defer sp.Close()

	if sp.Size() < payloads.InlineMaxSize {
		return s.addInline(ctx, reg, ownerId, sp, opts)
	}
	if res, ok, err := s.addBound(ctx, reg, spaceId, ownerId, sp, opts); err != nil {
		return Result{}, err
	} else if ok {
		return res, nil
	}
	return s.addFull(ctx, reg, spaceId, ownerId, sp, opts)
}

// addInline registers the bytes inside the sealed enc blob — no store,
// no broker, durable by construction (the row rides the CRDT).
func (s *Service) addInline(ctx context.Context, reg Registrar, ownerId string, sp *spool, opts AddOpts) (Result, error) {
	fileId, err := reg.RegisterFile(ctx, ownerId, RegisterOpts{
		Size: sp.Size(),
		Enc: payloads.EncPayload{
			Name:      opts.Name,
			SHA256:    sp.SHA256(),
			Mime:      opts.Mime,
			Inline:    sp.Bytes(),
			Variant:   opts.Variant,
			VariantOf: opts.VariantOf,
		},
	})
	if err != nil {
		return Result{}, err
	}
	return Result{FileId: fileId, Size: sp.Size(), Inline: true, Durable: true}, nil
}

// addBound tries the per-space dedup index: on a hit the new row
// reuses the donor's rootCid, wrapped key and receipt (BIND) — no
// bytes move. Any mismatch (donor row gone, sealed, inconsistent)
// falls through to the full path.
func (s *Service) addBound(ctx context.Context, reg Registrar, spaceId, ownerId string, sp *spool, opts AddOpts) (Result, bool, error) {
	ref, ok, err := s.store.LookupContent(ctx, spaceId, sp.SHA256())
	if err != nil || !ok {
		return Result{}, false, err
	}
	donor, err := reg.Row(ctx, ref.OwnerId, ref.FileId)
	if err != nil || donor.Sealed || donor.RootCid != ref.Root.String() ||
		len(donor.Enc.Key) == 0 || !bytes.Equal(donor.Enc.SHA256, sp.SHA256()) {
		return Result{}, false, nil
	}
	// Same crash-window guard as addFull: the row must never exist
	// without its CAR ref (the donor's ref alone dies with the donor).
	if err = s.store.SetKV(ctx, store.IntentKey(spaceId, ref.Root), ownerId); err != nil {
		return Result{}, false, err
	}
	fileId, err := reg.RegisterFile(ctx, ownerId, RegisterOpts{
		RootCid:     donor.RootCid,
		Size:        sp.Size(),
		NetworkSign: donor.NetworkSign,
		Enc: payloads.EncPayload{
			Key:       donor.Enc.Key,
			Name:      opts.Name,
			SHA256:    sp.SHA256(),
			Mime:      opts.Mime,
			Variant:   opts.Variant,
			VariantOf: opts.VariantOf,
		},
	})
	if err != nil {
		return Result{}, false, err
	}
	if err = s.store.AddRefs(ctx, spaceId, ref.Root, fileId); err != nil {
		return Result{}, false, err
	}
	durable := donor.NetworkSign != ""
	if !durable && s.queue != nil {
		// The donor's pending job signs only the donor's row; a row
		// bound to a not-yet-durable donor needs its own drive.
		if err = s.queue.Enqueue(ctx, KindDurable, spaceId, fileId); err != nil {
			return Result{}, false, err
		}
	}
	if err = s.store.DeleteKV(ctx, store.IntentKey(spaceId, ref.Root)); err != nil {
		return Result{}, false, err
	}
	return Result{
		FileId:  fileId,
		RootCid: donor.RootCid,
		Size:    sp.Size(),
		Durable: durable,
		Bound:   true,
	}, true, nil
}

// addFull runs the S3 tier: encrypt → DAG → local CARv2 → register →
// enqueue the durable phase.
func (s *Service) addFull(ctx context.Context, reg Registrar, spaceId, ownerId string, sp *spool, opts AddOpts) (Result, error) {
	key := make([]byte, fileKeySize)
	if _, err := rand.Read(key); err != nil {
		return Result{}, err
	}
	build, err := s.store.NewBuild(ctx, spaceId)
	if err != nil {
		return Result{}, err
	}
	ct, err := crypt.NewEncryptReader(key, sp.Reader())
	if err != nil {
		build.Discard()
		return Result{}, err
	}
	node, err := fileservice.NewFileHandler(build.Blockstore()).AddFile(ctx, ct)
	if err != nil {
		build.Discard()
		return Result{}, fmt.Errorf("fileupload: build dag: %w", err)
	}
	info, err := build.Finalize(ctx, node.Cid())
	if err != nil {
		return Result{}, fmt.Errorf("fileupload: finalize car: %w", err)
	}
	root := info.Root

	// Intent marker BEFORE the row write: the process can die between
	// any two of the following writes, and a registered row whose CAR
	// is unreferenced (and unqueued) must never look like garbage to
	// GC. The marker pins the root; the GC's sweep heals a stale one
	// by re-linking the row through the owner recorded here.
	if err = s.store.SetKV(ctx, store.IntentKey(spaceId, root), ownerId); err != nil {
		return Result{}, err
	}

	fileId, err := reg.RegisterFile(ctx, ownerId, RegisterOpts{
		RootCid: root.String(),
		Size:    sp.Size(),
		Enc: payloads.EncPayload{
			Key:       key,
			Name:      opts.Name,
			SHA256:    sp.SHA256(),
			Mime:      opts.Mime,
			Variant:   opts.Variant,
			VariantOf: opts.VariantOf,
		},
	})
	if err != nil {
		return Result{}, err
	}
	if err = s.store.AddRefs(ctx, spaceId, root, fileId); err != nil {
		return Result{}, err
	}
	if err = s.store.RecordContent(ctx, spaceId, sp.SHA256(), root, fileId, ownerId); err != nil {
		return Result{}, err
	}

	// The persisted job owns the durable phase; Enqueue wakes the worker.
	if s.queue != nil {
		if err = s.queue.Enqueue(ctx, KindDurable, spaceId, fileId); err != nil {
			return Result{}, err
		}
	}
	// Row, ref and job all landed: the intent marker has done its job.
	if err = s.store.DeleteKV(ctx, store.IntentKey(spaceId, root)); err != nil {
		return Result{}, err
	}
	return Result{FileId: fileId, RootCid: root.String(), Size: sp.Size()}, nil
}

// DriveDurable drives an already registered row to a verified receipt
// (the queue worker path). Transport failures return at once — the
// queue owns that backoff; only the broker's activation-window codes
// are retried here. No-op when the row is already durable or inline;
// ErrLimited on a limit refusal.
func (s *Service) DriveDurable(ctx context.Context, reg Registrar, spaceId, ownerId, fileId string) error {
	row, err := reg.Row(ctx, ownerId, fileId)
	if err != nil {
		return err
	}
	if row.NetworkSign != "" || row.Inline() {
		return nil
	}
	root, err := cid.Decode(row.RootCid)
	if err != nil {
		return fmt.Errorf("fileupload: row %s rootCid: %w", fileId, err)
	}
	h, err := s.store.Open(ctx, spaceId, root)
	if err != nil {
		return fmt.Errorf("fileupload: drive %s: %w", fileId, err)
	}
	complete := h.Complete()
	size, err := h.Size()
	_ = h.Close()
	if err != nil {
		return err
	}
	if !complete {
		return fmt.Errorf("fileupload: drive %s: local bytes incomplete", fileId)
	}
	sign, err := s.makeDurable(ctx, spaceId, root, size)
	if err != nil {
		return err
	}
	return reg.SetNetworkSign(ctx, ownerId, fileId, sign)
}

// makeDurable drives one root through Upload → presigned PUT →
// RequestSign → receipt verify, retrying transient per-item outcomes
// (the broker activates a space lazily on first contact) until the
// durable-wait budget is spent. The budget bounds the retries only,
// never the PUT — that is ctx's job. Returns the verified networkSign.
func (s *Service) makeDurable(ctx context.Context, spaceId string, root cid.Cid, carSize int64) (string, error) {
	retryUntil := time.Now().Add(s.durableWait)
	for {
		sign, retryable, err := s.durableAttempt(ctx, spaceId, root, carSize)
		if err == nil {
			return sign, nil
		}
		if !retryable || !time.Now().Before(retryUntil) {
			return "", err
		}
		select {
		case <-ctx.Done():
			return "", err
		case <-time.After(s.retryDelay):
		}
	}
}

// durableAttempt is one pass of the durable phase. retryable marks
// outcomes worth an immediate re-attempt — only the per-item business
// codes of the broker's lazy space activation window, i.e. cases where
// we ARE talking to the network. Transport-level failures (offline,
// node down, S3 unreachable) go back to the queue's backoff, as do
// ErrLimited and verification failures.
func (s *Service) durableAttempt(ctx context.Context, spaceId string, root cid.Cid, carSize int64) (sign string, retryable bool, err error) {
	upResp, err := s.broker.Upload(ctx, spaceId, []*fileprotov2.UploadRequestItem{
		{RootCid: root.Bytes(), Size: uint64(carSize)},
	})
	if err != nil {
		return "", false, err
	}
	if len(upResp.Results) != 1 {
		return "", false, fmt.Errorf("fileupload: upload returned %d results for 1 item", len(upResp.Results))
	}
	if code := upResp.Results[0].Code; code != fileprotov2.ErrCode_Ok {
		return "", itemRetryable(code), itemErr("upload", code)
	}

	h, err := s.store.Open(ctx, spaceId, root)
	if err != nil {
		return "", false, err
	}
	putErr := s.broker.Put(ctx, upResp.Results[0].Upload, io.NewSectionReader(h, 0, carSize), carSize)
	_ = h.Close()
	if putErr != nil {
		return "", false, putErr
	}

	signResp, err := s.broker.RequestSign(ctx, spaceId, [][]byte{root.Bytes()})
	if err != nil {
		return "", false, err
	}
	if len(signResp.Results) != 1 {
		return "", false, fmt.Errorf("fileupload: sign returned %d results for 1 item", len(signResp.Results))
	}
	if code := signResp.Results[0].Code; code != fileprotov2.ErrCode_Ok {
		return "", itemRetryable(code), itemErr("sign", code)
	}
	sign, err = s.broker.VerifyReceipt(signResp.Results[0].Receipt, spaceId, root, uint64(carSize))
	if err != nil {
		return "", false, err
	}
	return sign, false, nil
}

// itemRetryable classifies per-item outcomes: ErrUnexpected covers the
// broker's lazy space activation window; ErrNotResponsible is a
// routing signal; ErrCidNotFound after a PUT is the staging visibility
// window. Limit and auth outcomes are terminal.
func itemRetryable(code fileprotov2.ErrCode) bool {
	switch code {
	case fileprotov2.ErrCode_ErrUnexpected,
		fileprotov2.ErrCode_ErrNotResponsible,
		fileprotov2.ErrCode_ErrCidNotFound:
		return true
	default:
		return false
	}
}

func itemErr(phase string, code fileprotov2.ErrCode) error {
	if code == fileprotov2.ErrCode_ErrLimitExceeded {
		return ErrLimited
	}
	return fmt.Errorf("fileupload: %s: %v", phase, code)
}
