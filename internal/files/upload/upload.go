// Package upload is the client upload path of the files subsystem
// (SYN-27): spool → tier decision (inline / BIND dedup / full) →
// encrypt → UnixFS DAG → local CARv2 → register the payloads row →
// best-effort durable phase against the fileV2 broker (presigned PUT +
// receipt-verified RequestSign).
//
// Registration is unconditional; only backup is gated. A file whose
// durable phase fails (quota, offline) stays registered with local
// bytes — the SYN-29 drive-toward-durable queue retries it later.
package upload

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonfile/fileproto/fileprotov2"
	"github.com/anyproto/any-sync/commonfile/fileservice"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/files/crypt"
	"github.com/anyproto/any-sync-sdk/internal/files/store"
	"github.com/anyproto/any-sync-sdk/internal/payloads"
)

var log = logger.NewNamed("sdk.files.upload")

// ErrLimited — the broker refused backup (storage limit). The file is
// registered and locally available; it is not durable.
var ErrLimited = errors.New("fileupload: storage limit exceeded")

const (
	// defaultMemSpool is the spool's in-memory threshold; larger inputs
	// spill to a temp file in the store scratch dir.
	defaultMemSpool = 8 << 20
	// defaultDurableWait bounds the INLINE durable phase of Attach when
	// the caller's ctx has no sooner deadline. It only covers the
	// happy-ish online case (including a short broker activation
	// window); anything longer is the persistent queue's job — Attach
	// must never hold the caller hostage to the network.
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

// Enqueuer is the SYN-29 persistent-queue seam. The durable job is
// enqueued BEFORE the inline durable phase runs and removed on its
// success, so a crash mid-upload leaves a persisted job instead of a
// forgotten unsigned row. Nil = no queue (tests).
type Enqueuer interface {
	Enqueue(ctx context.Context, kind, spaceId, fileId string) error
	Remove(ctx context.Context, kind, spaceId, fileId string) error
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

// AddOpts carries caller metadata for one file.
type AddOpts struct {
	Name string
	Mime string
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
// payloads row and, for the S3 tier, lands the CARv2 locally and runs
// the durable phase. Durable-phase failures do not fail Add — the row
// is registered and the bytes are local; Result.Durable reports the
// outcome (ErrLimited and transient broker errors are logged and left
// to the retry queue).
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
			Name:   opts.Name,
			SHA256: sp.SHA256(),
			Mime:   opts.Mime,
			Inline: sp.Bytes(),
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
	fileId, err := reg.RegisterFile(ctx, ownerId, RegisterOpts{
		RootCid:     donor.RootCid,
		Size:        sp.Size(),
		NetworkSign: donor.NetworkSign,
		Enc: payloads.EncPayload{
			Key:    donor.Enc.Key,
			Name:   opts.Name,
			SHA256: sp.SHA256(),
			Mime:   opts.Mime,
		},
	})
	if err != nil {
		return Result{}, false, err
	}
	if err = s.store.AddRefs(ctx, spaceId, ref.Root, fileId); err != nil {
		return Result{}, false, err
	}
	return Result{
		FileId:  fileId,
		RootCid: donor.RootCid,
		Size:    sp.Size(),
		Durable: donor.NetworkSign != "",
		Bound:   true,
	}, true, nil
}

// addFull runs the S3 tier: encrypt → DAG → local CARv2 → register →
// durable phase.
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

	fileId, err := reg.RegisterFile(ctx, ownerId, RegisterOpts{
		RootCid: root.String(),
		Size:    sp.Size(),
		Enc: payloads.EncPayload{
			Key:    key,
			Name:   opts.Name,
			SHA256: sp.SHA256(),
			Mime:   opts.Mime,
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

	res := Result{FileId: fileId, RootCid: root.String(), Size: sp.Size()}
	// Enqueue-before-attempt: a crash anywhere in the durable phase
	// leaves a persisted job, not a forgotten unsigned row.
	if s.queue != nil {
		if err = s.queue.Enqueue(ctx, KindDurable, spaceId, fileId); err != nil {
			return Result{}, err
		}
	}
	sign, err := s.makeDurable(ctx, spaceId, root, info.Size)
	if err != nil {
		log.Info("durable phase deferred", zap.String("fileId", fileId),
			zap.String("spaceId", spaceId), zap.Error(err))
		return res, nil
	}
	if err = reg.SetNetworkSign(ctx, ownerId, fileId, sign); err != nil {
		return Result{}, err
	}
	if s.queue != nil {
		if err = s.queue.Remove(ctx, KindDurable, spaceId, fileId); err != nil {
			return Result{}, err
		}
	}
	res.Durable = true
	return res, nil
}

// DriveDurable runs ONE durable-phase attempt for an already
// registered row (the SYN-29 queue worker path — the queue owns the
// backoff, so no inner retry loop). No-op when the row is already
// durable or inline; ErrLimited on a limit refusal.
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
	sign, _, err := s.durableAttempt(ctx, spaceId, root, size)
	if err != nil {
		return err
	}
	return reg.SetNetworkSign(ctx, ownerId, fileId, sign)
}

// makeDurable drives one root through Upload → presigned PUT →
// RequestSign → receipt verify, retrying transient per-item outcomes
// (the broker activates a space lazily on first contact) within the
// durable-wait budget. Returns the verified networkSign.
func (s *Service) makeDurable(ctx context.Context, spaceId string, root cid.Cid, carSize int64) (string, error) {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.durableWait)
		defer cancel()
	}
	var lastErr error
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				if lastErr == nil {
					lastErr = ctx.Err()
				}
				return "", lastErr
			case <-time.After(s.retryDelay):
			}
		}
		sign, retryable, err := s.durableAttempt(ctx, spaceId, root, carSize)
		if err == nil {
			return sign, nil
		}
		if !retryable {
			return "", err
		}
		lastErr = err
	}
}

// durableAttempt is one pass of the durable phase. retryable marks
// outcomes worth another INLINE attempt — that is only the per-item
// business codes of the broker's lazy space activation window, i.e.
// cases where we ARE talking to the network. Transport-level failures
// (offline, node down, S3 unreachable) are never retried inline: the
// app is offline-first, so they defer to the persistent queue
// immediately instead of stalling the caller. ErrLimited and
// verification failures are terminal.
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
