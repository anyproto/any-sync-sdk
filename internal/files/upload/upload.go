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
	"sync"
	"time"

	"github.com/anyproto/any-sync/commonfile/fileproto/fileprotov2"
	"github.com/anyproto/any-sync/commonfile/fileservice"
	"github.com/ipfs/go-cid"

	"github.com/anyproto/any-sync-sdk/internal/files/crypt"
	"github.com/anyproto/any-sync-sdk/internal/files/store"
	"github.com/anyproto/any-sync-sdk/internal/payloads"
	"github.com/anyproto/any-sync-sdk/space"
)

// ErrLimited — the broker refused backup (storage limit). The file is
// registered and locally available; it is not durable.
var ErrLimited = errors.New("fileupload: storage limit exceeded")

const (
	// defaultMemSpool is the spool's in-memory threshold; larger inputs
	// spill to a temp file in the store scratch dir.
	defaultMemSpool = 8 << 20
	// defaultDurableWait bounds the retries of one durable-phase step
	// over the broker's lazy space-activation window; past it the
	// queue's backoff takes over. Short: the queue worker is serial.
	defaultDurableWait = 15 * time.Second
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
	// FindRow resolves a row by fileId alone (space-wide).
	FindRow(ctx context.Context, fileId string) (payloads.Row, error)
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

	// driving serializes DriveDurable per root, so rows that share
	// content never upload it side by side.
	drivingMu sync.Mutex
	driving   map[string]chan struct{}

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
		driving:     map[string]chan struct{}{},
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
		if ownerRefused(err) {
			// No row was written; the donor keeps its CAR, but this
			// marker would pin it past every sweep.
			_ = s.store.DeleteKV(ctx, store.IntentKey(spaceId, ref.Root))
		}
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
		if ownerRefused(err) {
			// No row was written, so nothing will ever reference this
			// CAR; the intent marker would pin it past every sweep.
			s.dropUnregistered(ctx, spaceId, root)
		}
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

// ownerRefused reports whether RegisterFile refused the owner before
// writing anything: the owner is deleted, gone, or not resolvable yet.
// Any other error may come from the row write itself, and then the CAR
// must stay pinned for the sweep to heal.
func ownerRefused(err error) bool {
	return errors.Is(err, space.ErrObjectDeleted) ||
		errors.Is(err, space.ErrObjectNotFound) ||
		errors.Is(err, payloads.ErrOwnerUnknown)
}

// dropUnregistered removes a finalized CAR that no row will reference,
// and its intent marker. Best effort: a failure leaves what the sweep
// would have left anyway.
func (s *Service) dropUnregistered(ctx context.Context, spaceId string, root cid.Cid) {
	if err := s.store.Delete(ctx, spaceId, root); err != nil {
		return
	}
	_ = s.store.DeleteKV(ctx, store.IntentKey(spaceId, root))
}

// DriveDurable drives an already registered row to a verified receipt
// (the queue worker path). Transport failures return at once — the
// queue owns that backoff; only the broker's activation-window codes
// are retried here. No-op when the row is already durable or inline; a
// row whose content another row already backed up takes that receipt
// without an upload; ErrLimited on a limit refusal.
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
	unlock, err := s.lockRoot(ctx, spaceId+"/"+row.RootCid)
	if err != nil {
		return err
	}
	defer unlock()
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
	sign, ok := s.siblingSign(ctx, reg, spaceId, root, fileId)
	if !ok {
		if sign, err = s.makeDurable(ctx, spaceId, root, size); err != nil {
			return err
		}
	}
	return reg.SetNetworkSign(ctx, ownerId, fileId, sign)
}

// lockRoot waits for the exclusive drive of one root.
func (s *Service) lockRoot(ctx context.Context, key string) (unlock func(), err error) {
	for {
		s.drivingMu.Lock()
		busy, ok := s.driving[key]
		if !ok {
			done := make(chan struct{})
			s.driving[key] = done
			s.drivingMu.Unlock()
			return func() {
				s.drivingMu.Lock()
				delete(s.driving, key)
				s.drivingMu.Unlock()
				close(done)
			}, nil
		}
		s.drivingMu.Unlock()
		select {
		case <-busy:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// siblingSign returns the receipt of another row bound to the same
// root: the receipt covers the root, not the row, so rows that share
// content share one upload.
func (s *Service) siblingSign(ctx context.Context, reg Registrar, spaceId string, root cid.Cid, fileId string) (string, bool) {
	info, err := s.store.Info(ctx, spaceId, root)
	if err != nil {
		return "", false
	}
	for _, ref := range info.Refs {
		if ref == fileId {
			continue
		}
		if sib, err := reg.FindRow(ctx, ref); err == nil && sib.NetworkSign != "" && sib.RootCid == root.String() {
			return sib.NetworkSign, true
		}
	}
	return "", false
}

// makeDurable drives one root through Upload → presigned PUT →
// RequestSign → receipt verify and returns the verified networkSign.
// The bytes move once: only the two broker RPCs retry, each over the
// broker's transient per-item outcomes. Transport-level failures
// (offline, node down, object store unreachable) are never retried
// here — they go back to the queue's backoff, as do ErrLimited and
// verification failures.
func (s *Service) makeDurable(ctx context.Context, spaceId string, root cid.Cid, carSize int64) (string, error) {
	var presigned *fileprotov2.PresignedUpload
	err := s.retryItem(ctx, func() (fileprotov2.ErrCode, error) {
		resp, err := s.broker.Upload(ctx, spaceId, []*fileprotov2.UploadRequestItem{
			{RootCid: root.Bytes(), Size: uint64(carSize)},
		})
		if err != nil {
			return 0, err
		}
		if len(resp.Results) != 1 {
			return 0, fmt.Errorf("fileupload: upload returned %d results for 1 item", len(resp.Results))
		}
		presigned = resp.Results[0].Upload
		return resp.Results[0].Code, nil
	}, "upload")
	if err != nil {
		return "", err
	}

	h, err := s.store.Open(ctx, spaceId, root)
	if err != nil {
		return "", err
	}
	putErr := s.broker.Put(ctx, presigned, io.NewSectionReader(h, 0, carSize), carSize)
	_ = h.Close()
	if putErr != nil {
		return "", putErr
	}

	var receipt *fileprotov2.NetworkSignReceipt
	err = s.retryItem(ctx, func() (fileprotov2.ErrCode, error) {
		resp, err := s.broker.RequestSign(ctx, spaceId, [][]byte{root.Bytes()})
		if err != nil {
			return 0, err
		}
		if len(resp.Results) != 1 {
			return 0, fmt.Errorf("fileupload: sign returned %d results for 1 item", len(resp.Results))
		}
		receipt = resp.Results[0].Receipt
		return resp.Results[0].Code, nil
	}, "sign")
	if err != nil {
		return "", err
	}
	return s.broker.VerifyReceipt(receipt, spaceId, root, uint64(carSize))
}

// retryItem runs one broker RPC until its per-item code is Ok, retrying
// the transient codes until the durable-wait budget is spent.
func (s *Service) retryItem(ctx context.Context, call func() (fileprotov2.ErrCode, error), phase string) error {
	retryUntil := time.Now().Add(s.durableWait)
	for {
		code, err := call()
		if err != nil {
			return err
		}
		if code == fileprotov2.ErrCode_Ok {
			return nil
		}
		if !itemRetryable(code) || !time.Now().Before(retryUntil) {
			return itemErr(phase, code)
		}
		timer := time.NewTimer(s.retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return itemErr(phase, code)
		case <-timer.C:
		}
	}
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
