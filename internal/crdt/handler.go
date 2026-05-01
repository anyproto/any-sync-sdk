package crdt

import (
	"context"
	"errors"

	"github.com/anyproto/any-store/v2/anyenc"
)

// ErrUnknownDataset is returned when a change targets a dataset that has no
// registered handler. Per spec §8.2 the SDK still persists such changes via
// any-sync; this in-memory state just skips them.
var ErrUnknownDataset = errors.New("crdt: no handler for dataset")

// ErrValidation is the sentinel returned (via errors.Join) when a handler
// rejects an operation. Per-op rejections drop just the offending op from
// the apply step; whole-change validation failures (path syntax) abort the
// Change entirely.
var ErrValidation = errors.New("crdt: validation rejected")

// ChangeCtx is the per-callback context handed to a Handler. It carries the
// originating Change (read-only metadata: VersionId, ChangeId, Timestamp,
// Creator, ObjectAuthor, …) and the record's pre-op state.
//
// ObjectAuthor is the constant root-signer (object creator); Creator is the
// per-change signer (who wrote THIS change). For the root change they
// coincide; for shared spaces / multi-author objects they diverge — handlers
// that gate "only the author of this message can edit it" should read
// Creator, while handlers that stamp object-level provenance should read
// ObjectAuthor.
//
// Before evolves across ops in the same RecordChange: op[1]'s Before is
// op[0]'s after — but only counting ops that actually landed (rejected ops
// don't mutate state). Nil during BeforeCreate, since the record didn't
// exist before this Change.
//
// Pointer-passed; do not retain the pointer past the callback.
type ChangeCtx struct {
	Change *Change
	Before *anyenc.Value
}

// Sibling describes a write the handler wants applied to a different
// dataset on the same Controller, atomically with the triggering change.
// The Record's `_ver` entries are stamped by the apply loop with the
// triggering Change's VersionId — handlers must not set them.
type Sibling struct {
	Dataset string
	Record  RecordChange
}

// Sink collects same-record derived ops and cross-dataset sibling writes
// emitted during a single RecordChange's Modify callback. Pooled at the
// Controller, reset between RecordChanges, drained by the apply loop.
//
// Derived ops are folded into the same any-store Modify call (zero extra
// reads/writes); sibling writes execute as separate UpsertIds in the same
// transaction after the outer Modify returns.
type Sink struct {
	derived []Op
	sibling []Sibling
}

// Derive queues an op to apply on the same record, in the same Modify
// callback as the triggering op. Inherits the change's VersionId.
func (s *Sink) Derive(op Op) { s.derived = append(s.derived, op) }

// Project queues a write to a different dataset on the same Controller.
// Applied in the same WriteTx as the triggering change.
func (s *Sink) Project(dataset string, rec RecordChange) {
	s.sibling = append(s.sibling, Sibling{Dataset: dataset, Record: rec})
}

// reset clears both slices for pool reuse without releasing capacity.
func (s *Sink) reset() {
	for i := range s.derived {
		s.derived[i] = Op{}
	}
	s.derived = s.derived[:0]
	for i := range s.sibling {
		s.sibling[i] = Sibling{}
	}
	s.sibling = s.sibling[:0]
}

// Handler owns one dataset. Lifecycle hooks fire inside any-store's Modify
// callback so handlers can read pre-state from ctx.Before and emit derived
// or sibling writes via sink without an extra DB round-trip.
//
// Per-op error returns from BeforeModify drop just the offending op; other
// ops in the same RecordChange still apply. BeforeCreate / BeforeDelete are
// per-record, and an error there drops the whole RecordChange.
//
// All three Before* methods may be no-ops (see DefaultHandler).
type Handler interface {
	Dataset() string
	Version() int

	// Init runs once at registration (NewController / RegisterHandler).
	// Use it to acquire dependencies; nil for stateless handlers.
	Init(ctx context.Context) error

	BeforeCreate(ctx *ChangeCtx, rec *RecordChange, sink *Sink) error
	BeforeModify(ctx *ChangeCtx, rec *RecordChange, op *Op, sink *Sink) error
	BeforeDelete(ctx *ChangeCtx, rec *RecordChange, sink *Sink) error
}

// DefaultHandler is a no-op handler accepting every op for the given
// dataset. Useful as a base for tests and as a convenient embed.
type DefaultHandler struct {
	DatasetName    string
	HandlerVersion int
}

func (d DefaultHandler) Dataset() string { return d.DatasetName }

func (d DefaultHandler) Version() int {
	if d.HandlerVersion == 0 {
		return 1
	}
	return d.HandlerVersion
}

func (DefaultHandler) Init(_ context.Context) error { return nil }

func (DefaultHandler) BeforeCreate(_ *ChangeCtx, _ *RecordChange, _ *Sink) error { return nil }
func (DefaultHandler) BeforeModify(_ *ChangeCtx, _ *RecordChange, _ *Op, _ *Sink) error {
	return nil
}
func (DefaultHandler) BeforeDelete(_ *ChangeCtx, _ *RecordChange, _ *Sink) error { return nil }
