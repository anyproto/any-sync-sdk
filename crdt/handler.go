package crdt

import "errors"

// ErrUnknownDataset is returned when a change targets a dataset that has no
// registered handler. Per spec §8.2 the SDK still persists such changes via
// any-sync; this in-memory state just skips them.
var ErrUnknownDataset = errors.New("crdt: no handler for dataset")

// ErrValidation is the sentinel returned (via errors.Join) when a handler
// rejects an operation. Validation errors drop the offending op from any-store
// state without affecting the rest of the batch.
var ErrValidation = errors.New("crdt: validation rejected")

// Handler owns one dataset. It validates incoming ops against schema and
// per-record permissions and may pre-process ops before they hit the apply
// loop. v1 handlers are minimal — they don't carry per-handler storage.
//
// The signature matches spec §8 with two simplifications:
//  1. No db/tx parameter — the in-memory Controller is the only backing store.
//  2. Apply is omitted because the apply algorithm is centralized; handlers
//     control behavior through Validate alone in this phase. Phase 2 will add
//     hooks if/when needed.
type Handler interface {
	Dataset() string
	Version() int
	Validate(rec RecordChange, op Op) error
}

// DefaultHandler is a no-op handler accepting every op for the given dataset
// at version 1. Useful as a base for tests and as a convenient embed.
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

func (DefaultHandler) Validate(_ RecordChange, _ Op) error { return nil }
