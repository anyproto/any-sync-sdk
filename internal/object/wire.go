// Package object owns the change-payload wire codec, the per-space
// VersionId allocator, and the binder that ties one any-sync ObjectTree
// to one crdt.Controller. See doc.go for the broader role.
package object

import (
	"errors"
	"fmt"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// Wire format for the bytes that go inside any-sync's
// objecttree.Change.Data. anyenc-encoded object with short keys to keep
// the on-wire payload small. Path/Type/Variant strings dominate; arenas
// are pooled by the caller (Codec).
//
// Structure:
//
//	{
//	  d: <dataset>,           // string
//	  v: <dataVersion>,       // string
//	  t: [<traceIds...>],     // optional []string
//	  r: [<record...>],       // []record
//	}
//	record: { i?:id, u?:upsert, x?:variant, o:[op...] }
//	op:     { t:opType, p?:[path...], v?:payload }
//
// VersionId, ChangeId, AddSeq, ObjectId, SpaceId, Timestamp travel on
// the any-sync envelope and are NOT part of this payload.
//
// TODO: switch to any-store v2 anyenc + MarshalCompressed for the
// large-payload wins called out in docs/cold_restore_perf.md. The
// controller still operates on v1 anyenc today, so deferring until that
// migration to avoid v1↔v2 conversion at the codec boundary.
const (
	keyDataset     = "d"
	keyDataVersion = "v"
	keyTraceIds    = "t"
	keyRecords     = "r"

	keyRecordId      = "i"
	keyRecordUpsert  = "u"
	keyRecordVariant = "x"
	keyRecordOps     = "o"

	keyOpType    = "t"
	keyOpPath    = "p"
	keyOpPayload = "v"
)

// ErrEmptyPayload is returned by Decode when the wire bytes are empty
// or the parsed top-level value is not an object.
var ErrEmptyPayload = errors.New("object: empty or non-object change payload")

// Codec encodes/decodes the change payload. Pools its own arena and
// parser — both are cheap to reuse across many writes.
//
// Not safe for concurrent use; the apply path is single-threaded per
// object and writes go through a per-object lock anyway.
type Codec struct {
	arena  anyenc.Arena
	parser anyenc.Parser
	scratch []byte
}

// NewCodec returns a fresh codec.
func NewCodec() *Codec { return &Codec{} }

// Encode serialises the caller-supplied portion of a Change into
// wire bytes. Reuses the codec's arena across calls.
//
// The CRDT envelope fields (VersionId, ChangeId, AddSeq, ObjectId,
// SpaceId, Timestamp) are NOT encoded — they're carried by any-sync's
// outer Change struct.
func (c *Codec) Encode(ch *crdt.Change) ([]byte, error) {
	c.arena.Reset()
	a := &c.arena

	root := a.NewObject()
	if ch.Dataset != "" {
		root.Set(keyDataset, a.NewString(ch.Dataset))
	}
	if ch.DataVersion != "" {
		root.Set(keyDataVersion, a.NewString(ch.DataVersion))
	}
	if len(ch.TraceIds) > 0 {
		traces := a.NewArray()
		for i, t := range ch.TraceIds {
			traces.SetArrayItem(i, a.NewString(t))
		}
		root.Set(keyTraceIds, traces)
	}
	recs := a.NewArray()
	for i := range ch.Records {
		recs.SetArrayItem(i, encodeRecord(a, &ch.Records[i]))
	}
	root.Set(keyRecords, recs)

	c.scratch = root.MarshalTo(c.scratch[:0])
	out := make([]byte, len(c.scratch))
	copy(out, c.scratch)
	return out, nil
}

func encodeRecord(a *anyenc.Arena, rec *crdt.RecordChange) *anyenc.Value {
	obj := a.NewObject()
	if rec.Id != "" {
		obj.Set(keyRecordId, a.NewString(rec.Id))
	}
	if rec.Upsert {
		obj.Set(keyRecordUpsert, a.NewTrue())
	}
	if rec.Variant != "" {
		obj.Set(keyRecordVariant, a.NewString(rec.Variant))
	}
	ops := a.NewArray()
	for i := range rec.Ops {
		ops.SetArrayItem(i, encodeOp(a, &rec.Ops[i]))
	}
	obj.Set(keyRecordOps, ops)
	return obj
}

func encodeOp(a *anyenc.Arena, op *crdt.Op) *anyenc.Value {
	obj := a.NewObject()
	obj.Set(keyOpType, a.NewString(string(op.Type)))
	if len(op.Path) > 0 {
		path := a.NewArray()
		for i, seg := range op.Path {
			path.SetArrayItem(i, a.NewString(seg))
		}
		obj.Set(keyOpPath, path)
	}
	if op.Payload != nil {
		// Cross-arena Set is fine here — Op.Payload may live on the
		// caller's arena, but we only need it valid during MarshalTo
		// (Encode marshals before returning, so the caller's arena
		// won't have been reset yet).
		obj.Set(keyOpPayload, op.Payload)
	}
	return obj
}

// Decode parses wire bytes into the caller-portion of a crdt.Change.
// The returned Change has Dataset, DataVersion, TraceIds, and Records
// populated; the CRDT envelope fields stay zero — the caller fills
// them in from the any-sync Change envelope before passing to
// Controller.ApplyChange.
//
// Op.Payload values point into the codec's parser arena. They remain
// valid until the next Decode call. Callers that retain Records past
// the next Decode must clone the payload bytes.
func (c *Codec) Decode(raw []byte) (crdt.Change, error) {
	if len(raw) == 0 {
		return crdt.Change{}, ErrEmptyPayload
	}
	root, err := c.parser.Parse(raw)
	if err != nil {
		return crdt.Change{}, fmt.Errorf("object: parse change payload: %w", err)
	}
	if root == nil || root.Type() != anyenc.TypeObject {
		return crdt.Change{}, ErrEmptyPayload
	}
	out := crdt.Change{
		Dataset:     root.GetString(keyDataset),
		DataVersion: root.GetString(keyDataVersion),
	}
	if traces := root.GetArray(keyTraceIds); len(traces) > 0 {
		out.TraceIds = make([]string, len(traces))
		for i, t := range traces {
			out.TraceIds[i] = string(t.GetStringBytes())
		}
	}
	if recs := root.GetArray(keyRecords); len(recs) > 0 {
		out.Records = make([]crdt.RecordChange, len(recs))
		for i, rec := range recs {
			out.Records[i] = decodeRecord(rec)
		}
	}
	return out, nil
}

func decodeRecord(v *anyenc.Value) crdt.RecordChange {
	rec := crdt.RecordChange{
		Id:      v.GetString(keyRecordId),
		Upsert:  v.GetBool(keyRecordUpsert),
		Variant: v.GetString(keyRecordVariant),
	}
	if ops := v.GetArray(keyRecordOps); len(ops) > 0 {
		rec.Ops = make([]crdt.Op, len(ops))
		for i, op := range ops {
			rec.Ops[i] = decodeOp(op)
		}
	}
	return rec
}

func decodeOp(v *anyenc.Value) crdt.Op {
	op := crdt.Op{Type: crdt.OpType(v.GetString(keyOpType))}
	if path := v.GetArray(keyOpPath); len(path) > 0 {
		op.Path = make([]string, len(path))
		for i, seg := range path {
			op.Path[i] = string(seg.GetStringBytes())
		}
	}
	if p := v.Get(keyOpPayload); p != nil {
		op.Payload = p
	}
	return op
}
