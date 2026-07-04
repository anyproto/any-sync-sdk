package payloads

import (
	"context"
	"fmt"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// Handler is the payloads dataset behavior.
//
// Two validation layers, split along the local/inbound line like the
// other built-ins:
//
//   - PreValidate (local writes, before the DAG): STRICT by shape.
//     The dataset is SDK-written only (the public Modify API fences
//     it off), so exactly three change shapes exist — register a
//     file, record a networkSign, delete a row — and anything else
//     rejects the whole write.
//   - BeforeCreate / BeforeModify (inbound + replay): tolerant per-op
//     drop. BeforeCreate stamps the derived `author`; BeforeModify
//     enforces immutability (rootCid / size / enc are create-time
//     facts) and the networkSign-requires-rootCid rule, dropping only
//     the offending op.
type Handler struct {
	crdt.DefaultHandler
}

func (Handler) Init(_ context.Context) error { return nil }

// BeforeCreate stamps the derived `author` from the creating change's
// signer (the per-change Creator, not the tree-root author — for
// collective durability another member may register rows in the same
// payloads object). Tolerates an empty Creator (hand-built changes in
// tests, pre-bind drains) by simply not stamping.
func (Handler) BeforeCreate(ctx *crdt.ChangeCtx, _ *crdt.RecordChange, sink *crdt.Sink) error {
	if ctx == nil || ctx.Change == nil || sink == nil {
		return nil
	}
	if creator := ctx.Change.Creator; creator != "" {
		a := &anyenc.Arena{}
		sink.Derive(crdt.Op{
			Type:    crdt.OpSet,
			Path:    []string{FieldAuthor},
			Payload: a.NewString(creator),
		})
	}
	return nil
}

// BeforeModify is the inbound-tolerant guard on existing rows: a
// returned error drops just this op, the rest of the record still
// applies.
func (Handler) BeforeModify(ctx *crdt.ChangeCtx, _ *crdt.RecordChange, op *crdt.Op, _ *crdt.Sink) error {
	if op.Type == crdt.OpUnset || op.Type == crdt.OpDelete {
		return nil
	}
	if op.Type != crdt.OpSet {
		return fmt.Errorf("payloads: op %s not supported", op.Type)
	}
	// Multi-field $set is the create shape; on an existing row it
	// could only be a re-create or an immutability bypass.
	if len(op.Path) == 0 {
		return fmt.Errorf("payloads: multi-field $set on an existing row")
	}
	switch op.Path[0] {
	case FieldRootCid, FieldSize, FieldEnc, FieldObjectId:
		// Create-time facts. Immutable once present; a late fill on a
		// row that somehow lacks one (cross-version tolerance) is
		// allowed.
		if ctx != nil && ctx.Before != nil && ctx.Before.Get(op.Path[0]) != nil {
			return fmt.Errorf("payloads: %s is immutable", op.Path[0])
		}
		return nil
	case FieldNetworkSign:
		if op.Payload == nil || op.Payload.Type() != anyenc.TypeString {
			return fmt.Errorf("payloads: networkSign must be a string")
		}
		if ctx == nil || ctx.Before == nil || ctx.Before.Get(FieldRootCid) == nil {
			return fmt.Errorf("payloads: networkSign requires rootCid (inline rows are never signed)")
		}
		return nil
	default:
		// Undeclared fields are already rejected by the non-Dynamic
		// schema; this is unreachable defense.
		return fmt.Errorf("payloads: unknown field %q", op.Path[0])
	}
}

// PreValidateMulti implements crdt.LocalPreValidatorMulti — the strict
// local shape gate, batch-first: bulk flows (a pasted doc with hundreds
// of images) register and sign files in as few changes as possible.
//
// Batches are homogeneous: a change is either N register-file creates
// (empty-id records; their fileIds resolve to the derived seed with a
// digit suffix per extra record — seed, seed:1, …), N networkSign
// writes (per-record pre-state via the getter), or N row deletes.
func (Handler) PreValidateMulti(ch *crdt.Change, get crdt.RecordGetter) error {
	if ch == nil || len(ch.Records) == 0 {
		return nil
	}
	shape, err := recordShape(&ch.Records[0])
	if err != nil {
		return err
	}
	for i := 1; i < len(ch.Records); i++ {
		s, err := recordShape(&ch.Records[i])
		if err != nil {
			return err
		}
		if s != shape {
			return fmt.Errorf("payloads: mixed write shapes in one change (%s and %s)", shape, s)
		}
	}
	switch shape {
	case shapeCreate:
		for i := range ch.Records {
			rec := &ch.Records[i]
			if rec.Id != "" || !rec.Upsert {
				return fmt.Errorf("payloads: create must use an empty record id with upsert")
			}
			if err := validateCreatePayload(rec.Ops[0].Payload); err != nil {
				return err
			}
		}
		return nil
	case shapeDelete:
		for i := range ch.Records {
			if ch.Records[i].Id == "" {
				return fmt.Errorf("payloads: delete requires an explicit fileId")
			}
		}
		return nil
	default: // shapeSign
		for i := range ch.Records {
			rec := &ch.Records[i]
			op := &rec.Ops[0]
			if rec.Id == "" {
				return fmt.Errorf("payloads: networkSign write requires an explicit fileId")
			}
			if rec.Upsert {
				return fmt.Errorf("payloads: networkSign write must not upsert")
			}
			if op.Payload == nil || op.Payload.Type() != anyenc.TypeString {
				return fmt.Errorf("payloads: networkSign must be a string (fileId %s)", rec.Id)
			}
			if l := len(op.Payload.GetStringBytes()); l == 0 || l > MaxNetworkSignLen {
				return fmt.Errorf("payloads: networkSign length %d out of range (1..%d) (fileId %s)", l, MaxNetworkSignLen, rec.Id)
			}
			var before *anyenc.Value
			if get != nil {
				before = get(rec.Id)
			}
			if before == nil {
				return fmt.Errorf("payloads: networkSign write targets a missing row (fileId %s)", rec.Id)
			}
			if before.Get(FieldRootCid) == nil {
				return fmt.Errorf("payloads: networkSign requires rootCid — inline rows are never signed (fileId %s)", rec.Id)
			}
		}
		return nil
	}
}

type writeShape string

const (
	shapeCreate writeShape = "create"
	shapeDelete writeShape = "delete"
	shapeSign   writeShape = "networkSign"
)

// recordShape classifies one record into the three permitted write
// shapes, requiring exactly one op per record.
func recordShape(rec *crdt.RecordChange) (writeShape, error) {
	if len(rec.Ops) != 1 {
		return "", fmt.Errorf("payloads: exactly one op per record, got %d", len(rec.Ops))
	}
	op := &rec.Ops[0]
	switch {
	case op.Type == crdt.OpDelete:
		return shapeDelete, nil
	case op.Type == crdt.OpSet && len(op.Path) == 0:
		return shapeCreate, nil
	case op.Type == crdt.OpSet && len(op.Path) == 1 && op.Path[0] == FieldNetworkSign:
		return shapeSign, nil
	default:
		return "", fmt.Errorf("payloads: unsupported write shape (op %s, path %v)", op.Type, op.Path)
	}
}

// validateCreatePayload checks the multi-field $set object of a
// register-file create:
//
//   - size: required number >= 0;
//   - enc:  required object {kid: non-empty string, ct: non-empty binary};
//   - rootCid: optional non-empty string; ABSENT means the inline
//     tier, which requires size < InlineMaxSize and no networkSign;
//   - networkSign: optional (a BIND reuses an existing sign at
//     create), requires rootCid;
//   - objectId: optional non-empty string (the parent object; optional
//     for cross-version tolerance — rows written before the field
//     existed sync in without it).
func validateCreatePayload(v *anyenc.Value) error {
	if v == nil || v.Type() != anyenc.TypeObject {
		return fmt.Errorf("payloads: create payload must be an object")
	}
	obj, _ := v.Object()
	var badKey error
	obj.Visit(func(k []byte, _ *anyenc.Value) {
		switch string(k) {
		case FieldRootCid, FieldSize, FieldNetworkSign, FieldEnc, FieldObjectId:
		default:
			if badKey == nil {
				badKey = fmt.Errorf("payloads: create payload has unknown field %q", string(k))
			}
		}
	})
	if badKey != nil {
		return badKey
	}

	size := v.Get(FieldSize)
	if size == nil || size.Type() != anyenc.TypeNumber || size.GetFloat64() < 0 {
		return fmt.Errorf("payloads: create requires a non-negative numeric size")
	}
	enc := v.Get(FieldEnc)
	if enc == nil || enc.Type() != anyenc.TypeObject {
		return fmt.Errorf("payloads: create requires the sealed enc object")
	}
	if kid := enc.Get(EncKeyId); kid == nil || kid.Type() != anyenc.TypeString || len(kid.GetStringBytes()) == 0 {
		return fmt.Errorf("payloads: enc.kid must be a non-empty string")
	}
	if ct := enc.Get(EncKeyCiphertext); ct == nil || ct.Type() != anyenc.TypeBinary || len(ct.GetBytes()) == 0 {
		return fmt.Errorf("payloads: enc.ct must be non-empty binary")
	}

	if oid := v.Get(FieldObjectId); oid != nil {
		if oid.Type() != anyenc.TypeString || len(oid.GetStringBytes()) == 0 {
			return fmt.Errorf("payloads: objectId must be a non-empty string")
		}
	}

	rootCid := v.Get(FieldRootCid)
	sign := v.Get(FieldNetworkSign)
	if rootCid == nil {
		// Inline tier: bytes ride inside enc, no S3, no sign.
		if v.GetFloat64(FieldSize) >= InlineMaxSize {
			return fmt.Errorf("payloads: a row without rootCid is inline and must have size < %d", InlineMaxSize)
		}
		if sign != nil {
			return fmt.Errorf("payloads: networkSign requires rootCid (inline rows are never signed)")
		}
		return nil
	}
	if rootCid.Type() != anyenc.TypeString || len(rootCid.GetStringBytes()) == 0 {
		return fmt.Errorf("payloads: rootCid must be a non-empty string")
	}
	if sign != nil {
		if sign.Type() != anyenc.TypeString {
			return fmt.Errorf("payloads: networkSign must be a string")
		}
		if l := len(sign.GetStringBytes()); l == 0 || l > MaxNetworkSignLen {
			return fmt.Errorf("payloads: networkSign length %d out of range (1..%d)", l, MaxNetworkSignLen)
		}
	}
	return nil
}
