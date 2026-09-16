package crdt

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// SchemaHandlerVersion is the generic schema handler's LOCAL logic
// version (HandlerReg.Version). Every dataset the SDK applies through
// this handler carries it, so a change here rebuilds their rows from the
// DAG (docs/versioning.md). v2: the createTime / modifyTime stamps
// are TypeDateTime instants, not epoch numbers.
const SchemaHandlerVersion = 2

// SchemaHandler is the generic dataset handler: it enforces a
// schema.Dataset's behavioral declaration (required fields, write-once /
// author-gated mutability, apply-time stamps, id rules, delete gates,
// declared value shapes) with no bespoke code. A dataset registration
// with a declared Schema and no Handler gets one automatically.
//
// Every verdict is arrival-order-independent, per the replica-determinism
// contract (see ChangeCtx): id and required checks are intrinsic to the
// change; write-once fields are writable ONLY in the record's creating
// change (never a presence probe — concurrent late fills would diverge);
// author gates compare the per-change Creator against the record's
// creator stamp, a derived-at-create immutable fact; author-gated
// deletes of never-created records are rejected, which converges because
// an author's own create→delete is self-causally ordered, so a delete
// arriving before its record's create can only be a non-author's.
//
// The declaration is compiled once at construction into a flat rule
// table; the accept path of BeforeModify is one map lookup with zero
// allocations. BeforeModify writes to the sink only on acceptance (the
// modifyTime bump via DeriveOnce), so the controller's multi-field
// per-key salvage may safely re-probe it.
type SchemaHandler struct {
	rules       map[string]*fieldRule
	requiredIds []string

	creatorField    string
	createTimeField string
	modifyTimeField string

	idRule   schema.IdRule
	idRe     *regexp.Regexp
	idMaxLen int
	deleteBy schema.DeletePolicy
	dynamic  bool
}

type fieldRule struct {
	scope     schema.Scope
	required  bool
	mutableBy schema.Mutability
	stamp     schema.Stamp
	shape     *schema.Schema // nil = unconstrained
}

// NewSchemaHandler compiles a dataset declaration into a generic
// handler. The declaration is validated first; the returned handler is
// read-only after construction.
func NewSchemaHandler(ds schema.Dataset) (*SchemaHandler, error) {
	if err := schema.ValidateDatasetDecl(ds); err != nil {
		return nil, err
	}
	ds = ds.Normalized()
	h := &SchemaHandler{
		rules:    make(map[string]*fieldRule, len(ds.Fields)),
		idRule:   ds.IdRule,
		deleteBy: ds.DeleteBy,
		dynamic:  ds.Dynamic,
	}
	if ds.IdRule == schema.IdUser {
		re, maxLen, err := schema.CompileIdPattern(ds)
		if err != nil {
			return nil, err
		}
		h.idRe, h.idMaxLen = re, maxLen
	}
	for i := range ds.Fields {
		f := &ds.Fields[i]
		h.rules[f.Id] = &fieldRule{
			scope:     f.Scope,
			required:  f.Required,
			mutableBy: f.MutableBy,
			stamp:     f.Stamp,
			shape:     f.Schema,
		}
		if f.Required {
			h.requiredIds = append(h.requiredIds, f.Id)
		}
		switch f.Stamp {
		case schema.StampCreator:
			h.creatorField = f.Id
		case schema.StampCreateTime:
			h.createTimeField = f.Id
		case schema.StampModifyTime:
			h.modifyTimeField = f.Id
		}
	}
	return h, nil
}

func (h *SchemaHandler) Init(_ context.Context) error { return nil }

// BeforeCreate validates the creating change (id rule, required fields,
// declared value shapes) and derives the declared stamps from the
// change's metadata — no DB reads. An error drops the whole RecordChange.
func (h *SchemaHandler) BeforeCreate(ctx *ChangeCtx, rec *RecordChange, sink *Sink) error {
	if err := h.checkRecordId(rec); err != nil {
		return err
	}
	if err := h.checkCreatePayload(rec); err != nil {
		return err
	}
	if ctx == nil || ctx.Change == nil || sink == nil {
		return nil
	}
	h.deriveCreateStamps(ctx, sink)
	return nil
}

func (h *SchemaHandler) checkRecordId(rec *RecordChange) error {
	if h.idRule == schema.IdAuto {
		if rec.Id != "" {
			return fmt.Errorf("%w: dataset derives record ids, explicit id %q rejected", ErrValidation, rec.Id)
		}
		return nil
	}
	if rec.Id == "" {
		return fmt.Errorf("%w: dataset requires caller-supplied record ids", ErrValidation)
	}
	if len(rec.Id) > h.idMaxLen {
		return fmt.Errorf("%w: record id longer than %d", ErrValidation, h.idMaxLen)
	}
	if !h.idRe.MatchString(rec.Id) {
		return fmt.Errorf("%w: record id %q does not match the dataset id pattern", ErrValidation, rec.Id)
	}
	return nil
}

// checkCreatePayload enforces required-field presence and declared value
// shapes over the creating ops. Presence is established by content-adding
// ops ($set / $addToSet / $inc / $incGated) on the field's head.
func (h *SchemaHandler) checkCreatePayload(rec *RecordChange) error {
	var present map[string]struct{}
	if len(h.requiredIds) > 0 {
		present = make(map[string]struct{}, len(h.requiredIds))
	}
	for i := range rec.Ops {
		op := &rec.Ops[i]
		switch op.Type {
		case OpSet:
			if len(op.Path) == 0 {
				if err := h.visitMultiField(op, present); err != nil {
					return err
				}
				continue
			}
			// Only a WHOLE-field write satisfies required — a deep
			// path (title.x) doesn't put a valid value at the field.
			if len(op.Path) == 1 && present != nil {
				present[op.Path[0]] = struct{}{}
			}
			if err := h.checkOpShape(op); err != nil {
				return err
			}
		case OpAddToSet, OpInc, OpIncGated:
			if len(op.Path) == 1 && present != nil {
				present[op.Path[0]] = struct{}{}
			}
			if err := h.checkOpShape(op); err != nil {
				return err
			}
		}
	}
	for _, id := range h.requiredIds {
		if present == nil {
			return fmt.Errorf("%w: required field %q missing from create", ErrValidation, id)
		}
		if _, ok := present[id]; !ok {
			return fmt.Errorf("%w: required field %q missing from create", ErrValidation, id)
		}
	}
	return nil
}

// visitMultiField validates a multi-field $set payload's keys and marks
// their heads present.
func (h *SchemaHandler) visitMultiField(op *Op, present map[string]struct{}) error {
	if op.Payload == nil || op.Payload.Type() != anyenc.TypeObject {
		return fmt.Errorf("%w: multi-field $set payload must be an object", ErrValidation)
	}
	obj, _ := op.Payload.Object()
	var firstErr error
	obj.Visit(func(k []byte, v *anyenc.Value) {
		if firstErr != nil {
			return
		}
		key := string(k)
		head, rest, dotted := strings.Cut(key, ".")
		if !dotted && present != nil {
			// Dotted keys never satisfy required: {title.x: 1} doesn't
			// put a valid value at `title`.
			present[head] = struct{}{}
		}
		rule, ok := h.rules[head]
		if !ok || rule.shape == nil {
			return
		}
		shape := rule.shape
		if dotted {
			sub, valid := subShape(shape, rest)
			if !valid {
				firstErr = fmt.Errorf("%w: field %q: path descends below the declared shape", ErrValidation, key)
				return
			}
			if sub == nil {
				return
			}
			shape = sub
		}
		if err := schema.ValidateValue(shape, v); err != nil {
			firstErr = fmt.Errorf("%w: field %q: %v", ErrValidation, key, err)
		}
	})
	return firstErr
}

func (h *SchemaHandler) deriveCreateStamps(ctx *ChangeCtx, sink *Sink) {
	if h.creatorField == "" && h.createTimeField == "" && h.modifyTimeField == "" {
		return
	}
	a := &anyenc.Arena{}
	if h.creatorField != "" && ctx.Change.Creator != "" {
		sink.Derive(Op{Type: OpSet, Path: []string{h.creatorField}, Payload: a.NewString(ctx.Change.Creator)})
	}
	if ctx.Change.Timestamp <= 0 {
		// No envelope clock, no time stamps: an instant at the epoch would
		// be indistinguishable from a real one, where an absent field
		// still reads as "unknown".
		return
	}
	if h.createTimeField != "" {
		sink.Derive(Op{Type: OpSet, Path: []string{h.createTimeField}, Payload: a.NewDateTimeMillis(ctx.Change.Timestamp * 1000)})
	}
	if h.modifyTimeField != "" {
		sink.Derive(Op{Type: OpSet, Path: []string{h.modifyTimeField}, Payload: a.NewDateTimeMillis(ctx.Change.Timestamp * 1000)})
	}
}

// BeforeModify is the per-op gate on existing records. An error drops
// just this op (the controller salvages multi-field ops per key by
// re-probing, so combined rejections keep their innocent keys).
func (h *SchemaHandler) BeforeModify(ctx *ChangeCtx, _ *RecordChange, op *Op, sink *Sink) error {
	if op.Type == OpDelete {
		return nil
	}
	if len(op.Path) == 0 {
		return h.beforeModifyMulti(ctx, op, sink)
	}
	rule, ok := h.rules[op.Path[0]]
	if !ok {
		// Undeclared head: non-Dynamic datasets never get here (the
		// controller's field-class check rejects first); Dynamic free
		// keyspace carries no behavioral semantics.
		return nil
	}
	if err := h.checkMutable(ctx, op.Path[0], rule); err != nil {
		return err
	}
	if op.Type == OpUnset {
		h.bumpModifyTime(ctx, sink)
		return nil
	}
	if err := h.checkOpShape(op); err != nil {
		return err
	}
	h.bumpModifyTime(ctx, sink)
	return nil
}

// beforeModifyMulti validates every key of a combined $set/$unset; the
// first violation rejects the combined op and the controller re-probes
// per key. On full acceptance the modifyTime bump is derived once.
func (h *SchemaHandler) beforeModifyMulti(ctx *ChangeCtx, op *Op, sink *Sink) error {
	if op.Payload == nil || op.Payload.Type() != anyenc.TypeObject {
		return fmt.Errorf("%w: multi-field %s payload must be an object", ErrValidation, op.Type)
	}
	obj, _ := op.Payload.Object()
	var firstErr error
	touched := false
	obj.Visit(func(k []byte, v *anyenc.Value) {
		if firstErr != nil {
			return
		}
		head, rest, dotted := strings.Cut(string(k), ".")
		rule, ok := h.rules[head]
		if !ok {
			return
		}
		touched = true
		if err := h.checkMutable(ctx, head, rule); err != nil {
			firstErr = err
			return
		}
		if op.Type == OpUnset || rule.shape == nil {
			return
		}
		shape := rule.shape
		if dotted {
			sub, valid := subShape(shape, rest)
			if !valid {
				firstErr = fmt.Errorf("%w: field %q: path descends below the declared shape", ErrValidation, string(k))
				return
			}
			if sub == nil {
				return
			}
			shape = sub
		}
		if err := schema.ValidateValue(shape, v); err != nil {
			firstErr = fmt.Errorf("%w: field %q: %v", ErrValidation, string(k), err)
		}
	})
	if firstErr != nil {
		return firstErr
	}
	if touched {
		h.bumpModifyTime(ctx, sink)
	}
	return nil
}

// checkMutable enforces the field's post-create write rule. Write-once
// fields are writable only in the creating change, so any modify on an
// existing record is rejected — never a presence probe (see the type
// comment on convergence).
func (h *SchemaHandler) checkMutable(ctx *ChangeCtx, field string, rule *fieldRule) error {
	switch rule.mutableBy {
	case schema.MutableByAnyone:
		return nil
	case schema.MutableByAuthor:
		if ctx == nil || ctx.Change == nil || ctx.Change.Creator == "" {
			return fmt.Errorf("%w: field %q is author-mutable and the change has no creator", ErrValidation, field)
		}
		if ctx.Before == nil {
			return fmt.Errorf("%w: field %q is author-mutable and the record has no creator stamp", ErrValidation, field)
		}
		creator := ctx.Before.Get(h.creatorField)
		if creator == nil || creator.Type() != anyenc.TypeString ||
			string(creator.GetStringBytes()) != ctx.Change.Creator {
			return fmt.Errorf("%w: field %q is mutable by its author only", ErrValidation, field)
		}
		return nil
	default:
		return fmt.Errorf("%w: field %q is write-once (set at create only)", ErrValidation, field)
	}
}

// checkOpShape validates a single-path op payload against the declared
// value shape. Deep paths validate against the resolvable sub-shape and
// pass when the shape doesn't constrain that depth.
func (h *SchemaHandler) checkOpShape(op *Op) error {
	if len(op.Path) == 0 {
		return nil
	}
	rule, ok := h.rules[op.Path[0]]
	if !ok || rule.shape == nil {
		return nil
	}
	shape := rule.shape
	if len(op.Path) > 1 {
		sub, valid := subShapeSegs(shape, op.Path[1:])
		if !valid {
			return fmt.Errorf("%w: field %q: path descends below the declared shape", ErrValidation, strings.Join(op.Path, "."))
		}
		if sub == nil {
			return nil
		}
		shape = sub
	}
	switch op.Type {
	case OpSet:
		if err := schema.ValidateValue(shape, op.Payload); err != nil {
			return fmt.Errorf("%w: field %q: %v", ErrValidation, op.Path[0], err)
		}
	case OpInc, OpIncGated:
		if shape.Kind != schema.KindNumber {
			return fmt.Errorf("%w: field %q: %s on a non-number field", ErrValidation, op.Path[0], op.Type)
		}
	case OpAddToSet, OpPull:
		if shape.Kind != schema.KindArray {
			return fmt.Errorf("%w: field %q: %s on a non-array field", ErrValidation, op.Path[0], op.Type)
		}
		if op.Type == OpAddToSet && shape.Items != nil {
			if err := schema.ValidateValue(shape.Items, op.Payload); err != nil {
				return fmt.Errorf("%w: field %q element: %v", ErrValidation, op.Path[0], err)
			}
		}
	}
	return nil
}

func (h *SchemaHandler) bumpModifyTime(ctx *ChangeCtx, sink *Sink) {
	if h.modifyTimeField == "" || sink == nil || ctx == nil || ctx.Change == nil {
		return
	}
	// Pre-check the queue so repeated accepted ops in one RecordChange
	// (and salvage re-probes) don't each allocate an arena just for
	// DeriveOnce to drop the duplicate.
	for i := range sink.derived {
		if len(sink.derived[i].Path) == 1 && sink.derived[i].Path[0] == h.modifyTimeField {
			return
		}
	}
	a := &anyenc.Arena{}
	sink.Derive(Op{Type: OpSet, Path: []string{h.modifyTimeField}, Payload: a.NewDateTimeMillis(ctx.Change.Timestamp * 1000)})
}

// BeforeDelete enforces the dataset's delete gate. Author-gated deletes
// require the record's creator stamp to match the deleting change's
// Creator; a never-created record (no creation marker) is rejected —
// convergent because an author's own create→delete is causally ordered,
// so a delete racing ahead of its create is never the author's.
func (h *SchemaHandler) BeforeDelete(ctx *ChangeCtx, _ *RecordChange, _ *Sink) error {
	if h.deleteBy != schema.DeleteByAuthor {
		return nil
	}
	if ctx == nil || ctx.Change == nil || ctx.Change.Creator == "" {
		return fmt.Errorf("%w: dataset deletes are author-only and the change has no creator", ErrValidation)
	}
	if ctx.Before == nil || ctx.Before.Get(VersionsKey) == nil {
		return fmt.Errorf("%w: dataset deletes are author-only; record does not exist here yet", ErrValidation)
	}
	creator := ctx.Before.Get(h.creatorField)
	if creator == nil || creator.Type() != anyenc.TypeString ||
		string(creator.GetStringBytes()) != ctx.Change.Creator {
		return fmt.Errorf("%w: only the record's author may delete it", ErrValidation)
	}
	return nil
}

// PreValidateMulti is the strict local gate: it re-runs the structural
// checks (id rules, required fields, value shapes, write-once /
// unknown-field rules) with agent-readable errors before the change
// enters the DAG. Author-identity gates are left to apply time — the
// local change's Creator isn't stamped yet at pre-validation, and local
// apply is synchronous, surfacing those rejections in the ModifyResult.
func (h *SchemaHandler) PreValidateMulti(ch *Change, get RecordGetter) error {
	if ch == nil {
		return nil
	}
	for i := range ch.Records {
		rec := &ch.Records[i]
		if hasDelete(rec.Ops) {
			continue
		}
		var before *anyenc.Value
		if rec.Id != "" && get != nil {
			before = get(rec.Id)
		}
		if before == nil {
			if !rec.Upsert {
				continue // strict update of an absent record: no-op
			}
			if err := h.checkRecordId(rec); err != nil {
				return fmt.Errorf("record %q: %w", rec.Id, err)
			}
			if err := h.checkCreatePayload(rec); err != nil {
				return fmt.Errorf("record %q: %w", rec.Id, err)
			}
			continue
		}
		for j := range rec.Ops {
			op := &rec.Ops[j]
			if err := h.preValidateExistingOp(op); err != nil {
				return fmt.Errorf("record %q: %w", rec.Id, err)
			}
		}
	}
	return nil
}

// preValidateExistingOp mirrors BeforeModify's structural half (no
// author checks, no sink).
func (h *SchemaHandler) preValidateExistingOp(op *Op) error {
	if op.Type == OpDelete {
		return nil
	}
	if len(op.Path) == 0 {
		if op.Payload == nil || op.Payload.Type() != anyenc.TypeObject {
			return fmt.Errorf("%w: multi-field %s payload must be an object", ErrValidation, op.Type)
		}
		obj, _ := op.Payload.Object()
		var firstErr error
		obj.Visit(func(k []byte, v *anyenc.Value) {
			if firstErr != nil {
				return
			}
			head, rest, dotted := strings.Cut(string(k), ".")
			rule, ok := h.rules[head]
			if !ok {
				return
			}
			if rule.mutableBy == schema.MutableNever {
				firstErr = fmt.Errorf("%w: field %q is write-once (set at create only)", ErrValidation, head)
				return
			}
			if op.Type == OpUnset || rule.shape == nil {
				return
			}
			shape := rule.shape
			if dotted {
				sub, valid := subShape(shape, rest)
				if !valid {
					firstErr = fmt.Errorf("%w: field %q: path descends below the declared shape", ErrValidation, string(k))
					return
				}
				if sub == nil {
					return
				}
				shape = sub
			}
			if err := schema.ValidateValue(shape, v); err != nil {
				firstErr = fmt.Errorf("%w: field %q: %v", ErrValidation, string(k), err)
			}
		})
		return firstErr
	}
	rule, ok := h.rules[op.Path[0]]
	if !ok {
		return nil
	}
	if rule.mutableBy == schema.MutableNever {
		return fmt.Errorf("%w: field %q is write-once (set at create only)", ErrValidation, op.Path[0])
	}
	return h.checkOpShape(op)
}

// subShape descends a value shape along a dotted path remainder.
// Returns (shape, true) when the path resolves ((nil, true) =
// unconstrained: a free-shape object or an array interior), and
// (nil, false) when the path is INVALID for the declared shape —
// descending under a scalar, or into a property the shape's closed
// object doesn't declare. Callers reject the false case; treating it
// as unconstrained would let a dotted write smuggle arbitrary
// structure into a declared scalar field.
func subShape(s *schema.Schema, rest string) (*schema.Schema, bool) {
	for rest != "" {
		var seg string
		seg, rest, _ = strings.Cut(rest, ".")
		switch s.Kind {
		case schema.KindObject:
			if s.Properties == nil {
				return nil, true // free shape: any layout below
			}
			next, ok := s.Properties[seg]
			if !ok {
				return nil, false // closed object: undeclared sub-field
			}
			s = next
		case schema.KindArray:
			return nil, true // element paths unconstrained in v1
		default:
			return nil, false // scalar: nothing underneath
		}
	}
	return s, true
}

// subShapeSegs is subShape over pre-split path segments.
func subShapeSegs(s *schema.Schema, segs []string) (*schema.Schema, bool) {
	for _, seg := range segs {
		switch s.Kind {
		case schema.KindObject:
			if s.Properties == nil {
				return nil, true
			}
			next, ok := s.Properties[seg]
			if !ok {
				return nil, false
			}
			s = next
		case schema.KindArray:
			return nil, true
		default:
			return nil, false
		}
	}
	return s, true
}
