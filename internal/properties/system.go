// The system properties handler — the single crdt.Handler every user
// object registers. See doc.go for the package role.

package properties

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
)

// Dataset is the name every regular object uses for its base-scope
// property writes on its own CRDT. The handler is registered against
// a SHARED per-space collection (also called "objects") via the
// Controller's shared-collection override — every regular object's
// values land in one row in that collection, keyed by the change's
// ObjectId. Type objects don't register this handler; their own
// metadata (any.name etc.) lives in their per-type-object storage
// behind a different dataset.
//
// See docs/06-data-structure.md § "Storage" — the per-space
// `objects` collection model.
const Dataset = "objects"

// HandlerVersion is the DataVersion string stamped on every change
// this handler emits against an object's `properties` dataset. Bump
// the suffix when validation rules change in a way that must reject
// stale writers (see docs/types-properties-proposal.md § "Change-
// level DataVersion" — data-dataset version is a hardcoded handler
// identifier in Phase 1).
const HandlerVersion = "systemPropertyHandler-v1"

// Sentinels — wrap crdt.ErrValidation in handler returns.
var (
	ErrInvalidPath        = errors.New("properties: path must be {typeId}.{propId}")
	ErrUnknownProperty    = errors.New("properties: unknown (typeId, propId)")
	ErrKindMismatch       = errors.New("properties: payload kind does not match property kind")
)

// SystemPropertiesHandler validates property writes on every user
// object. Corresponds to the `baseProperty` handler named in
// docs/06-data-structure.md § "Handlers" — renamed to emphasize its
// scope (the per-space `properties` system dataset) and to
// distinguish it from typetype.PropertyHandler (which governs
// property definitions on type objects).
//
// Variant routing (`_base` / `_account` / `_device`) is handled by
// the apply loop via RecordChange.Variant — the handler is variant-
// agnostic and only validates kinds against the type Registry.
//
// Path shape for a single-path op: `[typeId, propId]`. The handler
// looks the kind up via Registry.LookupKind and silently drops the
// op if (a) the path doesn't fit the shape, (b) the property is
// unknown, or (c) the payload's anyenc kind doesn't match the
// declared kind. "Per-op atomicity" — other ops in the same
// RecordChange still apply.
//
// Multi-field $set with empty Path expects the payload to be an
// object whose keys are dotted "{typeId}.{propId}" paths. Same kind
// rules apply per-key; if any key fails, the whole op drops (we
// don't mutate payloads to filter individual keys in v1).
type SystemPropertiesHandler struct {
	// Registry resolves declared kinds. May be nil — when nil the
	// handler skips kind validation entirely (passes everything),
	// which is the bring-up mode before the type system is wired.
	Registry types.Registry
}

// New constructs a SystemPropertiesHandler bound to a Registry. Pass
// nil to disable kind validation (early bring-up).
func New(reg types.Registry) *SystemPropertiesHandler {
	return &SystemPropertiesHandler{Registry: reg}
}

func (*SystemPropertiesHandler) Dataset() string              { return Dataset }
func (*SystemPropertiesHandler) Version() int                 { return 1 }
func (*SystemPropertiesHandler) Init(_ context.Context) error { return nil }

// BeforeCreate validates every op in the creation payload, then
// auto-stamps the `any`-scope auto fields (author, createdAt,
// spaceId) via sink.Derive so every newly minted row in the per-
// space `objects` collection carries them. The stamps are derived
// from the change envelope (Creator from the signing identity,
// Timestamp from the change wire, SpaceId from the apply context),
// not from caller input — these fields are ScopeAuto in the `any`
// type, read-only by convention.
//
// Stricter than BeforeModify on validation: any per-op validation
// failure rejects the whole record, because a creation with a bad
// field implies a buggy or version-mismatched writer (and the
// DataVersion gate on ApplyChange already filters those upstream —
// by the time a create lands here, we should know its schema).
func (h *SystemPropertiesHandler) BeforeCreate(ctx *crdt.ChangeCtx, rec *crdt.RecordChange, sink *crdt.Sink) error {
	if h.Registry != nil {
		for i := range rec.Ops {
			if err := h.validateOp(&rec.Ops[i]); err != nil {
				return err
			}
		}
	}
	stampAutoFields(ctx, sink)
	return nil
}

// stampAutoFields queues derived ops for the row-root auto fields
// — `author`, `createdAt`, `spaceId` — that live alongside `id` at
// the top of the record (NOT under `any.*`). They are derived from
// the change envelope and stamped once at record creation;
// BeforeCreate fires only on first-touch so they aren't re-applied
// to existing rows. The derived ops inherit the change's VersionId,
// so concurrent peer-side BeforeCreate stamps converge under
// standard LWW.
//
// Derived ops route to row root (see Modify in controller.go) so
// these stamps land at `record.author` / `record.createdAt` /
// `record.spaceId`, not under any variant subdoc — consistent with
// `id`, which is also at row root.
func stampAutoFields(ctx *crdt.ChangeCtx, sink *crdt.Sink) {
	if ctx == nil || ctx.Change == nil || sink == nil {
		return
	}
	a := &anyenc.Arena{}
	if creator := ctx.Change.Creator; creator != "" {
		sink.Derive(crdt.Op{
			Type:    crdt.OpSet,
			Path:    []string{"author"},
			Payload: a.NewString(creator),
		})
	}
	if ts := ctx.Change.Timestamp; ts > 0 {
		sink.Derive(crdt.Op{
			Type:    crdt.OpSet,
			Path:    []string{"createdAt"},
			Payload: a.NewNumberInt(int(ts)),
		})
	}
	if spaceId := ctx.Change.SpaceId; spaceId != "" {
		sink.Derive(crdt.Op{
			Type:    crdt.OpSet,
			Path:    []string{"spaceId"},
			Payload: a.NewString(spaceId),
		})
	}
}

// BeforeModify validates one op against the Registry. Drops the op
// silently on any validation failure; other ops in the same
// RecordChange still apply.
func (h *SystemPropertiesHandler) BeforeModify(_ *crdt.ChangeCtx, _ *crdt.RecordChange, op *crdt.Op, _ *crdt.Sink) error {
	if h.Registry == nil {
		return nil
	}
	return h.validateOp(op)
}

// BeforeDelete is a no-op — deleting a property record (the per-
// object property store entry) is allowed; no Registry lookup
// applies to the record-as-a-whole.
func (*SystemPropertiesHandler) BeforeDelete(_ *crdt.ChangeCtx, _ *crdt.RecordChange, _ *crdt.Sink) error {
	return nil
}

// validateOp routes by op shape. Single-path: validate one
// (typeId, propId, payload-kind). Multi-field: validate every key.
// Other op kinds (delete, addToSet, pull, inc, incGated) follow the
// same single-path rule using op.Payload's kind.
func (h *SystemPropertiesHandler) validateOp(op *crdt.Op) error {
	if op.Type == crdt.OpDelete {
		return nil // record-level delete; no per-property validation
	}
	if len(op.Path) == 0 {
		// Multi-field $set/$unset.
		return h.validateMultiField(op)
	}
	return h.validateSinglePath(op.Path, op.Payload, op.Type)
}

func (h *SystemPropertiesHandler) validateSinglePath(path []string, payload *anyenc.Value, opType crdt.OpType) error {
	if len(path) < 2 {
		return fmt.Errorf("%w: %w", crdt.ErrValidation, ErrInvalidPath)
	}
	typeId, propId := path[0], path[1]
	declared, ok := h.Registry.LookupKind(typeId, propId)
	if !ok {
		return fmt.Errorf("%w: %w (%s, %s)", crdt.ErrValidation, ErrUnknownProperty, typeId, propId)
	}
	// $unset payloads are absent / ignored — no kind to match.
	// $set / $addToSet / $pull / $inc / $incGated all carry the
	// candidate value as Payload, against which we kind-match.
	if opType == crdt.OpUnset {
		return nil
	}
	got := schema.KindOf(payload)
	if got != declared {
		return fmt.Errorf("%w: %w (declared=%s got=%s, %s.%s)",
			crdt.ErrValidation, ErrKindMismatch, declared, got, typeId, propId)
	}
	return nil
}

// validateMultiField walks the payload of a multi-field $set/$unset.
// Every top-level key must be a dotted "typeId.propId" pair; any
// failure rejects the whole op (see SystemPropertiesHandler doc on
// the "no payload mutation" v1 limitation).
func (h *SystemPropertiesHandler) validateMultiField(op *crdt.Op) error {
	if op.Payload == nil || op.Payload.Type() != anyenc.TypeObject {
		return nil
	}
	obj, _ := op.Payload.Object()
	var firstErr error
	obj.Visit(func(k []byte, v *anyenc.Value) {
		if firstErr != nil {
			return
		}
		key := string(k)
		dot := strings.IndexByte(key, '.')
		if dot <= 0 || dot == len(key)-1 {
			firstErr = fmt.Errorf("%w: %w (key=%q)", crdt.ErrValidation, ErrInvalidPath, key)
			return
		}
		typeId, propId := key[:dot], key[dot+1:]
		// Reject if any further dots (nested paths not supported in v1
		// validation; treat as invalid to keep semantics simple).
		if strings.IndexByte(propId, '.') >= 0 {
			firstErr = fmt.Errorf("%w: %w (key=%q)", crdt.ErrValidation, ErrInvalidPath, key)
			return
		}
		declared, ok := h.Registry.LookupKind(typeId, propId)
		if !ok {
			firstErr = fmt.Errorf("%w: %w (%s, %s)", crdt.ErrValidation, ErrUnknownProperty, typeId, propId)
			return
		}
		if op.Type == crdt.OpUnset {
			return
		}
		got := schema.KindOf(v)
		if got != declared {
			firstErr = fmt.Errorf("%w: %w (declared=%s got=%s, %s.%s)",
				crdt.ErrValidation, ErrKindMismatch, declared, got, typeId, propId)
		}
	})
	return firstErr
}
