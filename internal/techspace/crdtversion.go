// The CRDT version mark — one record on the space-index object that
// names the newest CRDT data-model version an SDK has written this
// account with. It is what lets an older SDK refuse an account a
// newer one has already touched, instead of misreading or overwriting
// data shaped by rules it does not know.
//
// Rule: the version only ever goes up. The handler enforces it on
// every replica at apply time, so two devices racing to stamp the
// record converge on the maximum — an older device's lower stamp is
// dropped everywhere, including on the older device once the higher
// one arrives. At Open, a stored version above space.CRDTVersion
// refuses the boot; below it, the SDK raises the mark. At runtime, a
// higher version arriving through sync flips the SDK read-only: every
// user-authored synced write fails with space.ErrCRDTVersionNewer
// while reads keep serving, so a client can show "upgrade required"
// without losing the data on screen.

package techspace

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/space"
)

const (
	// CRDTVersionDataset is the mark's dataset on the space-index object.
	CRDTVersionDataset = "crdtVersion"
	// CRDTVersionHandlerVersion is stamped on every change to it.
	CRDTVersionHandlerVersion = "crdtVersionHandler-v1"
	// CRDTVersionRecordId is the single record's id.
	CRDTVersionRecordId = "crdtVersion"
	// FieldCRDTVersion holds the version number.
	FieldCRDTVersion = "version"
)

// CRDTVersionSchema declares the mark: one synced integer.
func CRDTVersionSchema() schema.Dataset {
	return schema.Dataset{Fields: []schema.Field{
		{Id: FieldCRDTVersion, Name: "CRDT version", Schema: schema.Leaf(schema.KindNumber), Scope: schema.ScopeSynced,
			Description: "Highest CRDT version any device of the account has written; never decreases."},
	}}
}

// CRDTVersionHandler validates ops on the crdtVersion dataset: the one
// record, a positive integer version that never decreases, no deletes.
// OnVersion, when set, is told every version the handler admits — the
// service's hook for noticing a newer mark arriving through sync. It
// never influences the verdict, so replicas stay deterministic.
type CRDTVersionHandler struct {
	OnVersion func(version int)
}

func (CRDTVersionHandler) Init(_ context.Context) error { return nil }

func (h CRDTVersionHandler) BeforeCreate(_ *crdt.ChangeCtx, rec *crdt.RecordChange, _ *crdt.Sink) error {
	if rec.Id != CRDTVersionRecordId {
		return fmt.Errorf("%w: crdtVersion: the record id is %q", crdt.ErrValidation, CRDTVersionRecordId)
	}
	version, found := 0, false
	for i := range rec.Ops {
		v, ok, err := crdtVersionFromOp(&rec.Ops[i])
		if err != nil {
			return err
		}
		if ok {
			version, found = v, true
		}
	}
	if !found {
		return fmt.Errorf("%w: crdtVersion: %s required", crdt.ErrValidation, FieldCRDTVersion)
	}
	h.notify(version)
	return nil
}

func (h CRDTVersionHandler) BeforeModify(ctx *crdt.ChangeCtx, rec *crdt.RecordChange, op *crdt.Op, _ *crdt.Sink) error {
	if rec.Id != CRDTVersionRecordId {
		return fmt.Errorf("%w: crdtVersion: the record id is %q", crdt.ErrValidation, CRDTVersionRecordId)
	}
	v, ok, err := crdtVersionFromOp(op)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if ctx != nil && ctx.Before != nil {
		if cur := int(ctx.Before.GetFloat64(FieldCRDTVersion)); v < cur {
			return fmt.Errorf("%w: crdtVersion: version %d is below the stored %d — the mark never decreases", crdt.ErrValidation, v, cur)
		}
	}
	h.notify(v)
	return nil
}

// BeforeDelete refuses: the mark is permanent.
func (CRDTVersionHandler) BeforeDelete(_ *crdt.ChangeCtx, _ *crdt.RecordChange, _ *crdt.Sink) error {
	return fmt.Errorf("%w: crdtVersion: the mark cannot be deleted", crdt.ErrValidation)
}

func (h CRDTVersionHandler) notify(version int) {
	if h.OnVersion != nil {
		h.OnVersion(version)
	}
}

// crdtVersionFromOp reads the version an op carries: a $set at
// ["version"] with a number payload, or the multi-field form (empty
// path, object payload) holding only `version`. Any other op, path or
// value is a validation error; (0, false, nil) means the op does not
// touch the version.
func crdtVersionFromOp(op *crdt.Op) (int, bool, error) {
	if op == nil {
		return 0, false, nil
	}
	if op.Type != crdt.OpSet {
		return 0, false, fmt.Errorf("%w: crdtVersion: only $set is allowed", crdt.ErrValidation)
	}
	only := fmt.Errorf("%w: crdtVersion: the record holds only %s", crdt.ErrValidation, FieldCRDTVersion)
	var v *anyenc.Value
	switch {
	case len(op.Path) == 1 && op.Path[0] == FieldCRDTVersion:
		v = op.Payload
	case len(op.Path) == 0 && op.Payload != nil && op.Payload.Type() == anyenc.TypeObject:
		obj := op.Payload.GetObject()
		if obj == nil || obj.Len() != 1 {
			return 0, false, only
		}
		v = op.Payload.Get(FieldCRDTVersion)
		if v == nil {
			return 0, false, only
		}
	default:
		return 0, false, only
	}
	if v == nil || v.Type() != anyenc.TypeNumber {
		return 0, false, fmt.Errorf("%w: crdtVersion: %s must be a number", crdt.ErrValidation, FieldCRDTVersion)
	}
	f := v.GetFloat64()
	if f < 1 || f != math.Trunc(f) || f > math.MaxInt32 {
		return 0, false, fmt.Errorf("%w: crdtVersion: %s must be a positive integer", crdt.ErrValidation, FieldCRDTVersion)
	}
	return int(f), true, nil
}

// crdtVersionVerdict decides what an SDK supporting `supported` does
// with a stored mark: raise it (write), refuse (error), or nothing.
func crdtVersionVerdict(stored, supported int) (write bool, err error) {
	switch {
	case stored > supported:
		return false, &space.CRDTVersionNewerError{Stored: stored, Supported: supported}
	case stored < supported:
		return true, nil
	}
	return false, nil
}

// crdtVersionHandler is the handler instance wired into this service's
// store: admitted versions feed onCRDTVersion.
func (s *Service) crdtVersionHandler() CRDTVersionHandler {
	return CRDTVersionHandler{OnVersion: s.onCRDTVersion}
}

// onCRDTVersion records the highest version any applied change
// carried. A version above what this SDK supports flips the write
// gate — a local raise never trips it, since the service only ever
// writes its own supported version.
func (s *Service) onCRDTVersion(version int) {
	for {
		seen := s.crdtVersionSeen.Load()
		if int64(version) <= seen {
			return
		}
		if s.crdtVersionSeen.CompareAndSwap(seen, int64(version)) {
			return
		}
	}
}

// storedCRDTVersion reads the mark off the index object; 0 when absent.
func (s *Service) storedCRDTVersion(ctx context.Context) (int, error) {
	obj, err := s.indexObj(ctx)
	if err != nil {
		return 0, err
	}
	v := obj.Controller().Get(ctx, CRDTVersionDataset, CRDTVersionRecordId)
	if v == nil || v.Get("_deletedAt") != nil {
		return 0, nil
	}
	return int(v.GetFloat64(FieldCRDTVersion)), nil
}

// EnsureCRDTVersion is the boot check: a stored version above
// space.CRDTVersion refuses with CRDTVersionNewerError; a lower or
// absent one is raised to the supported version in one synced write.
// Boot-serial (sdk.Open, after Open of this service).
func (s *Service) EnsureCRDTVersion(ctx context.Context) error {
	if !s.open.Load() {
		return errors.New("techspace: service not open")
	}
	stored, err := s.storedCRDTVersion(ctx)
	if err != nil {
		return fmt.Errorf("techspace: read crdt version: %w", err)
	}
	s.onCRDTVersion(stored)
	write, err := crdtVersionVerdict(stored, space.CRDTVersion)
	if err != nil {
		return err
	}
	if !write {
		return nil
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return err
	}
	arena := &anyenc.Arena{}
	change := crdt.Change{
		Dataset:     CRDTVersionDataset,
		DataVersion: CRDTVersionHandlerVersion,
		Records: []crdt.RecordChange{{
			Id:     CRDTVersionRecordId,
			Upsert: true,
			Ops:    []crdt.Op{{Type: crdt.OpSet, Path: []string{FieldCRDTVersion}, Payload: arena.NewNumberInt(space.CRDTVersion)}},
		}},
	}
	res, err := obj.LocalWrite(ctx, change)
	if err != nil {
		return fmt.Errorf("techspace: write crdt version: %w", err)
	}
	if len(res.Rejections) > 0 {
		return fmt.Errorf("techspace: write crdt version: rejected: %v", res.Rejections[0])
	}
	return nil
}

// CRDTVersion reports the mark: the supported version, the stored one
// (the highest this replica has seen applied — the record's value, or
// the peak an inbound change carried) and whether the stored one is
// newer, which is the read-only state.
func (s *Service) CRDTVersion() space.CRDTVersionState {
	stored := int(s.crdtVersionSeen.Load())
	return space.CRDTVersionState{
		Supported: space.CRDTVersion,
		Stored:    stored,
		Newer:     stored > space.CRDTVersion,
	}
}

// WriteGate is the account-wide write gate every store consults: nil
// while the stored CRDT version is at or below the supported one, the
// CRDTVersionNewerError once a higher mark has been seen.
func (s *Service) WriteGate() error {
	if st := s.CRDTVersion(); st.Newer {
		return &space.CRDTVersionNewerError{Stored: st.Stored, Supported: st.Supported}
	}
	return nil
}
