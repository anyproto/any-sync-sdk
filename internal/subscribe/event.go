package subscribe

import (
	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/anyencx"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/space"
)

// ObjectsDataset is the CRDT dataset whose changes feed the
// shared-objects scope — the per-space `objects` collection, where
// every object's property values land regardless of object kind.
//
// User-facing terminology calls these "property values"; the dataset
// is named "objects" because each row is one object's values record.
// Unrelated to typetype.DatasetPropertyDefs (type-definition metadata
// on type objects).
const ObjectsDataset = properties.Dataset

// Event is a single CRDT apply event handed to the engine.
//
// Pipeline contract: receive change → apply to CRDT → commit the
// any-store tx → THEN build this event. By the time a consumer sees
// an Event, every projected $set/$unset is already durable in the
// controller's any-store; a Query against the same dataset run from
// the same process reflects the same state.
//
// Carries the routing tuple plus enough about the change for the
// engine to classify each record (filter + sort + window membership)
// without re-querying:
//
//   - VersionId — per-change DAG order. Forwarded on the wire as
//     SubscriptionEvent.VersionId so fence-and-replay consumers can
//     dedupe across snapshots.
//
//   - Records — the post-apply effect of the change, projected to a
//     flat list of $set / $unset ops per record. Ops payloads are
//     deep-cloned off the controller's storage onto event-owned
//     arenas, so an Event is safe to retain past the build call.
type Event struct {
	SpaceId   string
	ObjectId  string
	Dataset   string
	VersionId crdt.VersionId
	Records   []EventRecord
}

// EventRecord is one record's worth of projected change inside an
// Event. Id is the record id within Dataset (for shared per-space
// datasets like "objects" this equals ObjectId).
type EventRecord struct {
	Id string
	// Created is true when this change first materialised the record.
	// The engine uses it (combined with sentinel/visibility state) to
	// decide Added vs Updated emit semantics.
	Created bool
	// Deleted is true when the change tombstoned this record. Ops is
	// empty in that case; the engine drops the entry from its held
	// set.
	Deleted bool
	// Ops is the projected $set / $unset operations on the post-apply
	// record. Empty when Deleted is true. Surfaced verbatim to
	// SubscriptionEvent SubRecord.Ops for atomic-update consumers.
	Ops []space.EventOp
}

// PostValueFn returns the post-apply value for one record in the
// change being built. Provided by the per-space layer (the
// spaceobjects Store holds the controller). Index is into ch.Records.
//
// May return nil for tombstoned / dropped records (e.g. a gated op
// that didn't land); callers should treat nil as "the record no
// longer exists at this point in the timeline".
type PostValueFn func(recordIndex int) *anyenc.Value

// BuildEvent projects (ch, recordIds, derivedOps, postValue) into the
// wire Event shape — runs the same per-record projection an engine
// caller would need, with deep-cloned op payloads. afterApplyFor
// builds this once per change before handing it to Engine.OnApply.
//
// Safe to call with a nil change — returns a zero Event in that case;
// callers should check ch != nil themselves if they want to skip work.
func BuildEvent(ch *crdt.Change, recordIds []string, derivedOps [][]crdt.Op, postValue PostValueFn) Event {
	if ch == nil {
		return Event{}
	}
	return Event{
		SpaceId:   ch.SpaceId,
		ObjectId:  ch.ObjectId,
		Dataset:   ch.Dataset,
		VersionId: ch.VersionId,
		Records:   projectRecords(ch, recordIds, derivedOps, postValue),
	}
}

// projectRecords walks ch.Records and produces the EventRecord slice
// for the wire — every op is mapped to a $set or $unset against the
// post-apply state, and a record-level "delete" op is collapsed into
// EventRecord.Deleted.
//
// derivedOps[i] carries the auto-stamped extras the apply path added
// to record i beyond rc.Ops (author / createdAt / spaceId / _ver.id).
// They project through the same projectOp path as input ops — the
// EventRecord's consumer can't tell them apart, which is the point:
// a viewer reconstructs a fresh record with all its fields in one
// go, no second-class wire form for auto fields.
//
// Op.Payload values are deep-cloned via anyencutil.Value.FillCopy so
// the resulting Event is safe to outlive the build call.
func projectRecords(ch *crdt.Change, recordIds []string, derivedOps [][]crdt.Op, postValue PostValueFn) []EventRecord {
	if len(ch.Records) == 0 {
		return nil
	}
	out := make([]EventRecord, 0, len(ch.Records))
	for i, rc := range ch.Records {
		rid := rc.Id
		if i < len(recordIds) && recordIds[i] != "" {
			rid = recordIds[i]
		}
		er := EventRecord{Id: rid}
		if hasRecordDelete(rc.Ops) {
			er.Deleted = true
			out = append(out, er)
			continue
		}
		var post *anyenc.Value
		if postValue != nil {
			post = postValue(i)
		}
		for _, op := range rc.Ops {
			projected, ok := projectOp(op, post)
			if !ok {
				continue
			}
			er.Ops = append(er.Ops, projected)
		}
		// Derived stamps are record-level (author / createdAt / _ver.id
		// land alongside the input ops on the same record).
		if i < len(derivedOps) {
			for _, op := range derivedOps[i] {
				if isCreationMarker(op) {
					// The marker's value is always ch.VersionId
					// (newRecord stamps _ver.id = ch.VersionId), which
					// the Event already carries. Surface it as the
					// Created flag, not a redundant $set op.
					er.Created = true
					continue
				}
				projected, ok := projectOp(op, post)
				if !ok {
					continue
				}
				er.Ops = append(er.Ops, projected)
			}
		}
		out = append(out, er)
	}
	return out
}

// hasRecordDelete reports whether ops contains a record-level delete.
// Mirrors the apply-side helper of the same name.
func hasRecordDelete(ops []crdt.Op) bool {
	for _, op := range ops {
		if op.Type == crdt.OpDelete {
			return true
		}
	}
	return false
}

// isCreationMarker reports whether op is the synthetic _ver.id stamp
// the apply path emits exactly once — on the change that first
// materialises a record (recordModifier.Modify, the `creating`
// branch). The min-rule re-stamp on later upserts does NOT emit a
// derived op, so the marker's presence in a record's derivedOps is
// an unambiguous "this change created the record" signal.
func isCreationMarker(op crdt.Op) bool {
	return op.Type == crdt.OpSet &&
		len(op.Path) == 2 &&
		op.Path[0] == crdt.VersionsKey &&
		op.Path[1] == crdt.IdField
}

// projectOp converts a single input op against a post-apply record
// snapshot into a $set / $unset op the wire ships. ok=false means
// the op should be dropped from the projection (currently only used
// for crdt.OpDelete, which is signalled by EventRecord.Deleted at
// the record level).
func projectOp(op crdt.Op, post *anyenc.Value) (space.EventOp, bool) {
	switch op.Type {
	case crdt.OpDelete:
		// Record-level — handled out-of-band via EventRecord.Deleted.
		return space.EventOp{}, false
	case crdt.OpSet, crdt.OpUnset:
		// Already in the wire-friendly form. Clone the payload off
		// any caller-owned arena so the event can outlive the build
		// call.
		return space.EventOp{
			Type:    op.Type,
			Path:    clonePath(op.Path),
			Payload: anyencx.Clone(op.Payload),
		}, true
	default:
		// $inc / $addToSet / $pull / $incGated — derive the post-apply
		// value at the op's path and emit a $set (or $unset when the
		// op didn't actually land or the path went away).
		path := clonePath(op.Path)
		val := lookupPath(post, path)
		if val == nil {
			return space.EventOp{
				Type: crdt.OpUnset,
				Path: path,
			}, true
		}
		return space.EventOp{
			Type:    crdt.OpSet,
			Path:    path,
			Payload: anyencx.Clone(val),
		}, true
	}
}

// clonePath copies an op path so callers can't mutate the input op's
// slice through the projected event.
func clonePath(path []string) []string {
	if len(path) == 0 {
		return nil
	}
	out := make([]string, len(path))
	copy(out, path)
	return out
}

// lookupPath walks v one segment at a time and returns the value at
// the path tail. Empty path returns v itself. Returns nil for any
// missing intermediate.
func lookupPath(v *anyenc.Value, path []string) *anyenc.Value {
	if v == nil {
		return nil
	}
	if len(path) == 0 {
		return v
	}
	return v.Get(path...)
}
