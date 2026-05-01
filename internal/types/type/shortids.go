// The known-shortIds collection — sibling dataset on every type
// object. PropertyHandler appends one row per "important" change so
// the per-space `properties` DataVersion gate (see ApplyChange in
// crdt/) can answer "do I have the schema state this change was
// written against?".

package typetype

import (
	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// ShortIdsDataset is the name of the sibling collection registered on
// every type object's Controller alongside DatasetPropertyDefs. One row
// per important change to the type's properties dataset.
//
// docs/types-properties-proposal.md § "Known-shortIds set":
//
//	{ shortId, versionId, changeId }
//
// Append-only in v1; no GC. The shortId record id is
// base58(xxh3-64(changeId)) — the same derivation as
// crdt.DeriveRecordId, since a content-addressable changeId is the
// only thing we hash.
const ShortIdsDataset = "shortIds"

// ShortId row field names.
const (
	ShortIdFieldChangeId   = "changeId"   // the changeId the shortId was derived from
	ShortIdFieldPropId     = "propId"     // the property record this shortId stamps (added or removed)
	ShortIdFieldKind       = "kind"       // on-wire kind label, present on add rows
	ShortIdFieldRemoved    = "removed"    // bool, true on removal rows
)

// shortIdRow returns the RecordChange that records an "added" shortId
// (a property creation). Caller hands it to sink.Project against
// ShortIdsDataset; the apply loop stamps `_ver` with the triggering
// change's VersionId.
//
// We don't stamp `versionId` ourselves — the apply loop's existing
// `_ver` machinery is the source of truth. Storing it as a regular
// field would duplicate (and risk diverging from) `_ver.id`.
func shortIdRow(changeId, propId, kindLabel string) crdt.RecordChange {
	a := &anyenc.Arena{}
	payload := a.NewObject()
	payload.Set(ShortIdFieldChangeId, a.NewString(changeId))
	payload.Set(ShortIdFieldPropId, a.NewString(propId))
	payload.Set(ShortIdFieldKind, a.NewString(kindLabel))
	return crdt.RecordChange{
		Id:     crdt.DeriveRecordId(changeId),
		Upsert: true,
		Ops: []crdt.Op{{
			Type:    crdt.OpSet,
			Payload: payload,
		}},
	}
}

// removalShortIdRow returns the RecordChange that records a "removed"
// shortId (a property deletion). Same shape as shortIdRow plus
// `removed: true` and no `kind`.
func removalShortIdRow(changeId, propId string) crdt.RecordChange {
	a := &anyenc.Arena{}
	payload := a.NewObject()
	payload.Set(ShortIdFieldChangeId, a.NewString(changeId))
	payload.Set(ShortIdFieldPropId, a.NewString(propId))
	payload.Set(ShortIdFieldRemoved, a.NewTrue())
	return crdt.RecordChange{
		Id:     crdt.DeriveRecordId(changeId),
		Upsert: true,
		Ops: []crdt.Op{{
			Type:    crdt.OpSet,
			Payload: payload,
		}},
	}
}
