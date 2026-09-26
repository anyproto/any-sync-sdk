// Package payloads is the built-in file `payloads` dataset — the
// node-readable per-file index of the files subsystem (docs/files.md).
//
// One row per file, living in a plaintext (node-readable) derived
// child object under the file's owner:
//
//   - cleartext, node-readable: id (the fileId), rootCid, size,
//     networkSign, author — everything a filenode-v2 broker needs for
//     refcount / GC / quota / durability without any space key;
//   - one sealed field `enc` — AES-GCM under a key derived from the
//     space read key — grouping the member-only secrets
//     {key, name, sha256, mime, inline}.
//
// The object's tree changes ship UNencrypted at the any-sync level
// (object.PlaintextSpec); the privacy boundary is the `enc` field,
// sealed and unsealed by this package. `enc` stays sealed at rest in
// the materialized row; typed readers unseal at read time, so a
// keyless reader materializes the row like everyone else and simply
// can't open `enc`.
package payloads

import (
	"errors"

	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// ErrOwnerUnknown: the payloads id for this owner is not locally
// resolvable yet. The id depends on the owner's class (signed vs
// derived — see DerivedOwnerSeed), and nothing local discloses it:
// no head entry for the owner, and no payloads tree of either shape.
// A signed owner that hasn't synced and a derived owner that never
// grew a payloads tree are locally indistinguishable, so resolving
// here would be a guess whose answer flips once the owner arrives.
// Resolvable after sync delivers the owner (or its payloads tree).
var ErrOwnerUnknown = errors.New("payloads: owner class unknown; payloads id not resolvable yet")

// NotWrittenError wraps a registration failure from before the row
// write: no row exists, so nothing references what the caller staged
// for it.
type NotWrittenError struct{ Err error }

func (e *NotWrittenError) Error() string {
	if e.Err == nil {
		return "payloads: row not written"
	}
	return e.Err.Error()
}
func (e *NotWrittenError) Unwrap() error { return e.Err }

// NotWritten reports whether err says registration wrote no row.
func NotWritten(err error) bool {
	var nw *NotWrittenError
	return errors.As(err, &nw)
}

// Dataset is the CRDT dataset name; on disk the collection is
// `<payloadsObjectId>_payloads`.
const Dataset = "payloads"

// HandlerVersion is the DataVersion stamped on every payloads change.
// Hardcoded handler identifier (same convention as the other built-in
// datasets); bump the suffix when validation rules must reject stale
// writers.
const HandlerVersion = "payloads-v1"

// ChangeType is the tree-root change type of a payloads object. It is
// the plaintext-class anchor: the root is immutable, signed, and
// cleartext, so keyed and keyless readers agree the object is
// node-readable and restricted to this dataset.
const ChangeType = "payloads"

// WellKnownDeriveSeed is the ChangePayload seed of a payloads object
// whose owner is a SIGNED object. Combined with ParentId (the owner
// object) it derives the same deterministic child id on every peer, and
// binds the payloads object to the owner so any-sync cascade-deletes it
// with the owner.
const WellKnownDeriveSeed = "builtin:payloads"

// DerivedOwnerSeed is the ChangePayload seed of a payloads object whose
// owner is a DERIVED object. A derived object cannot be a tree parent
// (objecttree.ErrDerivedParent), so the payloads object can't take
// the parented WellKnownDeriveSeed shape signed owners get. It is derived
// UNPARENTED instead, with the ownerId folded into the seed so each
// derived owner still resolves to its own deterministic, per-owner
// payloads id (an unparented tree has no ParentId to carry that
// uniqueness). The two are coupled: unparented + owner-in-seed must move
// together, or every derived owner would collide on a single id.
func DerivedOwnerSeed(ownerId string) string {
	return WellKnownDeriveSeed + "/" + ownerId
}

// InlineMaxSize is the inline-tier cutoff: a file strictly smaller
// than this rides inside `enc` (Inline bytes) with no rootCid / S3 /
// networkSign — the filenode never sees it.
const InlineMaxSize = 4096

// MaxNetworkSignLen bounds the recorded networkSign — it is an opaque
// receipt string ("{fileNetworkId}/{sign(rootCid)}"), not a blob.
const MaxNetworkSignLen = 2048

// Row field names. Full names, matching the row-field convention of
// the objects dataset (author/createdAt/spaceId/modifiedAt/modifiedBy);
// short keys are a
// change-envelope concern, not a row concern.
const (
	FieldRootCid     = "rootCid"     // string; ABSENT for inline rows
	FieldSize        = "size"        // number; plaintext byte size
	FieldNetworkSign = "networkSign" // string; absent until durable; requires rootCid
	FieldAuthor      = "author"      // string; derived from the change signer
	FieldObjectId    = "objectId"    // string; the object this file is bound to (cleartext so the files view can filter by parent)
	FieldEnc         = "enc"         // object {kid, ct}; sealed member-only secrets
)

// Keys inside the cleartext `enc` wrapper object.
const (
	EncKeyId         = "kid" // ACL key-record id the blob was sealed under
	EncKeyCiphertext = "ct"  // AES-GCM sealed EncPayload (nonce prepended)
)

// Schema declares the payloads dataset. Non-Dynamic: undeclared
// fields are rejected by the controller on both the local and the
// inbound route, so the cleartext surface can never silently grow.
func Schema() schema.Dataset {
	return schema.Dataset{Fields: []schema.Field{
		{Id: FieldRootCid, Name: "Root CID", Schema: schema.Leaf(schema.KindString), Scope: schema.ScopeSynced,
			Description: "Root CID of the encrypted DAG; absent for an inline file."},
		{Id: FieldSize, Name: "Size", Schema: schema.Leaf(schema.KindNumber), Scope: schema.ScopeSynced,
			Description: "Plaintext size in bytes."},
		{Id: FieldNetworkSign, Name: "Network sign", Schema: schema.Leaf(schema.KindString), Scope: schema.ScopeSynced,
			Description: "Broker signature; present once the file is durable on the network."},
		{Id: FieldAuthor, Name: "Author", Schema: schema.Leaf(schema.KindString), Scope: schema.ScopeDerived,
			Description: "Account identity that attached the file; derived."},
		{Id: FieldObjectId, Name: "Object", Schema: schema.Leaf(schema.KindString), Scope: schema.ScopeSynced,
			Description: "Id of the object the file is bound to."},
		{Id: FieldEnc, Name: "Sealed", Schema: &schema.Schema{Kind: schema.KindObject}, Scope: schema.ScopeSynced,
			Description: "Member-only sealed secrets: {kid, ct}."},
	}}
}
