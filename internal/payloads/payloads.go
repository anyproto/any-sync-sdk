// Package payloads is the built-in file `payloads` dataset — the
// node-readable per-file index of the files subsystem (docs/07b, 07c
// on the files-design branch; SYN-21).
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
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

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

// WellKnownDeriveSeed is the ChangePayload seed of every payloads
// object. Combined with ParentId (the owner object) it derives the
// same deterministic child id on every peer.
const WellKnownDeriveSeed = "builtin:payloads"

// InlineMaxSize is the inline-tier cutoff: a file strictly smaller
// than this rides inside `enc` (Inline bytes) with no rootCid / S3 /
// networkSign — the filenode never sees it.
const InlineMaxSize = 4096

// MaxNetworkSignLen bounds the recorded networkSign — it is an opaque
// receipt string ("{fileNetworkId}/{sign(rootCid)}"), not a blob.
const MaxNetworkSignLen = 2048

// Row field names. Full names, matching the row-field convention of
// the objects dataset (author/createdAt/spaceId); short keys are a
// change-envelope concern, not a row concern.
const (
	FieldRootCid     = "rootCid"     // string; ABSENT for inline rows
	FieldSize        = "size"        // number; plaintext byte size
	FieldNetworkSign = "networkSign" // string; absent until durable; requires rootCid
	FieldAuthor      = "author"      // string; derived from the change signer
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
		{Id: FieldRootCid, Schema: schema.Leaf(schema.KindString), Scope: schema.ScopeSynced},
		{Id: FieldSize, Schema: schema.Leaf(schema.KindNumber), Scope: schema.ScopeSynced},
		{Id: FieldNetworkSign, Schema: schema.Leaf(schema.KindString), Scope: schema.ScopeSynced},
		{Id: FieldAuthor, Schema: schema.Leaf(schema.KindString), Scope: schema.ScopeDerived},
		{Id: FieldEnc, Schema: &schema.Schema{Kind: schema.KindObject}, Scope: schema.ScopeSynced},
	}}
}
