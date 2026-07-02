package space

import "context"

// PayloadsView is the read-only surface over the space's file
// `payloads` objects — the node-readable per-file index of the files
// subsystem. It exposes exactly the cleartext row fields, so a keyless
// embedder (the filenode-v2 broker: headless + selective sync) can
// enumerate and index every registered file without holding any space
// key; the member-only secrets stay sealed and are never surfaced here.
//
// Obtained from Space.Payloads.
type PayloadsView interface {
	// ListObjects returns the objectIds of the materialized payloads
	// objects in this space (one per file owner, derived lazily on the
	// owner's first registration). Classified by the tree root's
	// changeType — signed and content-addressed, so the set can't be
	// spoofed. Trees known only as heads-only stubs (selective sync) or
	// deleted trees are excluded.
	ListObjects(ctx context.Context) ([]string, error)

	// ListRows returns every live row of one payloads object. The
	// object must be materialized locally (an id from ListObjects);
	// rows come back in collection order.
	ListRows(ctx context.Context, payloadsObjectId string) ([]PayloadRow, error)
}

// PayloadRow is the cleartext (broker) view of one payloads record —
// everything a node needs for refcount / GC / quota / durability. The
// sealed member-only secrets (wrapped file key, name, sha256, mime,
// inline bytes) are not exposed on this surface.
type PayloadRow struct {
	// FileId identifies the row — derived from the creating change,
	// deterministic and never reused.
	FileId string

	// RootCid is the UnixFS root of the encrypted file. Empty for
	// inline-tier rows (bytes ride inside the sealed blob).
	RootCid string

	// Size is the plaintext byte size (cleartext hint; quota uses the
	// node's own measurement).
	Size int64

	// NetworkSign is the node's durable-custody receipt. Empty until
	// the file is durable; always empty for inline rows.
	NetworkSign string

	// Author is the account that registered the file (derived from the
	// creating change's signer).
	Author string

	// Sealed reports whether the row's member-only secrets remain
	// closed to this identity. True for a keyless reader (the broker
	// view — not an error); false when the local account holds the
	// space key, even though the secrets are still not exposed here.
	Sealed bool
}
