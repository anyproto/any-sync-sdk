package space

import (
	"context"
	"io"
)

// Files is the per-space file surface (files v2, docs/07c). Files
// always bind to an existing object — there are no standalone file
// objects; the first Attach lazily creates the object's derived
// payloads child, and the space-wide files listing is the payloads
// dataset itself (see PayloadsView; the queryable files view lands
// with SYN-30).
//
// The storage tiers (inline vs content-addressed + node backup) are
// invisible here: Attach picks the tier from the content, and Open
// resolves it through the availability ladder (local cache → network).
//
// SYN-27 shipped Attach, SYN-28 Open/Get; Pin / Offload / Status
// follow in SYN-29/26/30.
type Files interface {
	// Attach ingests r as a file bound to objectId. The whole reader is
	// consumed. Registration is durable in the CRDT immediately; for
	// node-backed files the backup ("durable") phase runs best-effort
	// within ctx — a false FileInfo.Durable means the file is registered
	// and locally available but not yet backed up (retried in the
	// background; observable via file status once SYN-29 lands).
	Attach(ctx context.Context, objectId string, r io.Reader, opts AttachOpts) (FileInfo, error)

	// Open returns a random-access reader over the file's verified
	// plaintext. Content not yet local streams in on demand (every
	// fetched block persists, so reads accrete toward a complete local
	// copy); a file that is neither local nor durable is not openable
	// until the P2P block layer lands. The reader is bound to ctx.
	Open(ctx context.Context, fileId string, variant Variant) (FileReader, error)

	// Get returns the file's current info (member-only fields like
	// Name/Mime are empty for a keyless reader).
	Get(ctx context.Context, fileId string) (FileInfo, error)
}

// Variant selects which representation of a file to open. Variants
// (thumbnails etc.) are sibling payload rows tagged in their sealed
// meta; resolution lands with SYN-30 — only VariantOriginal is
// accepted until then.
type Variant string

// VariantOriginal is the file's primary content.
const VariantOriginal Variant = ""

// FileReader is a seekable, sized view of a file's plaintext.
type FileReader interface {
	io.Reader
	io.Seeker
	io.Closer
	// Size is the plaintext byte length.
	Size() int64
}

// AttachOpts is caller metadata for one attached file. All fields ride
// inside the sealed (member-only) part of the payloads row.
type AttachOpts struct {
	// Name is the user-facing file name.
	Name string
	// Mime is the content-type hint.
	Mime string
}

// FileInfo describes one attached file.
type FileInfo struct {
	// FileId identifies the file within its space (the payloads row id).
	FileId string
	// ObjectId is the object the file is bound to.
	ObjectId string
	// RootCid is the content address of the encrypted file. Empty for
	// inline-tier files (which live inside the row itself).
	RootCid string
	// Size is the plaintext byte size.
	Size int64
	// Inline reports the inline tier (no RootCid, no backup needed).
	Inline bool
	// Durable reports whether the file is backed up on the network (a
	// verified custody receipt is recorded on the row). Inline files
	// are durable by construction.
	Durable bool
	// Name is the user-facing file name (member-only; empty without
	// the space key).
	Name string
	// Mime is the content-type hint (member-only).
	Mime string
	// Cached reports a complete local copy (always true for inline).
	Cached bool
}
