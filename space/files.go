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
// invisible here: Attach picks the tier from the content.
//
// SYN-27 ships Attach; Open / Get / Pin / Offload / Status follow in
// SYN-28/29/26/30.
type Files interface {
	// Attach ingests r as a file bound to objectId. The whole reader is
	// consumed. Registration is durable in the CRDT immediately; for
	// node-backed files the backup ("durable") phase runs best-effort
	// within ctx — a false FileInfo.Durable means the file is registered
	// and locally available but not yet backed up (retried in the
	// background; observable via file status once SYN-29 lands).
	Attach(ctx context.Context, objectId string, r io.Reader, opts AttachOpts) (FileInfo, error)
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
}
