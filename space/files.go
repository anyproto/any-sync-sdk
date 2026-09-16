package space

import (
	"context"
	"errors"
	"io"
)

// Typed file errors — match with errors.Is, never by message.
var (
	// ErrFileNotAvailable — the file's content is neither local nor
	// fetchable right now: the file is not backed up yet (the P2P rung
	// lands later), the network advertises no public read base, or a
	// read hit a not-yet-fetched range while offline.
	ErrFileNotAvailable = errors.New("space: file content not available")

	// ErrFileNotBackedUp — Offload refused: the local bytes are the
	// only copy of a file whose backup hasn't completed, and the SDK
	// never drops the only copy.
	ErrFileNotBackedUp = errors.New("space: file not backed up")

	// ErrFileVariantInvalid — Attach variant options are inconsistent:
	// Variant/VariantOf not set together, or the original is bound to
	// a different object.
	ErrFileVariantInvalid = errors.New("space: invalid file variant options")
)

// Files is the per-space file surface (docs/files.md). Files
// always bind to an existing object — there are no standalone file
// objects; the first Attach lazily creates the object's derived
// payloads child, and the space-wide files listing is the payloads
// dataset itself (see PayloadsView).
//
// The storage tiers (inline vs content-addressed + node backup) are
// invisible here: Attach picks the tier from the content, and Open
// resolves it through the availability ladder (local cache → network).
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

	// Status derives the file's durability state on read: durable
	// (verified receipt on the row, inline included), in-flight
	// (registered; backup pending or being driven), or limited (the
	// network refused backup for storage limit; retried on a slow
	// cadence and on Retry).
	Status(ctx context.Context, fileId string) (FileStatus, error)

	// SubscribeStatus delivers FileStatus events for this space's
	// files on LOCAL transitions — attach, backup progress/failure,
	// pin completion, manual retries. (Remote flips — another device
	// finishing a backup — are visible via Status/Get reads; a synced
	// event feed arrives with the SYN-30 files view.) The returned
	// function unsubscribes.
	SubscribeStatus(cb func(FileStatus)) (unsubscribe func())

	// Stats returns this space's aggregate durability counts (UI
	// badges: "n files not backed up").
	Stats(ctx context.Context) (FileStats, error)

	// Pin schedules a full background fetch of the file's content into
	// the local store (survives restarts; on-demand reads stay the
	// default without it).
	Pin(ctx context.Context, fileId string) error

	// Retry makes the file's pending background work due immediately —
	// a limited file after a quota raise, or any stalled backup. No-op
	// with no pending work; re-enqueues the backup when the row is
	// unsigned and the bytes are local.
	Retry(ctx context.Context, fileId string) error

	// Offload drops the file's local bytes, keeping the file itself —
	// a later Open transparently refetches from the network. Refused
	// unless the file is backed up (never drops the only copy); inline
	// files are a no-op. Content shared with other files (dedup) loses
	// its local bytes for all of them — each stays refetchable; Pin a
	// sibling to keep it hot.
	Offload(ctx context.Context, fileId string) error

	// Delete removes the file: its payload row is deleted in one synced
	// CRDT change (every member sees the file disappear), pending
	// background work is cancelled, and the local content ref is
	// released so cache GC reclaims the bytes (unreferenced CARs age
	// out past the safety-sweep grace period — deletion never races a
	// settling sync). Deleting an original also deletes its variant
	// rows — they are unresolvable without it; a keyless reader (who
	// cannot see VariantOf) deletes only the addressed row. Content
	// shared with a surviving row (dedup/BIND) is untouched: each row
	// holds its own ref. The network copy is NOT reclaimed here —
	// fileprotov2 has no delete RPC yet; the broker's row-driven
	// accounting stops counting the rows once the deletion syncs.
	// ErrNotFound when no such file exists (Delete is not idempotent
	// over the wire — a second call fails like any other read).
	Delete(ctx context.Context, fileId string) error

	// List returns the space's files as typed infos. Opts.ObjectId
	// restricts to one object's files (the fast path — one indexed
	// lookup). The unfiltered listing walks every file in the space:
	// fine for human-scale spaces, but a consumer tracking a very
	// large space should page with Opts.Limit or drive Query/Changes
	// instead of re-listing.
	List(ctx context.Context, opts FileListOpts) ([]FileInfo, error)

	// Query returns the generic query surface (filter / sort /
	// subscribe — see space.Query) over the payload rows of ONE
	// object's files. Rows expose the cleartext fields (fileId,
	// rootCid, size, networkSign, objectId); the member-only meta
	// stays sealed — use Get/List for typed access to it.
	//
	// ErrNotFound until the object's first file is attached (the
	// backing dataset materializes with the first Attach) — fall back
	// to List/Changes until then.
	Query(objectId string) (Query, error)
}

// FileListOpts filters List.
type FileListOpts struct {
	// ObjectId restricts the listing to files bound to one object.
	ObjectId string
	// Limit caps the result (0 = unlimited). Applied after ObjectId.
	Limit int
}

// FileSyncState is the durability state of one file.
type FileSyncState string

const (
	// FileStateDurable — a verified network receipt is recorded (or the
	// file is inline and rides the CRDT).
	FileStateDurable FileSyncState = "durable"
	// FileStateInFlight — registered, backup not confirmed yet (queued,
	// uploading, or driven by another device).
	FileStateInFlight FileSyncState = "inflight"
	// FileStateLimited — the network refused backup (storage limit).
	FileStateLimited FileSyncState = "limited"
)

// FileStatus is the point-in-time durability + availability view of
// one file.
type FileStatus struct {
	FileId   string
	ObjectId string
	State    FileSyncState
	// Cached reports a complete local copy (inline always true).
	Cached bool
	// Attempts counts failed background attempts since the last
	// success/enqueue; 0 when no work is pending.
	Attempts int
	// LastErr is the last background-attempt failure ("" when none).
	LastErr string
}

// FileStats are per-space aggregate counts.
type FileStats struct {
	Total    int
	Durable  int
	InFlight int
	Limited  int
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
	// Variant + VariantOf attach this content as an alternate
	// representation of an existing file on the SAME object (the
	// embedder produces the bytes — e.g. a thumbnail it rendered).
	// The variant is an ordinary sibling file with its own tier,
	// durability and lifecycle; Open(originalId, variant) resolves it.
	// Both must be set together; VariantOf must reference a file bound
	// to the same objectId.
	Variant   Variant
	VariantOf string
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
	// Variant/VariantOf tag alternate representations (member-only;
	// empty for originals and keyless readers).
	Variant   Variant
	VariantOf string
}
