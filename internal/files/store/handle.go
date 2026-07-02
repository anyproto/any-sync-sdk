package store

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/ipfs/go-cid"

	"github.com/anyproto/any-sync-sdk/internal/files/carfile"
)

// Handle is an open stored file. Reads verify every block against its
// cid; a partial file accepts fetched blocks via WriteBlock and flips
// to complete when the last section lands. All handles on the same
// file share one live rowState, so concurrent downloaders never lose
// each other's progress and a store-level Offload/Delete is observed
// immediately.
type Handle struct {
	s       *Store
	spaceId string
	f       *os.File
	car     *carfile.Sparse
	entry   *rowEntry
	closed  bool
}

// Open opens (spaceId, root) for reading and — while partial — sparse
// filling. ErrNotFound without a row; ErrNoBytes when the row exists
// but the bytes were offloaded or lost (refetch via CreateSparse).
func (s *Store) Open(ctx context.Context, spaceId string, root cid.Cid) (*Handle, error) {
	id := rowId(spaceId, root)
	entry := s.rows.acquire(id)
	h, err := s.open(ctx, spaceId, root, entry)
	if err != nil {
		s.rows.release(id, entry)
		return nil, err
	}
	return h, nil
}

func (s *Store) open(ctx context.Context, spaceId string, root cid.Cid, entry *rowEntry) (*Handle, error) {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if !entry.loaded {
		doc, err := s.files.FindId(ctx, rowId(spaceId, root))
		if err != nil {
			if errors.Is(err, anystore.ErrDocNotFound) {
				return nil, ErrNotFound
			}
			return nil, err
		}
		entry.state = string(doc.Value().GetStringBytes(fieldState))
		entry.have = decodeBitmap(doc.Value())
		entry.loaded = true
	}
	switch entry.state {
	case StateOffload, stateGone:
		return nil, ErrNoBytes
	}
	f, err := os.OpenFile(s.carPath(spaceId, root), os.O_RDWR, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNoBytes
		}
		return nil, err
	}
	car, err := carfile.OpenSparse(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if entry.state == StatePartial && len(entry.have) != len(newBitmap(car.NumSections())) {
		// Unreadable persisted bitmap: assume nothing, refetch;
		// re-verified writes are idempotent.
		entry.have = newBitmap(car.NumSections())
	}
	h := &Handle{s: s, spaceId: spaceId, f: f, car: car, entry: entry}
	_ = s.Touch(ctx, spaceId, root)
	return h, nil
}

// Root returns the file's root cid.
func (h *Handle) Root() cid.Cid { return h.car.Root() }

// Size returns the total CAR size (== the S3 object size).
func (h *Handle) Size() (int64, error) {
	fi, err := h.f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// NumBlocks returns the number of blocks in the file.
func (h *Handle) NumBlocks() int { return h.car.NumSections() }

// Complete reports whether every section is present.
func (h *Handle) Complete() bool {
	h.entry.mu.Lock()
	defer h.entry.mu.Unlock()
	return h.entry.state == StateComplete
}

// HasBlock reports whether c's section has arrived.
func (h *Handle) HasBlock(c cid.Cid) bool {
	i, ok := h.car.Lookup(c)
	if !ok {
		return false
	}
	h.entry.mu.Lock()
	defer h.entry.mu.Unlock()
	return h.hasLocked(i)
}

func (h *Handle) hasLocked(i int) bool {
	return h.entry.state == StateComplete || h.entry.have.has(i)
}

// MissingBlocks returns the cids whose sections haven't arrived, in
// file order — the fetch worklist for a full download.
func (h *Handle) MissingBlocks() []cid.Cid {
	h.entry.mu.Lock()
	defer h.entry.mu.Unlock()
	if h.entry.state != StatePartial {
		return nil
	}
	var out []cid.Cid
	for i := 0; i < h.car.NumSections(); i++ {
		if !h.entry.have.has(i) {
			out = append(out, h.car.Section(i).Cid)
		}
	}
	return out
}

// ReadBlock returns the verified bytes of block c. ErrBlockMissing
// when the section hasn't arrived — or fails verification (a torn
// sparse write reads exactly like an absent block: refetch it).
func (h *Handle) ReadBlock(ctx context.Context, c cid.Cid) ([]byte, error) {
	i, ok := h.car.Lookup(c)
	if !ok {
		return nil, fmt.Errorf("%w: %s not in file %s", ErrBlockMissing, c, h.car.Root())
	}
	h.entry.mu.Lock()
	present := h.hasLocked(i)
	h.entry.mu.Unlock()
	if !present {
		return nil, fmt.Errorf("%w: %s", ErrBlockMissing, c)
	}
	data, err := h.car.ReadBlock(i)
	if err != nil {
		if errors.Is(err, carfile.ErrCorrupt) {
			h.markMissing(ctx, i)
			return nil, fmt.Errorf("%w: %s (failed verification)", ErrBlockMissing, c)
		}
		return nil, err
	}
	return data, nil
}

// WriteBlock verifies and lands a fetched block, persisting the bitmap
// advance. When the last section arrives the file is synced and the
// row flips to complete. A no-op once the row left the partial state
// (another handle finished it, or the store offloaded/deleted it).
func (h *Handle) WriteBlock(ctx context.Context, c cid.Cid, data []byte) error {
	h.entry.mu.Lock()
	defer h.entry.mu.Unlock()
	if h.entry.state != StatePartial {
		return nil
	}
	i, err := h.car.WriteBlock(c, data)
	if err != nil {
		return err
	}
	if h.entry.have.has(i) {
		return nil
	}
	h.entry.have.set(i)
	if h.entry.have.full(h.car.NumSections()) {
		if err = h.f.Sync(); err != nil {
			return err
		}
		h.entry.state = StateComplete
		return h.s.updateExisting(ctx, h.id(), func(a *anyenc.Arena, v *anyenc.Value) {
			v.Set(fieldState, a.NewString(StateComplete))
			v.Del(fieldHave)
		})
	}
	// The bitmap is persisted without an fsync of the data write —
	// after a crash it may claim a section the page cache lost.
	// Tolerable by construction: ReadBlock re-verifies every block and
	// a failure routes back through markMissing → refetch.
	return h.s.updateExisting(ctx, h.id(), func(a *anyenc.Arena, v *anyenc.Value) {
		v.Set(fieldHave, a.NewString(base64.StdEncoding.EncodeToString(h.entry.have)))
	})
}

// markMissing records a verification failure so the fetch path
// re-downloads the section. A complete file that lost a block to disk
// corruption is reopened as partial with only that section missing.
func (h *Handle) markMissing(ctx context.Context, i int) {
	h.entry.mu.Lock()
	defer h.entry.mu.Unlock()
	switch h.entry.state {
	case StateComplete:
		h.entry.state = StatePartial
		h.entry.have = newBitmap(h.car.NumSections())
		for j := 0; j < h.car.NumSections(); j++ {
			if j != i {
				h.entry.have.set(j)
			}
		}
	case StatePartial:
		if h.entry.have.has(i) {
			h.entry.have[i/8] &^= 1 << (i % 8)
		}
	default:
		return // offloaded/gone: nothing to record
	}
	_ = h.s.updateExisting(ctx, h.id(), func(a *anyenc.Arena, v *anyenc.Value) {
		v.Set(fieldState, a.NewString(StatePartial))
		v.Set(fieldHave, a.NewString(base64.StdEncoding.EncodeToString(h.entry.have)))
	})
}

// ReadAt serves the raw CAR bytes (the exact S3 object bytes) — the
// upload path streams the finished file from here. Refused until the
// file is complete: a partial file's holes would stream as zeros.
func (h *Handle) ReadAt(p []byte, off int64) (int, error) {
	if !h.Complete() {
		return 0, fmt.Errorf("%w: raw stream of an incomplete file", ErrNoBytes)
	}
	return h.f.ReadAt(p, off)
}

// Close releases the file handle and the shared row state reference.
func (h *Handle) Close() error {
	if h.closed {
		return nil
	}
	h.closed = true
	h.s.rows.release(h.id(), h.entry)
	return h.f.Close()
}

func (h *Handle) id() string { return rowId(h.spaceId, h.car.Root()) }
