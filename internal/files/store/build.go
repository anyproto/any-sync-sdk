package store

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/anyproto/any-sync-sdk/internal/files/carfile"
)

// Build is an in-progress upload-side CAR: the UnixFS DAG builder
// streams blocks in, Finalize lands the file under its root cid and
// records the metadata row as complete. Discard (or a crash — temps
// are swept on store open) leaves no trace.
type Build struct {
	s       *Store
	spaceId string
	tmpPath string
	b       *carfile.Builder
	done    bool
}

// NewBuild opens a build temp for spaceId.
func (s *Store) NewBuild(ctx context.Context, spaceId string) (*Build, error) {
	if spaceId == "" {
		return nil, errors.New("filestore: spaceId required")
	}
	var rnd [10]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return nil, err
	}
	tmpPath := filepath.Join(s.root, "tmp", hex.EncodeToString(rnd[:])+".car")
	b, err := carfile.NewBuilder(tmpPath)
	if err != nil {
		return nil, err
	}
	return &Build{s: s, spaceId: spaceId, tmpPath: tmpPath, b: b}, nil
}

// Put appends DAG blocks in build order.
func (b *Build) Put(ctx context.Context, blks []blocks.Block) error {
	return b.b.Put(ctx, blks)
}

// Finalize completes the CAR, moves it into place and upserts the
// metadata row (state=complete, refs attached). If the same content is
// already stored (a crash-replayed build) the existing file wins and
// the temp is dropped. Any failure after the rename removes the placed
// file again — a CAR without a metadata row would be invisible to
// listing and GC forever.
func (b *Build) Finalize(ctx context.Context, root cid.Cid, refs ...string) (Info, error) {
	if b.done {
		return Info{}, errors.New("filestore: build already finalized")
	}
	b.done = true
	if err := b.b.Finalize(ctx, root); err != nil {
		b.cleanup()
		return Info{}, err
	}

	final := b.s.carPath(b.spaceId, root)
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		b.cleanup()
		return Info{}, err
	}

	var info Info
	err := b.s.withRow(rowId(b.spaceId, root), func(rs *rowState) error {
		placed := false
		if _, err := os.Stat(final); err == nil {
			b.cleanup()
		} else if err = os.Rename(b.tmpPath, final); err != nil {
			b.cleanup()
			return err
		} else {
			placed = true
		}
		fail := func(err error) error {
			if placed {
				_ = os.Remove(final)
			}
			return err
		}

		fi, err := os.Stat(final)
		if err != nil {
			return fail(err)
		}
		f, err := os.Open(final)
		if err != nil {
			return fail(err)
		}
		cf, err := carfile.Open(f)
		_ = f.Close()
		if err != nil {
			return fail(fmt.Errorf("filestore: finalized car invalid: %w", err))
		}

		if err = b.s.upsertRow(ctx, b.spaceId, root, func(a *anyenc.Arena, v *anyenc.Value) {
			v.Set(fieldState, a.NewString(StateComplete))
			v.Set(fieldSize, sizeValue(a, fi.Size()))
			v.Set(fieldSections, a.NewNumberInt(cf.NumSections()))
			v.Del(fieldHave)
			addRefs(a, v, refs)
		}); err != nil {
			return fail(err)
		}
		rs.state = StateComplete
		rs.have = nil
		rs.loaded = true
		info = Info{
			SpaceId:    b.spaceId,
			Root:       root,
			State:      StateComplete,
			Size:       fi.Size(),
			Sections:   cf.NumSections(),
			Present:    cf.NumSections(),
			LastAccess: time.Now(),
		}
		return nil
	})
	if err != nil {
		return Info{}, err
	}
	// Refs may have merged with an existing row; report the row's view.
	return b.s.Info(ctx, b.spaceId, info.Root)
}

// Discard abandons the build and removes the temp.
func (b *Build) Discard() {
	if b.done {
		return
	}
	b.done = true
	b.b.Discard()
	b.cleanup()
}

func (b *Build) cleanup() {
	_ = os.Remove(b.tmpPath)
}

// CreateSparse registers a remote CARv2 for sparse download: the head
// and index ranges (fetched from the object start and tail) become the
// local skeleton, the row starts partial with the head-delivered,
// cid-verified sections pre-marked. If the file and its row already
// exist (racing fetchers, refetch of a still-known file) the existing
// state is kept and refs merged; a file with no row — the debris of a
// crashed Delete or Finalize — is replaced with a fresh skeleton.
func (s *Store) CreateSparse(ctx context.Context, spaceId string, head, index []byte, refs ...string) (Info, error) {
	probe, err := carfile.PeekRoot(head)
	if err != nil {
		return Info{}, err
	}
	id := rowId(spaceId, probe)
	err = s.withRow(id, func(rs *rowState) error {
		final := s.carPath(spaceId, probe)
		if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
			return err
		}
		if _, statErr := os.Stat(final); statErr == nil {
			if _, rowErr := s.files.FindId(ctx, id); rowErr == nil {
				// Bytes and row both present: merge refs, keep state.
				if len(refs) > 0 {
					return s.AddRefs(ctx, spaceId, probe, refs...)
				}
				return nil
			} else if !errors.Is(rowErr, anystore.ErrDocNotFound) {
				return rowErr
			}
			// Orphan file with no row: unusable debris, rebuild.
			if err := os.Remove(final); err != nil {
				return err
			}
		}

		root, present, err := carfile.CreateSparse(final, head, index)
		if err != nil {
			return err
		}
		f, err := os.Open(final)
		if err != nil {
			return err
		}
		cf, err := carfile.Open(f)
		_ = f.Close()
		if err != nil {
			return err
		}
		fi, err := os.Stat(final)
		if err != nil {
			return err
		}

		have := newBitmap(cf.NumSections())
		for _, i := range present {
			have.set(i)
		}
		state := StatePartial
		if have.full(cf.NumSections()) {
			state = StateComplete
		}

		if err = s.upsertRow(ctx, spaceId, root, func(a *anyenc.Arena, v *anyenc.Value) {
			v.Set(fieldState, a.NewString(state))
			v.Set(fieldSize, sizeValue(a, fi.Size()))
			v.Set(fieldSections, a.NewNumberInt(cf.NumSections()))
			if state == StatePartial {
				v.Set(fieldHave, a.NewString(base64.StdEncoding.EncodeToString(have)))
			} else {
				v.Del(fieldHave)
			}
			addRefs(a, v, refs)
		}); err != nil {
			_ = os.Remove(final)
			return err
		}
		rs.state = state
		rs.have = have
		if state == StateComplete {
			rs.have = nil
		}
		rs.loaded = true
		return nil
	})
	if err != nil {
		return Info{}, err
	}
	return s.Info(ctx, spaceId, probe)
}
