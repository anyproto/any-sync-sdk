package upload

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
)

// spool buffers a caller's non-seekable reader while hashing sha256
// and counting size, so the tier decision (inline / BIND / full
// upload) happens before any row or CAR is written. Small inputs stay
// in memory; past memLimit the buffer spills to a temp file in the
// store's scratch dir (same filesystem as the CARs, swept on store
// open after a crash).
type spool struct {
	mem  []byte
	f    *os.File
	size int64
	sha  []byte
}

// fillSpool drains r completely. The spool is usable (multiple
// sequential Reader() passes) until Close.
func fillSpool(dir string, memLimit int64, r io.Reader) (*spool, error) {
	sp := &spool{}
	h := sha256.New()
	buf := make([]byte, 64<<10)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			h.Write(chunk)
			sp.size += int64(n)
			if sp.f == nil && sp.size > memLimit {
				if spillErr := sp.spill(dir); spillErr != nil {
					sp.Close()
					return nil, spillErr
				}
			}
			if sp.f != nil {
				if _, wErr := sp.f.Write(chunk); wErr != nil {
					sp.Close()
					return nil, wErr
				}
			} else {
				sp.mem = append(sp.mem, chunk...)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			sp.Close()
			return nil, err
		}
	}
	sp.sha = h.Sum(nil)
	return sp, nil
}

// spill moves the memory buffer into a fresh temp file and continues
// there.
func (sp *spool) spill(dir string) error {
	var rnd [10]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, "spool-"+hex.EncodeToString(rnd[:])+".tmp"),
		os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	if _, err = f.Write(sp.mem); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return err
	}
	sp.f = f
	sp.mem = nil
	return nil
}

// Size is the plaintext byte count.
func (sp *spool) Size() int64 { return sp.size }

// SHA256 is the plaintext content hash (the dedup key).
func (sp *spool) SHA256() []byte { return sp.sha }

// Bytes returns the in-memory content; valid only when the spool never
// spilled (the inline tier, which is far below any memLimit).
func (sp *spool) Bytes() []byte { return sp.mem }

// Reader returns a fresh reader over the full content.
func (sp *spool) Reader() io.Reader {
	if sp.f != nil {
		return io.NewSectionReader(sp.f, 0, sp.size)
	}
	return bytes.NewReader(sp.mem)
}

// Close releases the spill file, if any.
func (sp *spool) Close() {
	if sp.f != nil {
		name := sp.f.Name()
		_ = sp.f.Close()
		_ = os.Remove(name)
		sp.f = nil
	}
	sp.mem = nil
}
