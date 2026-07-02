package carfile

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-varint"
)

// CreateSparse writes the skeleton of a remote CARv2 to path: the head
// bytes (pragma + v2 header + at least the CARv1 header, fetched from
// the start of the object) verbatim at offset 0, the index bytes
// (fetched from Header.IndexOffset to the object end) verbatim at
// their native offset, and a hole in between. Block frames then arrive
// out of order via Sparse.WriteBlock; once every section is written
// the file is byte-identical to the source object.
//
// Both byte slices come from the source object, so structure is
// validated (single-root CARv2 head, parseable iterable index) and any
// sections fully covered by the head bytes are read back and verified
// against their cids — `present` reports the ones that pass, so a
// caller never records an unverified block as present.
func CreateSparse(path string, head, idx []byte) (root cid.Cid, present []int, err error) {
	hdr, err := parseHead(head)
	if err != nil {
		return cid.Undef, nil, err
	}
	if int64(len(head)) > int64(hdr.IndexOffset) {
		// Never let head bytes overlap the index region we write below.
		head = head[:hdr.IndexOffset]
	}
	if _, err = index.ReadFrom(bytes.NewReader(idx)); err != nil {
		return cid.Undef, nil, fmt.Errorf("carfile: sparse index: %w", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return cid.Undef, nil, err
	}
	defer f.Close()
	if _, err = f.WriteAt(head, 0); err != nil {
		return cid.Undef, nil, err
	}
	if _, err = f.WriteAt(idx, int64(hdr.IndexOffset)); err != nil {
		return cid.Undef, nil, err
	}
	size := int64(hdr.IndexOffset) + int64(len(idx))
	if err = f.Truncate(size); err != nil {
		return cid.Undef, nil, err
	}
	if err = f.Sync(); err != nil {
		return cid.Undef, nil, err
	}

	// Re-open through the normal path — proves header, root and index
	// are coherent before the caller records anything.
	full, err := os.Open(path)
	if err != nil {
		return cid.Undef, nil, err
	}
	defer full.Close()
	file, err := Open(full)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("carfile: sparse skeleton invalid: %w", err)
	}
	for i := 0; i < file.NumSections(); i++ {
		sec := file.Section(i)
		if sec.Offset+sec.Size > int64(len(head)) {
			continue
		}
		if _, err := file.ReadBlock(i); err == nil {
			present = append(present, i)
		}
	}
	return file.Root(), present, nil
}

// ParseHeader validates a fetched head range (the first bytes of a
// remote CARv2 — 4 KiB always suffices) and returns the v2 header, so
// a remote fetcher can locate the index region (Header.IndexOffset to
// the object end) with one more Range read.
func ParseHeader(head []byte) (carv2.Header, error) {
	return parseHead(head)
}

// PeekRoot parses the root cid out of a fetched head range without
// touching disk.
func PeekRoot(head []byte) (cid.Cid, error) {
	cr, err := carv2.NewReader(bytes.NewReader(head))
	if err != nil {
		return cid.Undef, fmt.Errorf("carfile: peek root: %w", err)
	}
	roots, err := cr.Roots()
	if err != nil {
		return cid.Undef, fmt.Errorf("carfile: peek root: %w", err)
	}
	if len(roots) != 1 {
		return cid.Undef, fmt.Errorf("carfile: peek root: %d roots, want 1", len(roots))
	}
	return roots[0], nil
}

// parseHead validates the fetched head range and returns the v2
// header. The range must cover the CARv1 header (root) too — a 4 KiB
// head fetch always does.
func parseHead(head []byte) (carv2.Header, error) {
	cr, err := carv2.NewReader(bytes.NewReader(head))
	if err != nil {
		return carv2.Header{}, fmt.Errorf("carfile: sparse head: %w", err)
	}
	if cr.Version != 2 {
		return carv2.Header{}, fmt.Errorf("carfile: sparse head: CARv%d, want CARv2", cr.Version)
	}
	roots, err := cr.Roots()
	if err != nil {
		return carv2.Header{}, fmt.Errorf("carfile: sparse head misses CARv1 header: %w", err)
	}
	if len(roots) != 1 {
		return carv2.Header{}, fmt.Errorf("carfile: sparse head: %d roots, want 1", len(roots))
	}
	return cr.Header, nil
}

// Sparse is a File whose data payload is still being filled. WriteBlock
// verifies a fetched block and lands its frame at the native offset.
type Sparse struct {
	*File
	w io.WriterAt
}

// OpenSparse opens f (an *os.File, or anything Reader+WriterAt) for
// sparse filling.
func OpenSparse(f interface {
	io.ReaderAt
	io.WriterAt
}) (*Sparse, error) {
	file, err := Open(f)
	if err != nil {
		return nil, err
	}
	return &Sparse{File: file, w: f}, nil
}

// WriteBlock verifies data against c (verify-before-persist) and
// writes the reconstructed frame at the section's native offset,
// returning the section number. The frame must fill its section
// exactly, so a wrong-codec or wrong-length block cannot silently
// corrupt the byte-identity invariant.
func (s *Sparse) WriteBlock(c cid.Cid, data []byte) (int, error) {
	i, ok := s.Lookup(c)
	if !ok {
		return 0, fmt.Errorf("%w: %s", ErrNotIndexed, c)
	}
	if err := verifyBlock(c, data); err != nil {
		return 0, fmt.Errorf("carfile: block %s: %w", c, err)
	}
	sec := s.Section(i)
	cb := c.Bytes()
	frame := make([]byte, 0, sec.Size)
	frame = append(frame, varint.ToUvarint(uint64(len(cb)+len(data)))...)
	frame = append(frame, cb...)
	frame = append(frame, data...)
	if int64(len(frame)) != sec.Size {
		return 0, fmt.Errorf("carfile: block %s frame is %d bytes, section is %d", c, len(frame), sec.Size)
	}
	if _, err := s.w.WriteAt(frame, sec.Offset); err != nil {
		return 0, fmt.Errorf("carfile: write section %d: %w", i, err)
	}
	return i, nil
}
