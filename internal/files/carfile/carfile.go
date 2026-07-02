// Package carfile reads and writes the files byte-layer pack format:
// one standard CARv2 (pragma + v2 header + CARv1 data payload +
// embedded multihash-sorted index) per file, keyed by its UnixFS root
// cid. The local cache file is byte-identical to the uploaded S3
// object, so uploads stream the file verbatim and downloads
// reconstruct it verbatim — including sparse reconstruction, where
// cid-verified blocks land at their native offsets as they arrive
// (sparse.go).
//
// The package deals in raw block bytes only; IPLD decoding and DAG
// traversal belong to callers. Every read re-verifies the block
// against its cid — a torn sparse write or on-disk corruption surfaces
// as ErrCorrupt, never as bad plaintext.
package carfile

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
)

var (
	// ErrCorrupt reports a section whose bytes do not verify against
	// the index or the block's cid. For a sparse file this is also how
	// a not-yet-fetched (all-zero) section reads — callers track
	// presence separately and treat ErrCorrupt as "absent, refetch".
	ErrCorrupt = errors.New("carfile: section corrupt or absent")

	// ErrNotIndexed reports a cid the file's index does not contain.
	ErrNotIndexed = errors.New("carfile: cid not in index")
)

// Section is one block frame of the CARv1 data payload:
// uvarint(len(cid)+len(data)) || cid || data.
type Section struct {
	// Cid identifies the block. Reconstructed from the index
	// multihash (our files are CIDv1/dag-pb-only), so match by
	// multihash, not full-cid equality.
	Cid cid.Cid
	// Offset is the absolute file offset of the frame start.
	Offset int64
	// Size is the full frame size (varint + cid + data).
	Size int64
}

// File is a read view over a complete or sparse CARv2, resolved
// through the embedded index. The underlying ReaderAt must cover at
// least the header and index regions; data sections may be absent
// (sparse) and read as ErrCorrupt.
type File struct {
	r        io.ReaderAt
	root     cid.Cid
	header   carv2.Header
	sections []Section
	byMh     map[string]int
}

// Open parses the CARv2 header, root and embedded index from r.
func Open(r io.ReaderAt) (*File, error) {
	cr, err := carv2.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("carfile: read header: %w", err)
	}
	if cr.Version != 2 {
		return nil, fmt.Errorf("carfile: CARv%d, want CARv2", cr.Version)
	}
	roots, err := cr.Roots()
	if err != nil {
		return nil, fmt.Errorf("carfile: read roots: %w", err)
	}
	if len(roots) != 1 {
		return nil, fmt.Errorf("carfile: %d roots, want 1", len(roots))
	}
	ir, err := cr.IndexReader()
	if err != nil {
		return nil, fmt.Errorf("carfile: index reader: %w", err)
	}
	idx, err := index.ReadFrom(ir)
	if err != nil {
		return nil, fmt.Errorf("carfile: read index: %w", err)
	}
	it, ok := idx.(index.IterableIndex)
	if !ok {
		return nil, fmt.Errorf("carfile: index codec %v is not iterable", idx.Codec())
	}

	f := &File{r: r, root: roots[0], header: cr.Header}
	// Index offsets are relative to the CARv1 data payload start and
	// point at the frame varint. Sizes come from the gaps between
	// consecutive offsets — no data reads, so this works on a sparse
	// file whose payload hasn't arrived yet.
	err = it.ForEach(func(mh multihash.Multihash, rel uint64) error {
		c := cid.NewCidV1(cid.DagProtobuf, append(multihash.Multihash(nil), mh...))
		f.sections = append(f.sections, Section{Cid: c, Offset: int64(cr.Header.DataOffset + rel)})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("carfile: iterate index: %w", err)
	}
	if len(f.sections) == 0 {
		return nil, errors.New("carfile: empty index")
	}
	sort.Slice(f.sections, func(i, j int) bool { return f.sections[i].Offset < f.sections[j].Offset })
	dataEnd := int64(cr.Header.DataOffset + cr.Header.DataSize)
	for i := range f.sections {
		end := dataEnd
		if i+1 < len(f.sections) {
			end = f.sections[i+1].Offset
		}
		f.sections[i].Size = end - f.sections[i].Offset
		if f.sections[i].Size <= 0 {
			return nil, fmt.Errorf("carfile: non-positive section size at offset %d", f.sections[i].Offset)
		}
	}
	f.byMh = make(map[string]int, len(f.sections))
	for i, s := range f.sections {
		f.byMh[string(s.Cid.Hash())] = i
	}
	return f, nil
}

// Root returns the single root cid (the file's UnixFS root).
func (f *File) Root() cid.Cid { return f.root }

// Header returns the parsed CARv2 header (data/index offsets and
// sizes for range math).
func (f *File) Header() carv2.Header { return f.header }

// NumSections returns the number of blocks in the index.
func (f *File) NumSections() int { return len(f.sections) }

// Section returns the i-th section in file order.
func (f *File) Section(i int) Section { return f.sections[i] }

// Lookup resolves a cid (by multihash) to its section number.
func (f *File) Lookup(c cid.Cid) (int, bool) {
	i, ok := f.byMh[string(c.Hash())]
	return i, ok
}

// ReadBlock reads and verifies section i, returning the block data.
// The frame is parsed and the data re-hashed against the cid before
// any byte is returned.
func (f *File) ReadBlock(i int) ([]byte, error) {
	if i < 0 || i >= len(f.sections) {
		return nil, fmt.Errorf("carfile: section %d out of range", i)
	}
	sec := f.sections[i]
	frame := make([]byte, sec.Size)
	if _, err := f.r.ReadAt(frame, sec.Offset); err != nil {
		return nil, fmt.Errorf("carfile: read section %d: %w", i, err)
	}
	data, c, err := parseFrame(frame)
	if err != nil || !bytes.Equal(c.Hash(), sec.Cid.Hash()) {
		return nil, fmt.Errorf("%w: section %d frame", ErrCorrupt, i)
	}
	if err = verifyBlock(c, data); err != nil {
		return nil, fmt.Errorf("%w: section %d", err, i)
	}
	return data, nil
}

// ParseFrame splits one raw section frame (uvarint || cid || data) into
// its cid and data — the remote fetch path decodes Range-read frames
// with it before handing each block to WriteBlock for verification.
func ParseFrame(frame []byte) (c cid.Cid, data []byte, err error) {
	data, c, err = parseFrame(frame)
	return c, data, err
}

// parseFrame splits a section frame into its cid and data. The frame
// must be exactly one whole section (trailing bytes are an error, so a
// wrong-length write can't hide).
func parseFrame(frame []byte) (data []byte, c cid.Cid, err error) {
	l, vn, err := varint.FromUvarint(frame)
	if err != nil {
		return nil, cid.Undef, err
	}
	if int(l) != len(frame)-vn {
		return nil, cid.Undef, fmt.Errorf("carfile: frame length %d != section %d", l, len(frame)-vn)
	}
	cn, c, err := cid.CidFromBytes(frame[vn:])
	if err != nil {
		return nil, cid.Undef, err
	}
	return frame[vn+cn:], c, nil
}

// Verify re-hashes data with the cid's own prefix and compares — the
// standalone verification gate for callers that consume a block
// without persisting it.
func Verify(c cid.Cid, data []byte) error {
	return verifyBlock(c, data)
}

// verifyBlock re-hashes data with the cid's own prefix and compares.
func verifyBlock(c cid.Cid, data []byte) error {
	sum, err := c.Prefix().Sum(data)
	if err != nil || !sum.Equals(c) {
		return ErrCorrupt
	}
	return nil
}
