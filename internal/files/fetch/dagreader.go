package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/boxo/ipld/merkledag"
	unixfs "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

// dagReader is the no-preload targeted range reader over a UnixFS
// file DAG: Read resolves exactly the root→leaf path covering the
// current offset via UnixFS blocksizes and touches only those nodes —
// no read-ahead, so a seek costs a handful of blocks instead of the
// stock DagReader's ~9 MiB. Interior nodes are memoized (they're
// small and few: fanout 174); leaf payloads are held one at a time.
//
// The NodeGetter underneath (store.Handle.NodeGetter) serves local
// blocks and pulls misses through the fetch ladder, verifying and
// persisting each before it's used — so every byte this reader
// returns came from a cid-verified block.
type dagReader struct {
	ctx    context.Context
	getter ipld.NodeGetter
	root   cid.Cid

	size int64
	pos  int64

	interior map[cid.Cid]*interiorNode
	leaf     []byte
	leafOff  int64
}

// interiorNode is the memoized shape of one non-leaf DAG node: child
// links and each child's subtree start relative to the node.
type interiorNode struct {
	children []cid.Cid
	starts   []int64 // starts[i] = sum of blocksizes[:i]
	width    int64   // total blocksize of this subtree
}

// newDagReader loads the root node (resolving size) and positions at 0.
func newDagReader(ctx context.Context, getter ipld.NodeGetter, root cid.Cid) (*dagReader, error) {
	r := &dagReader{ctx: ctx, getter: getter, root: root, interior: map[cid.Cid]*interiorNode{}}
	size, err := r.load(root)
	if err != nil {
		return nil, err
	}
	r.size = size
	return r, nil
}

// load fetches and parses one node: an interior node is memoized and
// its width returned; a leaf sets r.leaf (caller fixes leafOff) and
// returns its length.
func (r *dagReader) load(c cid.Cid) (width int64, err error) {
	node, err := r.getter.Get(r.ctx, c)
	if err != nil {
		return 0, err
	}
	pn, ok := node.(*merkledag.ProtoNode)
	if !ok {
		return 0, fmt.Errorf("filefetch: node %s is not dag-pb", c)
	}
	fsn, err := unixfs.FSNodeFromBytes(pn.Data())
	if err != nil {
		return 0, fmt.Errorf("filefetch: node %s: %w", c, err)
	}
	links := node.Links()
	if len(links) == 0 {
		r.leaf = fsn.Data()
		return int64(len(r.leaf)), nil
	}
	if fsn.NumChildren() != len(links) {
		return 0, fmt.Errorf("filefetch: node %s: %d blocksizes for %d links", c, fsn.NumChildren(), len(links))
	}
	in := &interiorNode{
		children: make([]cid.Cid, len(links)),
		starts:   make([]int64, len(links)),
	}
	var off int64
	for i, l := range links {
		in.children[i] = l.Cid
		in.starts[i] = off
		off += int64(fsn.BlockSize(i))
	}
	in.width = off
	r.interior[c] = in
	return off, nil
}

// resolve walks root→leaf for pos, setting r.leaf/r.leafOff.
func (r *dagReader) resolve(pos int64) error {
	c, start := r.root, int64(0)
	for {
		in, ok := r.interior[c]
		if !ok {
			width, err := r.load(c)
			if err != nil {
				return err
			}
			if in, ok = r.interior[c]; !ok {
				// load hit a leaf and populated r.leaf.
				if pos-start >= width {
					return fmt.Errorf("filefetch: offset %d beyond leaf at %d+%d", pos, start, width)
				}
				r.leafOff = start
				return nil
			}
		}
		i := childFor(in, pos-start)
		if i < 0 {
			return fmt.Errorf("filefetch: offset %d not covered at node %s", pos, c)
		}
		start += in.starts[i]
		c = in.children[i]
	}
}

// childFor picks the child whose subtree covers rel.
func childFor(in *interiorNode, rel int64) int {
	if rel < 0 || rel >= in.width {
		return -1
	}
	// Children are equal-width except the last; direct math would work
	// for our balanced DAGs, but a scan is robust to any layout.
	for i := len(in.starts) - 1; i >= 0; i-- {
		if rel >= in.starts[i] {
			return i
		}
	}
	return -1
}

func (r *dagReader) Read(p []byte) (int, error) {
	if r.pos >= r.size {
		return 0, io.EOF
	}
	if r.leaf == nil || r.pos < r.leafOff || r.pos >= r.leafOff+int64(len(r.leaf)) {
		if err := r.resolve(r.pos); err != nil {
			return 0, err
		}
	}
	n := copy(p, r.leaf[r.pos-r.leafOff:])
	if n == 0 {
		return 0, errors.New("filefetch: empty leaf")
	}
	r.pos += int64(n)
	return n, nil
}

func (r *dagReader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.pos + offset
	case io.SeekEnd:
		abs = r.size + offset
	default:
		return 0, fmt.Errorf("filefetch: bad whence %d", whence)
	}
	if abs < 0 {
		return 0, errors.New("filefetch: negative seek")
	}
	r.pos = abs
	return abs, nil
}

// Size is the UnixFS file size (== ciphertext == plaintext length).
func (r *dagReader) Size() int64 { return r.size }
