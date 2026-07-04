package carfile

import (
	"context"
	"fmt"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	mh "github.com/multiformats/go-multihash"
)

// placeholderRoot occupies the CARv1 header while the DAG is being
// built — the real root exists only after the last block. It is the
// same shape as every real root (CIDv1/dag-pb/sha2-256), so the
// finalize-time swap is a fixed-size in-place patch.
var placeholderRoot = func() cid.Cid {
	sum, err := mh.Sum(nil, mh.SHA2_256, -1)
	if err != nil {
		panic(err)
	}
	return cid.NewCidV1(cid.DagProtobuf, sum)
}()

// Builder streams DAG blocks into a new CARv2 at path. Blocks are
// appended in put order (the build's traversal order defines the
// canonical object layout); Finalize writes the embedded index and
// patches the real root in. The file at path must not already exist.
type Builder struct {
	path string
	bs   *blockstore.ReadWrite
	done bool
}

// NewBuilder creates the CARv2 skeleton at path and returns a Builder
// accepting blocks.
func NewBuilder(path string) (*Builder, error) {
	bs, err := blockstore.OpenReadWrite(path, []cid.Cid{placeholderRoot})
	if err != nil {
		return nil, fmt.Errorf("carfile: open builder: %w", err)
	}
	return &Builder{path: path, bs: bs}, nil
}

// Put appends blocks (deduplicating repeated cids).
func (b *Builder) Put(ctx context.Context, blks []blocks.Block) error {
	return b.bs.PutMany(ctx, blks)
}

// Finalize writes the index, patches root in place and closes the
// file. root must be one of the written blocks.
func (b *Builder) Finalize(ctx context.Context, root cid.Cid) error {
	if b.done {
		return fmt.Errorf("carfile: builder already finalized")
	}
	b.done = true
	if has, err := b.bs.Has(ctx, root); err != nil {
		return err
	} else if !has {
		return fmt.Errorf("carfile: root %s was not written", root)
	}
	if err := b.bs.Finalize(); err != nil {
		return fmt.Errorf("carfile: finalize: %w", err)
	}
	if err := carv2.ReplaceRootsInFile(b.path, []cid.Cid{root}); err != nil {
		return fmt.Errorf("carfile: set root: %w", err)
	}
	return nil
}

// Discard abandons the build and releases the file handle; the caller
// removes the temp file.
func (b *Builder) Discard() {
	if !b.done {
		b.done = true
		b.bs.Discard()
	}
}
