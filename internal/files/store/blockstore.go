package store

import (
	"context"
	"errors"

	"github.com/anyproto/any-sync/commonfile/fileblockstore"
	"github.com/ipfs/boxo/ipld/merkledag"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

// Blockstore adapts a Build to any-sync's fileblockstore.BlockStore so
// fileservice.FileHandler.AddFile can stream the UnixFS DAG straight
// into the CAR. Write-only: the DAG builder never reads back.
func (b *Build) Blockstore() fileblockstore.BlockStore {
	return &buildBlockstore{b: b}
}

type buildBlockstore struct{ b *Build }

func (x *buildBlockstore) Add(ctx context.Context, bs []blocks.Block) error {
	return x.b.Put(ctx, bs)
}

func (x *buildBlockstore) Get(ctx context.Context, k cid.Cid) (blocks.Block, error) {
	return nil, fileblockstore.ErrCIDNotFound
}

func (x *buildBlockstore) GetMany(ctx context.Context, ks []cid.Cid) <-chan blocks.Block {
	ch := make(chan blocks.Block)
	close(ch)
	return ch
}

func (x *buildBlockstore) Delete(ctx context.Context, c cid.Cid) error {
	return errors.New("filestore: build blockstore is append-only")
}

// NodeGetter adapts a Handle to ipld.NodeGetter for DAG traversal
// (ufsio readers, the targeted range reader). fetch, when non-nil, is
// consulted for blocks not yet local — SYN-28's remote ladder plugs in
// there; the fetched block is persisted via WriteBlock before use. A
// nil fetch serves local-only.
func (h *Handle) NodeGetter(fetch func(ctx context.Context, c cid.Cid) ([]byte, error)) ipld.NodeGetter {
	return &handleGetter{h: h, fetch: fetch}
}

type handleGetter struct {
	h     *Handle
	fetch func(ctx context.Context, c cid.Cid) ([]byte, error)
}

func (g *handleGetter) Get(ctx context.Context, c cid.Cid) (ipld.Node, error) {
	data, err := g.h.ReadBlock(ctx, c)
	if err != nil {
		if g.fetch == nil || !errors.Is(err, ErrBlockMissing) {
			return nil, err
		}
		if data, err = g.fetch(ctx, c); err != nil {
			return nil, err
		}
		// Persist-before-use: WriteBlock verifies the bytes against c.
		if err = g.h.WriteBlock(ctx, c, data); err != nil {
			return nil, err
		}
	}
	b, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		return nil, err
	}
	return merkledag.DecodeProtobufBlock(b)
}

func (g *handleGetter) GetMany(ctx context.Context, ks []cid.Cid) <-chan *ipld.NodeOption {
	ch := make(chan *ipld.NodeOption, len(ks))
	go func() {
		defer close(ch)
		for _, c := range ks {
			n, err := g.Get(ctx, c)
			select {
			case ch <- &ipld.NodeOption{Node: n, Err: err}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch
}
