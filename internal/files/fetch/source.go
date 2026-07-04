package fetch

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ipfs/go-cid"

	"github.com/anyproto/any-sync-sdk/internal/files/carfile"
	"github.com/anyproto/any-sync-sdk/internal/files/store"
)

// maxFetchSpan caps one coalesced Range read. Sequential streaming
// pulls a few 1 MiB leaves per request instead of one request per
// block; a cold random seek still fetches only the sections it needs.
const maxFetchSpan = 4 << 20

// PeerSource is the SYN-24 P2P rung of the block ladder. Nil until the
// P2P block layer lands; when present it is consulted before the
// public GET (peers are free, egress is not).
type PeerSource interface {
	GetBlock(ctx context.Context, spaceId string, c cid.Cid) ([]byte, error)
}

// remoteFetcher builds the miss-handler for Handle.NodeGetter: resolve
// the cid to its section, coalesce forward over contiguous missing
// sections (up to maxFetchSpan), one Range read, then verify-persist
// every fetched block. Only the requested block is returned — the
// NodeGetter persists it via WriteBlock; the coalesced extras are
// persisted here so they're local by the time the reader reaches them.
func remoteFetcher(spaceId string, h *store.Handle, peer PeerSource, rc *remoteCar) func(ctx context.Context, c cid.Cid) ([]byte, error) {
	return func(ctx context.Context, c cid.Cid) ([]byte, error) {
		if peer != nil {
			if data, err := peer.GetBlock(ctx, spaceId, c); err == nil {
				return data, nil
			}
		}
		if rc == nil {
			return nil, ErrNotAvailable
		}
		i, ok := h.SectionIndex(c)
		if !ok {
			return nil, fmt.Errorf("filefetch: %s not in the car index of %s", c, h.Root())
		}
		first := h.Section(i)
		last := i
		end := first.Offset + first.Size
		for j := i + 1; j < h.NumBlocks(); j++ {
			sec := h.Section(j)
			if h.HasSection(j) || sec.Offset+sec.Size-first.Offset > maxFetchSpan {
				break
			}
			end = sec.Offset + sec.Size
			last = j
		}
		data, err := rc.readRange(ctx, first.Offset, end-first.Offset)
		if err != nil {
			return nil, err
		}
		var want []byte
		off := int64(0)
		for j := i; j <= last; j++ {
			sec := h.Section(j)
			bc, bdata, err := carfile.ParseFrame(data[off : off+sec.Size])
			off += sec.Size
			if err != nil {
				if j == i {
					return nil, fmt.Errorf("filefetch: section %d frame: %w", j, err)
				}
				continue // a bad extra is just not persisted; refetched on demand
			}
			if j == i {
				if !bytes.Equal(bc.Hash(), c.Hash()) {
					return nil, fmt.Errorf("filefetch: section %d holds %s, wanted %s", j, bc, c)
				}
				want = bdata
				continue // the NodeGetter persists the wanted block
			}
			_ = h.WriteBlock(ctx, bc, bdata) // verifies; failure = not persisted
		}
		return want, nil
	}
}
