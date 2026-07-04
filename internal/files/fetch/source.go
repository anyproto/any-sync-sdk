package fetch

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/anyproto/any-sync-sdk/internal/files/carfile"
	"github.com/anyproto/any-sync-sdk/internal/files/store"
)

// maxFetchSpan caps one coalesced Range read. Sequential streaming
// pulls a few 1 MiB leaves per request instead of one request per
// block; a cold random seek still fetches only the sections it needs.
const maxFetchSpan = 4 << 20

// CarSource range-reads one immutable CARv2 object. Both the HTTP
// public-read reader (remoteCar) and the LAN peer reader (the p2p
// package's peerCar) implement it, so seed and remoteFetcher are
// source-agnostic. Exported so the p2p package can supply a peer source.
type CarSource interface {
	// ReadRange fetches exactly [off, off+length) (short only at EOF,
	// which the callers treat as an error outside the head probe).
	ReadRange(ctx context.Context, off, length int64) ([]byte, error)
	// ReadProbe fetches the head range and the whole-object size.
	ReadProbe(ctx context.Context) (head []byte, total int64, err error)
}

// PeerSource selects a LAN peer that holds a file in full and returns a
// CarSource that reads the file's CAR object from it. (nil, false) — no
// peer found / p2p disabled — means the fetch uses the public GET only.
// The returned source is consulted BEFORE the public GET (peers are
// free, egress is not); it must be cheap and bounded (one FileCheck
// round) so it never spikes latency when no peer has the file.
type PeerSource interface {
	SourceFor(ctx context.Context, spaceId string, root cid.Cid) (CarSource, bool)
}

// peerPreferDeadline bounds each peer read. On a healthy LAN a ~1 MiB
// range returns in a few ms; this generous cap means a stalling peer
// costs at most one deadline per file before we demote it to HTTP.
const peerPreferDeadline = 800 * time.Millisecond

// ladderedCar prefers a LAN peer, falling back to HTTP. The FIRST peer
// failure demotes the peer for the rest of this file (the struct is
// per-fetch), so a stall is paid at most once, never per block — and on
// a healthy LAN the peer serves the whole file with zero HTTP egress.
type ladderedCar struct {
	peer     CarSource
	http     CarSource // nil for a non-durable file with no public object
	demoted  bool
}

func (l *ladderedCar) ReadRange(ctx context.Context, off, length int64) ([]byte, error) {
	if l.peer != nil && !l.demoted {
		pctx, cancel := context.WithTimeout(ctx, peerPreferDeadline)
		data, err := l.peer.ReadRange(pctx, off, length)
		cancel()
		if err == nil {
			return data, nil
		}
		l.demoted = true // stop using this peer for the rest of the file
	}
	if l.http == nil {
		return nil, ErrNotAvailable
	}
	return l.http.ReadRange(ctx, off, length)
}

func (l *ladderedCar) ReadProbe(ctx context.Context) ([]byte, int64, error) {
	if l.peer != nil && !l.demoted {
		pctx, cancel := context.WithTimeout(ctx, peerPreferDeadline)
		head, total, err := l.peer.ReadProbe(pctx)
		cancel()
		if err == nil {
			return head, total, nil
		}
		l.demoted = true
	}
	if l.http == nil {
		return nil, 0, ErrNotAvailable
	}
	return l.http.ReadProbe(ctx)
}

// remoteFetcher builds the miss-handler for Handle.NodeGetter: resolve
// the cid to its section, coalesce forward over contiguous missing
// sections (up to maxFetchSpan), one Range read, then verify-persist
// every fetched block. Only the requested block is returned — the
// NodeGetter persists it via WriteBlock; the coalesced extras are
// persisted here so they're local by the time the reader reaches them.
func remoteFetcher(spaceId string, h *store.Handle, rc CarSource) func(ctx context.Context, c cid.Cid) ([]byte, error) {
	return func(ctx context.Context, c cid.Cid) ([]byte, error) {
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
		data, err := rc.ReadRange(ctx, first.Offset, end-first.Offset)
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
