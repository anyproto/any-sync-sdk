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

// PeerSource selects a LAN peer that holds a file in full. It returns a
// CarSource that reads the file's CAR object from that peer, plus a ban
// hook the fetcher calls when the peer serves INVALID bytes (so a bad
// peer is not re-selected). ok=false — no peer holds the file / p2p
// disabled — means the fetch uses the public GET only. Selection must be
// cheap and bounded (one FileCheck round) so it never spikes latency
// when no peer has the file.
type PeerSource interface {
	SourceFor(ctx context.Context, spaceId string, root cid.Cid) (src CarSource, ban func(), ok bool)
}

// peerPreferDeadline bounds each read from a LAN peer. On a healthy
// LAN a ~1 MiB range returns in a few ms; this generous cap means a
// stalling peer costs at most one deadline before we demote it to HTTP
// for this fetch.
const peerPreferDeadline = 800 * time.Millisecond

// PeerReadDeadliner is the optional interface a CarSource implements to
// state a read budget other than the LAN default. A range served across
// the internet — through a relay, at a few MB/s — takes orders of
// magnitude longer than the same range on a LAN, and the LAN figure
// simply expires mid-transfer. Where there is no HTTP fallback (a file
// that never reached a durability node) that expiry is the whole fetch,
// so a file that a peer holds in full never arrives.
type PeerReadDeadliner interface {
	// PeerReadDeadline returns the budget for one range read, or zero
	// to take the LAN default.
	PeerReadDeadline() time.Duration
}

// peerDeadline is the budget for one read from the current peer source.
func (fs *fetchSources) peerDeadline() time.Duration {
	if d, ok := fs.peer.(PeerReadDeadliner); ok {
		if v := d.PeerReadDeadline(); v > 0 {
			return v
		}
	}
	return peerPreferDeadline
}

// fetchSources are the ordered CAR sources for one fetch: a LAN peer
// (preferred — free, offline-capable) laddered over the public HTTP
// object. Either may be nil. It carries the validation-aware fallback so
// a lying peer is caught and skipped rather than corrupting the fetch:
//
//   - peer read succeeds AND validates → use it (zero HTTP egress);
//   - peer serves INVALID bytes (validate fails) → ban the peer and fall
//     back to HTTP (a bad peer must never make a durable file unreadable);
//   - peer read errors (timeout / transport) → demote for the rest of
//     this fetch when HTTP exists (no ban — could be transient/slow);
//     with no HTTP fallback, surface the error and keep the peer for the
//     next block (it is the only source).
//
// One fetchSources per fetch; not shared across goroutines.
type fetchSources struct {
	peer    CarSource
	http    CarSource // nil for a non-durable file with no public object
	banPeer func()    // bans the peer for future selection; nil-safe
	peerOff bool      // peer demoted/banned for the rest of this fetch
}

func (fs *fetchSources) available() bool { return fs.peer != nil || fs.http != nil }

// readRange fetches exactly [off, off+length), preferring the peer.
// validate (may be nil) rejects bytes the peer served that fail an
// integrity check (e.g. the wanted block's cid) — triggering the ban +
// HTTP fallback. HTTP bytes are validated too (a broken CDN is an error,
// not a silent corruption).
func (fs *fetchSources) readRange(ctx context.Context, off, length int64, validate func([]byte) error) ([]byte, error) {
	if fs.peer != nil && !fs.peerOff {
		pctx, cancel := context.WithTimeout(ctx, fs.peerDeadline())
		data, err := fs.peer.ReadRange(pctx, off, length)
		cancel()
		switch {
		case err == nil && (validate == nil || validate(data) == nil):
			return data, nil
		case err == nil:
			// Peer served invalid content: ban it and never trust it again.
			if fs.banPeer != nil {
				fs.banPeer()
			}
			fs.peerOff = true
		case fs.http != nil:
			// Transient peer error but we have a fallback: demote for the
			// rest of this fetch (no ban — it may just be slow).
			fs.peerOff = true
		default:
			// Transient error and the peer is our only source: fail this
			// block, keep the peer for the next (matches per-block retry).
			return nil, err
		}
	}
	if fs.http == nil {
		return nil, ErrNotAvailable
	}
	data, err := fs.http.ReadRange(ctx, off, length)
	if err != nil {
		return nil, err
	}
	if validate != nil {
		if verr := validate(data); verr != nil {
			return nil, verr
		}
	}
	return data, nil
}

// readProbe fetches the head range + object size, with the same
// peer→HTTP validation ladder as readRange.
func (fs *fetchSources) readProbe(ctx context.Context, validate func([]byte) error) (head []byte, total int64, err error) {
	if fs.peer != nil && !fs.peerOff {
		pctx, cancel := context.WithTimeout(ctx, fs.peerDeadline())
		head, total, err = fs.peer.ReadProbe(pctx)
		cancel()
		switch {
		case err == nil && (validate == nil || validate(head) == nil):
			return head, total, nil
		case err == nil:
			if fs.banPeer != nil {
				fs.banPeer()
			}
			fs.peerOff = true
		case fs.http != nil:
			fs.peerOff = true
		default:
			return nil, 0, err
		}
	}
	if fs.http == nil {
		return nil, 0, ErrNotAvailable
	}
	head, total, err = fs.http.ReadProbe(ctx)
	if err != nil {
		return nil, 0, err
	}
	if validate != nil {
		if verr := validate(head); verr != nil {
			return nil, 0, verr
		}
	}
	return head, total, nil
}

// remoteFetcher builds the miss-handler for Handle.NodeGetter: resolve
// the cid to its section, coalesce forward over contiguous missing
// sections (up to maxFetchSpan), one Range read, then verify-persist
// every fetched block. Only the requested block is returned — the
// NodeGetter persists it via WriteBlock; the coalesced extras are
// persisted here so they're local by the time the reader reaches them.
//
// The wanted block (the first section of the range) is verified inside
// the readRange call, so a peer that serves wrong bytes is banned and
// the range is refetched over HTTP — a bad peer can neither corrupt the
// store nor make a durable file unreadable.
func remoteFetcher(spaceId string, h *store.Handle, fs *fetchSources) func(ctx context.Context, c cid.Cid) ([]byte, error) {
	return func(ctx context.Context, c cid.Cid) ([]byte, error) {
		if fs == nil || !fs.available() {
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
		// validate the wanted block (first frame of the range) against
		// its cid — this is what makes a lying peer fall through to HTTP.
		validate := func(data []byte) error {
			if int64(len(data)) < first.Size {
				return fmt.Errorf("filefetch: short range for %s", c)
			}
			bc, _, err := carfile.ParseFrame(data[:first.Size])
			if err != nil {
				return fmt.Errorf("filefetch: wanted frame: %w", err)
			}
			if !bytes.Equal(bc.Hash(), c.Hash()) {
				return fmt.Errorf("filefetch: range holds %s, wanted %s", bc, c)
			}
			return nil
		}
		data, err := fs.readRange(ctx, first.Offset, end-first.Offset, validate)
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
				want = bdata // already cid-verified by validate
				continue     // the NodeGetter persists the wanted block
			}
			_ = h.WriteBlock(ctx, bc, bdata) // verifies; failure = not persisted
		}
		return want, nil
	}
}
