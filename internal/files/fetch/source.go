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

// PeerSource selects a peer that holds a file in full — on the local
// network or, more slowly, across the internet. It returns a
// CarSource that reads the file's CAR object from that peer, plus a ban
// hook the fetcher calls when the peer serves INVALID bytes (so a bad
// peer is not re-selected). ok=false — no peer holds the file / p2p
// disabled — means the fetch uses the public GET only. Selection is
// bounded as a whole, not only per candidate: a relayed FileCheck is
// orders of magnitude slower than a LAN one, so an unbounded sweep
// would spike latency exactly when no peer has the file.
type PeerSource interface {
	SourceFor(ctx context.Context, spaceId string, root cid.Cid) (src CarSource, ban func(), ok bool)
}

// peerPreferDeadline bounds each read from a LAN peer. On a healthy
// LAN a ~1 MiB range returns in a few ms; this generous cap means a
// stalling peer costs at most one deadline before we demote it to HTTP
// for this fetch.
const peerPreferDeadline = 800 * time.Millisecond

// MaxPeerReadLen is the largest single read a peer serves; the FileP2P
// server rejects anything longer outright. The fetcher coalesces at
// most maxFetchSpan (4 MiB) per read, so this is generous headroom, and
// it bounds a peer read's budget no matter what object size the peer
// itself reported.
const MaxPeerReadLen = 8 << 20

// PeerReadBudgeter is the optional interface a CarSource implements to
// state a read budget other than the LAN default. A read is one round
// trip plus a transfer, and both differ by orders of magnitude between
// a LAN and a relayed internet path, so a single fixed figure either
// expires mid-range on the largest reads or wastes seconds on a stalled
// 4 KiB probe. Where there is no HTTP fallback an expiry is the whole
// fetch, so a file that a peer holds in full never arrives.
type PeerReadBudgeter interface {
	// PeerReadBudget returns the round-trip allowance for one read and
	// the transfer rate (bytes/s) the peer is expected to sustain; the
	// read's budget is roundTrip + length/rate. A zero roundTrip takes
	// the LAN default; a zero rate charges nothing per byte.
	PeerReadBudget() (roundTrip time.Duration, bytesPerSecond int64)
}

// peerDeadline is the budget for reading length bytes from the current
// peer. length is clamped to MaxPeerReadLen: seed sizes its index read
// from the total the peer reported, and a stalling peer that reports an
// absurd total must not buy itself an unbounded budget.
func (fs *fetchSources) peerDeadline(length int64) time.Duration {
	roundTrip, rate := peerPreferDeadline, int64(0)
	if b, ok := fs.peer.(PeerReadBudgeter); ok {
		rt, bps := b.PeerReadBudget()
		if rt > 0 {
			roundTrip = rt
		}
		rate = bps
	}
	if rate > 0 {
		length = min(length, MaxPeerReadLen)
		roundTrip += time.Duration(length) * time.Second / time.Duration(rate)
	}
	return roundTrip
}

// peerReadFailed applies the ladder's verdict on a failed peer read.
// With an HTTP fallback the peer is demoted for the rest of this fetch;
// if the read exhausted the budget this peer itself declared, it is
// also banned, so a stalled relay costs one budget per banTTL rather
// than one per Open (a read error only demotes; selection already
// admitted the peer, and the next Open would admit it again). A caller
// giving up (ctx done) says nothing about the peer. With no fallback
// the error surfaces and the peer stays: it is the only source, and
// the next block retries it.
func (fs *fetchSources) peerReadFailed(ctx context.Context, expired bool, err error) error {
	if fs.http == nil {
		return err
	}
	fs.peerOff = true
	if expired && ctx.Err() == nil && fs.banPeer != nil {
		fs.banPeer()
	}
	return nil
}

// fetchSources are the ordered CAR sources for one fetch: a LAN peer
// (preferred — free, offline-capable) laddered over the public HTTP
// object. Either may be nil. It carries the validation-aware fallback so
// a lying peer is caught and skipped rather than corrupting the fetch:
//
//   - peer read succeeds AND validates → use it (zero HTTP egress);
//   - peer serves INVALID bytes (validate fails) → ban the peer and fall
//     back to HTTP (a bad peer must never make a durable file unreadable);
//   - peer read errors → demote for the rest of this fetch when HTTP
//     exists; a read that exhausted the peer's own budget also bans it
//     (see peerReadFailed). With no HTTP fallback, surface the error
//     and keep the peer for the next block (it is the only source).
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
		pctx, cancel := context.WithTimeout(ctx, fs.peerDeadline(length))
		data, err := fs.peer.ReadRange(pctx, off, length)
		expired := pctx.Err() != nil
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
		default:
			if err = fs.peerReadFailed(ctx, expired, err); err != nil {
				return nil, err
			}
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
		pctx, cancel := context.WithTimeout(ctx, fs.peerDeadline(headProbeLen))
		head, total, err = fs.peer.ReadProbe(pctx)
		expired := pctx.Err() != nil
		cancel()
		switch {
		case err == nil && (validate == nil || validate(head) == nil):
			return head, total, nil
		case err == nil:
			if fs.banPeer != nil {
				fs.banPeer()
			}
			fs.peerOff = true
		default:
			if err = fs.peerReadFailed(ctx, expired, err); err != nil {
				return nil, 0, err
			}
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
