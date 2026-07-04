// Package fetch is the client download path of the files subsystem
// (SYN-28): resolve a rootCid through the block-source ladder — local
// store → peer (SYN-24 seam) → public CARv2 GET with HTTP Range — and
// expose verified plaintext as a seekable reader.
//
// Downloads are public-read first (SYN-32): for a durable file the
// object URL is {publicReadBaseUrl}/blob/{spaceId}/{rootCid}, built
// offline from the cached base; there is no per-file broker
// round-trip. Every fetched block is cid-verified and persisted into
// the local sparse CARv2 before use, so streaming, seeking and
// interrupted downloads all accrete toward a complete local copy.
package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/ipfs/go-cid"

	"github.com/anyproto/any-sync-sdk/internal/files/carfile"
	"github.com/anyproto/any-sync-sdk/internal/files/crypt"
	"github.com/anyproto/any-sync-sdk/internal/files/store"
	"github.com/anyproto/any-sync-sdk/space"
)

// ErrNotAvailable — the content is not local and cannot be fetched:
// the file is not durable (P2P arrives with SYN-24) or the network has
// no public read base. Wraps the public sentinel so SDK consumers
// match it with errors.Is(err, space.ErrFileNotAvailable).
var ErrNotAvailable = fmt.Errorf("filefetch: %w", space.ErrFileNotAvailable)

// BaseURL resolves the network's public read base ("" = none). The
// SDK wires a persisted-cache provider over the broker Info RPC.
type BaseURL func(ctx context.Context) (string, error)

// Service is the download orchestrator. One per SDK.
type Service struct {
	store *store.Store
	base  BaseURL
	peer  PeerSource // nil until SYN-24
	hc    *http.Client
}

// New builds the Service over the local store and the base-URL source.
func New(st *store.Store, base BaseURL) *Service {
	return &Service{store: st, base: base, hc: http.DefaultClient}
}

// File is an open verified-plaintext view of one stored file.
type File struct {
	r    io.ReadSeeker
	h    *store.Handle
	size int64
}

func (f *File) Read(p []byte) (int, error)                { return f.r.Read(p) }
func (f *File) Seek(off int64, whence int) (int64, error) { return f.r.Seek(off, whence) }
func (f *File) Close() error                              { return f.h.Close() }

// Size is the plaintext byte length.
func (f *File) Size() int64 { return f.size }

// Open returns a seekable plaintext reader over (spaceId, root),
// decrypting with the file key. Not-local content is seeded from the
// public object (header + index → sparse CAR) and blocks stream in on
// demand; durable=false with no local bytes is ErrNotAvailable. ref is
// recorded on the local row (refcount for GC).
func (s *Service) Open(ctx context.Context, spaceId string, root cid.Cid, key []byte, durable bool, ref string) (*File, error) {
	h, err := s.store.Open(ctx, spaceId, root)
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrNoBytes) {
		h, err = s.seed(ctx, spaceId, root, durable, ref)
	}
	if err != nil {
		return nil, err
	}

	var fetchFn func(ctx context.Context, c cid.Cid) ([]byte, error)
	if !h.Complete() {
		rc, rcErr := s.remote(ctx, spaceId, root, durable)
		if rcErr != nil && s.peer == nil {
			// Offline-first: the locally-present ranges of a partial
			// file stay readable with no network. Only a read that
			// actually hits a hole surfaces the fetch error.
			holeErr := rcErr
			fetchFn = func(context.Context, cid.Cid) ([]byte, error) { return nil, holeErr }
		} else {
			fetchFn = remoteFetcher(spaceId, h, s.peer, rc)
		}
	}

	dr, err := newDagReader(ctx, h.NodeGetter(fetchFn), root)
	if err != nil {
		_ = h.Close()
		return nil, err
	}
	ct, err := crypt.NewReader(key, dr)
	if err != nil {
		_ = h.Close()
		return nil, err
	}
	return &File{r: ct, h: h, size: dr.Size()}, nil
}

// Fetch pulls every missing block of (spaceId, root) into the local
// store (the full-download path behind Pin and tests). No-op when
// already complete.
func (s *Service) Fetch(ctx context.Context, spaceId string, root cid.Cid, durable bool, ref string) error {
	h, err := s.store.Open(ctx, spaceId, root)
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrNoBytes) {
		h, err = s.seed(ctx, spaceId, root, durable, ref)
	}
	if err != nil {
		return err
	}
	defer h.Close()
	if h.Complete() {
		return nil
	}
	rc, err := s.remote(ctx, spaceId, root, durable)
	if err != nil && s.peer == nil {
		return err
	}
	fetchFn := remoteFetcher(spaceId, h, s.peer, rc)
	for _, c := range h.MissingBlocks() {
		if h.HasBlock(c) {
			continue // a coalesced read already landed it
		}
		data, err := fetchFn(ctx, c)
		if err != nil {
			return err
		}
		if err = h.WriteBlock(ctx, c, data); err != nil {
			return err
		}
	}
	if !h.Complete() {
		return fmt.Errorf("filefetch: %s still incomplete after full fetch", root)
	}
	return nil
}

// seed creates the local sparse CARv2 skeleton from the remote object:
// one head Range (pragma + headers, reveals the object size), one tail
// Range (the embedded index), then CreateSparse. The skeleton's root
// must match the requested root — a wrong object at the URL is
// rejected before anything is recorded.
func (s *Service) seed(ctx context.Context, spaceId string, root cid.Cid, durable bool, ref string) (*store.Handle, error) {
	rc, err := s.remote(ctx, spaceId, root, durable)
	if err != nil {
		return nil, err
	}
	head, total, err := rc.readProbe(ctx)
	if err != nil {
		return nil, err
	}
	// Reject a wrong object BEFORE anything is written: if the served
	// object's root matched a different local file, CreateSparse would
	// merge into (and any cleanup would then destroy) that unrelated
	// file's state.
	probeRoot, err := carfile.PeekRoot(head)
	if err != nil {
		return nil, fmt.Errorf("filefetch: remote head: %w", err)
	}
	if !probeRoot.Equals(root) {
		return nil, fmt.Errorf("filefetch: object at public url has root %s, wanted %s", probeRoot, root)
	}
	hdr, err := carfile.ParseHeader(head)
	if err != nil {
		return nil, fmt.Errorf("filefetch: remote head: %w", err)
	}
	idxOff := int64(hdr.IndexOffset)
	var idx []byte
	if int64(len(head)) >= total {
		// The probe swallowed the whole object.
		if idxOff > int64(len(head)) {
			return nil, fmt.Errorf("filefetch: object claims %d total bytes but its index starts at %d", total, idxOff)
		}
		head = head[:total]
		idx = head[idxOff:]
		head = head[:idxOff]
	} else {
		if idx, err = rc.readRange(ctx, idxOff, total-idxOff); err != nil {
			return nil, err
		}
	}
	if _, err = s.store.CreateSparse(ctx, spaceId, head, idx, ref); err != nil {
		return nil, err
	}
	return s.store.Open(ctx, spaceId, root)
}

// remote builds the public-object range reader; ErrNotAvailable when
// the file is not durable or the network has no public read base.
func (s *Service) remote(ctx context.Context, spaceId string, root cid.Cid, durable bool) (*remoteCar, error) {
	if !durable {
		return nil, fmt.Errorf("%w: not durable and not local (P2P fetch lands with SYN-24)", ErrNotAvailable)
	}
	base, err := s.base(ctx)
	if err != nil {
		return nil, fmt.Errorf("filefetch: resolve public read base: %w", err)
	}
	if base == "" {
		return nil, fmt.Errorf("%w: network advertises no public read base", ErrNotAvailable)
	}
	return &remoteCar{hc: s.hc, url: blobURL(base, spaceId, root)}, nil
}

// blobURL is the public object address: {base}/blob/{spaceId}/{rootCid}.
func blobURL(base, spaceId string, root cid.Cid) string {
	return strings.TrimRight(base, "/") + "/blob/" + spaceId + "/" + root.String()
}
