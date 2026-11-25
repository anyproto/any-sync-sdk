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

// ErrObjectTooLarge — the remote object is larger than the cap the
// caller set (OpenLimited): refused before its index or blocks are read.
var ErrObjectTooLarge = errors.New("filefetch: object exceeds the caller's size cap")

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

// SetPeer wires the LAN p2p source, consulted before the public GET.
// Call once during SDK boot; nil leaves fetches public-read only.
func (s *Service) SetPeer(p PeerSource) { s.peer = p }

// Object names one stored file for a fetch.
type Object struct {
	SpaceId string
	Root    cid.Cid
	// Durable reports that the network holds a public object for it.
	Durable bool
	// Author is the account that registered the file; its devices are
	// the likeliest holders. Empty when unknown.
	Author string
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

// Open returns a seekable plaintext reader over obj, decrypting with
// the file key. Not-local content is seeded from a peer or the public
// object (header + index → sparse CAR) and blocks stream in on demand;
// with no local bytes and no source it is ErrNotAvailable. ref is
// recorded on the local row (refcount for GC).
func (s *Service) Open(ctx context.Context, obj Object, key []byte, ref string) (*File, error) {
	return s.OpenLimited(ctx, obj, key, ref, 0)
}

// OpenLimited is Open for an object the caller did not register
// itself (a contact's avatar): a remote object whose total size exceeds
// maxObject bytes fails with ErrObjectTooLarge at the probe, before
// its index or any block is fetched. 0 means no cap.
func (s *Service) OpenLimited(ctx context.Context, obj Object, key []byte, ref string, maxObject int64) (*File, error) {
	h, err := s.store.Open(ctx, obj.SpaceId, obj.Root)
	// seed hands back the sources it selected so a not-local file pays
	// for peer selection once, not again for the reader.
	var src *fetchSources
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrNoBytes) {
		h, src, err = s.seed(ctx, obj, ref, maxObject)
	}
	if err != nil {
		return nil, err
	}

	var fetchFn func(ctx context.Context, c cid.Cid) ([]byte, error)
	if !h.Complete() {
		var srcErr error
		if src == nil {
			src, srcErr = s.source(ctx, obj)
		}
		if srcErr != nil {
			// Offline-first: the locally-present ranges of a partial
			// file stay readable with no network. Only a read that
			// actually hits a hole surfaces the fetch error.
			holeErr := srcErr
			fetchFn = func(context.Context, cid.Cid) ([]byte, error) { return nil, holeErr }
		} else {
			fetchFn = remoteFetcher(obj.SpaceId, h, src)
		}
	}

	dr, err := newDagReader(ctx, h.NodeGetter(fetchFn), obj.Root)
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

// Fetch pulls every missing block of obj into the local store (the
// full-download path behind Pin and tests). No-op when already
// complete.
func (s *Service) Fetch(ctx context.Context, obj Object, ref string) error {
	h, err := s.store.Open(ctx, obj.SpaceId, obj.Root)
	var src *fetchSources
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrNoBytes) {
		h, src, err = s.seed(ctx, obj, ref, 0)
	}
	if err != nil {
		return err
	}
	defer h.Close()
	if h.Complete() {
		return nil
	}
	if src == nil {
		if src, err = s.source(ctx, obj); err != nil {
			return err
		}
	}
	fetchFn := remoteFetcher(obj.SpaceId, h, src)
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
		return fmt.Errorf("filefetch: %s still incomplete after full fetch", obj.Root)
	}
	return nil
}

// seed creates the local sparse CARv2 skeleton from the remote object:
// one head Range (pragma + headers, reveals the object size), one tail
// Range (the embedded index), then CreateSparse. The skeleton's root
// must match the requested root — a wrong object at the URL is
// rejected before anything is recorded.
func (s *Service) seed(ctx context.Context, obj Object, ref string, maxObject int64) (*store.Handle, *fetchSources, error) {
	spaceId, root := obj.SpaceId, obj.Root
	src, err := s.source(ctx, obj)
	if err != nil {
		return nil, nil, err
	}
	// Validate the probed head's root against the requested root INSIDE
	// the ladder: a peer (or CDN) serving a wrong/garbage object is
	// rejected and — for the peer — banned + fallen back to HTTP, before
	// anything is written. A wrong object merged via CreateSparse would
	// corrupt an unrelated local file's state.
	head, total, err := src.readProbe(ctx, func(head []byte) error {
		probeRoot, perr := carfile.PeekRoot(head)
		if perr != nil {
			return fmt.Errorf("filefetch: remote head: %w", perr)
		}
		if !probeRoot.Equals(root) {
			return fmt.Errorf("filefetch: object has root %s, wanted %s", probeRoot, root)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if maxObject > 0 && total > maxObject {
		return nil, nil, fmt.Errorf("%w: %d bytes, cap %d", ErrObjectTooLarge, total, maxObject)
	}
	hdr, err := carfile.ParseHeader(head)
	if err != nil {
		return nil, nil, fmt.Errorf("filefetch: remote head: %w", err)
	}
	idxOff := int64(hdr.IndexOffset)
	var idx []byte
	if int64(len(head)) >= total {
		// The probe swallowed the whole object.
		if idxOff > int64(len(head)) {
			return nil, nil, fmt.Errorf("filefetch: object claims %d total bytes but its index starts at %d", total, idxOff)
		}
		head = head[:total]
		idx = head[idxOff:]
		head = head[:idxOff]
	} else {
		if idx, err = src.readRange(ctx, idxOff, total-idxOff, nil); err != nil {
			return nil, nil, err
		}
	}
	if _, err = s.store.CreateSparse(ctx, spaceId, head, idx, ref); err != nil {
		return nil, nil, err
	}
	h, err := s.store.Open(ctx, spaceId, root)
	if err != nil {
		return nil, nil, err
	}
	return h, src, nil
}

// source builds the CAR object reader for a fetch: a peer that holds
// the file (preferred — free, works offline) laddered over the public
// HTTP object (durable files only). Returns ErrNotAvailable only when
// NEITHER is available (not durable / no public base AND no peer holds
// it); the peer selection's own reason is kept. The peer is tried first
// per read with a bounded deadline and demoted to HTTP on the first
// failure (see fetchSources).
func (s *Service) source(ctx context.Context, obj Object) (*fetchSources, error) {
	fs := &fetchSources{}
	// A typed-nil *remoteCar must NOT be stored in the CarSource
	// interface field (it would read as non-nil); assign only when real.
	if httpRC, httpErr := s.httpSource(ctx, obj); httpRC != nil {
		fs.http = httpRC
	} else if s.peer == nil {
		// No peer to try and no HTTP: surface why HTTP is unavailable.
		return nil, httpErr
	}
	var peerErr error
	if s.peer != nil {
		// Sole: with no public object the peer layer is the only way to
		// the bytes, and may dial for them.
		src, ban, err := s.peer.SourceFor(ctx, PeerQuery{
			SpaceId: obj.SpaceId, Root: obj.Root, Author: obj.Author, Sole: fs.http == nil,
		})
		if err == nil {
			fs.peer, fs.banPeer = src, ban
		}
		peerErr = err
	}
	if fs.available() {
		return fs, nil
	}
	switch {
	case peerErr == nil:
		return nil, ErrNotAvailable
	case errors.Is(peerErr, ErrNotAvailable):
		return nil, peerErr
	}
	return nil, fmt.Errorf("%w: %v", ErrNotAvailable, peerErr)
}

// httpSource builds the public-object range reader; ErrNotAvailable when
// the file is not durable or the network has no public read base.
func (s *Service) httpSource(ctx context.Context, obj Object) (*remoteCar, error) {
	if !obj.Durable {
		return nil, fmt.Errorf("%w: not durable", ErrNotAvailable)
	}
	base, err := s.base(ctx)
	if err != nil {
		return nil, fmt.Errorf("filefetch: resolve public read base: %w", err)
	}
	if base == "" {
		return nil, fmt.Errorf("%w: network advertises no public read base", ErrNotAvailable)
	}
	return &remoteCar{hc: s.hc, url: blobURL(base, obj.SpaceId, obj.Root)}, nil
}

// blobURL is the public object address: {base}/blob/{spaceId}/{rootCid}.
func blobURL(base, spaceId string, root cid.Cid) string {
	return strings.TrimRight(base, "/") + "/blob/" + spaceId + "/" + root.String()
}
