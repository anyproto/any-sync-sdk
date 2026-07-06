// Package filep2p is the SDK's peer-to-peer file transfer: a read-only
// server that streams a device's stored CAR objects to LAN peers, and a
// PeerSource that fetches file objects from LAN peers before the public
// HTTP path. Blocks are ciphertext and every fetched block is verified
// against its cid downstream, so an untrusted peer can neither read the
// content nor corrupt the fetcher's store.
package filep2p

import (
	"context"
	"errors"
	"io"
	"sync/atomic"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonfile/fileproto/filep2p"
	"github.com/anyproto/any-sync/commonfile/fileproto/filep2p/filep2perr"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/server"
	"github.com/ipfs/go-cid"

	"github.com/anyproto/any-sync-sdk/internal/files/store"
)

// CName is the app-component name of the FileP2P server.
const CName = "sdk.p2p.fileserver"

// maxObjectReadLen caps one ObjectRead response so a peer can't force a
// huge allocation. The fetcher coalesces at most maxFetchSpan (4 MiB)
// per read, so this is generous headroom.
const maxObjectReadLen = 8 << 20

// Authorizer reports whether the peer may read the given space's files.
// Wired to the p2p peer store's advertisement check (App.LocalPeerHasSpace)
// — a peer may fetch a space's blocks only if it advertised sharing that
// space. Blocks are ciphertext, so this is defense in depth, not the
// confidentiality boundary.
type Authorizer func(peerId, spaceId string) bool

// Server answers FileP2P from the local file store. It is an app
// component so it registers on the DRPC mux during Init — BEFORE the
// server's accept loop starts — avoiding a data race with concurrent
// serving that a post-Start registration would cause. The file store is
// injected later via SetStore (it is built after the app starts); until
// then the handlers report "not held". It never mutates anything.
type Server struct {
	filep2p.DRPCFileP2PUnimplementedServer
	store      atomic.Pointer[store.Store]
	authorized Authorizer
}

func NewServer(authorized Authorizer) *Server {
	return &Server{authorized: authorized}
}

func (s *Server) Init(a *app.App) error {
	return filep2p.DRPCRegisterFileP2P(a.MustComponent(server.CName).(server.DRPCServer), s)
}

func (s *Server) Name() string { return CName }

// SetStore injects the file store once it is built (post app-start).
// Safe to call concurrently with serving; requests before it is set read
// as "file not held".
func (s *Server) SetStore(st *store.Store) { s.store.Store(st) }

func (s *Server) authz(ctx context.Context, spaceId string) error {
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return filep2perr.ErrForbidden
	}
	if spaceId == "" {
		return filep2perr.ErrInvalidRequest
	}
	if s.authorized != nil && !s.authorized(peerId, spaceId) {
		return filep2perr.ErrForbidden
	}
	return nil
}

// FileCheck reports how much of each requested file this device holds.
func (s *Server) FileCheck(ctx context.Context, req *filep2p.FileCheckRequest) (*filep2p.FileCheckResponse, error) {
	if err := s.authz(ctx, req.SpaceId); err != nil {
		return nil, err
	}
	st := s.store.Load()
	out := make([]*filep2p.FileAvailability, 0, len(req.RootCids))
	for _, raw := range req.RootCids {
		have := filep2p.Availability_None
		if root, err := cid.Cast(raw); err == nil {
			have = availability(ctx, st, req.SpaceId, root)
		}
		out = append(out, &filep2p.FileAvailability{RootCid: raw, Have: have})
	}
	return &filep2p.FileCheckResponse{Files: out}, nil
}

func availability(ctx context.Context, st *store.Store, spaceId string, root cid.Cid) filep2p.Availability {
	if st == nil {
		return filep2p.Availability_None
	}
	info, err := st.Info(ctx, spaceId, root)
	if err != nil || info.Sections == 0 {
		return filep2p.Availability_None
	}
	switch {
	case info.Present >= info.Sections:
		return filep2p.Availability_Full
	case info.Present > 0:
		return filep2p.Availability_Partial
	default:
		return filep2p.Availability_None
	}
}

// ObjectRead returns a byte range of a file's CAR object. Served only for
// files held in full (a partial CAR has holes that would stream as zeros).
func (s *Server) ObjectRead(ctx context.Context, req *filep2p.ObjectReadRequest) (*filep2p.ObjectReadResponse, error) {
	if err := s.authz(ctx, req.SpaceId); err != nil {
		return nil, err
	}
	if req.Length == 0 || req.Length > maxObjectReadLen {
		return nil, filep2perr.ErrInvalidRequest
	}
	root, err := cid.Cast(req.RootCid)
	if err != nil {
		return nil, filep2perr.ErrInvalidRequest
	}
	st := s.store.Load()
	if st == nil {
		return nil, filep2perr.ErrFileNotFound
	}
	h, err := st.Open(ctx, req.SpaceId, root)
	if err != nil {
		return nil, filep2perr.ErrFileNotFound
	}
	defer h.Close()
	if !h.Complete() {
		// We only serve whole objects; a partial holder isn't a source.
		return nil, filep2perr.ErrFileNotFound
	}
	total, err := h.Size()
	if err != nil {
		return nil, filep2perr.ErrUnexpected
	}
	buf := make([]byte, req.Length)
	n, err := h.ReadAt(buf, int64(req.Offset))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, filep2perr.ErrUnexpected
	}
	return &filep2p.ObjectReadResponse{Data: buf[:n], TotalSize: uint64(total)}, nil
}
