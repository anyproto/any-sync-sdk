package spaceimpl

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/anyproto/any-sync-sdk/internal/files/upload"
	"github.com/anyproto/any-sync-sdk/internal/payloads"
	"github.com/anyproto/any-sync-sdk/space"
)

// filesAPI implements space.Files over the SDK-level upload service
// and this space's payloads surface.
type filesAPI struct {
	s *spaceImpl
}

func (s *spaceImpl) Files() space.Files {
	return &filesAPI{s: s}
}

// Attach ingests r as a file bound to objectId. See space.Files.
func (f *filesAPI) Attach(ctx context.Context, objectId string, r io.Reader, opts space.AttachOpts) (space.FileInfo, error) {
	files := f.s.parent.files
	if files == nil {
		return space.FileInfo{}, errors.New("files: not configured")
	}
	if objectId == "" {
		return space.FileInfo{}, errors.New("files: objectId required")
	}
	ok, err := f.s.store.HasTree(ctx, objectId)
	if err != nil {
		return space.FileInfo{}, err
	}
	if !ok {
		return space.FileInfo{}, fmt.Errorf("files: attach to %s: %w", objectId, space.ErrNotFound)
	}
	res, err := files.Add(ctx, payloadsRegistrar{p: f.s.PayloadsInternal()}, f.s.id, objectId, r, upload.AddOpts{
		Name: opts.Name,
		Mime: opts.Mime,
	})
	if err != nil {
		return space.FileInfo{}, err
	}
	return space.FileInfo{
		FileId:   res.FileId,
		ObjectId: objectId,
		RootCid:  res.RootCid,
		Size:     res.Size,
		Inline:   res.Inline,
		Durable:  res.Durable,
	}, nil
}

// payloadsRegistrar adapts PayloadsAPI to the upload.Registrar seam.
type payloadsRegistrar struct {
	p *PayloadsAPI
}

func (r payloadsRegistrar) RegisterFile(ctx context.Context, ownerId string, opts upload.RegisterOpts) (string, error) {
	fileId, _, err := r.p.RegisterFile(ctx, ownerId, RegisterFileOpts{
		RootCid:     opts.RootCid,
		Size:        opts.Size,
		NetworkSign: opts.NetworkSign,
		Enc:         opts.Enc,
	})
	return fileId, err
}

func (r payloadsRegistrar) SetNetworkSign(ctx context.Context, ownerId, fileId, sign string) error {
	return r.p.SetNetworkSign(ctx, ownerId, fileId, sign)
}

func (r payloadsRegistrar) Row(ctx context.Context, ownerId, fileId string) (payloads.Row, error) {
	return r.p.GetRow(ctx, ownerId, fileId)
}
