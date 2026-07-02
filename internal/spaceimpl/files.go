package spaceimpl

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"

	filestore "github.com/anyproto/any-sync-sdk/internal/files/store"
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
		Name:     opts.Name,
		Mime:     opts.Mime,
		Cached:   true, // just written (inline rides the row itself)
	}, nil
}

// Open returns a seekable plaintext reader over the file. See
// space.Files.
func (f *filesAPI) Open(ctx context.Context, fileId string, variant space.Variant) (space.FileReader, error) {
	if variant != space.VariantOriginal {
		return nil, fmt.Errorf("files: variant %q: %w (variants land with SYN-30)", variant, space.ErrNotFound)
	}
	fetchSvc := f.s.parent.fetch
	if fetchSvc == nil {
		return nil, errors.New("files: not configured")
	}
	row, err := f.s.PayloadsInternal().FindRow(ctx, fileId)
	if err != nil {
		return nil, err
	}
	if row.Sealed {
		return nil, errors.New("files: no space key (cannot decrypt)")
	}
	if row.Inline() {
		return newInlineReader(row.Enc.Inline), nil
	}
	if len(row.Enc.Key) == 0 {
		return nil, fmt.Errorf("files: row %s has no file key", fileId)
	}
	root, err := cid.Decode(row.RootCid)
	if err != nil {
		return nil, fmt.Errorf("files: row %s rootCid: %w", fileId, err)
	}
	return fetchSvc.Open(ctx, f.s.id, root, row.Enc.Key, row.NetworkSign != "", fileId)
}

// Get returns the file's info. See space.Files.
func (f *filesAPI) Get(ctx context.Context, fileId string) (space.FileInfo, error) {
	row, err := f.s.PayloadsInternal().FindRow(ctx, fileId)
	if err != nil {
		return space.FileInfo{}, err
	}
	info := space.FileInfo{
		FileId:   row.Id,
		ObjectId: row.ObjectId,
		RootCid:  row.RootCid,
		Size:     row.Size,
		Inline:   row.Inline(),
		Durable:  row.Inline() || row.NetworkSign != "",
		Name:     row.Enc.Name,
		Mime:     row.Enc.Mime,
		Cached:   row.Inline(),
	}
	if !row.Inline() {
		if root, err := cid.Decode(row.RootCid); err == nil {
			if st, err := f.s.parent.filesStore().Info(ctx, f.s.id, root); err == nil {
				info.Cached = st.State == filestore.StateComplete
			}
		}
	}
	return info, nil
}

// inlineReader serves an inline-tier file from the unsealed row.
type inlineReader struct {
	*bytes.Reader
}

func newInlineReader(data []byte) *inlineReader {
	return &inlineReader{Reader: bytes.NewReader(data)}
}

func (r *inlineReader) Close() error { return nil }

func (r *inlineReader) Size() int64 { return r.Reader.Size() }

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
