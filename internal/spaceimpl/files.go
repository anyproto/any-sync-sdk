package spaceimpl

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"

	"github.com/anyproto/any-sync-sdk/internal/files/status"
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
	if res.Inline {
		// Full-tier attaches emit through the queue transitions; inline
		// never touches the queue, so emit its (terminal) status here.
		f.s.parent.fileStatusSubs.dispatch(f.s.id, space.FileStatus{
			FileId:   res.FileId,
			ObjectId: objectId,
			State:    space.FileStateDurable,
			Cached:   true,
		})
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
		if row.UnsealErr != nil {
			return nil, fmt.Errorf("files: row %s cannot be unsealed (poisoned or malformed): %w", fileId, row.UnsealErr)
		}
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

// Status derives the file's durability state on read. See space.Files.
func (f *filesAPI) Status(ctx context.Context, fileId string) (space.FileStatus, error) {
	return f.s.fileStatus(ctx, fileId)
}

// SubscribeStatus registers a local-transition listener. See
// space.Files.
func (f *filesAPI) SubscribeStatus(cb func(space.FileStatus)) (unsubscribe func()) {
	return f.s.parent.fileStatusSubs.add(f.s.id, cb)
}

// Stats aggregates this space's durability counts. See space.Files.
func (f *filesAPI) Stats(ctx context.Context) (space.FileStats, error) {
	var stats space.FileStats
	limited := map[string]bool{}
	if q := f.s.parent.fqueue; q != nil {
		jobs, err := q.ListSpace(ctx, f.s.id)
		if err != nil {
			return space.FileStats{}, err
		}
		for _, j := range jobs {
			if j.Kind == status.KindDurable && j.Limited {
				limited[j.FileId] = true
			}
		}
	}
	view := f.s.Payloads()
	objIds, err := view.ListObjects(ctx)
	if err != nil {
		return space.FileStats{}, err
	}
	for _, objId := range objIds {
		rows, err := view.ListRows(ctx, objId)
		if err != nil {
			return space.FileStats{}, err
		}
		for _, row := range rows {
			stats.Total++
			switch {
			case row.RootCid == "" || row.NetworkSign != "":
				stats.Durable++
			case limited[row.FileId]:
				stats.Limited++
			default:
				stats.InFlight++
			}
		}
	}
	return stats, nil
}

// Pin schedules a persistent full background fetch. See space.Files.
func (f *filesAPI) Pin(ctx context.Context, fileId string) error {
	q := f.s.parent.fqueue
	if q == nil {
		return errors.New("files: not configured")
	}
	row, err := f.s.PayloadsInternal().FindRow(ctx, fileId)
	if err != nil {
		return err
	}
	if row.Inline() {
		return nil // rides the row; nothing to fetch
	}
	return q.Enqueue(ctx, status.KindPin, f.s.id, fileId)
}

// Retry makes the file's pending background work due immediately,
// re-enqueueing the backup when the row is unsigned with complete
// local bytes. See space.Files.
func (f *filesAPI) Retry(ctx context.Context, fileId string) error {
	q := f.s.parent.fqueue
	if q == nil {
		return errors.New("files: not configured")
	}
	row, err := f.s.PayloadsInternal().FindRow(ctx, fileId)
	if err != nil {
		return err
	}
	kickedDurable, err := q.KickJob(ctx, status.KindDurable, f.s.id, fileId)
	if err != nil {
		return err
	}
	if _, err = q.KickJob(ctx, status.KindPin, f.s.id, fileId); err != nil {
		return err
	}
	if kickedDurable || row.Inline() || row.NetworkSign != "" {
		return nil
	}
	// Unsigned with no pending job (e.g. the row predates the queue or
	// its job was lost): re-enqueue when we hold the bytes to drive it.
	root, err := cid.Decode(row.RootCid)
	if err != nil {
		return err
	}
	info, err := f.s.parent.filesStore().Info(ctx, f.s.id, root)
	if err != nil || info.State != filestore.StateComplete {
		return nil // nothing local to upload; SYN-24 P2P may change this
	}
	return q.Enqueue(ctx, status.KindDurable, f.s.id, fileId)
}

// Offload drops the file's local bytes (refetchable-only). See
// space.Files.
func (f *filesAPI) Offload(ctx context.Context, fileId string) error {
	row, err := f.s.PayloadsInternal().FindRow(ctx, fileId)
	if err != nil {
		return err
	}
	if row.Inline() {
		return nil // rides the row; nothing local to drop
	}
	if row.NetworkSign == "" {
		return fmt.Errorf("files: offload %s: not backed up — local bytes are the only copy", fileId)
	}
	root, err := cid.Decode(row.RootCid)
	if err != nil {
		return err
	}
	if err = f.s.parent.filesStore().Offload(ctx, f.s.id, root); err != nil {
		if errors.Is(err, filestore.ErrNotFound) {
			return nil // nothing local (never fetched here)
		}
		return err
	}
	f.s.parent.fileStatusSubs.dispatch(f.s.id, space.FileStatus{
		FileId:   row.Id,
		ObjectId: row.ObjectId,
		State:    space.FileStateDurable,
		Cached:   false,
	})
	return nil
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
