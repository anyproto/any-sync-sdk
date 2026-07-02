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
	if err = f.validateVariant(ctx, objectId, opts); err != nil {
		return space.FileInfo{}, err
	}
	pa := f.s.PayloadsInternal()
	res, err := files.Add(ctx, payloadsRegistrar{p: pa}, f.s.id, objectId, r, upload.AddOpts{
		Name:      opts.Name,
		Mime:      opts.Mime,
		Variant:   string(opts.Variant),
		VariantOf: opts.VariantOf,
	})
	if err != nil {
		return space.FileInfo{}, err
	}
	// Seed the local fileId → payloads-object index so Get/Open resolve
	// with one lookup instead of a scan.
	if kv := f.s.parent.filesStore(); kv != nil {
		if payloadsObjId, derr := pa.ObjectId(ctx, objectId); derr == nil {
			_ = kv.SetKV(ctx, fileIndexKey(f.s.id, res.FileId), payloadsObjId)
		}
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
		FileId:    res.FileId,
		ObjectId:  objectId,
		RootCid:   res.RootCid,
		Size:      res.Size,
		Inline:    res.Inline,
		Durable:   res.Durable,
		Name:      opts.Name,
		Mime:      opts.Mime,
		Cached:    true, // just written (inline rides the row itself)
		Variant:   opts.Variant,
		VariantOf: opts.VariantOf,
	}, nil
}

// validateVariant enforces the variant attach contract: both fields
// together, and the original must be a file of the SAME object (that
// keeps sibling resolution a one-collection scan).
func (f *filesAPI) validateVariant(ctx context.Context, objectId string, opts space.AttachOpts) error {
	if opts.Variant == space.VariantOriginal && opts.VariantOf == "" {
		return nil
	}
	if opts.Variant == space.VariantOriginal || opts.VariantOf == "" {
		return fmt.Errorf("files: Variant and VariantOf must be set together: %w", space.ErrFileVariantInvalid)
	}
	orig, err := f.s.PayloadsInternal().FindRow(ctx, opts.VariantOf)
	if err != nil {
		return fmt.Errorf("files: variant original %s: %w", opts.VariantOf, err)
	}
	if orig.ObjectId != objectId {
		return fmt.Errorf("files: variant must attach to the original's object %s, not %s: %w", orig.ObjectId, objectId, space.ErrFileVariantInvalid)
	}
	return nil
}

// Open returns a seekable plaintext reader over the file (or one of
// its variants). See space.Files.
func (f *filesAPI) Open(ctx context.Context, fileId string, variant space.Variant) (space.FileReader, error) {
	fetchSvc := f.s.parent.fetch
	if fetchSvc == nil {
		return nil, errors.New("files: not configured")
	}
	row, err := f.s.PayloadsInternal().FindRow(ctx, fileId)
	if err != nil {
		return nil, err
	}
	if variant != space.VariantOriginal {
		if row, err = f.resolveVariant(ctx, row, variant); err != nil {
			return nil, err
		}
	}
	return f.openRow(ctx, row)
}

// openRow builds the reader pipeline for one resolved row.
func (f *filesAPI) openRow(ctx context.Context, row payloads.Row) (space.FileReader, error) {
	if row.Sealed {
		if row.UnsealErr != nil {
			return nil, fmt.Errorf("files: row %s cannot be unsealed (poisoned or malformed): %w", row.Id, row.UnsealErr)
		}
		return nil, errors.New("files: no space key (cannot decrypt)")
	}
	if row.Inline() {
		return newInlineReader(row.Enc.Inline), nil
	}
	if len(row.Enc.Key) == 0 {
		return nil, fmt.Errorf("files: row %s has no file key", row.Id)
	}
	root, err := cid.Decode(row.RootCid)
	if err != nil {
		return nil, fmt.Errorf("files: row %s rootCid: %w", row.Id, err)
	}
	return f.s.parent.fetch.Open(ctx, f.s.id, root, row.Enc.Key, row.NetworkSign != "", row.Id)
}

// resolveVariant finds the sibling row tagged {variantOf: orig,
// variant} among the original object's files (variants always bind to
// the same object — enforced at Attach).
func (f *filesAPI) resolveVariant(ctx context.Context, orig payloads.Row, variant space.Variant) (payloads.Row, error) {
	rows, err := f.s.PayloadsInternal().ListRows(ctx, orig.ObjectId)
	if err != nil {
		return payloads.Row{}, err
	}
	for _, r := range rows {
		if !r.Sealed && r.Enc.VariantOf == orig.Id && r.Enc.Variant == string(variant) {
			return r, nil
		}
	}
	return payloads.Row{}, fmt.Errorf("files: no %q variant of %s: %w", variant, orig.Id, space.ErrNotFound)
}

// Get returns the file's info. See space.Files.
func (f *filesAPI) Get(ctx context.Context, fileId string) (space.FileInfo, error) {
	row, err := f.s.PayloadsInternal().FindRow(ctx, fileId)
	if err != nil {
		return space.FileInfo{}, err
	}
	return f.rowToInfo(ctx, row), nil
}

// rowToInfo maps a typed row to the public FileInfo, resolving local
// availability from the store.
func (f *filesAPI) rowToInfo(ctx context.Context, row payloads.Row) space.FileInfo {
	info := space.FileInfo{
		FileId:    row.Id,
		ObjectId:  row.ObjectId,
		RootCid:   row.RootCid,
		Size:      row.Size,
		Inline:    row.Inline(),
		Durable:   row.Inline() || row.NetworkSign != "",
		Name:      row.Enc.Name,
		Mime:      row.Enc.Mime,
		Cached:    row.Inline(),
		Variant:   space.Variant(row.Enc.Variant),
		VariantOf: row.Enc.VariantOf,
	}
	if !row.Inline() {
		if root, err := cid.Decode(row.RootCid); err == nil {
			if st, err := f.s.parent.filesStore().Info(ctx, f.s.id, root); err == nil {
				info.Cached = st.State == filestore.StateComplete
			}
		}
	}
	return info
}

// List returns the space's files. See space.Files.
func (f *filesAPI) List(ctx context.Context, opts space.FileListOpts) ([]space.FileInfo, error) {
	pa := f.s.PayloadsInternal()
	var out []space.FileInfo
	appendRows := func(rows []payloads.Row) bool {
		for _, row := range rows {
			out = append(out, f.rowToInfo(ctx, row))
			if opts.Limit > 0 && len(out) >= opts.Limit {
				return false
			}
		}
		return true
	}
	if opts.ObjectId != "" {
		rows, err := pa.ListRows(ctx, opts.ObjectId)
		if err != nil {
			return nil, err
		}
		appendRows(rows)
		return out, nil
	}
	objIds, err := f.s.store.TreeIdsByChangeType(ctx, payloads.ChangeType)
	if err != nil {
		return nil, err
	}
	for _, objId := range objIds {
		rows, err := pa.listRowsIn(ctx, objId)
		if err != nil {
			return nil, err
		}
		if !appendRows(rows) {
			break
		}
	}
	return out, nil
}

// Query returns the generic query surface over one object's payload
// rows. See space.Files.
func (f *filesAPI) Query(objectId string) (space.Query, error) {
	ctx := context.Background() // local reads only (derive + existence)
	objId, ok, err := f.s.PayloadsInternal().existingObjectId(ctx, objectId)
	if err != nil {
		return nil, err
	}
	if !ok {
		// The payloads object materializes with the first Attach; there
		// is no collection to query (or subscribe to) before that.
		return nil, fmt.Errorf("files: %s has no files yet: %w", objectId, space.ErrNotFound)
	}
	return f.s.Query(objId, payloads.Dataset), nil
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
		return fmt.Errorf("files: offload %s — local bytes are the only copy: %w", fileId, space.ErrFileNotBackedUp)
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
