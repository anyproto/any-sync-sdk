package spaceimpl

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ipfs/go-cid"

	"github.com/anyproto/any-sync-sdk/internal/files/status"
	filestore "github.com/anyproto/any-sync-sdk/internal/files/store"
	"github.com/anyproto/any-sync-sdk/internal/files/upload"
	"github.com/anyproto/any-sync-sdk/internal/payloads"
	"github.com/anyproto/any-sync-sdk/space"
)

// RunFileJob executes one queue job (the status.Runner wired by
// sdk.Open). A job whose space or row disappeared (deleted) succeeds
// vacuously so the queue drops it — it must never resurrect a deleted
// space's storage by loading it.
func (s *Service) RunFileJob(ctx context.Context, job status.Job) error {
	if rec, ok := s.tsp.Get(ctx, job.SpaceId); !ok || rec.IsDeleted() {
		return nil
	}
	sp, err := s.Get(ctx, job.SpaceId)
	if err != nil {
		return err
	}
	impl := sp.(*spaceImpl)
	pa := impl.PayloadsInternal()
	row, err := pa.FindRow(ctx, job.FileId)
	if errors.Is(err, space.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	switch job.Kind {
	case status.KindDurable:
		if row.NetworkSign != "" || row.Inline() {
			return nil
		}
		if row.ObjectId == "" {
			return fmt.Errorf("files: row %s has no objectId; cannot resolve its payloads object", job.FileId)
		}
		err = s.files.DriveDurable(ctx, payloadsRegistrar{p: pa}, job.SpaceId, row.ObjectId, job.FileId)
		if errors.Is(err, upload.ErrLimited) {
			return fmt.Errorf("%w: %s", status.ErrLimited, job.FileId)
		}
		return err
	case status.KindPin:
		if row.Inline() {
			return nil
		}
		root, err := cid.Decode(row.RootCid)
		if err != nil {
			return err
		}
		return s.fetch.Fetch(ctx, job.SpaceId, root, row.NetworkSign != "", job.FileId)
	default:
		return fmt.Errorf("files: unknown job kind %q", job.Kind)
	}
}

// fileSubs is the SDK-wide registry of per-space file-status
// subscribers, fed by queue transitions and local attach events.
type fileSubs struct {
	mu   sync.Mutex
	next uint64
	m    map[string]map[uint64]func(space.FileStatus) // spaceId → subs
}

func (r *fileSubs) add(spaceId string, cb func(space.FileStatus)) (unsub func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m == nil {
		r.m = map[string]map[uint64]func(space.FileStatus){}
	}
	if r.m[spaceId] == nil {
		r.m[spaceId] = map[uint64]func(space.FileStatus){}
	}
	r.next++
	id := r.next
	r.m[spaceId][id] = cb
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(r.m[spaceId], id)
	}
}

func (r *fileSubs) dispatch(spaceId string, st space.FileStatus) {
	r.mu.Lock()
	cbs := make([]func(space.FileStatus), 0, len(r.m[spaceId]))
	for _, cb := range r.m[spaceId] {
		cbs = append(cbs, cb)
	}
	r.mu.Unlock()
	for _, cb := range cbs {
		cb(st)
	}
}

func (r *fileSubs) hasSubs(spaceId string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.m[spaceId]) > 0
}

// OnFileJobChange is the queue's OnChange hook: re-derive the file's
// status and push it to this space's subscribers. Best-effort — a
// derivation failure drops the event; Status reads stay the truth.
func (s *Service) OnFileJobChange(job status.Job, _ bool) {
	if !s.fileStatusSubs.hasSubs(job.SpaceId) {
		return
	}
	ctx := context.Background()
	sp, err := s.Get(ctx, job.SpaceId)
	if err != nil {
		return
	}
	st, err := sp.(*spaceImpl).fileStatus(ctx, job.FileId)
	if err != nil {
		return
	}
	s.fileStatusSubs.dispatch(job.SpaceId, st)
}

// SpaceFiles is the gc.RowResolver bulk surface: fileId → durable for
// every live file row of the space, one pass over its payloads
// objects. An unloadable space is an error — GC retains on it.
func (s *Service) SpaceFiles(ctx context.Context, spaceId string) (map[string]bool, error) {
	sp, err := s.Get(ctx, spaceId)
	if err != nil {
		return nil, err
	}
	impl := sp.(*spaceImpl)
	pa := impl.PayloadsInternal()
	objIds, err := impl.store.TreeIdsByChangeType(ctx, payloads.ChangeType)
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, objId := range objIds {
		rows, err := pa.listRowsIn(ctx, objId)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			out[row.Id] = row.NetworkSign != ""
		}
	}
	return out, nil
}

// ResolveIntent is the gc.RowResolver heal hook: find a live row of
// ownerId whose rootCid is root (the attach crash window left the row
// without its CAR ref).
func (s *Service) ResolveIntent(ctx context.Context, spaceId, ownerId string, root cid.Cid) (fileId string, ok bool, err error) {
	sp, err := s.Get(ctx, spaceId)
	if err != nil {
		return "", false, err
	}
	rows, err := sp.(*spaceImpl).PayloadsInternal().ListRows(ctx, ownerId)
	if err != nil {
		return "", false, err
	}
	rootStr := root.String()
	for _, row := range rows {
		if row.RootCid == rootStr {
			return row.Id, true, nil
		}
	}
	return "", false, nil
}

// fileStatus derives one file's FileStatus on read: the row decides
// durable; a pending queue job refines in-flight vs limited and
// carries attempt diagnostics.
func (s *spaceImpl) fileStatus(ctx context.Context, fileId string) (space.FileStatus, error) {
	row, err := s.PayloadsInternal().FindRow(ctx, fileId)
	if err != nil {
		return space.FileStatus{}, err
	}
	st := space.FileStatus{
		FileId:   row.Id,
		ObjectId: row.ObjectId,
		Cached:   row.Inline(),
	}
	if !row.Inline() {
		if root, err := cid.Decode(row.RootCid); err == nil {
			if info, err := s.parent.filesStore().Info(ctx, s.id, root); err == nil {
				st.Cached = info.State == filestore.StateComplete
			}
		}
	}
	if row.Inline() || row.NetworkSign != "" {
		st.State = space.FileStateDurable
		return st, nil
	}
	st.State = space.FileStateInFlight
	if q := s.parent.fqueue; q != nil {
		if job, ok, err := q.Get(ctx, status.KindDurable, s.id, fileId); err == nil && ok {
			st.Attempts = job.Attempts
			st.LastErr = job.LastErr
			if job.Limited {
				st.State = space.FileStateLimited
			}
		}
	}
	return st, nil
}
