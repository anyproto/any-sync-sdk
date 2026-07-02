// Package gc is the local-cache reclamation of the files subsystem
// (SYN-26). Eviction is embedder-triggered: Offload (per file, on the
// space surface) and FreeUp (an LRU sweep toward a byte budget) — the
// SDK never decides on its own to drop refetchable bytes. The only
// automatic work is the safety sweep: CARs no live row references
// (deleted files) past a grace period, and stale partials of durable
// files past a TTL.
//
// The safety rule everywhere: bytes are dropped ONLY when they are
// refetchable (a referencing row carries a network receipt) or
// unreferenced. Received-not-durable bytes are retained regardless of
// budget — dropping them could strand the only copy.
//
// Scale contract: the cache can hold hundreds of thousands of files,
// so every pass STREAMS the metadata (IterateAll/IterateLRU) and never
// materializes the full set; only the rows that will actually be acted
// on are collected (mutations are applied after the iteration closes).
package gc

import (
	"context"
	"errors"
	"time"

	"github.com/anyproto/any-sync/app/logger"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/files/store"
)

var log = logger.NewNamed("sdk.files.gc")

// Reclamation clocks. Vars so tests shrink them.
var (
	// UnreferencedGrace protects a CAR whose last live ref just
	// disappeared: deletion (and its sync) may still be settling, and a
	// racing Attach may be about to re-ref it.
	UnreferencedGrace = 24 * time.Hour
	// StalePartialTTL sweeps partials of durable files that nobody read
	// for a long time — interrupted downloads that were never resumed.
	// They are pure cache (refetchable from their first byte).
	StalePartialTTL = 7 * 24 * time.Hour
)

// RowResolver answers whether a fileId still has a live payloads row
// in its space and whether that row carries a network receipt.
// Implemented by the space layer; conservative errors (space not
// loadable) make the sweep retain.
type RowResolver interface {
	FileDurable(ctx context.Context, spaceId, fileId string) (durable, exists bool, err error)
}

// Service owns cache accounting, the budget sweep and the safety GC.
// One per SDK.
type Service struct {
	store  *store.Store
	rows   RowResolver
	cancel context.CancelFunc
	done   chan struct{}
}

// New builds the Service. Nothing runs automatically: reclamation is
// embedder-driven (Sweep / FreeUp / per-file Offload); Run starts the
// periodic sweep only when the embedder configured a cadence.
func New(st *store.Store, rows RowResolver) *Service {
	return &Service{store: st, rows: rows}
}

// Run starts the periodic safety sweep at the given cadence. No-op
// for interval <= 0 (the default mode — manual only).
func (s *Service) Run(interval time.Duration) {
	if interval <= 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})
	go func() {
		defer close(s.done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
				if err := s.Sweep(ctx); err != nil && !errors.Is(err, context.Canceled) {
					log.Warn("safety sweep", zap.Error(err))
				}
			}
		}
	}()
}

// Close stops the periodic sweep.
func (s *Service) Close() {
	if s.cancel != nil {
		s.cancel()
		<-s.done
	}
}

// CacheSize returns the local bytes held by file CARs (complete and
// partial; offloaded rows hold none). Streaming aggregation.
func (s *Service) CacheSize(ctx context.Context) (int64, error) {
	var total int64
	err := s.store.IterateAll(ctx, func(info store.Info) (bool, error) {
		if info.State == store.StateComplete || info.State == store.StatePartial {
			total += info.Size
		}
		return true, nil
	})
	return total, err
}

// evictAction is one planned reclamation (collected while streaming,
// applied after the iteration closes — no writes under an open iter).
type evictAction struct {
	spaceId    string
	root       cid.Cid
	size       int64
	referenced bool // live durable refs remain → offload; none → delete
}

// FreeUp drops local bytes until at least want bytes are reclaimed,
// least-recently-used first, touching only CARs that are safe to drop
// (every live ref durable → offload; no live refs → delete). Returns
// the bytes actually freed — less than want when nothing else is
// evictable.
func (s *Service) FreeUp(ctx context.Context, want int64) (freed int64, err error) {
	var (
		planned int64
		actions []evictAction
	)
	now := time.Now()
	err = s.store.IterateLRU(ctx, func(info store.Info) (bool, error) {
		if info.State != store.StateComplete && info.State != store.StatePartial {
			return true, nil
		}
		evictable, referenced, rerr := s.classify(ctx, info)
		if rerr != nil || !evictable {
			return true, nil
		}
		if !referenced && now.Sub(info.LastAccess) <= UnreferencedGrace {
			// A ref may be about to land (an Attach between the CAR
			// finalize and the row write — the process can die between
			// any two lines). The safety sweep reaps it after grace.
			return true, nil
		}
		actions = append(actions, evictAction{
			spaceId: info.SpaceId, root: info.Root, size: info.Size, referenced: referenced,
		})
		planned += info.Size
		return planned < want, nil
	})
	if err != nil {
		return 0, err
	}
	for _, a := range actions {
		if a.referenced {
			err = s.store.Offload(ctx, a.spaceId, a.root)
		} else {
			err = s.store.Delete(ctx, a.spaceId, a.root)
		}
		if err != nil {
			log.Warn("evict failed", zap.String("root", a.root.String()), zap.Error(err))
			err = nil
			continue
		}
		freed += a.size
	}
	return freed, nil
}

// sweepAction is one planned safety-GC mutation.
type sweepAction struct {
	spaceId   string
	root      cid.Cid
	deadRefs  []string // refs whose rows are gone → prune
	remove    bool     // unreferenced past grace → delete the CAR + row
	stalePart bool     // durable partial past TTL → offload
}

// Sweep is the automatic safety pass: prune refs whose rows are gone,
// delete CARs unreferenced past the grace period, and drop stale
// durable partials past the TTL. Never touches referenced complete
// CARs and never drops non-durable bytes.
func (s *Service) Sweep(ctx context.Context) error {
	now := time.Now()
	var actions []sweepAction
	err := s.store.IterateAll(ctx, func(info store.Info) (bool, error) {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		var (
			act     = sweepAction{spaceId: info.SpaceId, root: info.Root}
			live    int
			durable bool
		)
		for _, fileId := range info.Refs {
			d, exists, rerr := s.rows.FileDurable(ctx, info.SpaceId, fileId)
			if rerr != nil {
				return true, nil // conservative: unresolvable space retains
			}
			if !exists {
				act.deadRefs = append(act.deadRefs, fileId)
				continue
			}
			live++
			if d {
				durable = true
			}
		}
		switch {
		case live == 0 && now.Sub(info.LastAccess) > UnreferencedGrace:
			act.remove = true
		case info.State == store.StatePartial && durable && now.Sub(info.LastAccess) > StalePartialTTL:
			act.stalePart = true
		}
		if act.remove || act.stalePart || len(act.deadRefs) > 0 {
			actions = append(actions, act)
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	for _, act := range actions {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		for _, fileId := range act.deadRefs {
			if _, err := s.store.RemoveRef(ctx, act.spaceId, act.root, fileId); err != nil {
				log.Warn("ref prune failed", zap.String("root", act.root.String()), zap.Error(err))
			}
		}
		switch {
		case act.remove:
			if err := s.store.Delete(ctx, act.spaceId, act.root); err != nil {
				log.Warn("gc delete failed", zap.String("root", act.root.String()), zap.Error(err))
			}
		case act.stalePart:
			if err := s.store.Offload(ctx, act.spaceId, act.root); err != nil {
				log.Warn("gc stale-partial offload failed", zap.String("root", act.root.String()), zap.Error(err))
			}
		}
	}
	return nil
}

// classify resolves a CAR's live refs: evictable when every live ref
// is durable (refetchable) or none are left; referenced reports
// whether any live ref remains (offload vs delete).
func (s *Service) classify(ctx context.Context, info store.Info) (evictable, referenced bool, err error) {
	live := 0
	for _, fileId := range info.Refs {
		durable, exists, rerr := s.rows.FileDurable(ctx, info.SpaceId, fileId)
		if rerr != nil {
			return false, true, rerr
		}
		if !exists {
			continue
		}
		live++
		if !durable {
			return false, true, nil // a live not-yet-durable ref pins the bytes
		}
	}
	return true, live > 0, nil
}
