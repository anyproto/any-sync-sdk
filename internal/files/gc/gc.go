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

// RowResolver is the space layer's row surface for GC. Conservative
// errors (space not loadable) make the passes retain.
type RowResolver interface {
	// SpaceFiles returns fileId → durable (receipt present) for every
	// live file row of the space, in ONE pass over its payloads
	// objects. GC resolves refs against this map — a per-fileId lookup
	// would cost a full tree scan for every dead ref.
	SpaceFiles(ctx context.Context, spaceId string) (map[string]bool, error)
	// ResolveIntent lists the live rows of ownerId whose rootCid is
	// root — the heal for the attach crash window (row written, ref/job
	// not). Empty when no such row exists (the crash preceded the row).
	ResolveIntent(ctx context.Context, spaceId, ownerId string, root cid.Cid) (fileIds []string, err error)
}

// Enqueue schedules a drive-toward-durable job for a healed row
// (wired to the files work queue; nil in tests that don't care).
type Enqueue func(ctx context.Context, spaceId, fileId string) error

// Service owns cache accounting, the budget sweep and the safety GC.
// One per SDK.
type Service struct {
	store   *store.Store
	rows    RowResolver
	enqueue Enqueue
	cancel  context.CancelFunc
	done    chan struct{}
}

// New builds the Service. Nothing runs automatically: reclamation is
// embedder-driven (Sweep / FreeUp / per-file Offload); Run starts the
// periodic sweep only when the embedder configured a cadence.
func New(st *store.Store, rows RowResolver, enqueue Enqueue) *Service {
	return &Service{store: st, rows: rows, enqueue: enqueue}
}

// rowsCache memoizes SpaceFiles per GC run (one listing per space per
// pass, however many CARs the pass touches).
type rowsCache struct {
	rows RowResolver
	m    map[string]map[string]bool
}

func newRowsCache(rows RowResolver) *rowsCache {
	return &rowsCache{rows: rows, m: map[string]map[string]bool{}}
}

func (c *rowsCache) space(ctx context.Context, spaceId string) (map[string]bool, error) {
	if files, ok := c.m[spaceId]; ok {
		return files, nil
	}
	files, err := c.rows.SpaceFiles(ctx, spaceId)
	if err != nil {
		return nil, err
	}
	c.m[spaceId] = files
	return files, nil
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
	cache := newRowsCache(s.rows)
	err = s.store.IterateLRU(ctx, func(info store.Info) (bool, error) {
		if info.State != store.StateComplete && info.State != store.StatePartial {
			return true, nil
		}
		evictable, referenced, rerr := s.classify(ctx, cache, info)
		if rerr != nil || !evictable {
			return true, nil
		}
		if !referenced {
			if now.Sub(info.LastAccess) <= UnreferencedGrace {
				// A ref may be about to land (an Attach between the CAR
				// finalize and the row write — the process can die
				// between any two lines). The sweep reaps it after grace.
				return true, nil
			}
			if intents, kerr := s.store.Intents(ctx, info.SpaceId, info.Root); kerr != nil || len(intents) > 0 {
				// An attach intent pins the root; Sweep owns healing it.
				return true, nil
			}
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
	// intents is set (instead of remove) for an unreferenced CAR
	// carrying attach-intent markers past grace: resolve each owner's
	// rows and re-link the crash-orphaned ones or, when no owner has a
	// row, clear the markers and delete the CAR.
	intents []store.Intent
}

// Sweep is the automatic safety pass: prune refs whose rows are gone,
// delete CARs unreferenced past the grace period, and drop stale
// durable partials past the TTL. Never touches referenced complete
// CARs and never drops non-durable bytes.
func (s *Service) Sweep(ctx context.Context) error {
	now := time.Now()
	cache := newRowsCache(s.rows)
	var actions []sweepAction
	err := s.store.IterateAll(ctx, func(info store.Info) (bool, error) {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		files, rerr := cache.space(ctx, info.SpaceId)
		if rerr != nil {
			return true, nil // conservative: unresolvable space retains
		}
		var (
			act        = sweepAction{spaceId: info.SpaceId, root: info.Root}
			live       int
			allDurable = true
		)
		for _, fileId := range info.Refs {
			durable, exists := files[fileId]
			if !exists {
				act.deadRefs = append(act.deadRefs, fileId)
				continue
			}
			live++
			if !durable {
				allDurable = false
			}
		}
		switch {
		case live == 0 && now.Sub(info.LastAccess) > UnreferencedGrace:
			if intents, kerr := s.store.Intents(ctx, info.SpaceId, info.Root); kerr == nil && len(intents) > 0 {
				act.intents = intents
			} else if kerr == nil {
				act.remove = true
			}
		case info.State == store.StatePartial && live > 0 && allDurable &&
			now.Sub(info.LastAccess) > StalePartialTTL:
			// EVERY live ref must be refetchable: dropping bytes while
			// one unsigned ref remains would strand that file (its own
			// row gates the remote rung).
			act.stalePart = true
		}
		if act.remove || act.stalePart || len(act.intents) > 0 || len(act.deadRefs) > 0 {
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
		case len(act.intents) > 0:
			s.healIntents(ctx, act.spaceId, act.root, act.intents)
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

// healIntents resolves a root's stale attach intents: the process died
// between an attach's row write and its ref/job writes. Every row of a
// marking owner that holds root gets its durable job and its ref, and
// the markers are cleared; when no owner has such a row, every crash
// preceded its row — the markers go, then the orphaned CAR. Nothing is
// referenced until every owner resolved and every job landed, so any
// failure leaves the CAR unreferenced and marked for the next sweep.
func (s *Service) healIntents(ctx context.Context, spaceId string, root cid.Cid, intents []store.Intent) {
	resolved := map[string]struct{}{}
	var fileIds []string
	for _, in := range intents {
		if _, done := resolved[in.OwnerId]; done {
			continue
		}
		resolved[in.OwnerId] = struct{}{}
		ids, err := s.rows.ResolveIntent(ctx, spaceId, in.OwnerId, root)
		if err != nil {
			return // conservative: retry next sweep
		}
		fileIds = append(fileIds, ids...)
	}
	if s.enqueue != nil {
		for _, fileId := range fileIds {
			if err := s.enqueue(ctx, spaceId, fileId); err != nil {
				log.Warn("intent heal: enqueue failed", zap.String("fileId", fileId), zap.Error(err))
				return
			}
		}
	}
	if err := s.store.AddRefs(ctx, spaceId, root, fileIds...); err != nil {
		log.Warn("intent heal: re-ref failed", zap.String("root", root.String()), zap.Error(err))
		return
	}
	for _, fileId := range fileIds {
		log.Info("intent heal: re-linked crash-orphaned file",
			zap.String("fileId", fileId), zap.String("root", root.String()))
	}
	keys := make([]string, len(intents))
	for i, in := range intents {
		keys[i] = in.Key
	}
	if err := s.store.DeleteKVs(ctx, keys); err != nil {
		log.Warn("intent heal: clear markers failed", zap.String("root", root.String()), zap.Error(err))
		return
	}
	if len(fileIds) > 0 {
		return
	}
	// An attach that marked the root since the sweep listed it pins it.
	if left, err := s.store.Intents(ctx, spaceId, root); err != nil || len(left) > 0 {
		return
	}
	if err := s.store.Delete(ctx, spaceId, root); err != nil {
		log.Warn("intent heal: delete failed", zap.String("root", root.String()), zap.Error(err))
	}
}

// classify resolves a CAR's live refs: evictable when every live ref
// is durable (refetchable) or none are left; referenced reports
// whether any live ref remains (offload vs delete).
func (s *Service) classify(ctx context.Context, cache *rowsCache, info store.Info) (evictable, referenced bool, err error) {
	files, err := cache.space(ctx, info.SpaceId)
	if err != nil {
		return false, true, err
	}
	live := 0
	for _, fileId := range info.Refs {
		durable, exists := files[fileId]
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
