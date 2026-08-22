package crdt

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

// MetaCollectionName is the shared collection for per-object metadata.
// Not prefixed by objectId — one collection holds metadata for all objects
// in the DB, keyed by objectId.
const MetaCollectionName = "_meta"

const (
	metaAddSeqKey          = "q"
	metaApplySeqKey        = "as"
	metaHandlerVersionsKey = "hv"
	// metaSpaceIdKey scopes a per-object row to its space. The SDK DB is
	// shared across spaces by default and AddSeq is per-space, so the
	// change-index query filters on this field. Absent on the
	// `space:<id>` watermark rows and on object rows written before
	// space-scoping (those stay invisible to the query until their next
	// change backfills the field — the accepted "index from now on"
	// behaviour).
	metaSpaceIdKey = "sp"

	// metaDeletedKey marks a per-object row as purged (object deletion).
	// Stamped in the same WriteTx as the shared-`objects` row removal,
	// bumping the row's applySeq to a fresh value, so the deletion surfaces
	// through the change-index feed as ObjectChange{Deleted:true} at an
	// applySeq strictly greater than the object's last content change.
	// Sticky: once set, never cleared (a deleted tree id is content-
	// addressable and any-sync permanently burns it, so the id never reuses).
	metaDeletedKey = "del"

	// metaDeletedHeadKey / metaReconcileVerKey are the deletion-reconcile
	// gate on the `space:<id>` row: the settings-tree head last reconciled
	// (dh) and the reconcile-logic version last applied (dv). See spacesync.
	metaDeletedHeadKey  = "dh"
	metaReconcileVerKey = "dv"

	// metaGenerationKey is the per-space rebuild epoch on the `space:<id>`
	// row. An sdk.db wipe drops the row, so the epoch changes — the signal a
	// consumer uses to detect a renumbered applySeq axis and full-reindex.
	metaGenerationKey = "gen"

	// metaReindexLocalKey holds the local-scope leaves captured for a
	// re-index that has not finished restoring them (reindex.go). Written
	// in the same upsert as the watermark rewind, cleared by
	// PersistVersions once the leaves are back — while it is present the
	// object's rebuild is in flight and the next load resumes it.
	metaReindexLocalKey = "rl"

	// spaceMetaKeyPrefix namespaces space-scoped rows inside the same
	// _meta collection. Colon is not a valid char in any-sync's
	// content-addressable object IDs, so "space:<id>" rows can't
	// collide with per-object rows keyed by objectId.
	spaceMetaKeyPrefix = "space:"
)

// ensureMetaIndexes ensures the indexes the _meta collection needs.
// Idempotent — safe to call on every open. The (sp, as) compound index
// backs QueryChangedObjects' "objects in this space with applySeq > N,
// ordered by applySeq" scan; the legacy (sp, q) index stays for the
// restore-watermark reads.
func ensureMetaIndexes(ctx context.Context, coll anystore.Collection) error {
	if err := coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name:   "idx__meta_sp_q",
		Fields: []string{metaSpaceIdKey, metaAddSeqKey},
		Sparse: true,
	}); err != nil {
		return err
	}
	return coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name:   "idx__meta_sp_as",
		Fields: []string{metaSpaceIdKey, metaApplySeqKey},
		Sparse: true,
	})
}

// SpaceMetaKey returns the _meta document id for a space's row.
func SpaceMetaKey(spaceId string) string { return spaceMetaKeyPrefix + spaceId }

// LoadMeta reads the per-object metadata from the _meta collection.
// Returns zero values if the document doesn't exist yet.
func LoadMeta(ctx context.Context, coll anystore.Collection, objectId string) (maxAddSeq, maxApplySeq uint64, handlerVersions map[string]int, err error) {
	v, err := loadMetaRow(ctx, coll, objectId)
	if err != nil || v == nil {
		return 0, 0, nil, err
	}
	maxAddSeq, maxApplySeq, handlerVersions = decodeMetaRow(v)
	return maxAddSeq, maxApplySeq, handlerVersions, nil
}

// loadMetaRow reads one _meta row; nil (no error) when it doesn't exist.
// The returned value is only valid until the next call on the collection.
func loadMetaRow(ctx context.Context, coll anystore.Collection, objectId string) (*anyenc.Value, error) {
	doc, err := coll.FindId(ctx, objectId)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return doc.Value(), nil
}

func decodeMetaRow(v *anyenc.Value) (maxAddSeq, maxApplySeq uint64, handlerVersions map[string]int) {
	maxAddSeq = uint64(v.GetInt(metaAddSeqKey))
	maxApplySeq = uint64(v.GetInt(metaApplySeqKey))
	if hv := v.Get(metaHandlerVersionsKey); hv != nil && hv.Type() == anyenc.TypeObject {
		handlerVersions = make(map[string]int)
		obj, _ := hv.Object()
		obj.Visit(func(k []byte, vv *anyenc.Value) {
			if vv.Type() == anyenc.TypeNumber {
				handlerVersions[string(k)] = vv.GetInt()
			}
		})
	}
	return maxAddSeq, maxApplySeq, handlerVersions
}

// PersistMeta writes per-object metadata to the _meta collection. Call
// inside the same WriteTx as the record mutations for atomicity.
//
// spaceId scopes the row for the change-index query; pass "" to leave it
// unset (unit tests, raw mode without a space). An unset row is
// invisible to QueryChangedObjects, which is the accepted lazy-backfill
// behaviour.
func PersistMeta(ctx context.Context, coll anystore.Collection, objectId string, maxAddSeq, maxApplySeq uint64, handlerVersions map[string]int, spaceId string) error {
	return persistMeta(ctx, coll, objectId, maxAddSeq, maxApplySeq, handlerVersions, spaceId, nil)
}

// persistMeta is PersistMeta with an optional extra mutation applied to
// the row in the same upsert — the re-index path uses it to write and
// clear its captured local leaves atomically with the watermark.
func persistMeta(ctx context.Context, coll anystore.Collection, objectId string, maxAddSeq, maxApplySeq uint64, handlerVersions map[string]int, spaceId string, extra func(a *anyenc.Arena, v *anyenc.Value)) error {
	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		v.Set(metaAddSeqKey, a.NewNumberInt(int(maxAddSeq)))
		if maxApplySeq > 0 {
			v.Set(metaApplySeqKey, a.NewNumberInt(int(maxApplySeq)))
		}
		if spaceId != "" {
			v.Set(metaSpaceIdKey, a.NewString(spaceId))
		}
		if len(handlerVersions) > 0 {
			hv := a.NewObject()
			for name, ver := range handlerVersions {
				hv.Set(name, a.NewNumberInt(ver))
			}
			v.Set(metaHandlerVersionsKey, hv)
		}
		if extra != nil {
			extra(a, v)
		}
		return v, true, nil
	})
	_, err := coll.UpsertId(ctx, objectId, mod)
	return err
}

// MetaOwnership reads the ownership fields off a per-object _meta row:
// the scoping spaceId (empty on pre-scoping rows) and the sticky purge
// marker. The orphan-collection GC uses it to decide whether a
// per-object collection's owner is live — a row claiming a live space
// without the purge marker proves liveness even when the object never
// got a shared `objects` row (base-dataset-only objects).
func MetaOwnership(v *anyenc.Value) (spaceId string, deleted bool) {
	return v.GetString(metaSpaceIdKey), v.GetBool(metaDeletedKey)
}

// ObjectSeq pairs an object id with its persisted max applySeq.
// Returned by QueryChangedObjects for the consumer-side change-index
// feed. Deleted is true when the row is a purged-object marker (the
// consumer evicts it) rather than a content change.
type ObjectSeq struct {
	ObjectId string
	ApplySeq uint64
	Deleted  bool
}

// QueryChangedObjects returns the objects in spaceId whose persisted max
// applySeq exceeds `since`, ordered ascending so the caller can page by
// passing the last returned ApplySeq as the next `since`. limit<=0
// means no cap. Backs the change-index "what changed" pull path.
//
// Keyed on applySeq (not AddSeq) so non-DAG mutations — the account
// mirror's injected applies and device-local writes — surface to
// consumers; legacy rows are seeded applySeq := addSeq by
// BackfillApplySeq, keeping pre-existing cursors on one axis.
//
// The spaceId filter naturally excludes the `space:<id>` watermark rows
// (no `sp` field) and per-object rows written before space-scoping.
func QueryChangedObjects(ctx context.Context, coll anystore.Collection, spaceId string, since uint64, limit int) ([]ObjectSeq, error) {
	filter, err := query.ParseCondition(map[string]any{
		metaSpaceIdKey:  spaceId,
		metaApplySeqKey: map[string]any{"$gt": int(since)},
	})
	if err != nil {
		return nil, fmt.Errorf("crdt: build changed-objects filter: %w", err)
	}
	sort, err := query.ParseSort(metaApplySeqKey)
	if err != nil {
		return nil, fmt.Errorf("crdt: build changed-objects sort: %w", err)
	}
	q := coll.Find(filter).Sort(sort)
	if limit > 0 {
		q = q.Limit(uint(limit))
	}
	iter, err := q.Iter(ctx)
	if err != nil {
		return nil, fmt.Errorf("crdt: changed-objects iter: %w", err)
	}
	defer iter.Close()
	var out []ObjectSeq
	for iter.Next() {
		doc, derr := iter.Doc()
		if derr != nil {
			return nil, derr
		}
		v := doc.Value()
		out = append(out, ObjectSeq{
			ObjectId: v.GetString(IdField),
			ApplySeq: uint64(v.GetInt(metaApplySeqKey)),
			Deleted:  v.GetBool(metaDeletedKey),
		})
	}
	return out, nil
}

// MaxObjectApplySeq returns the highest persisted per-object applySeq
// in spaceId — the current upper bound a change-index cursor can reach,
// and the ApplySeqAllocator's seed. Returns 0 when the space has no
// scoped object rows yet. Run BackfillApplySeq first so legacy rows
// participate.
func MaxObjectApplySeq(ctx context.Context, coll anystore.Collection, spaceId string) (uint64, error) {
	filter, err := query.ParseCondition(map[string]any{metaSpaceIdKey: spaceId})
	if err != nil {
		return 0, fmt.Errorf("crdt: build max-applyseq filter: %w", err)
	}
	sort, err := query.ParseSort("-" + metaApplySeqKey)
	if err != nil {
		return 0, fmt.Errorf("crdt: build max-applyseq sort: %w", err)
	}
	iter, err := coll.Find(filter).Sort(sort).Limit(1).Iter(ctx)
	if err != nil {
		return 0, fmt.Errorf("crdt: max-applyseq iter: %w", err)
	}
	defer iter.Close()
	if iter.Next() {
		doc, derr := iter.Doc()
		if derr != nil {
			return 0, derr
		}
		return uint64(doc.Value().GetInt(metaApplySeqKey)), nil
	}
	return 0, nil
}

// BackfillApplySeq seeds applySeq := addSeq on this space's legacy
// per-object _meta rows (rows written before the applySeq watermark
// existed). One-off per space, guarded by a flag on the space's own
// _meta row; idempotent. Keeps consumer cursors valid across the
// re-key: every historical position N (AddSeq units) means the same
// thing on the applySeq axis, and the allocator seeds past it.
func BackfillApplySeq(ctx context.Context, coll anystore.Collection, spaceId string) error {
	const backfillFlagKey = "asbf"
	spaceKey := SpaceMetaKey(spaceId)
	if doc, err := coll.FindId(ctx, spaceKey); err == nil {
		if doc.Value().GetBool(backfillFlagKey) {
			return nil
		}
	} else if !errors.Is(err, anystore.ErrDocNotFound) {
		return err
	}

	filter, err := query.ParseCondition(map[string]any{
		metaSpaceIdKey:  spaceId,
		metaApplySeqKey: map[string]any{"$exists": false},
	})
	if err != nil {
		return fmt.Errorf("crdt: build applyseq-backfill filter: %w", err)
	}
	// Collect ids first (the iterator and per-row updates can't share a
	// cursor), then stamp each row. Legacy rows are bounded by the
	// space's object count; this runs once per space ever.
	var ids []string
	var seqs []int
	iter, err := coll.Find(filter).Iter(ctx)
	if err != nil {
		return fmt.Errorf("crdt: applyseq backfill iter: %w", err)
	}
	for iter.Next() {
		doc, derr := iter.Doc()
		if derr != nil {
			_ = iter.Close()
			return derr
		}
		v := doc.Value()
		if q := v.GetInt(metaAddSeqKey); q > 0 {
			ids = append(ids, v.GetString(IdField))
			seqs = append(seqs, q)
		}
	}
	if err := iter.Err(); err != nil {
		_ = iter.Close()
		return fmt.Errorf("crdt: applyseq backfill iter: %w", err)
	}
	_ = iter.Close()
	for i, id := range ids {
		seq := seqs[i]
		mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
			if v.Get(metaApplySeqKey) != nil {
				return v, false, nil // raced a live apply — its stamp wins
			}
			v.Set(metaApplySeqKey, a.NewNumberInt(seq))
			return v, true, nil
		})
		if _, err := coll.UpdateId(ctx, id, mod); err != nil && !errors.Is(err, anystore.ErrDocNotFound) {
			return fmt.Errorf("crdt: applyseq backfill %s: %w", id, err)
		}
	}
	flagMod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		v.Set(backfillFlagKey, a.NewTrue())
		return v, true, nil
	})
	if _, err := coll.UpsertId(ctx, spaceKey, flagMod); err != nil {
		return fmt.Errorf("crdt: applyseq backfill flag: %w", err)
	}
	return nil
}

// HandlerVersions returns a map of dataset→version from the Controller's
// registered handlers.
func (c *Controller) HandlerVersions() map[string]int {
	hv := make(map[string]int, len(c.versions))
	for name, ver := range c.versions {
		hv[name] = ver
	}
	return hv
}

// PersistMeta writes the Controller's watermarks and handler versions to
// the _meta collection. The caller should pass a context carrying the same
// WriteTx as the record mutations for atomicity.
func (c *Controller) PersistMeta(ctx context.Context, metaColl anystore.Collection) error {
	return PersistMeta(ctx, metaColl, c.objectId, c.maxAddSeq, c.maxApplySeq, c.HandlerVersions(), c.spaceId)
}

// LoadAndSeedMeta reads metadata from the _meta collection, seeds the
// Controller's maxAddSeq, and returns the stored handler versions so the
// caller can compare them with current versions for re-indexing decisions.
func (c *Controller) LoadAndSeedMeta(ctx context.Context, metaColl anystore.Collection) (handlerVersions map[string]int, err error) {
	v, err := loadMetaRow(ctx, metaColl, c.objectId)
	if err != nil || v == nil {
		return nil, err
	}
	c.maxAddSeq, c.maxApplySeq, handlerVersions = decodeMetaRow(v)
	// An in-flight rebuild left its captured local leaves on the row;
	// copied out because the row buffer dies with the read.
	if rl := v.Get(metaReindexLocalKey); rl != nil {
		c.reindexPending = true
		c.reindexLocal = rl.MarshalTo(nil)
	}
	return handlerVersions, nil
}

// LoadSpaceMaxAddSeq reads the persisted space-level head-store
// watermark — the lower bound of "we've already replayed any-sync
// trees up to this LastAddSeq for this space". Returns 0 when no
// row exists yet (first boot or never caught up).
func LoadSpaceMaxAddSeq(ctx context.Context, coll anystore.Collection, spaceId string) (uint64, error) {
	doc, err := coll.FindId(ctx, SpaceMetaKey(spaceId))
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return uint64(doc.Value().GetInt(metaAddSeqKey)), nil
}

// PersistSpaceMaxAddSeq writes the space-level head-store watermark.
// Written once at the end of a successful catch-up pass — per-change
// progress is already captured by the per-object _meta rows, so this
// value is a coarse-grained "no work needed at startup" hint, not a
// per-change atomic counter.
func PersistSpaceMaxAddSeq(ctx context.Context, coll anystore.Collection, spaceId string, maxAddSeq uint64) error {
	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		v.Set(metaAddSeqKey, a.NewNumberInt(int(maxAddSeq)))
		return v, true, nil
	})
	_, err := coll.UpsertId(ctx, SpaceMetaKey(spaceId), mod)
	return err
}

// LoadSpaceDeletedGate reads the persisted deletion-reconcile gate for a
// space — the settings-tree head last reconciled (dh) and the
// reconcile-logic version last applied (dv). Returns ("", 0) when no row
// exists (fresh/rebuilt sdk.db), a guaranteed mismatch that forces exactly
// one reconcile sweep.
func LoadSpaceDeletedGate(ctx context.Context, coll anystore.Collection, spaceId string) (head string, ver int, err error) {
	doc, err := coll.FindId(ctx, SpaceMetaKey(spaceId))
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return "", 0, nil
		}
		return "", 0, err
	}
	v := doc.Value()
	return v.GetString(metaDeletedHeadKey), v.GetInt(metaReconcileVerKey), nil
}

// PersistSpaceDeletedGate writes the deletion-reconcile gate. Sets only
// dh/dv via ModifyFunc so the forward-catchup watermark "q" and the
// generation "gen" on the same space:<id> row are preserved. Called ONLY
// after a fully successful reconcile sweep.
func PersistSpaceDeletedGate(ctx context.Context, coll anystore.Collection, spaceId, head string, ver int) error {
	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		v.Set(metaDeletedHeadKey, a.NewString(head))
		v.Set(metaReconcileVerKey, a.NewNumberInt(ver))
		return v, true, nil
	})
	_, err := coll.UpsertId(ctx, SpaceMetaKey(spaceId), mod)
	return err
}

// PersistDeletionMark stamps the object's kept _meta row as deleted, in
// place: del=true, as=applySeq (a fresh seq > the object's last content
// applySeq), and re-asserts sp so a sparse-history object is guaranteed
// visible to the change-index query. Leaves q/hv intact. Call inside the
// same WriteTx as the shared-`objects` row removal so the deletion is
// announced atomically with the purge.
func PersistDeletionMark(ctx context.Context, coll anystore.Collection, objectId, spaceId string, applySeq uint64) error {
	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		v.Set(metaDeletedKey, a.NewTrue())
		v.Set(metaApplySeqKey, a.NewNumberInt(int(applySeq)))
		v.Set(metaSpaceIdKey, a.NewString(spaceId))
		return v, true, nil
	})
	_, err := coll.UpsertId(ctx, objectId, mod)
	return err
}

// MetaExists reports whether a space-scoped per-object _meta row exists —
// i.e. the object was ever materialized/indexed in this space. Gates the
// del-stamp for objects that were fed but never got a shared `objects` row
// (base-dataset-only). Read with the caller's (tx) ctx.
func MetaExists(ctx context.Context, coll anystore.Collection, objectId, spaceId string) (bool, error) {
	doc, err := coll.FindId(ctx, objectId)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return false, nil
		}
		return false, err
	}
	return doc.Value().GetString(metaSpaceIdKey) == spaceId, nil
}

// LoadOrInitGeneration returns the per-space rebuild epoch on the
// space:<id> row, minting a fresh UUID when the row (or the gen field) is
// absent — an sdk.db wipe drops the row, so the epoch changes and signals a
// renumbered applySeq axis to consumers. Idempotent; call once at store
// load (single-threaded) to mint eagerly, and on every consumer read.
func LoadOrInitGeneration(ctx context.Context, coll anystore.Collection, spaceId string) (string, error) {
	if doc, err := coll.FindId(ctx, SpaceMetaKey(spaceId)); err == nil {
		if g := doc.Value().GetString(metaGenerationKey); g != "" {
			return g, nil
		}
	} else if !errors.Is(err, anystore.ErrDocNotFound) {
		return "", err
	}
	gen := newObjectID()
	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		if v.GetString(metaGenerationKey) != "" { // raced another opener — keep theirs
			return v, false, nil
		}
		v.Set(metaGenerationKey, a.NewString(gen))
		return v, true, nil
	})
	if _, err := coll.UpsertId(ctx, SpaceMetaKey(spaceId), mod); err != nil {
		return "", err
	}
	// Re-read: a lost race means the persisted value is another opener's id.
	if doc, err := coll.FindId(ctx, SpaceMetaKey(spaceId)); err == nil {
		if g := doc.Value().GetString(metaGenerationKey); g != "" {
			return g, nil
		}
	}
	return gen, nil
}

// newObjectID mints a bson-style 12-byte ObjectID — 4-byte big-endian unix
// seconds + 8 random bytes — hex-encoded to a 24-char string. Same shape
// any-store uses for its own document/instance ids (its objectid package is
// internal, so we mint our own with stdlib crypto/rand, no extra dep).
// Time-ordered and unique; used as the per-space generation epoch, which
// only needs inequality across rebuilds.
func newObjectID() string {
	var b [12]byte
	binary.BigEndian.PutUint32(b[0:4], uint32(time.Now().Unix()))
	_, _ = rand.Read(b[4:])
	return hex.EncodeToString(b[:])
}

// PurgeSpaceMeta removes every _meta row belonging to spaceId: the
// space-scoped per-object watermark rows (matched on sp), any explicit
// objectIds (pre-scoping rows without sp), and the `space:<id>` row —
// dropping it rotates the generation on the next rebuild, the signal
// change-index consumers full-reindex on. Offload calls this: a stale
// MaxAddSeq watermark is NOT inert once a space can re-materialize
// (guest rejoin) — the rebuilt controllers would trust it and skip the
// entire cold-restore replay, leaving synced trees with zero rows.
func PurgeSpaceMeta(ctx context.Context, coll anystore.Collection, spaceId string, objectIds []string) error {
	filter, err := query.ParseCondition(map[string]any{metaSpaceIdKey: spaceId})
	if err != nil {
		return fmt.Errorf("crdt: build purge-meta filter: %w", err)
	}
	iter, err := coll.Find(filter).Iter(ctx)
	if err != nil {
		return fmt.Errorf("crdt: purge-meta iter: %w", err)
	}
	ids := make([]string, 0, 16)
	for iter.Next() {
		doc, derr := iter.Doc()
		if derr != nil {
			continue
		}
		if id := doc.Value().GetString("id"); id != "" {
			ids = append(ids, id)
		}
	}
	iterErr := iter.Err()
	_ = iter.Close()
	if iterErr != nil {
		return fmt.Errorf("crdt: purge-meta scan: %w", iterErr)
	}
	ids = append(ids, objectIds...)
	ids = append(ids, SpaceMetaKey(spaceId))
	var firstErr error
	for _, id := range ids {
		if derr := coll.DeleteId(ctx, id); derr != nil && !errors.Is(derr, anystore.ErrDocNotFound) {
			if firstErr == nil {
				firstErr = derr
			}
		}
	}
	return firstErr
}
