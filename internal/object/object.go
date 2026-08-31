package object

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree/updatelistener"
	"github.com/anyproto/any-sync/util/crypto"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

var log = logger.NewNamed("sdk.object")

// ErrTreeNotSet is retained for callers that historically distinguished
// "tree not yet bound" from other errors. The current constructor
// guarantees tree-on-construction, so this only surfaces if an Object
// is somehow zeroed out (defensive).
var ErrTreeNotSet = errors.New("object: tree not set")

// ErrClosed rejects writes on a closed (evicted) Object. Callers that
// resolved the Object before an eviction (e.g. the lazy schema-refresh
// Drop) retry with a fresh Store.Get.
var ErrClosed = errors.New("object: closed")

// Object binds one any-sync object tree to one crdt.Controller.
//
// Constructed via object.New, which builds the Object, runs the
// caller-supplied TreeFunc with the new Object as the synctree's
// update listener, and returns a fully-initialised Object with its
// tree bound. There is no intermediate state where an Object is
// returned without a tree — the listener wiring chicken/egg is
// resolved entirely inside New.
//
// After construction the Object is live: LocalWrite produces new
// changes, and inbound sync calls fire Update/Rebuild which re-run
// the same replay path. Run ColdRestore once before publishing the
// Object to readers to drain any tree-changes the controller's
// MaxAddSeq watermark hasn't caught up to yet.
//
// Not safe for concurrent LocalWrite — the apply path is serialised
// through a per-Object mutex.
// ApplyGate is the optional pre-apply hook the space layer wires to
// implement DataVersion-based deferral (the schema gate from
// docs/types-properties-proposal.md § "Detached changes"). Called
// before each ApplyChange in the replay path; rawPayload is the
// decoded wire bytes (for parking).
//
//   - return (true, nil)  → proceed with controller.ApplyChange.
//   - return (false, nil) → skip apply; the gate has parked the
//     change for later drain.
//   - return (_, err)     → the apply path logs and skips this row.
type ApplyGate func(ctx context.Context, ch *crdt.Change, rawPayload []byte) (proceed bool, err error)

// AfterApply fires after a successful ApplyChange — both the replay
// path and LocalWrite. Used by the space layer to drain parked
// changes whose missing shortIds may have just landed and to dispatch
// the change to subscribers.
//
// The Object firing the hook is passed in so the space layer can
// reach the Controller directly without going through a cache
// lookup. That matters because afterApply runs from inside the
// LoadFunc on a fresh joiner (synctree's afterBuild → Rebuild →
// replayLocked → applyDecodedLocked → afterApply); any cache.Pick
// on the same id would block on the load channel that hasn't
// closed yet, producing a self-recursive deadlock.
//
// res carries the per-record extras the apply path stamped beyond
// the input ops (handler-derived ops, _ver.id creation marker).
// Subscribers consume DerivedOps so a viewer can reconstruct a fresh
// record uniformly — no distinction between "user-supplied" and
// "auto" fields on the wire.
type AfterApply func(ctx context.Context, o *Object, ch *crdt.Change, res *crdt.ApplyResult)

// AfterReplay fires once after replayLocked finishes a batch of
// inbound/replayed changes of which at least one applied — every
// per-change AfterApply of the batch has run. The space layer flushes
// work it coalesced per batch (object-stamp events for live queries).
// Runs under the same tree lock as the applies.
type AfterReplay func(ctx context.Context, o *Object)

// PlaintextSpec declares a plaintext (node-readable) object class: an
// object whose tree changes are written UNencrypted at the any-sync
// level (ShouldBeEncrypted:false → ReadKeyId=="" on the wire), so a
// reader without the space read key — e.g. a filenode-v2 broker —
// can materialize them. Field-level secrecy inside such changes is
// the dataset layer's job (an SDK-sealed field, see internal/payloads).
//
// Datasets is the write allowlist. LocalWrite hard-errors on any other
// dataset (nothing leaks into the DAG); the inbound replay path
// tolerantly skips them (a peer can't smuggle rows into `objects`
// etc. through a plaintext tree).
type PlaintextSpec struct {
	Datasets map[string]struct{}
}

type Object struct {
	signKey     crypto.PrivKey
	codec       *Codec
	alloc       *VersionAllocator
	ctrl        *crdt.Controller
	spaceId     string
	gate        ApplyGate
	afterApply  AfterApply
	afterReplay AfterReplay
	// replaying is true while replayLocked iterates a batch. Read by
	// the AfterApply hook (same lock) to tell a bulk replay from a
	// single local write. See Replaying.
	replaying bool
	// writeGate, when set, is consulted at the top of LocalWrite — the
	// sole entry for user-authored DAG changes. A non-nil error rejects
	// the write (read-only guest spaces). Local-set and inbound apply
	// paths are never gated.
	writeGate func() error
	// plaintextSpecs maps a tree-root ChangeType to its plaintext
	// class declaration. The root's ChangeType is immutable, signed,
	// and cleartext, so keyed and keyless readers resolve the same
	// class. Empty/nil map = every object is a regular encrypted one.
	plaintextSpecs map[string]PlaintextSpec
	// onClose is the store's per-object resource-release seam, fired
	// exactly once from the close path (after the closed flag is set)
	// — releases handles the controller doesn't own (the object's
	// `__history` collection). Optional.
	onClose func()

	// tree's own lock (synctree.Lock) is the single mutex guarding
	// all writes into the Controller: LocalWrite, replayLocked (via
	// Update/Rebuild/ColdRestore), and ApplyDecoded (drain path).
	// Don't add a separate Object-level mutex — sync.Mutex is non-
	// reentrant, so any path that's already under tree.Lock (the
	// synchandler-driven Update) must NOT try to re-lock.
	tree   objecttree.ObjectTree
	closed bool
}

// Config carries the dependencies object.New wires onto a new
// Object. Gate and AfterApply are optional (nil = disabled).
type Config struct {
	SpaceId    string
	SignKey    crypto.PrivKey
	Controller *crdt.Controller
	Allocator  *VersionAllocator
	Gate       ApplyGate
	AfterApply AfterApply
	// AfterReplay runs once per replay batch after its applies (see
	// the AfterReplay type). Optional.
	AfterReplay AfterReplay
	// WriteGate rejects user-authored DAG writes when it returns a
	// non-nil error (read-only guest spaces). Optional — nil means
	// writable.
	WriteGate func() error
	// PlaintextSpecs declares which tree-root ChangeTypes are plaintext
	// object classes and which datasets they may carry. Optional — nil
	// means every object writes encrypted changes.
	PlaintextSpecs map[string]PlaintextSpec
	// OnClose runs once when the Object is closed (eviction or cache
	// shutdown), after the controller's own collection handles are
	// released. The store hooks per-object resources the controller
	// doesn't own (the `__history` collection handle). Optional.
	OnClose func()
}

// TreeFunc constructs the any-sync ObjectTree for the new Object,
// receiving the Object (as its UpdateListener) as input. Called
// exactly once from inside object.New; the returned tree is wired
// into the Object before New returns. Typical implementations call
// TreeBuilder.PutTree (creation/derivation path) and/or
// TreeBuilder.BuildTree (existing-tree path) with the listener
// passed straight through.
type TreeFunc func(listener updatelistener.UpdateListener) (objecttree.ObjectTree, error)

// New constructs a fully-initialised Object with its any-sync tree
// bound. The TreeFunc closure runs with the partially-built Object
// as its update listener; the returned tree is wired in atomically
// before New returns, so the Object is never observable to callers
// without a tree.
//
// During TreeFunc execution any synctree listener callbacks
// (Update / Rebuild fired by BuildSyncTreeOrGetRemote's initial
// rebuild) see o.tree == nil — replayLocked handles that by
// stamping ObjectAuthor / Creator from the explicit tree arg it
// receives rather than from o.tree.
func New(cfg Config, treeFunc TreeFunc) (*Object, error) {
	if cfg.Controller == nil {
		return nil, errors.New("object: New: nil Controller")
	}
	if treeFunc == nil {
		return nil, errors.New("object: New: nil TreeFunc")
	}
	o := &Object{
		signKey:        cfg.SignKey,
		codec:          NewCodec(),
		alloc:          cfg.Allocator,
		ctrl:           cfg.Controller,
		spaceId:        cfg.SpaceId,
		gate:           cfg.Gate,
		afterApply:     cfg.AfterApply,
		afterReplay:    cfg.AfterReplay,
		writeGate:      cfg.WriteGate,
		plaintextSpecs: cfg.PlaintextSpecs,
		onClose:        cfg.OnClose,
	}
	tree, err := treeFunc(o)
	if err != nil {
		return nil, err
	}
	if tree == nil {
		return nil, errors.New("object: New: TreeFunc returned nil tree")
	}
	o.tree = tree
	return o, nil
}

// Close detaches the tree listener, marks the Object closed, and
// closes the underlying tree. Blocks on tree.Lock — that's the same
// lock LocalWrite, the synchandler-driven Update, and the drain path
// all serialize on, so waiting here is the natural "let in-flight
// applies finish" barrier. Idempotent — second call is a no-op.
// ocache calls this on Remove and on cache shutdown.
//
// The listener is nilled before the tree closes so any Update /
// Rebuild that races the close (any-sync's tree may still hold this
// *Object in memory via SyncAll iteration past eviction) no-ops
// instead of driving a stale apply against a freshly-loaded peer
// Object.
//
// tree.Close drives any-sync's OnClose hook
// (objecttreebuilder.onClose → syncService.CloseReceiveQueue), which
// reaps the per-object multiqueue receive-queue goroutine. Without
// it that goroutine leaks for the process lifetime — eviction alone
// never reaches the synctree. tree.Close re-acquires tree.Lock
// internally, so it must run after the Unlock; the closed flag set
// under the lock guarantees exactly one caller reaches it.
//
// The close path also releases the controller's per-object collection
// handles (and fires cfg.OnClose for store-owned extras) — each open
// any-store handle pins planner sketches and caches, so a TTL-evicted
// object must not keep its `<objectId>_<dataset>` handles pinned for
// the process lifetime. Safe here: applies serialize on tree.Lock and
// check o.closed, so once the flag is set under the lock no apply can
// touch the controller's collections; unlocked readers re-open by
// name (see Controller.CloseOwnedCollections).
func (o *Object) Close() error {
	if o.tree == nil {
		if !o.closed {
			o.closed = true
			o.releaseResources()
		}
		return nil
	}
	o.tree.Lock()
	if o.closed {
		o.tree.Unlock()
		return nil
	}
	o.setListenerNilLocked()
	o.closed = true
	o.tree.Unlock()
	o.releaseResources()
	return o.tree.Close()
}

// TryClose is the non-blocking variant ocache GC uses. Returns
// (false, nil) when the tree is locked by an in-flight handler
// (LocalWrite, inbound synchandler, drain). ocache retries on the
// next tick. On success it closes the tree to reap the receive-queue
// goroutine — see Close for why the tree close runs unlocked and why
// collection handles are released here.
func (o *Object) TryClose(_ time.Duration) (bool, error) {
	if o.tree == nil {
		if !o.closed {
			o.closed = true
			o.releaseResources()
		}
		return true, nil
	}
	if !o.tree.TryLock() {
		return false, nil
	}
	if o.closed {
		o.tree.Unlock()
		return true, nil
	}
	o.setListenerNilLocked()
	o.closed = true
	o.tree.Unlock()
	o.releaseResources()
	return true, o.tree.Close()
}

// releaseResources drops the per-object any-store state the Object's
// residency pins: the controller's cached `<objectId>_<dataset>`
// collection handles, then the store's OnClose extras. Runs exactly
// once, from whichever close path flipped o.closed. Errors are logged,
// not returned — a failed handle close must not abort the eviction
// (the tree close and cache removal still have to happen).
func (o *Object) releaseResources() {
	if err := o.ctrl.CloseOwnedCollections(); err != nil {
		log.Warn("close owned collections", zap.String("objectId", o.Id()), zap.Error(err))
	}
	if o.onClose != nil {
		o.onClose()
	}
}

// setListenerNilLocked sets the synctree listener to nil. Caller
// must hold o.tree's lock. No-op when the underlying tree is not a
// synctree (e.g. tests with a plain objecttree mock).
func (o *Object) setListenerNilLocked() {
	if setter, ok := o.tree.(synctree.ListenerSetter); ok {
		setter.SetListener(nil)
	}
}

// ApplyDecoded applies a pre-stamped Change to the controller —
// bypasses the gate, used by the drain path when re-applying a
// previously-parked change whose dependencies have now landed.
// Takes tree.Lock (the apply-serializing mutex) and runs
// applyDecodedLocked. Drain operations may wait briefly on inbound
// sync — that's fine, drain is off the critical path.
//
// Rejects a closed Object like every other write entry point:
// post-close the controller's handles are released and
// collectionForWrite's open-by-name CREATES the collection if absent,
// so a drain landing on a just-closed (worse: just-purged) object
// would resurrect its `<objectId>_<dataset>` collection on disk. The
// drainer resolves objects via a fresh Store.Get per pass, so the
// retry lands on a live replacement.
func (o *Object) ApplyDecoded(ctx context.Context, ch crdt.Change) error {
	o.tree.Lock()
	defer o.tree.Unlock()
	if o.closed {
		return ErrClosed
	}
	// Defense-in-depth for the drain path: replayLocked already skips
	// non-allowlisted datasets on plaintext objects before parking, so
	// a parked change violating the allowlist shouldn't exist — but a
	// drain must never be the hole that materializes one.
	if err := checkPlaintextDataset(o.plaintextSpecFor(o.tree), ch.Dataset); err != nil {
		return err
	}
	_, err := o.applyDecodedLocked(ctx, ch)
	return err
}

// applyDecodedLocked is the unified per-change apply primitive.
// LocalWrite, replayLocked, and the drain path all reduce to this
// after they've prepared a Change with envelope filled (especially
// VersionId, which must come from any-sync's OrderId — see lexid.go
// docs). Caller must hold tree.Lock — that's the single mutex
// guarding every write into the Controller.
func (o *Object) applyDecodedLocked(ctx context.Context, ch crdt.Change) (crdt.ApplyResult, error) {
	if ch.VersionId == "" {
		return crdt.ApplyResult{}, errors.New("object: applyDecodedLocked requires pre-stamped VersionId (any-sync OrderId)")
	}
	o.stampObjectMeta(&ch)
	res, err := o.ctrl.ApplyChangeWithResult(ctx, ch)
	if err != nil {
		return res, err
	}
	if o.afterApply != nil {
		o.afterApply(ctx, o, &ch, &res)
	}
	return res, nil
}

// stampObjectMeta fills ch.ObjectAuthor / ObjectCreatedAt from the
// tree's root change, and ch.Creator from the per-change signer.
//
// ObjectAuthor / ObjectCreatedAt are constant across every change in
// a tree (the root is immutable + signed), so handlers that
// auto-stamp record-level fields like `author` and `createdAt` can
// rely on them regardless of which specific change is being applied.
//
// Creator comes from THIS change's any-sync envelope (the signer of
// the individual change, not the tree root) — looked up via
// tree.GetChange(ch.ChangeId). For the root change Creator and
// ObjectAuthor coincide; for subsequent changes in shared / multi-
// author objects they diverge.
//
// No-op when the tree isn't wired (tests / pre-bind drains), when
// the root has no Identity attached (derived trees in some flows),
// or when the change is not yet attached to the tree (hand-built
// changes in tests).
func (o *Object) stampObjectMeta(ch *crdt.Change) {
	o.stampObjectMetaFromTree(ch, o.tree)
}

// stampObjectMetaFromTree is the tree-explicit form of stampObjectMeta.
// Used by replayLocked when a tree-bound listener fires before
// SetTree has wired o.tree — the tree is in hand via the listener
// callback, but the Object hasn't been promoted to "the tree owner"
// yet. Without this path, replays during the build phase find
// o.tree == nil and silently skip Creator stamping, producing
// records whose author is missing on every joiner.
func (o *Object) stampObjectMetaFromTree(ch *crdt.Change, tree objecttree.ObjectTree) {
	if tree == nil {
		return
	}
	root := tree.Root()
	if root == nil {
		return
	}
	if ch.ObjectCreatedAt == 0 {
		ch.ObjectCreatedAt = root.Timestamp
	}
	if ch.ObjectAuthor == "" && root.Identity != nil {
		ch.ObjectAuthor = root.Identity.Account()
	}
	if (ch.Creator == "" || ch.PrevIds == nil) && ch.ChangeId != "" {
		if tc, err := tree.GetChange(ch.ChangeId); err == nil && tc != nil {
			if ch.Creator == "" && tc.Identity != nil {
				ch.Creator = tc.Identity.Account()
			}
			if ch.PrevIds == nil {
				ch.PrevIds = tc.PreviousIds
			}
		}
	}
}

// plaintextSpecFor resolves the object's plaintext class from the
// tree root's ChangeType. The tree is passed explicitly (not read from
// o.tree) because inbound replay may fire from the synctree build
// listener before o.tree is wired — same reason as
// stampObjectMetaFromTree. Returns nil for regular encrypted objects,
// a nil tree, or a root without change info (defensive: an
// unresolvable root must never silently downgrade to plaintext).
func (o *Object) plaintextSpecFor(tree objecttree.ObjectTree) *PlaintextSpec {
	if len(o.plaintextSpecs) == 0 || tree == nil {
		return nil
	}
	info := tree.ChangeInfo()
	if info == nil {
		return nil
	}
	if spec, ok := o.plaintextSpecs[info.ChangeType]; ok {
		return &spec
	}
	return nil
}

// checkPlaintextDataset returns the allowlist violation for writing
// dataset on a plaintext object, or nil when the write is fine (also
// for regular encrypted objects, spec == nil).
func checkPlaintextDataset(spec *PlaintextSpec, dataset string) error {
	if spec == nil {
		return nil
	}
	if _, ok := spec.Datasets[dataset]; ok {
		return nil
	}
	return fmt.Errorf("object: dataset %q is not allowed on a plaintext object — its changes ship unencrypted", dataset)
}

// Tree returns the bound tree, or nil if SetTree hasn't run.
func (o *Object) Tree() objecttree.ObjectTree { return o.tree }

// Controller returns the bound CRDT controller. Used for read paths
// that bypass the write-side locks (e.g. PropertiesAPI.Get hitting
// the controller's any-store collection directly).
func (o *Object) Controller() *crdt.Controller { return o.ctrl }

// Id returns the underlying tree id (= objectId), or "" if not set.
func (o *Object) Id() string {
	if o.tree == nil {
		return ""
	}
	return o.tree.Id()
}

// WriteResult bundles the identifiers a local write produces.
//
//   - VersionId is the peer-local lexid stamped onto the records.
//   - ChangeId is any-sync's content-addressable DAG change hash.
//   - RecordIds is the resolved id per record (post empty-id
//     resolution); for empty-id records it equals
//     base58(xxh3-64(ChangeId)) — the propId / shortId convention.
//   - Rejections lists per-op handler rejections — ops that the
//     change carries but the handler refused to apply (kind
//     mismatch, terminal status, immutable field, etc.). The change
//     still committed with a fresh VersionId, but those ops did
//     not land. Empty list means everything applied.
type WriteResult struct {
	VersionId  crdt.VersionId
	ChangeId   string
	RecordIds  []string
	Rejections []crdt.OpRejection
}

// LocalWrite applies a CRDT batch as a new tree change. Encodes the
// payload, signs and stores it via tree.AddContent, then runs the
// same per-change apply primitive the parked-replay drain uses
// (applyDecodedLocked) so there's a single apply path: any-sync
// generates the ChangeId/OrderId/AddSeq, the SDK stamps them onto
// the Change, and applyDecodedLocked lands it.
//
// Returns VersionId (= any-sync OrderId), ChangeId, and the resolved
// record ids. Errors from encode/AddContent/apply propagate; partial
// state is impossible because AddContent and ApplyChange each run in
// their own atomic step.
//
// Locking: tree.Lock is the single mutex guarding every write into
// the Controller. The sync-receiver path (synctree.AddRawChanges →
// Update/Rebuild → replayLocked) already runs under tree.Lock at
// the synctree layer; ColdRestore and ApplyDecoded acquire it
// themselves; LocalWrite acquires it here. tree.AddContent also
// requires the caller to hold tree.Lock — without it, any-sync
// logs "use tree when unlocked" at ERROR.
func (o *Object) LocalWrite(ctx context.Context, ch crdt.Change) (WriteResult, error) {
	if o.tree == nil {
		return WriteResult{}, ErrTreeNotSet
	}
	if o.writeGate != nil {
		if err := o.writeGate(); err != nil {
			return WriteResult{}, err
		}
	}

	// Pre-validate against the controller's structural rules
	// (DataVersion non-empty, dataset known, path syntax legal,
	// record-id resolution viable). A failure here keeps the
	// malformed change OUT of the any-sync DAG entirely — without
	// this, AddContent would happily sign and store it and only the
	// post-AddContent ApplyChange would reject, leaving a junk
	// change to ship to peers.
	if err := o.ctrl.ValidateChange(ch); err != nil {
		return WriteResult{}, fmt.Errorf("object: validate: %w", err)
	}

	o.tree.Lock()
	defer o.tree.Unlock()
	if o.closed {
		return WriteResult{}, ErrClosed
	}

	// Plaintext-class objects ship their changes UNencrypted, so only
	// the class's allowlisted datasets may enter the DAG — a write to
	// any other dataset (`objects` properties, an app dataset) would
	// leak its cleartext to the nodes. Hard error BEFORE AddContent:
	// nothing leaks and nothing junk syncs.
	spec := o.plaintextSpecFor(o.tree)
	if err := checkPlaintextDataset(spec, ch.Dataset); err != nil {
		return WriteResult{}, err
	}

	// Writer-side schema pre-flight: strict validation against the
	// current record state, BEFORE the change enters the DAG. A failure
	// here returns an agent-readable error and keeps the malformed
	// change out of any-sync entirely (no AddContent). Runs under
	// tree.Lock so the pre-state read and the apply below are
	// consistent with concurrent local writers. No-op for datasets
	// whose handler doesn't implement LocalPreValidator.
	if err := o.ctrl.PreValidateLocal(ctx, &ch); err != nil {
		return WriteResult{}, err
	}

	// Encode under tree.Lock — the codec's arena is shared with the
	// replay/Decode path and is not safe for concurrent use. Concurrent
	// LocalWrite callers (e.g. the background spaceIndex seeder racing
	// a foreground Modify) would otherwise corrupt the arena cache.
	payload, err := o.codec.Encode(&ch)
	if err != nil {
		return WriteResult{}, fmt.Errorf("object: encode: %w", err)
	}

	// Back-fill the apply-time timestamp before AddContent so the
	// downstream apply path (BeforeCreate / BeforeModify hooks) sees
	// the same value any-sync stamps on the wire. Mirrors how
	// replayLocked sets decoded.Timestamp = full.Timestamp on the
	// inbound side. ts() preserves caller-supplied positive values.
	ch.Timestamp = ts(ch.Timestamp)
	// Plaintext-class changes go out with ShouldBeEncrypted:false —
	// any-sync stamps ReadKeyId=="" and writes Data verbatim, which is
	// what lets a keyless reader (filenode-v2 broker) materialize them.
	res, err := o.tree.AddContent(ctx, objecttree.SignableChangeContent{
		Data:              payload,
		Key:               o.signKey,
		ShouldBeEncrypted: spec == nil,
		DataType:          ch.Dataset,
		Timestamp:         ch.Timestamp,
	})
	if err != nil {
		return WriteResult{}, fmt.Errorf("object: AddContent: %w", err)
	}
	if len(res.Added) == 0 {
		return WriteResult{}, errors.New("object: AddContent returned no changes")
	}

	// Fill envelope from any-sync's StorageChange. VersionId comes from
	// any-sync's own per-tree OrderId (lexid-monotonic, persisted by
	// any-sync) — NOT from the local allocator, which is reserved for
	// the future device-scope path. Using OrderId is what survives
	// process restart: any-sync owns the watermark.
	added := res.Added[0]
	ch.SpaceId = o.spaceId
	ch.ObjectId = o.tree.Id()
	ch.ChangeId = added.Id
	ch.AddSeq = added.AddSeq
	ch.VersionId = crdt.VersionId(added.OrderId)
	// ObjectAuthor / ObjectCreatedAt are stamped from the tree root in
	// applyDecodedLocked, so all paths (local write, inbound replay,
	// drain) agree on the same canonical values.

	// Resolve record ids before apply so the caller can correlate the
	// returned RecordIds with the input batch order even when the
	// apply itself produces no extra side effects.
	recordIds, idErr := crdt.ResolveRecordIds(ch)
	if idErr != nil {
		return WriteResult{}, fmt.Errorf("object: resolve record ids: %w", idErr)
	}

	// Apply through the same per-change primitive the drain path uses
	// — no gate (we own this change), no re-decode (we have it in
	// hand), no GetAfterAddSeq scan ("rebuild only this changeId").
	applyRes, applyErr := o.applyDecodedLocked(ctx, ch)
	if applyErr != nil {
		return WriteResult{}, fmt.Errorf("object: apply local: %w", applyErr)
	}
	return WriteResult{
		VersionId:  ch.VersionId,
		ChangeId:   ch.ChangeId,
		RecordIds:  recordIds,
		Rejections: applyRes.Rejections,
	}, nil
}

// LocalSet applies a device-local materialization: it writes only
// reserved local-namespace (crdt.LocalFieldPrefix) fields straight into
// the controller's materialised row — NO tree.AddContent, so nothing
// enters the any-sync DAG and nothing syncs to other devices. Each
// device computes its own value. The version is locally allocated
// (NextVersion of the field's current version), which is safe because
// no synced change ever writes a local-namespace path. Fires afterApply
// so Query/Subscribe see the change live, exactly like a synced write.
//
// Caller passes a Change with Dataset + Records (explicit ids, $set/$unset
// ops on local-prefixed paths). VersionId/ChangeId/AddSeq are ignored on
// input — VersionId is assigned here; the change never gets a ChangeId.
func (o *Object) LocalSet(ctx context.Context, ch crdt.Change) (WriteResult, error) {
	if o.tree == nil {
		return WriteResult{}, ErrTreeNotSet
	}
	o.tree.Lock()
	defer o.tree.Unlock()
	if o.closed {
		return WriteResult{}, ErrClosed
	}
	ch.Local = true
	ch.SpaceId = o.spaceId
	ch.ObjectId = o.tree.Id()
	ch.Timestamp = ts(ch.Timestamp)
	ch.VersionId = o.ctrl.NextLocalVersion(ctx, &ch)

	res, err := o.applyDecodedLocked(ctx, ch)
	if err != nil {
		return WriteResult{}, fmt.Errorf("object: local set: %w", err)
	}
	recordIds, _ := crdt.ResolveRecordIds(ch)
	return WriteResult{
		VersionId:  ch.VersionId,
		RecordIds:  recordIds,
		Rejections: res.Rejections,
	}, nil
}

// InjectedSet applies an account-mirror materialization: ops on
// account-class fields (or per-key-scoped dynamic heads) written
// straight into the controller's materialised row — NO tree.AddContent,
// nothing enters THIS object's any-sync DAG. Unlike LocalSet the
// VersionId is CALLER-SUPPLIED: the tech-space carrier tree's orderId
// for the change that produced the value, so per-path gating replays
// the carrier's converged order exactly. Safe because account paths
// have exactly one writer per device (the mirror) sourcing one tech
// tree. Fires afterApply so Query/Subscribe and the applySeq feed see
// the change live, exactly like every other apply.
//
// Caller passes a Change with Dataset + VersionId + Records (explicit
// ids, $set/$unset ops). ChangeId/AddSeq stay zero — the change never
// gets a DAG identity here; its provenance is the carrier change.
func (o *Object) InjectedSet(ctx context.Context, ch crdt.Change) (WriteResult, error) {
	if o.tree == nil {
		return WriteResult{}, ErrTreeNotSet
	}
	if ch.VersionId == "" {
		return WriteResult{}, errors.New("object: injected set requires a caller-supplied VersionId")
	}
	o.tree.Lock()
	defer o.tree.Unlock()
	if o.closed {
		return WriteResult{}, ErrClosed
	}
	ch.Injected = true
	ch.SpaceId = o.spaceId
	ch.ObjectId = o.tree.Id()
	ch.Timestamp = ts(ch.Timestamp)

	res, err := o.applyDecodedLocked(ctx, ch)
	if err != nil {
		return WriteResult{}, fmt.Errorf("object: injected set: %w", err)
	}
	recordIds, _ := crdt.ResolveRecordIds(ch)
	return WriteResult{
		VersionId:  ch.VersionId,
		RecordIds:  recordIds,
		Rejections: res.Rejections,
	}, nil
}

// Update implements updatelistener.UpdateListener. Fired by synctree
// from inside AddRawChanges / buildSyncTree, with the tree lock
// already held — we must NOT re-lock here.
//
// The tree must be opened with SetDeferredUpdater(true) — see
// spaceobjects/store.go:openTree. Without it, any-sync's default
// AddRawChangesWithUpdater order fires this listener BEFORE
// storage.AddAll, so replayLocked's IterateAfterAddSeq scan finds
// nothing new in storage and silently no-ops; tree heads advance
// while the controller stays out of sync.
func (o *Object) Update(tree objecttree.ObjectTree) error {
	return o.replayLocked(context.Background(), tree)
}

// Rebuild fires when synctree had to rescan from a snapshot, also
// with the tree lock held by the caller.
func (o *Object) Rebuild(tree objecttree.ObjectTree) error {
	return o.replayLocked(context.Background(), tree)
}

// ColdRestore replays everything from controller.MaxAddSeq forward.
// Run once at Open after SetTree. Acquires the tree lock — caller
// must NOT hold it.
func (o *Object) ColdRestore(ctx context.Context) error {
	if o.tree == nil {
		return ErrTreeNotSet
	}
	o.tree.Lock()
	defer o.tree.Unlock()
	return o.replayLocked(ctx, o.tree)
}

// replayLocked walks any-sync changes with AddSeq >
// controller.MaxAddSeq, decoding each payload, stamping its VersionId
// from any-sync's StorageChange.OrderId, and applying through the
// controller. The caller must hold tree.Lock — that's the single
// mutex guarding every write into the Controller, and it's already
// held by the synchandler-driven Update/Rebuild path and by
// ColdRestore (which takes it explicitly).
//
// Skips the root change (objectId == change.Id) — it carries no CRDT
// payload, just the tree header. Non-CRDT data types (e.g. settings
// changes injected by any-sync itself) decode-fail and skip silently.
//
// Uses tree.IterateAfterAddSeq so any-sync handles ReadKeyId-based
// decryption for us — going through the bare storage and reading
// Change.Data directly returns the encrypted payload, which the SDK
// codec can't parse (manifests as "unknown type N" anyenc errors and,
// crucially, silent drops of every inbound change on a fresh joiner /
// new device). The convert callback receives the already-decrypted
// bytes; we hand off the parsed Change to the iterate callback via
// Change.Model.
func (o *Object) replayLocked(ctx context.Context, tree objecttree.ObjectTree) error {
	if o.closed {
		// Stale callback after Close (listener detach race) — no-op.
		// The freshly-loaded peer Object owns subsequent applies.
		return nil
	}

	rootId := tree.Id()
	from := o.ctrl.MaxAddSeq()

	// Inbound side of the plaintext-class dataset allowlist: changes
	// on non-allowlisted datasets are skipped tolerantly (WARN, keep
	// iterating) — a peer must not be able to smuggle rows into
	// `objects` etc. through a plaintext tree. Local writes are the
	// strict side (LocalWrite hard-errors before AddContent).
	spec := o.plaintextSpecFor(tree)

	// captured outside the iterate closure so we can surface fatal
	// errors past IterateAfterAddSeq's bool return.
	var fatalErr error
	applied := 0

	convert := func(ch *objecttree.Change, decrypted []byte) (any, error) {
		// root has no CRDT payload by construction.
		if ch.Id == rootId {
			return nil, nil
		}
		if len(decrypted) == 0 {
			return nil, nil
		}
		decoded, decodeErr := o.codec.Decode(decrypted)
		if decodeErr != nil {
			// Non-CRDT payload (settings-tree change, foreign data
			// type, etc.) — skip silently. Returning the error here
			// would halt the entire iteration, which we explicitly
			// don't want; we want one bad row to not poison the rest.
			return nil, nil
		}
		// any-sync reuses `decrypted` across iterations, so anything we
		// retain past this callback (the parked-payload bytes for the
		// schema gate) must be cloned now.
		raw := append([]byte(nil), decrypted...)
		return &replayItem{decoded: decoded, raw: raw}, nil
	}

	iter := func(ch *objecttree.Change) bool {
		item, ok := ch.Model.(*replayItem)
		if !ok || item == nil {
			return true
		}
		decoded := item.decoded
		decoded.SpaceId = o.spaceId
		decoded.ObjectId = rootId
		decoded.ChangeId = ch.Id
		decoded.AddSeq = ch.AddSeq
		decoded.Timestamp = ch.Timestamp
		decoded.VersionId = crdt.VersionId(ch.OrderId)
		if err := checkPlaintextDataset(spec, decoded.Dataset); err != nil {
			log.Warn("skipping inbound change on plaintext object",
				zap.String("objectId", rootId),
				zap.String("changeId", ch.Id),
				zap.String("dataset", decoded.Dataset))
			return true
		}
		// Stamp ObjectAuthor / ObjectCreatedAt / Creator from the tree
		// we're iterating — applyDecodedLocked's o.stampObjectMeta
		// reads o.tree, which is nil when this replay fires from the
		// synctree's Update listener before SetTree has run (initial
		// build path on a fresh joiner). The decoded change here is
		// fully formed, so stamp it directly off the local `tree` and
		// the inner stampObjectMeta call no-ops on the already-filled
		// fields.
		o.stampObjectMetaFromTree(&decoded, tree)

		// Schema gate: a DataVersion referencing unknown shortIds
		// parks the change (proceed=false) — intentional skip, don't
		// advance our visibility of the watermark for this row but
		// keep iterating; the row goes into `_detached` and the
		// drainer picks it up later.
		//
		// gateErr means Park itself failed (disk, etc.) — abort the
		// iter so the watermark doesn't leapfrog this AddSeq. The
		// next replay (Update / Rebuild / boot) re-tries the whole
		// remainder.
		if o.gate != nil {
			proceed, gateErr := o.gate(ctx, &decoded, item.raw)
			if gateErr != nil {
				fatalErr = fmt.Errorf("object: gate %s: %w", ch.Id, gateErr)
				return false
			}
			if !proceed {
				return true
			}
		}

		// Same per-change primitive as LocalWrite and the drain path.
		// Apply errors here are infrastructure-level (any-store / disk)
		// — abort the iter so maxAddSeq doesn't leapfrog the failed
		// change. Per-op handler rejections (kind mismatch, etc.) are
		// surfaced inside ApplyResult.Rejections, NOT through err, and
		// don't halt the iter.
		if _, applyErr := o.applyDecodedLocked(ctx, decoded); applyErr != nil {
			fatalErr = fmt.Errorf("object: apply %s: %w", ch.Id, applyErr)
			return false
		}
		applied++
		return true
	}

	o.replaying = true
	iterErr := func() error {
		defer func() { o.replaying = false }()
		return tree.IterateAfterAddSeq(ctx, from, convert, iter)
	}()
	// Applied changes are committed whatever ended the iteration, so
	// the batch hook runs on the error paths too.
	if applied > 0 && o.afterReplay != nil {
		o.afterReplay(ctx, o)
	}
	if iterErr != nil {
		return iterErr
	}
	return fatalErr
}

// Replaying reports whether the caller is inside a replayLocked batch
// (inbound sync, cold restore, re-index rebuild) as opposed to a
// single LocalWrite / drained change. Meaningful only from the
// AfterApply hook, which runs under the same lock.
func (o *Object) Replaying() bool { return o.replaying }

// replayItem couples a decoded crdt.Change with the (cloned) decrypted
// bytes that produced it. The bytes are needed by the schema gate's
// Park path; the decoded Change is what applyDecodedLocked consumes.
type replayItem struct {
	decoded crdt.Change
	raw     []byte
}

// ts replaces a zero/negative timestamp with time.Now — matches the
// behavior any-sync's changeBuilder applies internally.
func ts(t int64) int64 {
	if t > 0 {
		return t
	}
	return time.Now().Unix()
}
