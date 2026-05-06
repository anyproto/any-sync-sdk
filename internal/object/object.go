package object

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/util/crypto"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// ErrTreeNotSet is returned when an Object method is called before
// SetTree. The constructor wires the tree post-build because the
// objecttree.UpdateListener (this Object) must exist before BuildTree
// can run.
var ErrTreeNotSet = errors.New("object: tree not set")

// Object binds one any-sync object tree to one crdt.Controller.
//
// Lifecycle:
//
//  1. NewObject — instantiate.
//  2. spaceService.TreeBuilder().BuildTree(ctx, id, BuildTreeOpts{Listener: obj})
//     to materialise the synctree with this Object as the update listener.
//  3. SetTree(tree) — caller wires the built tree back in.
//  4. ColdRestore(ctx) — replay any-sync changes the controller hasn't
//     seen yet (uses controller.MaxAddSeq watermark).
//
// After (4) the Object is live: LocalWrite produces new changes, and
// inbound sync calls fire Update/Rebuild which re-run the same replay
// path.
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
// changes whose missing shortIds may have just landed.
type AfterApply func(ctx context.Context, ch *crdt.Change)

type Object struct {
	signKey    crypto.PrivKey
	codec      *Codec
	alloc      *VersionAllocator
	ctrl       *crdt.Controller
	spaceId    string
	gate       ApplyGate
	afterApply AfterApply

	mu   sync.Mutex
	tree objecttree.ObjectTree
}

// NewObject returns an Object ready to be wired as an UpdateListener.
// The tree is set post-build via SetTree (BuildTree needs us as the
// listener at construction time).
func NewObject(spaceId string, signKey crypto.PrivKey, ctrl *crdt.Controller, alloc *VersionAllocator) *Object {
	return &Object{
		signKey: signKey,
		codec:   NewCodec(),
		alloc:   alloc,
		ctrl:    ctrl,
		spaceId: spaceId,
	}
}

// SetTree binds the built any-sync ObjectTree.
func (o *Object) SetTree(tree objecttree.ObjectTree) { o.tree = tree }

// SetGate wires the optional DataVersion gate. nil disables gating
// (all changes apply directly).
func (o *Object) SetGate(g ApplyGate) { o.gate = g }

// SetAfterApply wires a post-apply hook. nil disables.
func (o *Object) SetAfterApply(h AfterApply) { o.afterApply = h }

// ApplyDecoded applies a pre-stamped Change to the controller —
// bypasses the gate, used by the drain path when re-applying a
// previously-parked change whose dependencies have now landed.
// Thin wrapper around applyDecodedLocked that takes o.mu.
func (o *Object) ApplyDecoded(ctx context.Context, ch crdt.Change) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	_, err := o.applyDecodedLocked(ctx, ch)
	return err
}

// applyDecodedLocked is the unified per-change apply primitive.
// LocalWrite, replayLocked, and the drain path all reduce to this
// after they've prepared a Change with envelope filled (especially
// VersionId, which must come from any-sync's OrderId — see lexid.go
// docs). Caller must hold o.mu.
//
// The tree lock is NOT touched — this only writes to the
// controller's any-store collections.
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
		o.afterApply(ctx, &ch)
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
	if o.tree == nil {
		return
	}
	root := o.tree.Root()
	if root == nil {
		return
	}
	if ch.ObjectCreatedAt == 0 {
		ch.ObjectCreatedAt = root.Timestamp
	}
	if ch.ObjectAuthor == "" && root.Identity != nil {
		ch.ObjectAuthor = root.Identity.Account()
	}
	if ch.Creator == "" && ch.ChangeId != "" {
		if tc, err := o.tree.GetChange(ch.ChangeId); err == nil && tc != nil && tc.Identity != nil {
			ch.Creator = tc.Identity.Account()
		}
	}
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
// Locking: tree.Lock first, then o.mu. The sync-receiver path
// (synctree.AddRawChanges → Update/Rebuild → replayLocked) acquires
// the tree lock at the synctree layer and then takes o.mu inside
// replayLocked, so following the same order here avoids AB-BA
// deadlocks. tree.AddContent expects the caller to hold the tree
// lock — without it, any-sync logs "use tree when unlocked" at ERROR.
func (o *Object) LocalWrite(ctx context.Context, ch crdt.Change) (WriteResult, error) {
	if o.tree == nil {
		return WriteResult{}, ErrTreeNotSet
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

	payload, err := o.codec.Encode(&ch)
	if err != nil {
		return WriteResult{}, fmt.Errorf("object: encode: %w", err)
	}

	o.tree.Lock()
	defer o.tree.Unlock()
	o.mu.Lock()
	defer o.mu.Unlock()

	// Back-fill the apply-time timestamp before AddContent so the
	// downstream apply path (BeforeCreate / BeforeModify hooks) sees
	// the same value any-sync stamps on the wire. Mirrors how
	// replayLocked sets decoded.Timestamp = full.Timestamp on the
	// inbound side. ts() preserves caller-supplied positive values.
	ch.Timestamp = ts(ch.Timestamp)
	res, err := o.tree.AddContent(ctx, objecttree.SignableChangeContent{
		Data:              payload,
		Key:               o.signKey,
		ShouldBeEncrypted: true,
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

// Update implements updatelistener.UpdateListener. Fired by synctree
// from inside AddRawChanges / buildSyncTree, with the tree lock
// already held — we must NOT re-lock here.
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
// controller. The caller must hold the tree lock; o.mu serialises
// against concurrent local writes.
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
	o.mu.Lock()
	defer o.mu.Unlock()

	rootId := tree.Id()
	from := o.ctrl.MaxAddSeq()

	// captured outside the iterate closure so we can surface fatal
	// errors past IterateAfterAddSeq's bool return.
	var fatalErr error

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
		// ObjectAuthor / ObjectCreatedAt come from the tree root via
		// stampObjectMeta inside applyDecodedLocked.

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
		return true
	}

	if err := tree.IterateAfterAddSeq(ctx, from, convert, iter); err != nil {
		return err
	}
	return fatalErr
}

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
