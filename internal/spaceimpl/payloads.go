package spaceimpl

import (
	"context"
	"errors"
	"fmt"
	"sync"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/util/crypto"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/payloads"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/space"
)

// PayloadsAPI is the SDK-internal surface over the per-owner file
// `payloads` objects (SYN-21). Consumed by the upload path (SYN-27);
// a public Files API arrives with SYN-30.
//
// The per-owner payloads object is created LAZILY: RegisterFile
// derives it on first use; read paths resolve the deterministic
// derived id and treat a missing tree as "no rows" — they never
// create it. Owners with no files never grow a child tree.
type PayloadsAPI struct {
	s    *spaceImpl
	keys *aclKeyProvider
}

// PayloadsInternal returns the internal payloads surface (typed rows,
// register/sign writes). Not part of the public space.Space interface —
// the public read-only view is Space.Payloads (payloads_view.go).
func (s *spaceImpl) PayloadsInternal() *PayloadsAPI {
	return &PayloadsAPI{s: s, keys: &aclKeyProvider{s: s}}
}

// RegisterFileOpts describes one file registration.
type RegisterFileOpts struct {
	// RootCid is the UnixFS root of the encrypted file. Empty selects
	// the inline tier (bytes in Enc.Inline, no S3, no sign).
	RootCid string
	// Size is the plaintext byte size.
	Size int64
	// NetworkSign optionally carries an existing durable receipt at
	// create time (a BIND reusing already-durable content).
	NetworkSign string
	// Enc is the member-only plaintext this call seals.
	Enc payloads.EncPayload
}

// payloadsDeriveOpts is the one true derivation of a payloads object —
// every caller must agree on every field (Unencrypted included) or
// the derived ids diverge.
func payloadsDeriveOpts(ownerId string) spaceobjects.DeriveOpts {
	return spaceobjects.DeriveOpts{
		ChangeType:    payloads.ChangeType,
		ChangePayload: []byte(payloads.WellKnownDeriveSeed),
		ParentId:      ownerId,
		Unencrypted:   true,
	}
}

// ObjectId resolves the deterministic payloads-object id for an owner
// without creating anything.
func (p *PayloadsAPI) ObjectId(ctx context.Context, ownerId string) (string, error) {
	if ownerId == "" {
		return "", errors.New("payloads: ownerId required")
	}
	return p.s.store.DeriveId(ctx, payloadsDeriveOpts(ownerId))
}

// RegisterFile is the single-file convenience over RegisterFiles.
func (p *PayloadsAPI) RegisterFile(ctx context.Context, ownerId string, opts RegisterFileOpts) (fileId, payloadsObjId string, err error) {
	fileIds, payloadsObjId, err := p.RegisterFiles(ctx, ownerId, []RegisterFileOpts{opts})
	if err != nil {
		return "", "", err
	}
	return fileIds[0], payloadsObjId, nil
}

// RegisterFiles seals each file's Enc under the current space key and
// writes all rows into the owner's payloads object as ONE change (one
// DAG entry, one sync hop — bulk flows like a pasted doc with hundreds
// of images register in minimum hops), lazily deriving the object on
// first use. Returns the fileIds in input order (derived from the
// creating change: seed, seed:1, …) and the payloads objectId.
func (p *PayloadsAPI) RegisterFiles(ctx context.Context, ownerId string, files []RegisterFileOpts) (fileIds []string, payloadsObjId string, err error) {
	if ownerId == "" {
		return nil, "", errors.New("payloads: ownerId required")
	}
	if len(files) == 0 {
		return nil, "", errors.New("payloads: no files")
	}
	for i, opts := range files {
		if opts.Size < 0 {
			return nil, "", fmt.Errorf("payloads: file %d: negative size", i)
		}
		// Inline XOR — enforced here because only the pre-seal caller
		// can see the plaintext: inline bytes ⇔ no rootCid, exact
		// size, under the cutoff, never signed; a rootCid row carries
		// no inline bytes.
		if opts.RootCid == "" {
			if int64(len(opts.Enc.Inline)) != opts.Size {
				return nil, "", fmt.Errorf("payloads: file %d: inline row size %d != len(inline) %d", i, opts.Size, len(opts.Enc.Inline))
			}
			if opts.Size >= payloads.InlineMaxSize {
				return nil, "", fmt.Errorf("payloads: file %d: inline row must be < %d bytes, got %d (upload it and register with a rootCid)", i, payloads.InlineMaxSize, opts.Size)
			}
			if opts.NetworkSign != "" {
				return nil, "", fmt.Errorf("payloads: file %d: inline rows are never signed", i)
			}
		} else if len(opts.Enc.Inline) > 0 {
			return nil, "", fmt.Errorf("payloads: file %d: a rootCid row must not carry inline bytes", i)
		}
	}

	kid, key, err := p.keys.CurrentKey(ctx)
	if err != nil {
		return nil, "", err
	}

	// Lazy ensure: Derive is idempotent (deterministic id, per-id
	// load lock), so first-use creation and reuse are the same call.
	obj, err := p.s.store.Derive(ctx, payloadsDeriveOpts(ownerId))
	if err != nil {
		return nil, "", fmt.Errorf("payloads: derive payloads object: %w", err)
	}

	arena := &anyenc.Arena{}
	records := make([]crdt.RecordChange, 0, len(files))
	for _, opts := range files {
		ct, sealErr := payloads.SealEnc(key, opts.Enc)
		if sealErr != nil {
			return nil, "", sealErr
		}
		row := arena.NewObject()
		if opts.RootCid != "" {
			row.Set(payloads.FieldRootCid, arena.NewString(opts.RootCid))
		}
		row.Set(payloads.FieldSize, arena.NewNumberFloat64(float64(opts.Size)))
		row.Set(payloads.FieldObjectId, arena.NewString(ownerId))
		if opts.NetworkSign != "" {
			row.Set(payloads.FieldNetworkSign, arena.NewString(opts.NetworkSign))
		}
		enc := arena.NewObject()
		enc.Set(payloads.EncKeyId, arena.NewString(kid))
		enc.Set(payloads.EncKeyCiphertext, arena.NewBinary(ct))
		row.Set(payloads.FieldEnc, enc)
		// Empty id: the fileId derives from the creating changeId —
		// deterministic, content-addressed, never reused.
		records = append(records, crdt.RecordChange{
			Upsert: true,
			Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: row}},
		})
	}

	res, err := obj.LocalWrite(ctx, crdt.Change{
		Dataset:     payloads.Dataset,
		DataVersion: payloads.HandlerVersion,
		Records:     records,
	})
	if err != nil {
		return nil, "", err
	}
	if len(res.RecordIds) != len(files) {
		return nil, "", fmt.Errorf("payloads: write produced %d record ids for %d files", len(res.RecordIds), len(files))
	}
	return res.RecordIds, obj.Id(), nil
}

// SetNetworkSign is the single-file convenience over SetNetworkSigns.
func (p *PayloadsAPI) SetNetworkSign(ctx context.Context, ownerId, fileId, networkSign string) error {
	return p.SetNetworkSigns(ctx, ownerId, map[string]string{fileId: networkSign})
}

// SetNetworkSigns records the node's durable receipts on existing rows
// as ONE change (fileId → networkSign) — the sign phase of a bulk
// upload lands in one hop. The payloads object must already exist; a
// sign for a file that was never registered is a caller bug.
func (p *PayloadsAPI) SetNetworkSigns(ctx context.Context, ownerId string, signs map[string]string) error {
	if len(signs) == 0 {
		return errors.New("payloads: no signs")
	}
	for fileId, sign := range signs {
		if fileId == "" || sign == "" {
			return errors.New("payloads: fileId and networkSign required")
		}
	}
	obj, ok, err := p.existingObject(ctx, ownerId)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("payloads: owner %s has no payloads object", ownerId)
	}
	arena := &anyenc.Arena{}
	records := make([]crdt.RecordChange, 0, len(signs))
	for fileId, sign := range signs {
		records = append(records, crdt.RecordChange{
			Id: fileId,
			Ops: []crdt.Op{{
				Type:    crdt.OpSet,
				Path:    []string{payloads.FieldNetworkSign},
				Payload: arena.NewString(sign),
			}},
		})
	}
	_, err = obj.LocalWrite(ctx, crdt.Change{
		Dataset:     payloads.Dataset,
		DataVersion: payloads.HandlerVersion,
		Records:     records,
	})
	return err
}

// GetRow returns one typed row, unsealed when the caller holds the
// space key (Sealed stays true otherwise). space.ErrNotFound when the
// owner has no payloads object, no such row, or a tombstoned row.
func (p *PayloadsAPI) GetRow(ctx context.Context, ownerId, fileId string) (payloads.Row, error) {
	objId, ok, err := p.existingObjectId(ctx, ownerId)
	if err != nil {
		return payloads.Row{}, err
	}
	if !ok {
		return payloads.Row{}, space.ErrNotFound
	}
	return p.getRowIn(ctx, objId, fileId)
}

// getRowIn reads one row directly from a payloads object (already
// resolved — the bare-fileId lookup path scans objects without knowing
// the owner). space.ErrNotFound for a missing or tombstoned row.
func (p *PayloadsAPI) getRowIn(ctx context.Context, payloadsObjId, fileId string) (payloads.Row, error) {
	coll, err := resolveCollection(ctx, p.s.store, payloadsObjId, payloads.Dataset)
	if err != nil {
		return payloads.Row{}, err
	}
	if coll == nil {
		return payloads.Row{}, space.ErrNotFound
	}
	doc, err := coll.FindId(ctx, fileId)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return payloads.Row{}, space.ErrNotFound
		}
		return payloads.Row{}, err
	}
	v := doc.Value()
	if v.Get(crdt.DeletedAtField) != nil {
		return payloads.Row{}, space.ErrNotFound
	}
	return p.rowFromValue(ctx, v)
}

// FindRow resolves a bare fileId to its row by scanning the space's
// payloads objects (locally we always saw the row; the network stays
// unindexed). The SYN-30 queryable files view will subsume this scan.
func (p *PayloadsAPI) FindRow(ctx context.Context, fileId string) (payloads.Row, error) {
	if fileId == "" {
		return payloads.Row{}, errors.New("payloads: fileId required")
	}
	objIds, err := p.s.store.TreeIdsByChangeType(ctx, payloads.ChangeType)
	if err != nil {
		return payloads.Row{}, err
	}
	for _, objId := range objIds {
		row, err := p.getRowIn(ctx, objId, fileId)
		if err == nil {
			return row, nil
		}
		if !errors.Is(err, space.ErrNotFound) {
			return payloads.Row{}, err
		}
	}
	return payloads.Row{}, space.ErrNotFound
}

// ListRows returns every live row of the owner's payloads object,
// unsealed when possible. Empty (nil) when the object doesn't exist.
func (p *PayloadsAPI) ListRows(ctx context.Context, ownerId string) ([]payloads.Row, error) {
	objId, ok, err := p.existingObjectId(ctx, ownerId)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	vals, err := newQuery(p.s.store, objId, payloads.Dataset).All(ctx)
	if err != nil {
		return nil, err
	}
	rows := make([]payloads.Row, 0, len(vals))
	for _, v := range vals {
		row, err := p.rowFromValue(ctx, v)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// rowFromValue decodes and best-effort-unseals one row. An ErrNoKey
// unseal keeps the row sealed (the keyless-reader view); a corrupt
// blob propagates.
func (p *PayloadsAPI) rowFromValue(ctx context.Context, v *anyenc.Value) (payloads.Row, error) {
	row, err := payloads.RowFromValue(v)
	if err != nil {
		return payloads.Row{}, err
	}
	if err := row.Unseal(ctx, p.keys); err != nil {
		return payloads.Row{}, err
	}
	return row, nil
}

// existingObjectId resolves the owner's payloads object id and whether
// its tree exists locally — read paths never create it.
func (p *PayloadsAPI) existingObjectId(ctx context.Context, ownerId string) (string, bool, error) {
	if ownerId == "" {
		return "", false, errors.New("payloads: ownerId required")
	}
	objId, err := p.s.store.DeriveId(ctx, payloadsDeriveOpts(ownerId))
	if err != nil {
		return "", false, err
	}
	ok, err := p.s.store.HasTree(ctx, objId)
	if err != nil {
		return "", false, err
	}
	return objId, ok, nil
}

// existingObject loads the owner's payloads object if its tree exists
// locally; (nil, false, nil) when it doesn't.
func (p *PayloadsAPI) existingObject(ctx context.Context, ownerId string) (*object.Object, bool, error) {
	objId, ok, err := p.existingObjectId(ctx, ownerId)
	if err != nil || !ok {
		return nil, false, err
	}
	obj, err := p.s.store.Get(ctx, objId)
	if err != nil {
		return nil, false, err
	}
	return obj, true, nil
}

// aclKeyProvider is the ACL-backed payloads.KeyProvider: kid = the
// ACL key-record id of the read key, key = the payloads enc key
// derived from it (cached per kid — derivation is deterministic).
// Returns payloads.ErrNoKey when the local identity has no read
// access (the keyless-reader view), so typed reads degrade to
// cleartext-only rows instead of failing.
type aclKeyProvider struct {
	s *spaceImpl

	mu    sync.Mutex
	cache map[string]crypto.SymKey
}

func (p *aclKeyProvider) CurrentKey(ctx context.Context) (string, crypto.SymKey, error) {
	acl, err := p.aclList(ctx)
	if err != nil {
		return "", nil, err
	}
	acl.RLock()
	state := acl.AclState()
	kid := state.CurrentReadKeyId()
	readKey, err := state.CurrentReadKey()
	acl.RUnlock()
	if err != nil || readKey == nil {
		return "", nil, payloads.ErrNoKey
	}
	key, err := p.derived(kid, readKey)
	if err != nil {
		return "", nil, err
	}
	return kid, key, nil
}

func (p *aclKeyProvider) KeyById(ctx context.Context, kid string) (crypto.SymKey, error) {
	p.mu.Lock()
	if key, ok := p.cache[kid]; ok {
		p.mu.Unlock()
		return key, nil
	}
	p.mu.Unlock()
	acl, err := p.aclList(ctx)
	if err != nil {
		return nil, err
	}
	acl.RLock()
	keys, ok := acl.AclState().Keys()[kid]
	acl.RUnlock()
	if !ok || keys.ReadKey == nil {
		return nil, payloads.ErrNoKey
	}
	return p.derived(kid, keys.ReadKey)
}

func (p *aclKeyProvider) aclList(ctx context.Context) (list.AclList, error) {
	handle, err := p.s.app.GetSpace(ctx, p.s.id)
	if err != nil {
		return nil, fmt.Errorf("payloads: load space: %w", err)
	}
	acl := handle.Inner().Acl()
	if acl == nil {
		return nil, payloads.ErrNoKey
	}
	return acl, nil
}

func (p *aclKeyProvider) derived(kid string, readKey crypto.SymKey) (crypto.SymKey, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if key, ok := p.cache[kid]; ok {
		return key, nil
	}
	key, err := payloads.DeriveEncKey(readKey)
	if err != nil {
		return nil, err
	}
	if p.cache == nil {
		p.cache = make(map[string]crypto.SymKey)
	}
	p.cache[kid] = key
	return key, nil
}
