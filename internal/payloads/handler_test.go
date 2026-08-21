package payloads

import (
	"context"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// newPayloadsController builds a Controller with only the payloads
// dataset registered — the keyless-by-construction materialization
// harness: no ACL, no keys, no tree; exactly what a broker replaying
// cleartext changes runs.
func newPayloadsController(t *testing.T) *crdt.Controller {
	t.Helper()
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "payloads.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ctrl, err := crdt.NewController(ctx, "payloads-obj", db,
		crdt.HandlerReg{Name: Dataset, Handler: Handler{}, Schema: Schema()})
	require.NoError(t, err)
	return ctrl
}

// createRowPayload builds the register-file multi-field $set object.
func createRowPayload(a *anyenc.Arena, rootCid string, size int, sign string, kid string, ct []byte) *anyenc.Value {
	row := a.NewObject()
	if rootCid != "" {
		row.Set(FieldRootCid, a.NewString(rootCid))
	}
	row.Set(FieldSize, a.NewNumberInt(size))
	if sign != "" {
		row.Set(FieldNetworkSign, a.NewString(sign))
	}
	enc := a.NewObject()
	enc.Set(EncKeyId, a.NewString(kid))
	enc.Set(EncKeyCiphertext, a.NewBinary(ct))
	row.Set(FieldEnc, enc)
	return row
}

func createChange(a *anyenc.Arena, changeId, version, creator string, payload *anyenc.Value) crdt.Change {
	return crdt.Change{
		ObjectId:    "payloads-obj",
		Dataset:     Dataset,
		DataVersion: HandlerVersion,
		ChangeId:    changeId,
		VersionId:   crdt.VersionId(version),
		Creator:     creator,
		Records: []crdt.RecordChange{{
			Upsert: true,
			Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: payload}},
		}},
	}
}

// TestApply_KeylessMaterializationAndUnseal is the SYN-21 acceptance
// core: a change replayed with NO keys materializes the cleartext
// fields (rootCid/size/networkSign/author) and keeps enc sealed; a
// keyed reader then unseals the same row.
func TestApply_KeylessMaterializationAndUnseal(t *testing.T) {
	ctx := context.Background()
	ctrl := newPayloadsController(t)
	kp := newFakeProvider(t, "kid-1")
	_, key, err := kp.CurrentKey(ctx)
	require.NoError(t, err)
	ct, err := SealEnc(key, testEncPayload())
	require.NoError(t, err)

	a := &anyenc.Arena{}
	ch := createChange(a, "chA", "v1", "author-acc", createRowPayload(a, "bafyroot", 12345, "", "kid-1", ct))
	res, err := ctrl.ApplyChangeWithResult(ctx, ch)
	require.NoError(t, err)
	require.Empty(t, res.Rejections)

	fileId := crdt.DeriveRecordId("chA")
	v := ctrl.Get(ctx, Dataset, fileId)
	require.NotNil(t, v, "row must materialize without any key")

	row, err := RowFromValue(v)
	require.NoError(t, err)
	assert.Equal(t, fileId, row.Id)
	assert.Equal(t, "bafyroot", row.RootCid)
	assert.Equal(t, int64(12345), row.Size)
	assert.Equal(t, "author-acc", row.Author, "derived author must stamp from the change Creator")
	assert.True(t, row.Sealed, "enc stays sealed at rest")
	assert.Equal(t, "kid-1", row.EncKid)

	// Keyed reader: unseal succeeds.
	require.NoError(t, row.Unseal(ctx, kp))
	assert.False(t, row.Sealed)
	assert.Equal(t, testEncPayload(), row.Enc)

	// Keyless reader: same row stays sealed, cleartext intact, no error.
	sealedRow, err := RowFromValue(v)
	require.NoError(t, err)
	require.NoError(t, sealedRow.Unseal(ctx, newFakeProvider(t)))
	assert.True(t, sealedRow.Sealed)
	assert.Equal(t, "bafyroot", sealedRow.RootCid)
}

// TestApply_NetworkSignFlow: a sign lands on an existing rootCid row;
// immutable fields reject per-op; an inline row never accepts a sign.
func TestApply_NetworkSignFlow(t *testing.T) {
	ctx := context.Background()
	ctrl := newPayloadsController(t)
	kp := newFakeProvider(t, "kid-1")
	_, key, err := kp.CurrentKey(ctx)
	require.NoError(t, err)
	ct, err := SealEnc(key, testEncPayload())
	require.NoError(t, err)

	a := &anyenc.Arena{}
	// One S3-backed row, one inline row.
	_, err = ctrl.ApplyChangeWithResult(ctx, createChange(a, "chA", "v1", "acc", createRowPayload(a, "bafyroot", 5000, "", "kid-1", ct)))
	require.NoError(t, err)
	ctInline, err := SealEnc(key, EncPayload{Name: "t.txt", Inline: []byte("abc")})
	require.NoError(t, err)
	_, err = ctrl.ApplyChangeWithResult(ctx, createChange(a, "chB", "v2", "acc", createRowPayload(a, "", 3, "", "kid-1", ctInline)))
	require.NoError(t, err)

	rootRowId := crdt.DeriveRecordId("chA")
	inlineRowId := crdt.DeriveRecordId("chB")

	setField := func(version, rowId, field string, val *anyenc.Value) crdt.ApplyResult {
		res, err := ctrl.ApplyChangeWithResult(ctx, crdt.Change{
			ObjectId: "payloads-obj", Dataset: Dataset, DataVersion: HandlerVersion,
			ChangeId: "ch-" + version, VersionId: crdt.VersionId(version),
			Records: []crdt.RecordChange{{
				Id:  rowId,
				Ops: []crdt.Op{{Type: crdt.OpSet, Path: []string{field}, Payload: val}},
			}},
		})
		require.NoError(t, err)
		return res
	}

	// Sign on the rootCid row applies.
	res := setField("v3", rootRowId, FieldNetworkSign, a.NewString("net1/sig-of-bafyroot"))
	require.Empty(t, res.Rejections)
	row, err := RowFromValue(ctrl.Get(ctx, Dataset, rootRowId))
	require.NoError(t, err)
	assert.Equal(t, "net1/sig-of-bafyroot", row.NetworkSign)

	// Sign on the inline row drops (no rootCid — never signed).
	res = setField("v4", inlineRowId, FieldNetworkSign, a.NewString("net1/sig"))
	assert.NotEmpty(t, res.Rejections, "sign on an inline row must be rejected")
	row, err = RowFromValue(ctrl.Get(ctx, Dataset, inlineRowId))
	require.NoError(t, err)
	assert.Empty(t, row.NetworkSign)

	// Immutable fields reject on existing rows.
	for field, val := range map[string]*anyenc.Value{
		FieldRootCid: a.NewString("bafyother"),
		FieldSize:    a.NewNumberInt(1),
	} {
		res = setField("v5"+field, rootRowId, field, val)
		assert.NotEmpty(t, res.Rejections, "%s must be immutable", field)
	}
	row, err = RowFromValue(ctrl.Get(ctx, Dataset, rootRowId))
	require.NoError(t, err)
	assert.Equal(t, "bafyroot", row.RootCid)
	assert.Equal(t, int64(5000), row.Size)

	// Derived author and undeclared fields reject as input ops.
	res = setField("v6", rootRowId, FieldAuthor, a.NewString("mallory"))
	assert.NotEmpty(t, res.Rejections, "author is derived, not writable")
	res = setField("v7", rootRowId, "bogus", a.NewString("x"))
	assert.NotEmpty(t, res.Rejections, "undeclared field on a non-Dynamic dataset")
}

// TestApply_BatchCreate pins the batch shape: N files in one change,
// each empty-id record resolving to the derived seed with a digit
// suffix (seed, seed:1, …), all materializing independently.
func TestApply_BatchCreate(t *testing.T) {
	ctx := context.Background()
	ctrl := newPayloadsController(t)
	kp := newFakeProvider(t, "kid-1")
	_, key, err := kp.CurrentKey(ctx)
	require.NoError(t, err)
	ct, err := SealEnc(key, testEncPayload())
	require.NoError(t, err)

	a := &anyenc.Arena{}
	ch := crdt.Change{
		ObjectId: "payloads-obj", Dataset: Dataset, DataVersion: HandlerVersion,
		ChangeId: "chBatch", VersionId: "v1", Creator: "acc",
		Records: []crdt.RecordChange{
			{Upsert: true, Ops: []crdt.Op{{Type: crdt.OpSet, Payload: createRowPayload(a, "bafyone", 100, "", "kid-1", ct)}}},
			{Upsert: true, Ops: []crdt.Op{{Type: crdt.OpSet, Payload: createRowPayload(a, "bafytwo", 200, "", "kid-1", ct)}}},
		},
	}
	require.NoError(t, Handler{}.PreValidateMulti(&ch, nil))
	res, err := ctrl.ApplyChangeWithResult(ctx, ch)
	require.NoError(t, err)
	require.Empty(t, res.Rejections)

	seed := crdt.DeriveRecordId("chBatch")
	first, err := RowFromValue(ctrl.Get(ctx, Dataset, seed))
	require.NoError(t, err)
	assert.Equal(t, "bafyone", first.RootCid)
	second, err := RowFromValue(ctrl.Get(ctx, Dataset, seed+":1"))
	require.NoError(t, err)
	assert.Equal(t, "bafytwo", second.RootCid)
	assert.Equal(t, "acc", second.Author)
}

// TestPreValidate_Shapes pins the strict local write gate.
func TestPreValidate_Shapes(t *testing.T) {
	a := &anyenc.Arena{}
	h := Handler{}
	kid, ct := "kid-1", []byte("ciphertext")

	// rows the fake pre-state getter knows about.
	rootRow := a.NewObject()
	rootRow.Set(FieldRootCid, a.NewString("bafyroot"))
	inlineRow := a.NewObject()
	rows := map[string]*anyenc.Value{"file-1": rootRow, "file-2": rootRow, "file-inline": inlineRow}
	get := func(id string) *anyenc.Value { return rows[id] }

	valid := func(payload *anyenc.Value) crdt.Change {
		return crdt.Change{
			Dataset: Dataset, DataVersion: HandlerVersion,
			Records: []crdt.RecordChange{{Upsert: true, Ops: []crdt.Op{{Type: crdt.OpSet, Payload: payload}}}},
		}
	}

	// Valid S3-backed create.
	require.NoError(t, h.PreValidateMulti(ptr(valid(createRowPayload(a, "bafyroot", 100, "", kid, ct))), nil))
	// Valid BIND create (sign at create).
	require.NoError(t, h.PreValidateMulti(ptr(valid(createRowPayload(a, "bafyroot", 100, "net/sig", kid, ct))), nil))
	// Valid inline create.
	require.NoError(t, h.PreValidateMulti(ptr(valid(createRowPayload(a, "", 100, "", kid, ct))), nil))

	// Inline too big.
	assert.Error(t, h.PreValidateMulti(ptr(valid(createRowPayload(a, "", InlineMaxSize, "", kid, ct))), nil))
	// Sign without rootCid.
	assert.Error(t, h.PreValidateMulti(ptr(valid(createRowPayload(a, "", 100, "net/sig", kid, ct))), nil))
	// Missing enc.
	noEnc := a.NewObject()
	noEnc.Set(FieldSize, a.NewNumberInt(1))
	assert.Error(t, h.PreValidateMulti(ptr(valid(noEnc)), nil))
	// Missing size.
	noSize := a.NewObject()
	enc := a.NewObject()
	enc.Set(EncKeyId, a.NewString(kid))
	enc.Set(EncKeyCiphertext, a.NewBinary(ct))
	noSize.Set(FieldEnc, enc)
	assert.Error(t, h.PreValidateMulti(ptr(valid(noSize)), nil))
	// Unknown field in the create payload.
	unknown := createRowPayload(a, "bafyroot", 100, "", kid, ct)
	unknown.Set("bogus", a.NewString("x"))
	assert.Error(t, h.PreValidateMulti(ptr(valid(unknown)), nil))

	// Create must use empty id + upsert.
	withId := valid(createRowPayload(a, "bafyroot", 100, "", kid, ct))
	withId.Records[0].Id = "explicit"
	assert.Error(t, h.PreValidateMulti(&withId, nil))

	// Batch create: N empty-id upsert creates in one change are fine.
	multi := valid(createRowPayload(a, "bafyroot", 100, "", kid, ct))
	multi.Records = append(multi.Records, multi.Records[0])
	require.NoError(t, h.PreValidateMulti(&multi, nil))

	// Mixed shapes in one change are not.
	mixed := valid(createRowPayload(a, "bafyroot", 100, "", kid, ct))
	mixed.Records = append(mixed.Records, crdt.RecordChange{Id: "f1", Ops: []crdt.Op{{Type: crdt.OpDelete}}})
	assert.Error(t, h.PreValidateMulti(&mixed, nil))

	// One op per record.
	twoOps := valid(createRowPayload(a, "bafyroot", 100, "", kid, ct))
	twoOps.Records[0].Ops = append(twoOps.Records[0].Ops, twoOps.Records[0].Ops[0])
	assert.Error(t, h.PreValidateMulti(&twoOps, nil))

	// networkSign writes: explicit ids, no upsert, existing rows with
	// rootCid. BATCH is the primary shape — a bulk upload signs N
	// files in one change.
	signRec := func(id string) crdt.RecordChange {
		return crdt.RecordChange{
			Id:  id,
			Ops: []crdt.Op{{Type: crdt.OpSet, Path: []string{FieldNetworkSign}, Payload: a.NewString("net/sig")}},
		}
	}
	signCh := crdt.Change{
		Dataset: Dataset, DataVersion: HandlerVersion,
		Records: []crdt.RecordChange{signRec("file-1"), signRec("file-2")},
	}
	require.NoError(t, h.PreValidateMulti(&signCh, get))
	missing := crdt.Change{
		Dataset: Dataset, DataVersion: HandlerVersion,
		Records: []crdt.RecordChange{signRec("file-1"), signRec("file-gone")},
	}
	assert.Error(t, h.PreValidateMulti(&missing, get), "sign on a missing row")
	inline := crdt.Change{
		Dataset: Dataset, DataVersion: HandlerVersion,
		Records: []crdt.RecordChange{signRec("file-inline")},
	}
	assert.Error(t, h.PreValidateMulti(&inline, get), "sign on a row without rootCid")

	// Batch delete: explicit ids required.
	del := crdt.Change{
		Dataset: Dataset, DataVersion: HandlerVersion,
		Records: []crdt.RecordChange{
			{Id: "file-1", Ops: []crdt.Op{{Type: crdt.OpDelete}}},
			{Id: "file-2", Ops: []crdt.Op{{Type: crdt.OpDelete}}},
		},
	}
	require.NoError(t, h.PreValidateMulti(&del, get))
	del.Records[0].Id = ""
	assert.Error(t, h.PreValidateMulti(&del, get))

	// Anything else (e.g. $inc) is not a payloads shape.
	inc := crdt.Change{
		Dataset: Dataset, DataVersion: HandlerVersion,
		Records: []crdt.RecordChange{{Id: "file-1", Ops: []crdt.Op{{Type: crdt.OpInc, Path: []string{FieldSize}, Payload: a.NewNumberInt(1)}}}},
	}
	assert.Error(t, h.PreValidateMulti(&inc, get))
}

func ptr(ch crdt.Change) *crdt.Change { return &ch }

// The derived author is a $setCreate min-rule offer derived at the
// _ver.id marker sites: two members concurrently registering the same
// row converge on the causally-earliest registrant's signer in either
// delivery order — even though the losing order sees the earliest
// change as an all-rejected modify (multi-field $set on an existing
// row), which is exactly why the offer must not ride op acceptance.
func TestApply_AuthorConvergesAcrossDeliveryOrders(t *testing.T) {
	ctx := context.Background()
	a := &anyenc.Arena{}
	mk := func(changeId, version, creator string) crdt.Change {
		ch := createChange(a, changeId, version, creator,
			createRowPayload(a, "bafyroot", 1, "", "kid-1", []byte{1}))
		ch.Records[0].Id = "row-shared"
		return ch
	}
	early := func() crdt.Change { return mk("chE", "v1", "acc-early") }
	late := func() crdt.Change { return mk("chL", "v2", "acc-late") }

	inOrder := newPayloadsController(t)
	require.NoError(t, inOrder.ApplyChange(ctx, early()))
	require.NoError(t, inOrder.ApplyChange(ctx, late()))

	reversed := newPayloadsController(t)
	require.NoError(t, reversed.ApplyChange(ctx, late()))
	require.NoError(t, reversed.ApplyChange(ctx, early()))

	for name, ctrl := range map[string]*crdt.Controller{"in-order": inOrder, "reversed": reversed} {
		rec := ctrl.Get(ctx, Dataset, "row-shared")
		require.NotNil(t, rec, name)
		assert.Equal(t, "acc-early", rec.GetString(FieldAuthor), "%s: earliest registrant's signer", name)
		assert.Equal(t, "v1", rec.GetString("_ver", "id"), "%s: author matches the creation marker", name)
	}
}
