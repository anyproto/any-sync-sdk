package payloads

import (
	"context"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rowValue builds a materialized payloads record with the given enc
// wrapper.
func rowValue(a *anyenc.Arena, kid string, ct []byte) *anyenc.Value {
	v := a.NewObject()
	v.Set("id", a.NewString("f1"))
	v.Set(FieldRootCid, a.NewString("bafyroot"))
	v.Set(FieldSize, a.NewNumberFloat64(10))
	enc := a.NewObject()
	if kid != "" {
		enc.Set(EncKeyId, a.NewString(kid))
	}
	if len(ct) > 0 {
		enc.Set(EncKeyCiphertext, a.NewBinary(ct))
	}
	v.Set(FieldEnc, enc)
	return v
}

// TestRow_PoisonedRowDegradesToSealed pins the hostile-member
// tolerance: a row whose enc cannot be opened WITH a key (stamped kid
// mismatching the sealing key, or garbage ciphertext) stays Sealed
// with UnsealErr set — it must never error the read path, or one
// poisoned row would fail every listing for every member.
func TestRow_PoisonedRowDegradesToSealed(t *testing.T) {
	ctx := context.Background()
	kp := newFakeProvider(t, "kid-1", "kid-2")
	key2, err := kp.KeyById(ctx, "kid-2")
	require.NoError(t, err)

	t.Run("kid does not match the sealing key", func(t *testing.T) {
		ct, err := SealEnc(key2, testEncPayload())
		require.NoError(t, err)
		a := &anyenc.Arena{}
		row, err := RowFromValue(rowValue(a, "kid-1", ct)) // sealed with kid-2's key
		require.NoError(t, err)
		require.NoError(t, row.Unseal(ctx, kp), "poisoned row must not fail the read")
		assert.True(t, row.Sealed)
		assert.Error(t, row.UnsealErr)
	})

	t.Run("garbage ciphertext", func(t *testing.T) {
		a := &anyenc.Arena{}
		row, err := RowFromValue(rowValue(a, "kid-1", []byte("not a gcm blob")))
		require.NoError(t, err)
		require.NoError(t, row.Unseal(ctx, kp))
		assert.True(t, row.Sealed)
		assert.Error(t, row.UnsealErr)
	})

	t.Run("missing enc", func(t *testing.T) {
		a := &anyenc.Arena{}
		row, err := RowFromValue(rowValue(a, "", nil))
		require.NoError(t, err)
		require.NoError(t, row.Unseal(ctx, kp))
		assert.True(t, row.Sealed)
		assert.Error(t, row.UnsealErr)
	})

	t.Run("keyless reader stays sealed with no error recorded", func(t *testing.T) {
		ct, err := SealEnc(key2, testEncPayload())
		require.NoError(t, err)
		a := &anyenc.Arena{}
		row, err := RowFromValue(rowValue(a, "kid-unknown", ct))
		require.NoError(t, err)
		require.NoError(t, row.Unseal(ctx, kp))
		assert.True(t, row.Sealed)
		assert.NoError(t, row.UnsealErr, "no key is the normal broker view, not poison")
	})
}
