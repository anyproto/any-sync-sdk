package crypt

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"io"
	mrand "math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testData(t *testing.T, size int) (plain, key []byte) {
	t.Helper()
	r := mrand.New(mrand.NewSource(42))
	plain = make([]byte, size)
	r.Read(plain)
	key = make([]byte, KeySize)
	r.Read(key)
	return
}

func encrypt(t *testing.T, key, plain []byte) []byte {
	t.Helper()
	er, err := NewEncryptReader(key, bytes.NewReader(plain))
	require.NoError(t, err)
	ct, err := io.ReadAll(er)
	require.NoError(t, err)
	return ct
}

func TestEncryptFormat(t *testing.T) {
	// The ciphertext must match a raw zero-IV CFB encrypter — the
	// anytype-heart / fileservice wire format.
	plain, key := testData(t, 100000)
	ct := encrypt(t, key, plain)

	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	want := make([]byte, len(plain))
	cipher.NewCFBEncrypter(block, make([]byte, aes.BlockSize)).XORKeyStream(want, plain)
	require.True(t, bytes.Equal(want, ct))
	require.Len(t, ct, len(plain)) // stream cipher: no expansion
}

func TestReaderRoundTrip(t *testing.T) {
	plain, key := testData(t, 1<<20+123)
	ct := encrypt(t, key, plain)

	r, err := NewReader(key, bytes.NewReader(ct))
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.True(t, bytes.Equal(plain, got))
}

func TestReaderSeek(t *testing.T) {
	plain, key := testData(t, 1<<20)
	ct := encrypt(t, key, plain)

	r, err := NewReader(key, bytes.NewReader(ct))
	require.NoError(t, err)

	rnd := mrand.New(mrand.NewSource(7))
	buf := make([]byte, 1000)
	for i := 0; i < 200; i++ {
		off := int64(rnd.Intn(len(plain) - len(buf)))
		pos, err := r.Seek(off, io.SeekStart)
		require.NoError(t, err)
		require.Equal(t, off, pos)
		_, err = io.ReadFull(r, buf)
		require.NoError(t, err)
		assert.True(t, bytes.Equal(plain[off:off+int64(len(buf))], buf), "off=%d", off)
	}

	// SeekCurrent / SeekEnd
	pos, err := r.Seek(100, io.SeekStart)
	require.NoError(t, err)
	pos, err = r.Seek(50, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(150), pos)
	pos, err = r.Seek(-10, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(len(plain)-10), pos)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.True(t, bytes.Equal(plain[len(plain)-10:], got))

	_, err = r.Seek(-1, io.SeekStart)
	require.Error(t, err)
}

func TestBadKey(t *testing.T) {
	_, err := NewEncryptReader(make([]byte, 16), bytes.NewReader(nil))
	require.ErrorIs(t, err, ErrBadKey)
	_, err = NewReader(nil, bytes.NewReader(nil))
	require.ErrorIs(t, err, ErrBadKey)
}
