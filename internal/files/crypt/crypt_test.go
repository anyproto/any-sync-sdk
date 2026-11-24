package crypt

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"errors"
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

// holedCiphertext counts reads and fails the ones that touch [from, to):
// a partly fetched file with no source for the rest.
type holedCiphertext struct {
	*bytes.Reader
	from, to int64
	reads    int
}

var errHole = errors.New("block not available")

func (h *holedCiphertext) Read(p []byte) (int, error) {
	h.reads++
	pos, _ := h.Reader.Seek(0, io.SeekCurrent)
	if pos < h.to && pos+int64(len(p)) > h.from {
		return 0, errHole
	}
	return h.Reader.Read(p)
}

// A Seek reads no ciphertext, so it cannot fail on bytes that are not
// there: sizing a file by seeking to its end works on a partial copy,
// and the missing bytes fail the Read that needs them.
func TestReaderSeekReadsNothing(t *testing.T) {
	plain, key := testData(t, 4096)
	ct := &holedCiphertext{Reader: bytes.NewReader(encrypt(t, key, plain)), from: 1024, to: 4096}

	r, err := NewReader(key, ct)
	require.NoError(t, err)
	size, err := r.Seek(0, io.SeekEnd)
	require.NoError(t, err, "the end of the file is not local")
	require.Equal(t, int64(len(plain)), size)
	_, err = r.Seek(2000, io.SeekStart)
	require.NoError(t, err)
	require.Zero(t, ct.reads, "a seek reads no ciphertext")

	_, err = r.Read(make([]byte, 10))
	require.ErrorIs(t, err, errHole, "the read of missing bytes is what fails")

	// The reader stays usable for what is there.
	_, err = r.Seek(100, io.SeekStart)
	require.NoError(t, err)
	buf := make([]byte, 200)
	_, err = io.ReadFull(r, buf)
	require.NoError(t, err)
	require.True(t, bytes.Equal(plain[100:300], buf))
}

// Reading from a position past the end is the end of the stream.
func TestReaderSeekPastEnd(t *testing.T) {
	plain, key := testData(t, 100)
	r, err := NewReader(key, bytes.NewReader(encrypt(t, key, plain)))
	require.NoError(t, err)
	for _, off := range []int64{100, 105, 200, 5000} {
		_, err = r.Seek(off, io.SeekStart)
		require.NoError(t, err)
		n, err := r.Read(make([]byte, 10))
		require.Zero(t, n, "off=%d", off)
		require.ErrorIs(t, err, io.EOF, "off=%d", off)
	}
}

func TestBadKey(t *testing.T) {
	_, err := NewEncryptReader(make([]byte, 16), bytes.NewReader(nil))
	require.ErrorIs(t, err, ErrBadKey)
	_, err = NewReader(nil, bytes.NewReader(nil))
	require.ErrorIs(t, err, ErrBadKey)
}
