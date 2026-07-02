// Package crypt implements the files byte-layer cipher: whole-file
// AES-256-CFB with a zero IV and a random per-file key — the exact
// format anytype-heart writes (cfb.New(key, [aes.BlockSize]byte{})),
// so ciphertext and the derived UnixFS cids stay byte-compatible.
//
// A zero IV is safe here because a file key is generated fresh per
// file and never reused; integrity comes from the merkle cids over the
// ciphertext, not from the cipher (CFB carries no tags). Readers must
// therefore only feed cid-verified bytes into a decryptor.
package crypt

import (
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"fmt"
	"io"
)

// KeySize is the AES-256 key length in bytes.
const KeySize = 32

// ErrBadKey is returned for a key of the wrong length.
var ErrBadKey = errors.New("crypt: key must be 32 bytes")

// NewEncryptReader wraps r so reads return the AES-256-CFB ciphertext
// of its bytes (zero IV). Single forward pass; the stream state makes
// it single-use.
func NewEncryptReader(key []byte, r io.Reader) (io.Reader, error) {
	block, err := newBlock(key)
	if err != nil {
		return nil, err
	}
	var iv [aes.BlockSize]byte
	return &cipher.StreamReader{S: cipher.NewCFBEncrypter(block, iv[:]), R: r}, nil
}

// Reader decrypts an AES-256-CFB ciphertext stream with random access.
// Seek re-keys the stream from the ciphertext block boundary before the
// target offset: the 16 ciphertext bytes preceding the aligned position
// are the IV, so decryption never restarts from zero (ports
// anytype-heart's cfb.CFBDecryptor).
//
// The underlying ciphertext reader must serve only cid-verified bytes;
// Reader adds no integrity of its own.
type Reader struct {
	block cipher.Block
	ct    io.ReadSeeker
	sr    *cipher.StreamReader
	pos   int64
}

// NewReader positions a Reader at plaintext offset 0.
func NewReader(key []byte, ct io.ReadSeeker) (*Reader, error) {
	block, err := newBlock(key)
	if err != nil {
		return nil, err
	}
	r := &Reader{block: block, ct: ct}
	if err = r.seekTo(0); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Reader) Read(p []byte) (int, error) {
	n, err := r.sr.Read(p)
	r.pos += int64(n)
	return n, err
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.pos + offset
	case io.SeekEnd:
		end, err := r.ct.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, err
		}
		abs = end + offset
	default:
		return 0, fmt.Errorf("crypt: invalid whence %d", whence)
	}
	if abs < 0 {
		return 0, errors.New("crypt: negative seek offset")
	}
	if err := r.seekTo(abs); err != nil {
		return 0, err
	}
	return abs, nil
}

func (r *Reader) seekTo(off int64) error {
	aligned := (off / aes.BlockSize) * aes.BlockSize
	iv := make([]byte, aes.BlockSize) // zero IV at stream start
	if aligned == 0 {
		if _, err := r.ct.Seek(0, io.SeekStart); err != nil {
			return err
		}
	} else {
		if _, err := r.ct.Seek(aligned-aes.BlockSize, io.SeekStart); err != nil {
			return err
		}
		if _, err := io.ReadFull(r.ct, iv); err != nil {
			return err
		}
	}
	r.sr = &cipher.StreamReader{S: cipher.NewCFBDecrypter(r.block, iv), R: r.ct}
	if rem := off - aligned; rem > 0 {
		if _, err := io.CopyN(io.Discard, r.sr, rem); err != nil {
			return err
		}
	}
	r.pos = off
	return nil
}

func newBlock(key []byte) (cipher.Block, error) {
	if len(key) != KeySize {
		return nil, ErrBadKey
	}
	return aes.NewCipher(key)
}
