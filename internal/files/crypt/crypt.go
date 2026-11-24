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
// Seek only records the target; the next Read re-keys the stream from
// the ciphertext block boundary before it: the 16 ciphertext bytes
// preceding the aligned position are the IV, so decryption never
// restarts from zero (ports anytype-heart's cfb.CFBDecryptor). A seek
// costs no read of the ciphertext, and a position whose ciphertext
// cannot be had fails the Read, not the Seek.
//
// The underlying ciphertext reader must serve only cid-verified bytes;
// Reader adds no integrity of its own.
type Reader struct {
	block cipher.Block
	ct    io.ReadSeeker
	// sr is the stream keyed at pos; nil from a Seek to the next Read.
	sr  *cipher.StreamReader
	pos int64
}

// NewReader positions a Reader at plaintext offset 0.
func NewReader(key []byte, ct io.ReadSeeker) (*Reader, error) {
	block, err := newBlock(key)
	if err != nil {
		return nil, err
	}
	return &Reader{block: block, ct: ct}, nil
}

func (r *Reader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.sr == nil {
		sr, err := r.keyAt(r.pos)
		if err != nil {
			return 0, err
		}
		r.sr = sr
	}
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
	r.pos, r.sr = abs, nil
	return abs, nil
}

// keyAt returns the decrypting stream positioned at plaintext offset
// off. An offset past the end reads as io.EOF.
func (r *Reader) keyAt(off int64) (*cipher.StreamReader, error) {
	aligned := (off / aes.BlockSize) * aes.BlockSize
	iv := make([]byte, aes.BlockSize) // zero IV at stream start
	if aligned == 0 {
		if _, err := r.ct.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
	} else {
		if _, err := r.ct.Seek(aligned-aes.BlockSize, io.SeekStart); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(r.ct, iv); err != nil {
			return nil, pastEnd(err)
		}
	}
	sr := &cipher.StreamReader{S: cipher.NewCFBDecrypter(r.block, iv), R: r.ct}
	if rem := off - aligned; rem > 0 {
		if _, err := io.CopyN(io.Discard, sr, rem); err != nil {
			return nil, pastEnd(err)
		}
	}
	return sr, nil
}

// pastEnd reads a short ciphertext as the end of the stream.
func pastEnd(err error) error {
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return io.EOF
	}
	return err
}

func newBlock(key []byte) (cipher.Block, error) {
	if len(key) != KeySize {
		return nil, ErrBadKey
	}
	return aes.NewCipher(key)
}
