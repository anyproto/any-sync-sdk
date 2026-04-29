package crdt

import (
	"encoding/binary"

	"github.com/mr-tron/base58"
	"github.com/zeebo/xxh3"
)

// DeriveRecordId produces the default record id for an empty-id RecordChange.
//
// The derivation is base58(xxh3-64(changeId)). changeId is a content-
// addressable CID, so its bytes are uniformly random; xxh3 preserves that
// uniformity, and base58 encodes the resulting 8 bytes in up to 11 chars.
//
// Collision math (2^64 space): P(any collision) < 1e-6 up to ~6M derived ids
// within a single storage namespace; comfortable for any realistic scale.
func DeriveRecordId(changeId string) string {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], xxh3.HashString(changeId))
	return base58.FastBase58Encoding(buf[:])
}
