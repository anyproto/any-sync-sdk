// Package account is the account-level device-discovery record: every
// device of an account registers itself in one pkarr record addressed
// by a key derived from the identity key, and every device — a fresh
// restore included — resolves its siblings from it. See
// docs/19-account-discovery.md.
package account

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/anyproto/any-sync/net/transport/iroh"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/pkarr"
)

const (
	// RecordName is the TXT name of the record, relative to the key.
	RecordName = "_anydev"
	// MaxDevices bounds the devices one record lists; the packet size
	// limit trims further, oldest first.
	MaxDevices = 64
	// MaxRelays bounds the relay table.
	MaxRelays = 8
	// MaxAge is how long a device stays listed without refreshing its
	// entry — the same horizon as the disabled liveness tier.
	MaxAge = 30 * 24 * time.Hour

	recordVersion = 1
	recordTTL     = 60
	maxRelayLen   = 255
)

var (
	ErrWrongKey    = errors.New("account record: signed by another key")
	ErrBadRecord   = errors.New("account record: malformed payload")
	ErrNoRecord    = errors.New("account record: packet carries no record")
	errTooManyDevs = errors.New("account record: too many devices")
)

// Device is one entry: a peer and the relay it is homed at.
type Device struct {
	PeerId   string
	Relay    string
	LastSeen time.Time
}

// Record is the decoded payload.
type Record struct {
	Devices []Device
}

// Keys are the account's discovery keys: the pkarr signing key (also the
// record address) and the payload cipher.
type Keys struct {
	secret key.SecretKey
	pub    key.PublicKey
	enc    crypto.SymKey
}

// NewKeys wraps the derived keys (crypto.DeriveDiscoveryKeys).
func NewKeys(sign crypto.PrivKey, enc crypto.SymKey) (*Keys, error) {
	raw, err := sign.Raw()
	if err != nil {
		return nil, err
	}
	if len(raw) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("account record: sign key is %d bytes, want %d", len(raw), ed25519.PrivateKeySize)
	}
	sk, err := key.SecretKeyFromEd25519(ed25519.PrivateKey(raw))
	if err != nil {
		return nil, err
	}
	return &Keys{secret: sk, pub: sk.Public(), enc: enc}, nil
}

// Public is the record address.
func (k *Keys) Public() key.PublicKey { return k.pub }

// Z32 is the record address as pkarr relays name it.
func (k *Keys) Z32() string { return k.pub.EndpointID().Z32() }

// Seal encrypts and signs r. A record that does not fit a pkarr packet
// loses its oldest devices until it does.
func Seal(k *Keys, r Record) (*pkarr.SignedPacket, error) {
	devices := slices.Clone(r.Devices)
	sortNewestFirst(devices)
	if len(devices) > MaxDevices {
		devices = devices[:MaxDevices]
	}
	for {
		plain, err := Encode(Record{Devices: devices})
		if err != nil {
			return nil, err
		}
		sealed, err := k.enc.Encrypt(plain)
		if err != nil {
			return nil, err
		}
		packet, err := pkarr.FromTxtStrings(k.secret, RecordName, []string{base64.RawURLEncoding.EncodeToString(sealed)}, recordTTL)
		if err == nil {
			return packet, nil
		}
		if !errors.Is(err, pkarr.ErrPacketTooLarge) || len(devices) == 0 {
			return nil, err
		}
		devices = devices[:len(devices)-1]
	}
}

// Open verifies the packet belongs to k and decrypts its record.
func Open(k *Keys, p *pkarr.SignedPacket) (Record, error) {
	if !p.PublicKey().Equal(k.pub) {
		return Record{}, ErrWrongKey
	}
	values := p.TxtRecords(RecordName)
	if len(values) == 0 {
		return Record{}, ErrNoRecord
	}
	var joined string
	for _, v := range values {
		joined += v
	}
	sealed, err := base64.RawURLEncoding.DecodeString(joined)
	if err != nil {
		return Record{}, fmt.Errorf("%w: %v", ErrBadRecord, err)
	}
	plain, err := k.enc.Decrypt(sealed)
	if err != nil {
		return Record{}, fmt.Errorf("%w: %v", ErrBadRecord, err)
	}
	return Decode(plain)
}

// Merge returns cur with self upserted, devices unseen for maxAge
// dropped, newest first, at most MaxDevices. Any device of the
// account holds the key, so last-writer-wins races heal on the next
// cycle.
func Merge(cur Record, self Device, now time.Time, maxAge time.Duration) Record {
	out := make([]Device, 0, len(cur.Devices)+1)
	for _, d := range cur.Devices {
		if d.PeerId == self.PeerId || now.Sub(d.LastSeen) > maxAge {
			continue
		}
		out = append(out, d)
	}
	out = append(out, self)
	sortNewestFirst(out)
	if len(out) > MaxDevices {
		out = out[:MaxDevices]
	}
	return Record{Devices: out}
}

// Encode is the payload wire form: version, relay table (u8 count,
// u16-length strings), devices (u8 count; per device the 32-byte
// endpoint key, u8 relay index, u32 unix seconds). A device costs
// 37 bytes, so a record carries dozens of them.
func Encode(r Record) ([]byte, error) {
	if len(r.Devices) > MaxDevices {
		return nil, errTooManyDevs
	}
	var relays []string
	relayIdx := map[string]int{}
	for _, d := range r.Devices {
		if _, ok := relayIdx[d.Relay]; ok {
			continue
		}
		if len(d.Relay) > maxRelayLen {
			return nil, fmt.Errorf("account record: relay url too long: %q", d.Relay)
		}
		relayIdx[d.Relay] = len(relays)
		relays = append(relays, d.Relay)
	}
	if len(relays) > MaxRelays {
		return nil, errors.New("account record: too many relays")
	}
	out := []byte{recordVersion, byte(len(relays))}
	for _, u := range relays {
		out = binary.BigEndian.AppendUint16(out, uint16(len(u)))
		out = append(out, u...)
	}
	out = append(out, byte(len(r.Devices)))
	for _, d := range r.Devices {
		id, err := iroh.EndpointIdFromPeerId(d.PeerId)
		if err != nil {
			return nil, fmt.Errorf("account record: %s: %w", d.PeerId, err)
		}
		b := id.Bytes()
		out = append(out, b[:]...)
		out = append(out, byte(relayIdx[d.Relay]))
		seen := d.LastSeen.Unix()
		if seen < 0 {
			seen = 0
		}
		out = binary.BigEndian.AppendUint32(out, uint32(min(seen, int64(^uint32(0)))))
	}
	return out, nil
}

// Decode parses the payload wire form.
func Decode(b []byte) (Record, error) {
	rd := reader{b: b}
	if v := rd.u8(); v != recordVersion {
		return Record{}, fmt.Errorf("%w: version %d", ErrBadRecord, v)
	}
	n := int(rd.u8())
	if n > MaxRelays {
		return Record{}, ErrBadRecord
	}
	relays := make([]string, 0, n)
	for i := 0; i < n; i++ {
		l := int(rd.u16())
		relays = append(relays, string(rd.bytes(l)))
	}
	m := int(rd.u8())
	if m > MaxDevices {
		return Record{}, ErrBadRecord
	}
	devices := make([]Device, 0, m)
	for i := 0; i < m; i++ {
		id, err := key.EndpointIDFromSlice(rd.bytes(32))
		if err != nil {
			return Record{}, fmt.Errorf("%w: %v", ErrBadRecord, err)
		}
		idx := int(rd.u8())
		seen := rd.u32()
		if rd.err != nil || idx >= len(relays) {
			return Record{}, ErrBadRecord
		}
		peerId, err := iroh.PeerIdFromEndpointId(id)
		if err != nil {
			return Record{}, fmt.Errorf("%w: %v", ErrBadRecord, err)
		}
		devices = append(devices, Device{PeerId: peerId, Relay: relays[idx], LastSeen: time.Unix(int64(seen), 0).UTC()})
	}
	if rd.err != nil || len(rd.b) != 0 {
		return Record{}, ErrBadRecord
	}
	return Record{Devices: devices}, nil
}

func sortNewestFirst(devices []Device) {
	slices.SortStableFunc(devices, func(a, b Device) int { return b.LastSeen.Compare(a.LastSeen) })
}

type reader struct {
	b   []byte
	err error
}

func (r *reader) bytes(n int) []byte {
	if r.err != nil || len(r.b) < n {
		r.err = ErrBadRecord
		return make([]byte, n)
	}
	out := r.b[:n]
	r.b = r.b[n:]
	return out
}

func (r *reader) u8() byte    { return r.bytes(1)[0] }
func (r *reader) u16() uint16 { return binary.BigEndian.Uint16(r.bytes(2)) }
func (r *reader) u32() uint32 { return binary.BigEndian.Uint32(r.bytes(4)) }

// SignedPacket and PublicKey are re-exported so callers need no pkarr
// import for the client surface.
type (
	SignedPacket = pkarr.SignedPacket
	PublicKey    = key.PublicKey
)
