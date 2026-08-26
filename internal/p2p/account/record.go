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
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/anyproto/any-sync/net/transport/iroh"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/pkarr"
)

const (
	// RecordName is the TXT name of the record, relative to the key.
	RecordName = "_anydev"
	// MaxDevices bounds the devices one record lists: what fits the
	// 1000-byte pkarr packet after padding, AEAD and base64 with a
	// one-entry relay table (measured by TestMaxDevicesFit: 12 with a
	// 32-byte relay URL; the 13th pads into the next bucket and no longer
	// fits). Seal trims further, oldest first, when a record still does
	// not fit.
	MaxDevices = 12
	// MaxRelays bounds the relay table.
	MaxRelays = 8
	// MaxAge is how long a device stays listed without refreshing its
	// entry — the same horizon as the disabled liveness tier.
	MaxAge = 30 * 24 * time.Hour
	// RecordVersion is the payload version this code writes. A reader
	// parses newer versions as far as it understands them and ignores
	// the rest; a writer never replaces a newer record with an older
	// layout.
	RecordVersion = 1

	recordTTL   = 60
	maxRelayLen = 255
	// padBucket is the plaintext size granularity: the ciphertext length
	// then only reveals the device count to within a bucket.
	padBucket = 256
	// maxClockLead bounds how far ahead of now an entry may be dated;
	// a later stamp is clamped to now when merging.
	maxClockLead = 5 * time.Minute
)

var (
	ErrWrongKey  = errors.New("account record: signed by another key")
	ErrBadRecord = errors.New("account record: malformed payload")
	ErrNoRecord  = errors.New("account record: packet carries no record")
	// ErrTooManyDevices / ErrTooManyRelays are Encode's bounds; Seal
	// trims a record below them before encoding.
	ErrTooManyDevices = errors.New("account record: too many devices")
	ErrTooManyRelays  = errors.New("account record: too many relays")
)

// Device is one entry: a peer and the relay it is homed at.
type Device struct {
	PeerId   string
	Relay    string
	LastSeen time.Time
}

// Record is the decoded payload. Version is the payload version the
// writer used; Decode fills it, Encode writes RecordVersion.
type Record struct {
	Version uint8
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

// Seal encrypts and signs r with the current time as the packet
// timestamp. See SealAt.
func Seal(k *Keys, r Record) (*pkarr.SignedPacket, error) {
	return SealAt(k, r, 0)
}

// SealAt encrypts and signs r with a packet timestamp of at least minTs
// (microseconds): a relay refuses a packet older than the one it holds,
// and sibling clocks differ, so a writer signs past the packet it
// resolved. A record with more relays than the table holds drops the
// devices on the least used ones; one that does not fit a pkarr packet
// loses its oldest devices until it does.
func SealAt(k *Keys, r Record, minTs pkarr.Timestamp) (*pkarr.SignedPacket, error) {
	devices := trimRelays(sortedClone(r.Devices))
	if len(devices) > MaxDevices {
		devices = devices[:MaxDevices]
	}
	packet, err := sealDevices(k, devices)
	if errors.Is(err, pkarr.ErrPacketTooLarge) {
		// packet size grows with the device count: binary search the
		// largest prefix that fits
		lo, hi := 0, len(devices)-1
		packet, err = nil, nil
		for lo <= hi {
			mid := (lo + hi) / 2
			p, serr := sealDevices(k, devices[:mid])
			switch {
			case serr == nil:
				packet = p
				lo = mid + 1
			case errors.Is(serr, pkarr.ErrPacketTooLarge):
				hi = mid - 1
			default:
				return nil, serr
			}
		}
		if packet == nil {
			return nil, pkarr.ErrPacketTooLarge
		}
	}
	if err != nil {
		return nil, err
	}
	if packet.Timestamp() < minTs {
		return resignAt(k, packet, minTs)
	}
	return packet, nil
}

func sealDevices(k *Keys, devices []Device) (*pkarr.SignedPacket, error) {
	plain, err := Encode(Record{Devices: devices})
	if err != nil {
		return nil, err
	}
	return SealRaw(k, plain, 0)
}

// SealRaw pads, encrypts and signs an already encoded payload with a
// timestamp of at least minTs. Seal is the normal path; SealRaw lets
// tooling and tests write payload layouts this code does not produce.
func SealRaw(k *Keys, plain []byte, minTs pkarr.Timestamp) (*pkarr.SignedPacket, error) {
	if rem := len(plain) % padBucket; rem != 0 {
		plain = append(slices.Clone(plain), make([]byte, padBucket-rem)...)
	}
	sealed, err := k.enc.Encrypt(plain)
	if err != nil {
		return nil, err
	}
	packet, err := pkarr.FromTxtStrings(k.secret, RecordName, []string{base64.RawURLEncoding.EncodeToString(sealed)}, recordTTL)
	if err != nil {
		return nil, err
	}
	if packet.Timestamp() < minTs {
		return resignAt(k, packet, minTs)
	}
	return packet, nil
}

// resignAt re-signs a packet's DNS payload with a later timestamp. The
// wire form is <key 32><signature 64><timestamp 8><packet>; the
// signature covers the BEP-0044 signable bytes of timestamp and packet.
func resignAt(k *Keys, p *pkarr.SignedPacket, ts pkarr.Timestamp) (*pkarr.SignedPacket, error) {
	encoded := p.EncodedPacket()
	prefix := "3:seqi" + strconv.FormatUint(ts.Micros(), 10) + "e1:v" + strconv.Itoa(len(encoded)) + ":"
	signable := make([]byte, 0, len(prefix)+len(encoded))
	signable = append(signable, prefix...)
	signable = append(signable, encoded...)
	sig := k.secret.Sign(signable).Bytes()
	pub := k.pub.Bytes()
	out := make([]byte, 0, 32+64+8+len(encoded))
	out = append(out, pub[:]...)
	out = append(out, sig[:]...)
	out = binary.BigEndian.AppendUint64(out, ts.Micros())
	out = append(out, encoded...)
	return pkarr.FromBytes(out)
}

// trimRelays keeps the devices on the MaxRelays most used relays (ties
// to the relay with the newest device), so Encode's relay table always
// fits. devices is sorted newest first.
func trimRelays(devices []Device) []Device {
	count := map[string]int{}
	first := map[string]int{}
	for i, d := range devices {
		count[d.Relay]++
		if _, ok := first[d.Relay]; !ok {
			first[d.Relay] = i
		}
	}
	if len(count) <= MaxRelays {
		return devices
	}
	relays := make([]string, 0, len(count))
	for r := range count {
		relays = append(relays, r)
	}
	slices.SortFunc(relays, func(a, b string) int {
		if count[a] != count[b] {
			return count[b] - count[a]
		}
		return first[a] - first[b]
	})
	keep := map[string]bool{}
	for _, r := range relays[:MaxRelays] {
		keep[r] = true
	}
	return slices.DeleteFunc(devices, func(d Device) bool { return !keep[d.Relay] })
}

// Open verifies the packet belongs to k and decrypts its record. A
// payload that fails to decrypt or parse is corrupt (ErrBadRecord).
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

// Merge returns cur with self upserted, duplicates collapsed to their
// newest entry, devices unseen for maxAge dropped, entries Encode could
// not carry (no usable relay URL) dropped, stamps ahead of now clamped,
// newest first, at most MaxDevices. Any device of the account holds the
// key, so last-writer-wins races heal on the next cycle.
func Merge(cur Record, self Device, now time.Time, maxAge time.Duration) Record {
	newest := map[string]Device{}
	for _, d := range cur.Devices {
		if d.PeerId == self.PeerId || now.Sub(d.LastSeen) > maxAge || !usableRelay(d.Relay) {
			continue
		}
		if d.LastSeen.After(now) {
			d.LastSeen = now
		}
		if prev, ok := newest[d.PeerId]; ok && !d.LastSeen.After(prev.LastSeen) {
			continue
		}
		newest[d.PeerId] = d
	}
	out := make([]Device, 0, len(newest)+1)
	for _, d := range newest {
		out = append(out, d)
	}
	out = append(out, self)
	sortNewestFirst(out)
	if len(out) > MaxDevices {
		out = out[:MaxDevices]
	}
	return Record{Version: RecordVersion, Devices: out}
}

// usableRelay is the shape Encode carries and a dial can use: an http(s)
// URL with a host, at most maxRelayLen bytes.
func usableRelay(raw string) bool {
	if raw == "" || len(raw) > maxRelayLen {
		return false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return false
	}
	return u.Scheme == "https" || u.Scheme == "http"
}

// Encode is the payload wire form: version, relay table (u8 count,
// u16-length strings), devices (u8 count; per device the 32-byte
// endpoint key, u8 relay index, u32 unix seconds). A device costs
// 37 bytes.
func Encode(r Record) ([]byte, error) {
	if len(r.Devices) > MaxDevices {
		return nil, ErrTooManyDevices
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
		return nil, ErrTooManyRelays
	}
	out := []byte{RecordVersion, byte(len(relays))}
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

// Decode parses the payload wire form. A newer version is parsed as far
// as this layout goes and its trailing bytes are ignored, so fields can
// be appended without breaking older readers; this version accepts only
// zero padding after the structure.
func Decode(b []byte) (Record, error) {
	rd := reader{b: b}
	version := rd.u8()
	if rd.err != nil || version == 0 {
		return Record{}, fmt.Errorf("%w: version %d", ErrBadRecord, version)
	}
	n := int(rd.u8())
	if n > MaxRelays {
		return Record{}, ErrBadRecord
	}
	relays := make([]string, 0, n)
	for i := 0; i < n; i++ {
		l := int(rd.u16())
		if l > maxRelayLen {
			return Record{}, ErrBadRecord
		}
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
	if rd.err != nil || (version == RecordVersion && !allZero(rd.b)) {
		return Record{}, ErrBadRecord
	}
	return Record{Version: version, Devices: devices}, nil
}

// allZero reports whether b is padding only.
func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

func sortedClone(devices []Device) []Device {
	out := slices.Clone(devices)
	sortNewestFirst(out)
	return out
}

func sortNewestFirst(devices []Device) {
	slices.SortStableFunc(devices, func(a, b Device) int { return b.LastSeen.Compare(a.LastSeen) })
}

// reader is a bounds-checked cursor: a short read poisons it and every
// accessor then returns zero values without allocating.
type reader struct {
	b   []byte
	err error
}

func (r *reader) bytes(n int) []byte {
	if r.err != nil || n < 0 || len(r.b) < n {
		r.err = ErrBadRecord
		return nil
	}
	out := r.b[:n]
	r.b = r.b[n:]
	return out
}

func (r *reader) u8() byte {
	if b := r.bytes(1); b != nil {
		return b[0]
	}
	return 0
}

func (r *reader) u16() uint16 {
	if b := r.bytes(2); b != nil {
		return binary.BigEndian.Uint16(b)
	}
	return 0
}

func (r *reader) u32() uint32 {
	if b := r.bytes(4); b != nil {
		return binary.BigEndian.Uint32(b)
	}
	return 0
}

// SignedPacket, PublicKey and Timestamp are re-exported so callers need
// no pkarr import for the client surface.
type (
	SignedPacket = pkarr.SignedPacket
	PublicKey    = key.PublicKey
	Timestamp    = pkarr.Timestamp
)
