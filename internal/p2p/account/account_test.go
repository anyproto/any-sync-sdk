package account

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/go-iroh/dnsserver"
	"github.com/tmc/go-iroh/pkarr"
)

func newKeys(t *testing.T) *Keys {
	t.Helper()
	identity, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	sign, enc, err := crypto.DeriveDiscoveryKeys(identity)
	require.NoError(t, err)
	k, err := NewKeys(sign, enc)
	require.NoError(t, err)
	return k
}

func newPeerId(t *testing.T) string {
	t.Helper()
	_, pub, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	return pub.PeerId()
}

func TestRecordRoundTrip(t *testing.T) {
	k := newKeys(t)
	now := time.Now().UTC().Truncate(time.Second)
	rec := Record{Devices: []Device{
		{PeerId: newPeerId(t), Relay: "https://relay-eu.example/", LastSeen: now},
		{PeerId: newPeerId(t), Relay: "https://relay-us.example/", LastSeen: now.Add(-time.Hour)},
		{PeerId: newPeerId(t), Relay: "https://relay-eu.example/", LastSeen: now.Add(-2 * time.Hour)},
	}}
	packet, err := Seal(k, rec)
	require.NoError(t, err)
	got, err := Open(k, packet)
	require.NoError(t, err)
	assert.Equal(t, rec.Devices, got.Devices)

	// a sibling with the same identity derives the same keys
	other := newKeys(t)
	_, err = Open(other, packet)
	assert.ErrorIs(t, err, ErrWrongKey)

	// binary form is compact: 37 B per device plus the relay table
	plain, err := Encode(rec)
	require.NoError(t, err)
	assert.Less(t, len(plain), 3*37+2*30+8)
}

func TestSealTrimsToPacketSize(t *testing.T) {
	k := newKeys(t)
	now := time.Now()
	var rec Record
	for i := 0; i < MaxDevices; i++ {
		rec.Devices = append(rec.Devices, Device{PeerId: newPeerId(t), Relay: fmt.Sprintf("https://relay-%d.example/", i%MaxRelays), LastSeen: now.Add(-time.Duration(i) * time.Minute)})
	}
	packet, err := Seal(k, rec)
	require.NoError(t, err)
	got, err := Open(k, packet)
	require.NoError(t, err)
	require.NotEmpty(t, got.Devices)
	assert.Less(t, len(got.Devices), MaxDevices, "the packet limit trims the record")
	// the newest survive
	assert.Equal(t, rec.Devices[0].PeerId, got.Devices[0].PeerId)
	for i := 1; i < len(got.Devices); i++ {
		assert.False(t, got.Devices[i].LastSeen.After(got.Devices[i-1].LastSeen))
	}
}

func TestMerge(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	self := Device{PeerId: newPeerId(t), Relay: "https://r/", LastSeen: now}
	old := Device{PeerId: newPeerId(t), Relay: "https://r/", LastSeen: now.Add(-31 * 24 * time.Hour)}
	live := Device{PeerId: newPeerId(t), Relay: "https://r/", LastSeen: now.Add(-time.Hour)}
	stale := Device{PeerId: self.PeerId, Relay: "https://old/", LastSeen: now.Add(-2 * time.Hour)}

	got := Merge(Record{Devices: []Device{old, live, stale}}, self, now, MaxAge)
	require.Len(t, got.Devices, 2)
	assert.Equal(t, self, got.Devices[0], "own entry replaced and newest first")
	assert.Equal(t, live, got.Devices[1])

	var many Record
	for i := 0; i < MaxDevices+5; i++ {
		many.Devices = append(many.Devices, Device{PeerId: newPeerId(t), Relay: "https://r/", LastSeen: now.Add(-time.Duration(i) * time.Second)})
	}
	newest := Device{PeerId: self.PeerId, Relay: self.Relay, LastSeen: now.Add(time.Second)}
	got = Merge(many, newest, now.Add(time.Second), MaxAge)
	assert.Len(t, got.Devices, MaxDevices)
	assert.Equal(t, newest, got.Devices[0])
}

func TestDecodeRejectsGarbage(t *testing.T) {
	for _, b := range [][]byte{nil, {2}, {1, 1}, {1, 0, 1, 0}, {1, 0, 1}} {
		_, err := Decode(b)
		assert.ErrorIs(t, err, ErrBadRecord, "%v", b)
	}
	rec := Record{Devices: []Device{{PeerId: newPeerId(t), Relay: "https://r/", LastSeen: time.Now()}}}
	plain, err := Encode(rec)
	require.NoError(t, err)
	_, err = Decode(append(plain, 1))
	assert.ErrorIs(t, err, ErrBadRecord, "trailing bytes")
	_, err = Decode(append(plain, 0, 0, 0))
	assert.NoError(t, err, "zero padding")
}

func TestClientPublishResolve(t *testing.T) {
	ts := httptest.NewServer(dnsserver.New())
	defer ts.Close()
	ts2 := httptest.NewServer(dnsserver.New())
	defer ts2.Close()
	ctx := context.Background()
	k := newKeys(t)

	client, err := NewClient([]string{ts.URL, ts2.URL + "/pkarr/"})
	require.NoError(t, err)

	got, err := client.Resolve(ctx, k.Public())
	require.NoError(t, err)
	assert.Nil(t, got, "nothing published yet")

	now := time.Now().UTC().Truncate(time.Second)
	first := Record{Devices: []Device{{PeerId: newPeerId(t), Relay: "https://r/", LastSeen: now}}}
	p1, err := Seal(k, first)
	require.NoError(t, err)
	require.NoError(t, client.Publish(ctx, p1))

	got, err = client.Resolve(ctx, k.Public())
	require.NoError(t, err)
	require.NotNil(t, got)
	rec, err := Open(k, got)
	require.NoError(t, err)
	assert.Equal(t, first.Devices, rec.Devices)

	// a newer packet replaces it on every relay; the old one is stale
	second := Merge(first, Device{PeerId: newPeerId(t), Relay: "https://r/", LastSeen: now.Add(time.Second)}, now.Add(time.Second), MaxAge)
	p2, err := Seal(k, second)
	require.NoError(t, err)
	require.NoError(t, client.Publish(ctx, p2))
	assert.ErrorIs(t, client.Publish(ctx, p1), ErrStale)

	got, err = client.Resolve(ctx, k.Public())
	require.NoError(t, err)
	rec, err = Open(k, got)
	require.NoError(t, err)
	assert.Len(t, rec.Devices, 2)

	// one relay down: publish and resolve still succeed through the other
	ts2.Close()
	p3, err := Seal(k, Merge(second, Device{PeerId: newPeerId(t), Relay: "https://r/", LastSeen: now.Add(2 * time.Second)}, now.Add(2*time.Second), MaxAge))
	require.NoError(t, err)
	require.NoError(t, client.Publish(ctx, p3))
	got, err = client.Resolve(ctx, k.Public())
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.True(t, got.MoreRecentThan(p2))

	_, err = NewClient(nil)
	assert.Error(t, err)
	_, err = NewClient([]string{"not a url"})
	assert.Error(t, err)
}

// TestMaxDevicesFit is the measurement behind MaxDevices: with a
// one-entry relay table, MaxDevices devices fit one pkarr packet and one
// more does not.
func TestMaxDevicesFit(t *testing.T) {
	k := newKeys(t)
	now := time.Now()
	relay := "https://relay-eu.example.org/" // 29 bytes
	devices := func(n int) []Device {
		out := make([]Device, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, Device{PeerId: newPeerId(t), Relay: relay, LastSeen: now})
		}
		return out
	}
	_, err := sealDevices(k, devices(MaxDevices))
	require.NoError(t, err, "MaxDevices devices fit")
	_, err = sealDevices(k, devices(MaxDevices+1))
	require.Error(t, err, "one more does not")
	packet, err := Seal(k, Record{Devices: devices(MaxDevices + 3)})
	require.NoError(t, err, "Seal trims to what fits")
	got, err := Open(k, packet)
	require.NoError(t, err)
	assert.Len(t, got.Devices, MaxDevices)
}

func TestPaddingHidesDeviceCount(t *testing.T) {
	k := newKeys(t)
	now := time.Now()
	sizes := map[int]int{}
	for _, n := range []int{1, 3, 5} {
		var devices []Device
		for i := 0; i < n; i++ {
			devices = append(devices, Device{PeerId: newPeerId(t), Relay: "https://r/", LastSeen: now})
		}
		packet, err := Seal(k, Record{Devices: devices})
		require.NoError(t, err)
		sizes[n] = len(packet.Bytes())
	}
	assert.Equal(t, sizes[1], sizes[3], "same bucket, same size")
	assert.Equal(t, sizes[3], sizes[5])
}

func TestOpenRejectsForgedAndTampered(t *testing.T) {
	k := newKeys(t)
	other := newKeys(t)
	rec := Record{Devices: []Device{{PeerId: newPeerId(t), Relay: "https://r/", LastSeen: time.Now()}}}

	// a packet under another key never opens
	forged, err := Seal(other, rec)
	require.NoError(t, err)
	_, err = Open(k, forged)
	assert.ErrorIs(t, err, ErrWrongKey)

	// a relay serving another key's packet at this key's address: the
	// client verifies against the address and drops it
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(forged.RelayPayload())
	}))
	defer ts.Close()
	c, err := NewClient([]string{ts.URL})
	require.NoError(t, err)
	packet, err := c.Resolve(context.Background(), k.Public())
	assert.Error(t, err)
	assert.Nil(t, packet)

	// a payload whose ciphertext was tampered with does not open
	plain, err := Encode(rec)
	require.NoError(t, err)
	sealed, err := k.enc.Encrypt(plain)
	require.NoError(t, err)
	sealed[len(sealed)/2] ^= 0x01
	tampered, err := pkarr.FromTxtStrings(k.secret, RecordName, []string{base64.RawURLEncoding.EncodeToString(sealed)}, recordTTL)
	require.NoError(t, err)
	_, err = Open(k, tampered)
	assert.ErrorIs(t, err, ErrBadRecord)

	// a payload of a newer version opens as far as this layout goes and
	// reports its version
	plain[0] = RecordVersion + 1
	newer, err := SealRaw(k, append(plain, 0xff, 0xee), 0)
	require.NoError(t, err)
	got, err := Open(k, newer)
	require.NoError(t, err)
	assert.Equal(t, uint8(RecordVersion+1), got.Version)
	assert.Len(t, got.Devices, 1)
}

func TestReplayedOlderPacketIsRefused(t *testing.T) {
	ts := httptest.NewServer(dnsserver.New())
	defer ts.Close()
	k := newKeys(t)
	c, err := NewClient([]string{ts.URL})
	require.NoError(t, err)
	rec := Record{Devices: []Device{{PeerId: newPeerId(t), Relay: "https://r/", LastSeen: time.Now()}}}
	older, err := Seal(k, rec)
	require.NoError(t, err)
	newer, err := SealAt(k, rec, older.Timestamp()+10)
	require.NoError(t, err)
	require.NoError(t, c.Publish(context.Background(), newer))
	assert.ErrorIs(t, c.Publish(context.Background(), older), ErrStale, "an older packet never replaces a newer one")
	got, err := c.Resolve(context.Background(), k.Public())
	require.NoError(t, err)
	assert.Equal(t, newer.Timestamp(), got.Timestamp())
}

func TestDecodeAndMergeEdgeCases(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	id := newPeerId(t)
	rec := Record{Devices: []Device{{PeerId: id, Relay: "https://r/", LastSeen: now}}}
	plain, err := Encode(rec)
	require.NoError(t, err)

	// relay index out of range
	bad := append([]byte(nil), plain...)
	bad[len(bad)-5] = 7
	_, err = Decode(bad)
	assert.ErrorIs(t, err, ErrBadRecord, "relay index")

	// a relay longer than the wire limit is refused on decode
	long := []byte{RecordVersion, 1}
	long = append(long, 0x01, 0x00) // length 256
	long = append(long, make([]byte, 256)...)
	long = append(long, 0)
	_, err = Decode(long)
	assert.ErrorIs(t, err, ErrBadRecord, "relay length")

	// Merge: duplicates collapse to the newest, future stamps are clamped,
	// unusable relays and own entries are dropped
	self := Device{PeerId: newPeerId(t), Relay: "https://self/", LastSeen: now}
	merged := Merge(Record{Devices: []Device{
		{PeerId: id, Relay: "https://old/", LastSeen: now.Add(-time.Hour)},
		{PeerId: id, Relay: "https://new/", LastSeen: now.Add(-time.Minute)},
		{PeerId: newPeerId(t), Relay: "https://future/", LastSeen: now.Add(48 * time.Hour)},
		{PeerId: newPeerId(t), Relay: "relay-without-scheme", LastSeen: now},
		{PeerId: newPeerId(t), Relay: "https://" + strings.Repeat("x", 300) + "/", LastSeen: now},
		{PeerId: self.PeerId, Relay: "https://stale-self/", LastSeen: now.Add(-time.Hour)},
	}}, self, now, MaxAge)
	byId := map[string]Device{}
	for _, d := range merged.Devices {
		byId[d.PeerId] = d
	}
	assert.Len(t, merged.Devices, 3)
	assert.Equal(t, "https://new/", byId[id].Relay)
	assert.Equal(t, "https://self/", byId[self.PeerId].Relay)
	for _, d := range merged.Devices {
		assert.False(t, d.LastSeen.After(now), "clamped")
	}

	// more relays than the table holds: the devices on the least used
	// relays are dropped, the record still seals
	k := newKeys(t)
	var many []Device
	for i := 0; i < MaxRelays+3; i++ {
		many = append(many, Device{PeerId: newPeerId(t), Relay: fmt.Sprintf("https://relay-%d.example/", i), LastSeen: now.Add(-time.Duration(i) * time.Minute)})
	}
	many = append(many, Device{PeerId: newPeerId(t), Relay: "https://relay-0.example/", LastSeen: now})
	packet, err := Seal(k, Record{Devices: many})
	require.NoError(t, err)
	got, err := Open(k, packet)
	require.NoError(t, err)
	relays := map[string]bool{}
	for _, d := range got.Devices {
		relays[d.Relay] = true
	}
	assert.LessOrEqual(t, len(relays), MaxRelays)
	assert.True(t, relays["https://relay-0.example/"], "the most used relay stays")
}
