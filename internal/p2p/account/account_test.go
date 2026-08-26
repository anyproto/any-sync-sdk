package account

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/go-iroh/dnsserver"
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
	_, err = Decode(append(plain, 0))
	assert.ErrorIs(t, err, ErrBadRecord, "trailing bytes")
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
