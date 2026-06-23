package inbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memCursor is an in-memory stand-in for the synced tech-space cursor,
// monotonic-forward like techspace.SetInboxCursor.
type memCursor struct {
	mu  sync.Mutex
	off string
}

func (c *memCursor) load(context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.off, nil
}

func (c *memCursor) save(_ context.Context, o string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if o > c.off { // monotonic-forward (lexical, like ObjectID hex)
		c.off = o
	}
	return nil
}

func (c *memCursor) get() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.off
}

// signedMsg builds a real inbox message the way inboxclient.InboxAddMessage
// does: body encrypted to the receiver's pubkey, signature over the
// ciphertext by the sender.
func signedMsg(t *testing.T, id string, senderPriv crypto.PrivKey, receiverPub crypto.PubKey, plain []byte) *coordinatorproto.InboxMessage {
	t.Helper()
	ciphertext, err := receiverPub.Encrypt(plain)
	require.NoError(t, err)
	sig, err := senderPriv.Sign(ciphertext)
	require.NoError(t, err)
	return &coordinatorproto.InboxMessage{
		Id: id,
		Packet: &coordinatorproto.InboxPacket{
			SenderIdentity: senderPriv.GetPublic().Account(),
			Payload: &coordinatorproto.InboxPayload{
				PayloadType: coordinatorproto.InboxPayloadType_InboxPayloadOneToOneInvite,
				Body:        ciphertext,
			},
			SenderSignature: sig,
		},
	}
}

// afterOffset returns the messages following offset (coordinator
// semantics: everything with id > offset, in order).
func afterOffset(all []*coordinatorproto.InboxMessage, offset string) []*coordinatorproto.InboxMessage {
	out := all[:0:0]
	seen := offset == ""
	for _, m := range all {
		if seen {
			out = append(out, m)
		}
		if m.Id == offset {
			seen = true
		}
	}
	return out
}

// drainOnce runs one processAll pass synchronously (no background loop) so
// assertions are deterministic.
func (n *Notifier) drainOnce(ctx context.Context) { n.processAll(ctx) }

func TestNotifier_VerifiesDecryptsAndAdvances(t *testing.T) {
	ctx := context.Background()
	senderPriv, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	myPriv, myPub, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)

	var got []Message
	msgs := []*coordinatorproto.InboxMessage{
		signedMsg(t, "m1", senderPriv, myPub, []byte("hello")),
		signedMsg(t, "m2", senderPriv, myPub, []byte("world")),
	}
	cur := &memCursor{}
	n := New(Deps{
		Fetch: func(_ context.Context, offset string) ([]*coordinatorproto.InboxMessage, bool, error) {
			return afterOffset(msgs, offset), false, nil
		},
		MyKey:      myPriv,
		LoadCursor: cur.load,
		SaveCursor: cur.save,
		Handle:     func(_ context.Context, m Message) error { got = append(got, m); return nil },
	})

	n.drainOnce(ctx)
	require.Len(t, got, 2)
	assert.Equal(t, "hello", string(got[0].Body))
	assert.Equal(t, senderPriv.GetPublic().Account(), got[0].SenderIdentity)

	// Cursor advanced to the last id: a second drain delivers nothing.
	assert.Equal(t, "m2", cur.get())
	got = nil
	n.drainOnce(ctx)
	assert.Empty(t, got, "cursor should prevent reprocessing")
}

func TestNotifier_SkipsBadSignatureButAdvances(t *testing.T) {
	ctx := context.Background()
	senderPriv, _, _ := crypto.GenerateRandomEd25519KeyPair()
	wrongPriv, _, _ := crypto.GenerateRandomEd25519KeyPair()
	myPriv, myPub, _ := crypto.GenerateRandomEd25519KeyPair()

	good := signedMsg(t, "m1", senderPriv, myPub, []byte("ok"))
	// Tamper: signature from a different key → verify fails.
	bad := signedMsg(t, "m2", senderPriv, myPub, []byte("evil"))
	badSig, _ := wrongPriv.Sign(bad.Packet.Payload.Body)
	bad.Packet.SenderSignature = badSig
	after := signedMsg(t, "m3", senderPriv, myPub, []byte("after"))

	var got []string
	cur := &memCursor{}
	n := New(Deps{
		Fetch: func(_ context.Context, _ string) ([]*coordinatorproto.InboxMessage, bool, error) {
			return []*coordinatorproto.InboxMessage{good, bad, after}, false, nil
		},
		MyKey:      myPriv,
		LoadCursor: cur.load,
		SaveCursor: cur.save,
		Handle:     func(_ context.Context, m Message) error { got = append(got, string(m.Body)); return nil },
	})
	n.drainOnce(ctx)
	// bad message skipped (content failure), good + after delivered, cursor
	// advanced past all three.
	assert.Equal(t, []string{"ok", "after"}, got)
	assert.Equal(t, "m3", cur.get())
}

func TestNotifier_RetryHaltsCursor(t *testing.T) {
	ctx := context.Background()
	senderPriv, _, _ := crypto.GenerateRandomEd25519KeyPair()
	myPriv, myPub, _ := crypto.GenerateRandomEd25519KeyPair()

	m1 := signedMsg(t, "m1", senderPriv, myPub, []byte("first"))
	m2 := signedMsg(t, "m2", senderPriv, myPub, []byte("second"))

	var attempts int
	cur := &memCursor{}
	n := New(Deps{
		Fetch: func(_ context.Context, offset string) ([]*coordinatorproto.InboxMessage, bool, error) {
			return afterOffset([]*coordinatorproto.InboxMessage{m1, m2}, offset), false, nil
		},
		MyKey:      myPriv,
		LoadCursor: cur.load,
		SaveCursor: cur.save,
		Handle: func(_ context.Context, m Message) error {
			if m.Id == "m1" {
				attempts++
				if attempts < 2 {
					return fmt.Errorf("transient: %w", ErrRetry)
				}
			}
			return nil
		},
	})

	// First pass: m1 returns ErrRetry → cursor halts before m1, m2 not reached.
	n.drainOnce(ctx)
	assert.Equal(t, "", cur.get(), "cursor must not advance past a retry message")

	// Second pass: m1 now succeeds, then m2 — cursor reaches m2.
	n.drainOnce(ctx)
	assert.Equal(t, "m2", cur.get())
	assert.Equal(t, 2, attempts, "m1 retried exactly once")
}

func TestNotifier_FetchErrorDoesNotAdvance(t *testing.T) {
	ctx := context.Background()
	myPriv, _, _ := crypto.GenerateRandomEd25519KeyPair()
	cur := &memCursor{}
	n := New(Deps{
		Fetch: func(_ context.Context, _ string) ([]*coordinatorproto.InboxMessage, bool, error) {
			return nil, false, errors.New("offline")
		},
		MyKey:      myPriv,
		LoadCursor: cur.load,
		SaveCursor: cur.save,
		Handle:     func(_ context.Context, _ Message) error { return nil },
	})
	n.drainOnce(ctx) // must not panic or advance
	assert.Equal(t, "", cur.get())
}

// TestNotifier_OfflineThenOnlineDelivers simulates the receive side coming
// online: the first pass fails to fetch (offline), the second pass
// succeeds, and the message that was waiting is delivered with the cursor
// advancing. This is the 1-1-discovery analogue of "Bob was offline when
// Alice's invite landed in the inbox; it's delivered once he reconnects".
func TestNotifier_OfflineThenOnlineDelivers(t *testing.T) {
	ctx := context.Background()
	senderPriv, _, _ := crypto.GenerateRandomEd25519KeyPair()
	myPriv, myPub, _ := crypto.GenerateRandomEd25519KeyPair()
	waiting := signedMsg(t, "m1", senderPriv, myPub, []byte("invite"))

	online := false
	var got []string
	cur := &memCursor{}
	n := New(Deps{
		Fetch: func(_ context.Context, _ string) ([]*coordinatorproto.InboxMessage, bool, error) {
			if !online {
				return nil, false, errors.New("offline")
			}
			return []*coordinatorproto.InboxMessage{waiting}, false, nil
		},
		MyKey:      myPriv,
		LoadCursor: cur.load,
		SaveCursor: cur.save,
		Handle:     func(_ context.Context, m Message) error { got = append(got, string(m.Body)); return nil },
	})

	// Offline pass: nothing delivered, cursor unmoved.
	n.drainOnce(ctx)
	assert.Empty(t, got)
	assert.Equal(t, "", cur.get())

	// Come online: the waiting invite is delivered and the cursor advances.
	online = true
	n.drainOnce(ctx)
	assert.Equal(t, []string{"invite"}, got)
	assert.Equal(t, "m1", cur.get())
}

// TestNotifier_WarmupRunsBeforeFirstFetch checks the warmup hook fires once
// before the worker's first pass (used to pull the synced cursor current on
// a fresh device before fetching).
func TestNotifier_WarmupRunsBeforeFirstFetch(t *testing.T) {
	ctx := context.Background()
	myPriv, _, _ := crypto.GenerateRandomEd25519KeyPair()
	var mu sync.Mutex
	warmups, fetches := 0, 0
	warmedBeforeFetch := true
	cur := &memCursor{}
	n := New(Deps{
		Fetch: func(_ context.Context, _ string) ([]*coordinatorproto.InboxMessage, bool, error) {
			mu.Lock()
			if warmups == 0 {
				warmedBeforeFetch = false
			}
			fetches++
			mu.Unlock()
			return nil, false, nil
		},
		MyKey:      myPriv,
		LoadCursor: cur.load,
		SaveCursor: cur.save,
		Handle:     func(_ context.Context, _ Message) error { return nil },
		Warmup: func(context.Context) error {
			mu.Lock()
			warmups++
			mu.Unlock()
			return nil
		},
		Interval: time.Hour,
	})
	n.Run(ctx)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return fetches >= 1
	}, 2*time.Second, 10*time.Millisecond)
	n.Close()
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, warmups, "warmup runs exactly once")
	assert.True(t, warmedBeforeFetch, "warmup must precede the first fetch")
}

func TestNotifier_RunNotifyCloseLifecycle(t *testing.T) {
	ctx := context.Background()
	myPriv, _, _ := crypto.GenerateRandomEd25519KeyPair()
	var mu sync.Mutex
	var passes int
	cur := &memCursor{}
	n := New(Deps{
		Fetch: func(_ context.Context, _ string) ([]*coordinatorproto.InboxMessage, bool, error) {
			mu.Lock()
			passes++
			mu.Unlock()
			return nil, false, nil
		},
		MyKey:      myPriv,
		LoadCursor: cur.load,
		SaveCursor: cur.save,
		Handle:     func(_ context.Context, _ Message) error { return nil },
		Interval:   time.Hour, // only the initial pass + kicks fire
	})
	n.Run(ctx)
	n.Notify()
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return passes >= 1
	}, 2*time.Second, 10*time.Millisecond)
	n.Close()
	// Close is idempotent and Notify after close is a no-op (no panic).
	n.Close()
	n.Notify()
}
