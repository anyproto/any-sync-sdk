package inbox

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testDB(t *testing.T) anystore.DB {
	t.Helper()
	db, err := anystore.Open(context.Background(), filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
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

// drainNotifier runs one processAll pass synchronously (no background
// loop) so assertions are deterministic.
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
	n := New(Deps{
		Fetch: func(_ context.Context, offset string) ([]*coordinatorproto.InboxMessage, bool, error) {
			// Return only messages after offset (coordinator semantics).
			out := msgs[:0:0]
			seen := offset == ""
			for _, m := range msgs {
				if seen {
					out = append(out, m)
				}
				if m.Id == offset {
					seen = true
				}
			}
			return out, false, nil
		},
		MyKey:  myPriv,
		DB:     testDB(t),
		Handle: func(_ context.Context, m Message) error { got = append(got, m); return nil },
	})

	n.drainOnce(ctx)
	require.Len(t, got, 2)
	assert.Equal(t, "hello", string(got[0].Body))
	assert.Equal(t, senderPriv.GetPublic().Account(), got[0].SenderIdentity)

	// Cursor advanced to the last id: a second drain delivers nothing.
	off, err := n.loadCursor(ctx)
	require.NoError(t, err)
	assert.Equal(t, "m2", off)
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
	n := New(Deps{
		Fetch: func(_ context.Context, _ string) ([]*coordinatorproto.InboxMessage, bool, error) {
			return []*coordinatorproto.InboxMessage{good, bad, after}, false, nil
		},
		MyKey:  myPriv,
		DB:     testDB(t),
		Handle: func(_ context.Context, m Message) error { got = append(got, string(m.Body)); return nil },
	})
	n.drainOnce(ctx)
	// bad message skipped (content failure), good + after delivered, cursor
	// advanced past all three.
	assert.Equal(t, []string{"ok", "after"}, got)
	off, _ := n.loadCursor(ctx)
	assert.Equal(t, "m3", off)
}

func TestNotifier_RetryHaltsCursor(t *testing.T) {
	ctx := context.Background()
	senderPriv, _, _ := crypto.GenerateRandomEd25519KeyPair()
	myPriv, myPub, _ := crypto.GenerateRandomEd25519KeyPair()

	m1 := signedMsg(t, "m1", senderPriv, myPub, []byte("first"))
	m2 := signedMsg(t, "m2", senderPriv, myPub, []byte("second"))

	var attempts int
	n := New(Deps{
		Fetch: func(_ context.Context, offset string) ([]*coordinatorproto.InboxMessage, bool, error) {
			all := []*coordinatorproto.InboxMessage{m1, m2}
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
			return out, false, nil
		},
		MyKey: myPriv,
		DB:    testDB(t),
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
	off, _ := n.loadCursor(ctx)
	assert.Equal(t, "", off, "cursor must not advance past a retry message")

	// Second pass: m1 now succeeds, then m2 — cursor reaches m2.
	n.drainOnce(ctx)
	off, _ = n.loadCursor(ctx)
	assert.Equal(t, "m2", off)
	assert.Equal(t, 2, attempts, "m1 retried exactly once")
}

func TestNotifier_FetchErrorDoesNotAdvance(t *testing.T) {
	ctx := context.Background()
	myPriv, _, _ := crypto.GenerateRandomEd25519KeyPair()
	n := New(Deps{
		Fetch: func(_ context.Context, _ string) ([]*coordinatorproto.InboxMessage, bool, error) {
			return nil, false, errors.New("offline")
		},
		MyKey:  myPriv,
		DB:     testDB(t),
		Handle: func(_ context.Context, _ Message) error { return nil },
	})
	n.drainOnce(ctx) // must not panic or advance
	off, _ := n.loadCursor(ctx)
	assert.Equal(t, "", off)
}

func TestNotifier_RunNotifyCloseLifecycle(t *testing.T) {
	ctx := context.Background()
	myPriv, _, _ := crypto.GenerateRandomEd25519KeyPair()
	var mu sync.Mutex
	var passes int
	n := New(Deps{
		Fetch: func(_ context.Context, _ string) ([]*coordinatorproto.InboxMessage, bool, error) {
			mu.Lock()
			passes++
			mu.Unlock()
			return nil, false, nil
		},
		MyKey:    myPriv,
		DB:       testDB(t),
		Handle:   func(_ context.Context, _ Message) error { return nil },
		Interval: time.Hour, // only the initial pass + kicks fire
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
