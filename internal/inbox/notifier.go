// Package inbox is the coordinator-inbox notifier: the optional Layer-2
// discovery for 1-1 spaces (docs/13). It funnels the coordinator's push
// stream and a periodic poll into a single serialized worker that fetches
// inbox messages, verifies + decrypts each, and hands the result to a
// caller-supplied handler — advancing a device-local cursor only after a
// message is handled.
//
// It deliberately fixes the heart inbox bugs catalogued in docs/13:
// the cursor advances per-message AFTER the handler commits (no
// process-before-persist loss); content failures (verify/decrypt) are
// skipped with a loud log while transient handler failures (ErrRetry)
// halt the cursor and retry; both triggers feed one worker so there is
// no double-process race; the cursor is device-local (a plain any-store
// collection in sdk.db, never synced).
package inbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/util/crypto"
	"go.uber.org/zap"
)

var log = logger.NewNamed("sdk.inbox")

// ErrRetry, returned by a Handler, marks a TRANSIENT failure (e.g. a
// local write that should be retried). The notifier halts the cursor at
// that message and reprocesses it on the next pass. Any other handler
// outcome (nil, or a non-retry error) advances the cursor past the
// message — the handler is expected to swallow permanent conditions.
var ErrRetry = errors.New("inbox: retry")

// FetchFunc fetches inbox messages after offset (empty offset = from the
// beginning). Bodies are returned still encrypted — the notifier
// verifies + decrypts. Matches any-sync inboxclient.InboxFetch.
type FetchFunc func(ctx context.Context, offset string) (msgs []*coordinatorproto.InboxMessage, hasMore bool, err error)

// Message is one verified, decrypted inbox message handed to a Handler.
// SenderIdentity is the coordinator-verified account address (the body
// signature checked against it) — the authoritative peer identity, NOT
// anything self-declared inside Body.
type Message struct {
	Id             string
	SenderIdentity string
	PayloadType    coordinatorproto.InboxPayloadType
	Body           []byte
	Timestamp      int64
}

// Handler processes one verified message. Return nil on success, ErrRetry
// for a transient failure (halts + retries the cursor), or any other
// error to log-and-skip (advances the cursor). The handler must be
// idempotent: a message can be re-delivered (crash mid-pass, coordinator
// resend) and the single crash-replay reprocesses the last message.
type Handler func(ctx context.Context, m Message) error

// Deps configures a Notifier.
type Deps struct {
	// Fetch pulls messages from the coordinator inbox.
	Fetch FetchFunc
	// MyKey is the account private key used to decrypt message bodies
	// (bodies are ECIES-encrypted to our account public key on send).
	MyKey crypto.PrivKey
	// DB is the SDK's device-local store (sdk.db) — the cursor lives in
	// a plain collection here, never synced.
	DB anystore.DB
	// Handle receives each verified, decrypted message.
	Handle Handler
	// Interval is the poll fallback cadence. The push stream drives
	// latency; this is the safety net for missed/disconnected pushes.
	Interval time.Duration
	// CursorKey namespaces the cursor doc, so distinct payload streams
	// could keep independent offsets. Defaults to "onetoone".
	CursorKey string
}

// cursorCollection is the device-local any-store collection holding the
// fetch offset. Plain collection (not a CRDT controller), so it never
// enters the DAG and never syncs across devices.
const cursorCollection = "inbox_cursor"
const fieldOffset = "offset"

// Notifier owns the single serialized worker. Construct with New, drive
// with Run; Notify kicks an immediate pass; Close stops and drains.
type Notifier struct {
	deps Deps
	kick chan struct{}

	mu      sync.Mutex
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running bool
}

// New builds a Notifier. It does not start the worker — call Run.
func New(d Deps) *Notifier {
	if d.Interval <= 0 {
		d.Interval = 60 * time.Second
	}
	if d.CursorKey == "" {
		d.CursorKey = "onetoone"
	}
	return &Notifier{deps: d, kick: make(chan struct{}, 1)}
}

// Run starts the worker goroutine bound to a fresh cancellable context.
// Idempotent: a second call while running is a no-op.
func (n *Notifier) Run(context.Context) {
	n.mu.Lock()
	if n.running {
		n.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	n.running = true
	n.wg.Add(1)
	n.mu.Unlock()
	go n.loop(ctx)
}

// Notify wakes the worker for an immediate pass. Non-blocking and
// coalescing (buffered-1 kick). Called by the coordinator push forwarder
// and by send-on-initiate (so the sender's own devices converge quickly).
func (n *Notifier) Notify() {
	select {
	case n.kick <- struct{}{}:
	default:
	}
}

// Close stops the worker and waits for it to drain. Safe to call once.
func (n *Notifier) Close() {
	n.mu.Lock()
	if !n.running {
		n.mu.Unlock()
		return
	}
	n.running = false
	cancel := n.cancel
	n.mu.Unlock()
	cancel()
	n.wg.Wait()
}

func (n *Notifier) loop(ctx context.Context) {
	defer n.wg.Done()
	t := time.NewTicker(n.deps.Interval)
	defer t.Stop()
	// Initial pass: catch messages that arrived while offline, plus any
	// the push stream missed before this worker installed its handler.
	n.processAll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n.processAll(ctx)
		case <-n.kick:
			n.processAll(ctx)
		}
	}
}

// processAll drains the inbox from the persisted cursor. Fetch errors
// (offline / coordinator down / inbox unimplemented) end the pass without
// advancing — the next tick retries. A handler ErrRetry halts the cursor
// at the offending message; content failures skip past it.
func (n *Notifier) processAll(ctx context.Context) {
	offset, err := n.loadCursor(ctx)
	if err != nil {
		log.Warn("load cursor", zap.Error(err))
		return
	}
	for {
		if ctx.Err() != nil {
			return
		}
		msgs, hasMore, err := n.deps.Fetch(ctx, offset)
		if err != nil {
			// Offline / down / unimplemented — silent-ish; retry next tick.
			log.Debug("inbox fetch", zap.Error(err))
			return
		}
		if len(msgs) == 0 {
			return
		}
		for _, msg := range msgs {
			if ctx.Err() != nil {
				return
			}
			retry, derr := n.processOne(ctx, msg)
			if retry {
				// Transient — leave the cursor before this message and
				// reprocess on the next pass. Halt the whole drain so we
				// don't advance past it via a later message.
				log.Warn("inbox handler retry; halting cursor",
					zap.String("messageId", msg.Id), zap.Error(derr))
				return
			}
			// Success or content-skip: advance past this message.
			offset = msg.Id
			if err := n.saveCursor(ctx, offset); err != nil {
				// Couldn't persist the advance — stop so we don't process
				// the next message against an un-saved cursor (a crash
				// would otherwise lose it). Retry next tick.
				log.Warn("save cursor", zap.String("messageId", msg.Id), zap.Error(err))
				return
			}
		}
		if !hasMore {
			return
		}
	}
}

// processOne verifies + decrypts one message and dispatches it. Returns
// retry=true only when the handler asks to retry (transient). Content
// failures (nil packet, bad sender, bad signature, decrypt failure) are
// logged and reported as non-retry (skip-past) — they are non-transient,
// so retrying would wedge the cursor forever.
func (n *Notifier) processOne(ctx context.Context, msg *coordinatorproto.InboxMessage) (retry bool, err error) {
	if msg.Packet == nil || msg.Packet.Payload == nil {
		log.Warn("inbox message missing packet/payload; skipping", zap.String("messageId", msg.Id))
		return false, nil
	}
	pkt := msg.Packet
	senderPub, err := crypto.DecodeAccountAddress(pkt.SenderIdentity)
	if err != nil {
		log.Warn("inbox bad sender identity; skipping",
			zap.String("messageId", msg.Id), zap.String("sender", pkt.SenderIdentity), zap.Error(err))
		return false, nil
	}
	// The signature is over the ENCRYPTED body (matches inboxclient send).
	ok, err := senderPub.Verify(pkt.Payload.Body, pkt.SenderSignature)
	if err != nil || !ok {
		log.Warn("inbox signature verify failed; skipping",
			zap.String("messageId", msg.Id), zap.String("sender", pkt.SenderIdentity))
		return false, nil
	}
	plain, err := n.deps.MyKey.Decrypt(pkt.Payload.Body)
	if err != nil {
		log.Warn("inbox decrypt failed; skipping",
			zap.String("messageId", msg.Id), zap.Error(err))
		return false, nil
	}
	m := Message{
		Id:             msg.Id,
		SenderIdentity: pkt.SenderIdentity,
		PayloadType:    pkt.Payload.PayloadType,
		Body:           plain,
		Timestamp:      pkt.Payload.Timestamp,
	}
	if herr := n.deps.Handle(ctx, m); herr != nil {
		if errors.Is(herr, ErrRetry) {
			return true, herr
		}
		// Non-retry handler error: log and skip (advance). The handler is
		// responsible for swallowing permanent-but-benign conditions as
		// nil; a non-retry error here means "valid message, can't act on
		// it" — don't wedge the queue.
		log.Warn("inbox handler error; skipping",
			zap.String("messageId", msg.Id), zap.Error(herr))
		return false, nil
	}
	return false, nil
}

func (n *Notifier) loadCursor(ctx context.Context) (string, error) {
	coll, err := n.deps.DB.Collection(ctx, cursorCollection)
	if err != nil {
		return "", err
	}
	doc, err := coll.FindId(ctx, n.deps.CursorKey)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return "", nil
		}
		return "", err
	}
	return doc.Value().GetString(fieldOffset), nil
}

func (n *Notifier) saveCursor(ctx context.Context, offset string) error {
	coll, err := n.deps.DB.Collection(ctx, cursorCollection)
	if err != nil {
		return err
	}
	a := &anyenc.Arena{}
	doc := a.NewObject()
	doc.Set("id", a.NewString(n.deps.CursorKey))
	doc.Set(fieldOffset, a.NewString(offset))
	if err := coll.UpsertOne(ctx, doc); err != nil {
		return fmt.Errorf("inbox: upsert cursor: %w", err)
	}
	return nil
}
