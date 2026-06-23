package spaceimpl

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/inbox"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

var inboxLog = logger.NewNamed("sdk.onetoone.inbox")

// inboxPollInterval is the notifier's poll-fallback cadence (the push
// stream drives latency). inviteRetryInterval is the send-retry loop's
// cadence. Both var (not const) so tests can shorten them; read once when
// the loops start, so set before StartOneToOneInbox.
var (
	inboxPollInterval   = 60 * time.Second
	inviteRetryInterval = 60 * time.Second
)

// SetOneToOneInboxIntervalsForTest overrides the notifier poll and
// invite-retry intervals, returning a restore func. Test seam only — call
// before SDK.Open (the loops read the values once at start).
func SetOneToOneInboxIntervalsForTest(poll, retry time.Duration) (restore func()) {
	pp, rr := inboxPollInterval, inviteRetryInterval
	inboxPollInterval, inviteRetryInterval = poll, retry
	return func() { inboxPollInterval, inviteRetryInterval = pp, rr }
}

// oneToOneInviteToSend is the device-local marker value meaning this
// device still owes the peer an inbox notification for an initiated 1-1.
const oneToOneInviteToSend = "toSend"

// StartOneToOneInbox wires and starts the Layer-2 inbox subsystem: the
// receive notifier (coordinator push + poll → RegisterIncoming) and the
// send-retry loop. No-op when the inbox transport is unavailable (a
// coordinator-less deployment), leaving the out-of-band 1-1 path intact.
//
// Called from sdk.Open AFTER the tech space is open — the receive handler
// and send loop both write the tech space, so they must not run before it
// is ready.
func (s *Service) StartOneToOneInbox(ctx context.Context) {
	ic := s.app.InboxClient()
	keys := s.app.AccountKeys()
	if ic == nil || keys == nil {
		return
	}
	s.inboxNotifier = inbox.New(inbox.Deps{
		Fetch:  ic.InboxFetch,
		MyKey:  keys.SignKey,
		Handle: s.handleInboxMessage,
		// Account-scoped read position: load from / advance the synced
		// tech-space cursor (monotonic-forward via SetInboxCursor). A fresh
		// device seeds from it instead of replaying the whole inbox.
		LoadCursor: func(ctx context.Context) (string, error) { return s.tsp.GetInboxCursor(ctx), nil },
		SaveCursor: s.tsp.SetInboxCursor,
		// Warmup: pull the tech space current before the first fetch so a
		// fresh device seeds from the synced cursor, not offset 0.
		Warmup:   func(ctx context.Context) error { return s.tsp.SyncHeads(ctx) },
		Interval: inboxPollInterval,
	})
	// Coordinator push → kick the notifier. The event is body-less; the
	// notifier reacts by fetching.
	s.app.OnInboxMessage(func(*coordinatorproto.NotifySubscribeEvent) {
		if s.inboxNotifier != nil {
			s.inboxNotifier.Notify()
		}
	})
	s.inboxNotifier.Run(ctx)
	s.startInviteRetry()
}

// stopOneToOneInbox tears down the inbox subsystem. Called from Close
// before the tech space / db are torn down.
func (s *Service) stopOneToOneInbox() {
	s.app.OnInboxMessage(nil)
	if s.inviteCancel != nil {
		s.inviteCancel()
	}
	s.inviteWG.Wait()
	if s.inboxNotifier != nil {
		s.inboxNotifier.Close()
	}
}

// handleInboxMessage is the notifier's dispatch callback. It turns a
// verified, decrypted 1-1 invite into a pending row via RegisterIncoming.
// SenderIdentity is the coordinator-verified account — the authoritative
// peer identity; the body is a display-only profile snapshot (no
// self-declared identity is trusted).
func (s *Service) handleInboxMessage(ctx context.Context, m inbox.Message) error {
	if m.PayloadType != coordinatorproto.InboxPayloadType_InboxPayloadOneToOneInvite {
		// Unknown payload type — skip (advance cursor). Forward-compat with
		// future inbox payloads (e.g. regular invites).
		return nil
	}
	keys := s.app.AccountKeys()
	if keys != nil && m.SenderIdentity == keys.SignKey.GetPublic().Account() {
		// Defensive: never register an incoming from ourselves.
		return nil
	}
	hint := space.DecodeAccountMetadata(m.Body)
	if err := s.RegisterIncoming(ctx, m.SenderIdentity, hint); err != nil {
		// RegisterIncoming swallows benign conditions (existing row, sticky
		// decline) as nil; a real error here is a transient tech-space
		// write. Ask the notifier to retry rather than drop the invite.
		return fmt.Errorf("%w: register incoming: %v", inbox.ErrRetry, err)
	}
	inboxLog.Debug("registered incoming 1-1", zap.String("peer", m.SenderIdentity))
	return nil
}

// markOneToOneInviteToSend flags a freshly-initiated 1-1 as owing the peer
// an inbox notification and kicks the retry loop to deliver it now. Called
// by OneToOne (the initiate path) only — Accept must not notify back
// (v1 surfaces no delivery/accept signal). No-op when inbox is
// unavailable.
func (s *Service) markOneToOneInviteToSend(ctx context.Context, spaceId string) {
	if s.app.InboxClient() == nil {
		return
	}
	if _, err := s.tsp.SetOneToOneInviteState(ctx, spaceId, oneToOneInviteToSend); err != nil {
		inboxLog.Warn("mark invite toSend", zap.String("spaceId", spaceId), zap.Error(err))
		return
	}
	s.kickInviteRetry()
}

// startInviteRetry launches the send-retry loop bound to its own
// cancellable context. Drained by stopOneToOneInbox via inviteWG.
func (s *Service) startInviteRetry() {
	ctx, cancel := context.WithCancel(context.Background())
	s.inviteCancel = cancel
	s.inviteWG.Add(1)
	go s.inviteRetryLoop(ctx)
}

// kickInviteRetry wakes the send-retry loop immediately (buffered-1 kick).
func (s *Service) kickInviteRetry() {
	select {
	case s.inviteKick <- struct{}{}:
	default:
	}
}

// inviteRetryLoop redelivers pending 1-1 inbox notifications until ctx is
// cancelled. An initial pass covers invites left unsent from a previous
// session (offline at initiate); thereafter it runs on tick or kick.
func (s *Service) inviteRetryLoop(ctx context.Context) {
	defer s.inviteWG.Done()
	t := time.NewTicker(inviteRetryInterval)
	defer t.Stop()
	s.reconcileInvites(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.reconcileInvites(ctx)
		case <-s.inviteKick:
			s.reconcileInvites(ctx)
		}
	}
}

// reconcileInvites runs one send-retry pass over the current rows, wiring
// the real inbox send and tech-space marker-clear into the pure core.
func (s *Service) reconcileInvites(ctx context.Context) {
	clear := func(ctx context.Context, spaceId string) error {
		_, err := s.tsp.SetOneToOneInviteState(ctx, spaceId, "")
		return err
	}
	reconcileOneToOneInvites(ctx, s.tsp.List(ctx), s.sendOneToOneInvite, clear)
}

// reconcileOneToOneInvites is the pure send-retry core: for each 1-1 row
// still owing a notification it sends and clears the marker on confirmed
// delivery; a send failure (offline / coordinator down) leaves the marker
// set so the NEXT pass retries — this is the offline-at-initiate →
// delivered-once-online path. A malformed row (no peer) is cleared so the
// loop doesn't spin on it. send/clear are injected so the behavior is
// unit-testable without a live coordinator.
func reconcileOneToOneInvites(
	ctx context.Context,
	rows []techspace.SpaceIndexRecord,
	send func(ctx context.Context, receiverId string) error,
	clear func(ctx context.Context, spaceId string) error,
) {
	for _, r := range rows {
		if ctx.Err() != nil {
			return
		}
		if r.Type != space.SpaceTypeOneToOne || r.OneToOneInviteState != oneToOneInviteToSend {
			continue
		}
		if r.OneToOnePeer == "" {
			if err := clear(ctx, r.Id); err != nil {
				inboxLog.Warn("clear malformed invite marker", zap.String("spaceId", r.Id), zap.Error(err))
			}
			continue
		}
		if err := send(ctx, r.OneToOnePeer); err != nil {
			inboxLog.Debug("send 1-1 invite (will retry)",
				zap.String("spaceId", r.Id), zap.String("peer", r.OneToOnePeer), zap.Error(err))
			continue
		}
		if err := clear(ctx, r.Id); err != nil {
			inboxLog.Warn("clear invite marker", zap.String("spaceId", r.Id), zap.Error(err))
		}
	}
}

// sendOneToOneInvite posts one InboxPayloadOneToOneInvite to receiverId.
// The body is our own profile snapshot (display hint); any-sync encrypts
// it to the receiver's account key and signs it on send. Best-effort —
// the receiver can also discover the 1-1 out-of-band, and the space is
// re-derivable regardless.
func (s *Service) sendOneToOneInvite(ctx context.Context, receiverId string) error {
	ic := s.app.InboxClient()
	if ic == nil {
		return errors.New("spaceimpl: inbox unavailable")
	}
	recvPub, err := decodeIdentity(receiverId)
	if err != nil {
		return fmt.Errorf("spaceimpl: send invite: %w", err)
	}
	myId := s.app.AccountKeys().SignKey.GetPublic().Account()

	var hint space.AccountMetadata
	if prof, ok := s.tsp.GetProfile(ctx); ok {
		hint = space.AccountMetadata{Name: prof.Name, Description: prof.Description, IconCID: prof.IconCID}
	}
	body := space.EncodeAccountMetadata(hint)

	msg := &coordinatorproto.InboxMessage{
		Packet: &coordinatorproto.InboxPacket{
			SenderIdentity:   myId,
			ReceiverIdentity: receiverId,
			Payload: &coordinatorproto.InboxPayload{
				PayloadType: coordinatorproto.InboxPayloadType_InboxPayloadOneToOneInvite,
				Timestamp:   time.Now().Unix(),
				Body:        body,
			},
		},
	}
	if err := ic.InboxAddMessage(ctx, recvPub, msg); err != nil {
		return fmt.Errorf("spaceimpl: inbox add message: %w", err)
	}
	return nil
}
