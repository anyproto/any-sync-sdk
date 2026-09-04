package spaceimpl

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
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
// cadence. Atomics (nanoseconds) so the test seam can override or
// restore them while another SDK instance's loops start concurrently
// (parallel tests share the process); each loop reads its value once
// at start, so set before StartOneToOneInbox.
var (
	inboxPollInterval   = newAtomicInterval(60 * time.Second)
	inviteRetryInterval = newAtomicInterval(60 * time.Second)
)

// newAtomicInterval seeds the race-free carrier for an interval seam.
func newAtomicInterval(d time.Duration) *atomic.Int64 {
	var v atomic.Int64
	v.Store(int64(d))
	return &v
}

// SetOneToOneInboxIntervalsForTest overrides the notifier poll and
// invite-retry intervals, returning a restore func. Test seam only — call
// before SDK.Open (the loops read the values once at start).
func SetOneToOneInboxIntervalsForTest(poll, retry time.Duration) (restore func()) {
	pp, rr := inboxPollInterval.Swap(int64(poll)), inviteRetryInterval.Swap(int64(retry))
	return func() {
		inboxPollInterval.Store(pp)
		inviteRetryInterval.Store(rr)
	}
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
		// ReplayGuard: an empty cursor may only be trusted once the tech
		// space provably reflects the account's synced state — otherwise a
		// cold-restored device replays the inbox and resurrects resolved
		// invites (see inboxReplayGuard).
		ReplayGuard: s.inboxReplayGuard,
		Interval:    time.Duration(inboxPollInterval.Load()),
	})
	// Coordinator push → kick the notifier. The event is body-less; the
	// notifier reacts by fetching.
	s.app.OnInboxMessage(func(*coordinatorproto.NotifySubscribeEvent) {
		if s.inboxNotifier != nil {
			s.inboxNotifier.Notify()
		}
	})
	// Start the send-retry loop (which also publishes inviteCtx) BEFORE
	// the notifier: the first fetched message can dispatch immediately,
	// and handleRegularInvite reads inviteCtx.
	s.startInviteRetry()
	s.inboxNotifier.Run(ctx)
}

// inboxReplayGuard gates the notifier's empty-cursor (full-replay)
// pass: nil only when the tech space provably holds the account's
// synced state. One head-sync round runs first — its diff fetches and
// applies missing trees synchronously (failures park in the
// treesyncer) — then a zero parked-tree count confirms everything the
// diff discovered is applied. Until both hold, an empty cursor just
// means "not synced to this device yet", and replaying the inbox would
// resurrect long-resolved 1-1 invites as pending rows: the cold-restore
// stale-join-request bug. On a genuinely fresh account the round
// converges trivially, the guard passes with the cursor still empty,
// and replaying from the beginning is correct (nothing was ever
// processed). Each deferred pass retries the round, which also drives
// the treesyncer's parked-tree recovery.
func (s *Service) inboxReplayGuard(ctx context.Context) error {
	// SyncHeads no-ops (nil) on a not-open tech space — that must read
	// as "not ready", not as a clean round, or the guard silently
	// vanishes if the boot ordering ever changes.
	if s.tsp.SpaceId() == "" {
		return errors.New("tech space not open")
	}
	if err := s.tsp.SyncHeads(ctx); err != nil {
		return fmt.Errorf("tech space head-sync: %w", err)
	}
	if n := s.app.ParkedTreeCount(s.tsp.SpaceId()); n > 0 {
		return fmt.Errorf("tech space has %d parked trees", n)
	}
	return nil
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

// handleInboxMessage is the notifier's dispatch callback, fanning out by
// payload type. SenderIdentity is the coordinator-verified account — the
// authoritative sender identity; nothing self-declared in a body is
// trusted for identification.
func (s *Service) handleInboxMessage(ctx context.Context, m inbox.Message) error {
	keys := s.app.AccountKeys()
	if keys != nil && m.SenderIdentity == keys.SignKey.GetPublic().Account() {
		// Defensive: never register an incoming from ourselves.
		return nil
	}
	switch m.PayloadType {
	case coordinatorproto.InboxPayloadType_InboxPayloadOneToOneInvite:
		return s.handleOneToOneInvite(ctx, m)
	case coordinatorproto.InboxPayloadType_InboxPayloadRegularInvite:
		return s.handleRegularInvite(ctx, m)
	default:
		// Unknown payload type — skip (advance cursor). Forward-compat
		// with future inbox payloads.
		return nil
	}
}

// handleOneToOneInvite turns a verified, decrypted 1-1 invite into a
// pending row via RegisterIncoming. The body is the sender's metadata
// symkey (display-only profile resolution).
func (s *Service) handleOneToOneInvite(ctx context.Context, m inbox.Message) error {
	// The body carries the sender's metadata symkey; cache it so their
	// identityRepo profile (name/icon) resolves once the 1-1 is active.
	if symKey := string(m.Body); symKey != "" {
		_ = s.tsp.SetIdentityMetaKey(ctx, m.SenderIdentity, symKey)
	}
	if err := s.RegisterIncoming(ctx, m.SenderIdentity, space.AccountMetadata{}); err != nil {
		// RegisterIncoming swallows benign conditions (existing row, sticky
		// decline) as nil; a real error here is a transient tech-space
		// write. Ask the notifier to retry rather than drop the invite.
		return fmt.Errorf("%w: register incoming: %v", inbox.ErrRetry, err)
	}
	inboxLog.Debug("registered incoming 1-1", zap.String("peer", m.SenderIdentity))
	return nil
}

// handleRegularInvite registers a direct-add invite (SYN-46) as a SYNCED
// pending row. The sender already added us to the space's ACL — the
// message only tells our devices to surface the space; accept stays a
// local materialization gate. The pending status is synced (unlike the
// device-local 1-1 pending) because the synced inbox cursor means only
// one of our devices processes this message.
//
// Decode failures are non-retryable (the notifier logs and advances —
// never wedge the cursor on a malformed body). An existing row for the
// space no-ops — active membership, a pending join, sticky decline,
// terminal delete, and duplicate delivery are all respected by the same
// guard — with one exception: an ended join. That row records "not a
// member" account-wide, and the direct add just made the account one,
// so the invite registers over it and the accept path loads the space;
// nothing else watches the ACL of a space this account never loaded.
func (s *Service) handleRegularInvite(ctx context.Context, m inbox.Message) error {
	body, err := decodeRegularInviteBody(m.Body)
	if err != nil {
		return err
	}
	// Cache the sender's metadata symkey so their identityRepo profile
	// (name/icon) resolves — the pending row's inviter display.
	if body.SymKey != "" {
		_ = s.tsp.SetIdentityMetaKey(ctx, m.SenderIdentity, body.SymKey)
	}
	// Pull the tech space current before the no-clobber check: a
	// duplicate delivery processed on THIS device (per-device cursor lag)
	// while ANOTHER device already registered — and possibly accepted —
	// the row must see that row, not re-write invitePending over the
	// accept under CRDT LWW. We are online (an inbox fetch just
	// succeeded), so this is a cheap round; failure falls through to the
	// local view.
	_ = s.tsp.SyncHeads(ctx)
	if rec, ok := s.tsp.Get(ctx, body.SpaceId); ok {
		if !rec.JoinEnded() {
			return nil
		}
		if _, err := s.tsp.SetRemoteStatus(ctx, body.SpaceId, techspace.InvitePendingRemoteStatus); err != nil {
			return fmt.Errorf("%w: register direct-add invite over an ended join: %v", inbox.ErrRetry, err)
		}
		if err := s.clearLegacyJoinMarker(ctx, rec); err != nil {
			inboxLog.Warn("direct-add over an ended join", zap.String("spaceId", body.SpaceId), zap.Error(err))
		}
		// An ended join never loaded, so the row carries no name; the
		// sender's hint fills it the way a fresh registration would.
		if rec.Name == "" && body.Name != "" {
			if _, err := s.tsp.SetSpaceMetadata(ctx, body.SpaceId, body.Name, "", "", body.SpaceType); err != nil {
				inboxLog.Warn("direct-add name hint", zap.String("spaceId", body.SpaceId), zap.Error(err))
			}
		}
	} else if _, err := s.tsp.Add(ctx, techspace.SpaceIndexRecord{
		// Name/SpaceType are unauthenticated display hints, replaced by
		// the synced in-space values after accept. Type is left unknown —
		// the field is set-once, so a sender-supplied value must not
		// reach it; the post-accept load backfills it from the header.
		// No storage is materialized and no localStatus is set: pending
		// is synced-only.
		Id:           body.SpaceId,
		SpaceType:    body.SpaceType,
		Name:         body.Name,
		RemoteStatus: techspace.InvitePendingRemoteStatus,
	}); err != nil {
		return fmt.Errorf("%w: register direct-add invite: %v", inbox.ErrRetry, err)
	}
	// Tracked + cancellable like every other background tech-space
	// writer: bound to the inbox subsystem's ctx and drained by
	// stopOneToOneInbox before the tech space is torn down.
	if s.inviteCtx != nil {
		s.inviteWG.Add(1)
		go func() {
			defer s.inviteWG.Done()
			s.resolveInviteSenderProfile(s.inviteCtx, m.SenderIdentity, body.SpaceId)
		}()
	}
	inboxLog.Debug("registered direct-add invite",
		zap.String("spaceId", body.SpaceId), zap.String("sender", m.SenderIdentity))
	return nil
}

// resolveInviteSenderProfile best-effort resolves a direct-add sender's
// identityRepo profile (decrypted with the just-cached metadata symkey)
// into the identities directory and records the space as a sighting, so
// clients can show who the invite is from. Meant to run in a goroutine;
// failures are dropped — a duplicate notification retries, and the
// profile also resolves through the members watcher after accept.
func (s *Service) resolveInviteSenderProfile(ctx context.Context, identity, spaceId string) {
	_ = s.tsp.AddIdentitySpace(ctx, identity, spaceId)
	key := s.metadataSymKeyFor(ctx, identity)
	if key == nil {
		return
	}
	prof, ok := s.fetchIdentityProfile(ctx, identity, key)
	if !ok || (prof.Name == "" && prof.Description == "" && prof.IconCID == "") {
		return
	}
	_ = s.tsp.SetIdentityProfile(ctx, identity, prof.Name, prof.Description, prof.IconCID)
}

// markRegularInvitesToSend queues one durable inbox notification per
// account just added to spaceId (ACL AddAccounts), then kicks the send
// loop. Called after the ACL write succeeds — the membership is already
// effective; the notification only surfaces it on the receivers'
// devices. Failures to queue are logged and dropped (the ACL add stands
// regardless). No-op when the inbox transport is unavailable.
func (s *Service) markRegularInvitesToSend(ctx context.Context, spaceId string, identities []string) {
	if s.app.InboxClient() == nil {
		return
	}
	// The caller's ctx may arrive nearly exhausted (AddAccounts can burn
	// most of a request deadline in its log-not-ready retries). These are
	// fast local writes recording a durable obligation for an ACL add
	// that already happened — losing them to an expiring request
	// deadline would silently drop the notification forever.
	ctx = context.WithoutCancel(ctx)
	var self string
	if keys := s.app.AccountKeys(); keys != nil {
		self = keys.SignKey.GetPublic().Account()
	}
	var queued bool
	for _, id := range identities {
		if id == "" || id == self {
			continue
		}
		if err := s.tsp.AddInviteNotify(ctx, spaceId, id); err != nil {
			inboxLog.Warn("queue direct-add notification",
				zap.String("spaceId", spaceId), zap.String("receiver", id), zap.Error(err))
			continue
		}
		queued = true
	}
	if queued {
		s.kickInviteRetry()
	}
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
// cancellable context. Drained by stopOneToOneInbox via inviteWG. The
// ctx is kept on the Service so other inbox-subsystem goroutines
// (sender-profile resolution) share its lifetime.
func (s *Service) startInviteRetry() {
	ctx, cancel := context.WithCancel(context.Background())
	s.inviteCtx, s.inviteCancel = ctx, cancel
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
	t := time.NewTicker(time.Duration(inviteRetryInterval.Load()))
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
// the real inbox sends and tech-space marker-clears into the pure core.
func (s *Service) reconcileInvites(ctx context.Context) {
	clearOneToOne := func(ctx context.Context, spaceId string) error {
		_, err := s.tsp.SetOneToOneInviteState(ctx, spaceId, "")
		return err
	}
	reconcileInviteOutbox(ctx, s.tsp.List(ctx),
		s.sendOneToOneInvite, clearOneToOne,
		s.sendRegularInvite, s.tsp.ClearInviteNotify)
}

// errInviteUndeliverable marks a PERMANENT per-receiver send failure
// (e.g. an undecodable receiver identity): the outbox entry is cleared —
// loudly — instead of retried forever. Transient failures (offline,
// coordinator down) are any other error and keep the entry.
var errInviteUndeliverable = errors.New("spaceimpl: invite notification undeliverable")

// reconcileInviteOutbox is the pure send-retry core for both inbox
// notification kinds. For each row still owing notifications it sends and
// clears the marker on confirmed delivery; a transient send failure
// (offline / coordinator down) leaves the marker set so the NEXT pass
// retries — the offline-at-initiate → delivered-once-online path.
//
//   - 1-1 rows: single peer, marker is OneToOneInviteState="toSend". A
//     malformed row (no peer) is cleared so the loop doesn't spin on it.
//   - Regular rows: per-receiver entries in InviteNotifyPending (direct-add
//     notifications after AddAccounts), sent and cleared independently. A
//     permanent failure (errInviteUndeliverable) clears the entry with a
//     loud log.
//
// The send/clear funcs are injected so the behavior is unit-testable
// without a live coordinator.
func reconcileInviteOutbox(
	ctx context.Context,
	rows []techspace.SpaceIndexRecord,
	sendOneToOne func(ctx context.Context, receiverId string) error,
	clearOneToOne func(ctx context.Context, spaceId string) error,
	sendRegular func(ctx context.Context, receiverId, spaceId string) error,
	clearRegular func(ctx context.Context, spaceId, receiverId string) error,
) {
	for _, r := range rows {
		if ctx.Err() != nil {
			return
		}
		if r.Type == space.SpaceTypeOneToOne {
			if r.OneToOneInviteState != oneToOneInviteToSend {
				continue
			}
			if r.OneToOnePeer == "" {
				if err := clearOneToOne(ctx, r.Id); err != nil {
					inboxLog.Warn("clear malformed invite marker", zap.String("spaceId", r.Id), zap.Error(err))
				}
				continue
			}
			if err := sendOneToOne(ctx, r.OneToOnePeer); err != nil {
				inboxLog.Debug("send 1-1 invite (will retry)",
					zap.String("spaceId", r.Id), zap.String("peer", r.OneToOnePeer), zap.Error(err))
				continue
			}
			if err := clearOneToOne(ctx, r.Id); err != nil {
				inboxLog.Warn("clear invite marker", zap.String("spaceId", r.Id), zap.Error(err))
			}
			continue
		}
		for _, receiverId := range r.InviteNotifyPending {
			if ctx.Err() != nil {
				return
			}
			err := sendRegular(ctx, receiverId, r.Id)
			if err != nil && !errors.Is(err, errInviteUndeliverable) {
				inboxLog.Debug("send direct-add notification (will retry)",
					zap.String("spaceId", r.Id), zap.String("receiver", receiverId), zap.Error(err))
				continue
			}
			if err != nil {
				inboxLog.Warn("drop undeliverable direct-add notification",
					zap.String("spaceId", r.Id), zap.String("receiver", receiverId), zap.Error(err))
			}
			if cerr := clearRegular(ctx, r.Id, receiverId); cerr != nil {
				inboxLog.Warn("clear direct-add notification marker",
					zap.String("spaceId", r.Id), zap.String("receiver", receiverId), zap.Error(cerr))
			}
		}
	}
}

// sendOneToOneInvite posts one InboxPayloadOneToOneInvite to receiverId.
// The body is our metadata symkey; any-sync encrypts it to the receiver's
// account key and signs it on send. The receiver caches the key so our
// identityRepo profile (name/icon) resolves. Best-effort — the receiver
// can also discover the 1-1 out-of-band, and the space is re-derivable
// regardless.
func (s *Service) sendOneToOneInvite(ctx context.Context, receiverId string) error {
	ic := s.app.InboxClient()
	if ic == nil {
		return errors.New("spaceimpl: inbox unavailable")
	}
	recvPub, err := decodeIdentity(receiverId)
	if err != nil {
		return fmt.Errorf("spaceimpl: send invite: %w", err)
	}
	keys := s.app.AccountKeys()
	myId := keys.SignKey.GetPublic().Account()

	body, err := encodeSelfSymKeyMetadata(keys.SignKey)
	if err != nil {
		return fmt.Errorf("spaceimpl: send invite: derive metadata key: %w", err)
	}

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

// sendRegularInvite posts one InboxPayloadRegularInvite to receiverId for
// spaceId. The body carries our metadata symkey plus display hints read
// fresh off the row at send time; any-sync encrypts it to the receiver's
// account key and signs it on send. errInviteUndeliverable flags
// permanent failures (undecodable receiver) so the outbox entry is
// dropped instead of retried forever.
func (s *Service) sendRegularInvite(ctx context.Context, receiverId, spaceId string) error {
	ic := s.app.InboxClient()
	if ic == nil {
		return errors.New("spaceimpl: inbox unavailable")
	}
	recvPub, err := decodeIdentity(receiverId)
	if err != nil {
		return fmt.Errorf("%w: %v", errInviteUndeliverable, err)
	}
	keys := s.app.AccountKeys()
	myId := keys.SignKey.GetPublic().Account()

	b := regularInviteBody{SpaceId: spaceId}
	// Best-effort symkey: without it the receiver still gets the invite,
	// only our profile name resolves later (unlike the 1-1 invite, whose
	// body IS the symkey).
	if symKey, kerr := encodeSelfSymKeyMetadata(keys.SignKey); kerr == nil {
		b.SymKey = string(symKey)
	}
	if rec, ok := s.tsp.Get(ctx, spaceId); ok {
		b.Name = rec.Name
		b.SpaceType = rec.SpaceType
	}
	body, err := encodeRegularInviteBody(b)
	if err != nil {
		return fmt.Errorf("%w: %v", errInviteUndeliverable, err)
	}

	msg := &coordinatorproto.InboxMessage{
		Packet: &coordinatorproto.InboxPacket{
			SenderIdentity:   myId,
			ReceiverIdentity: receiverId,
			Payload: &coordinatorproto.InboxPayload{
				PayloadType: coordinatorproto.InboxPayloadType_InboxPayloadRegularInvite,
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
