package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/query"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// Chat-shaped tracked type for the read-tracking e2e: creations track
// as "message", deletes/edits untracked; the SDK materializes the
// per-record `unread` flag and the `unreadCount` row property.
const (
	rtDataset     = "rt_chat"
	rtDataVersion = "rt_chat-v1"
	rtTypeId      = "rt-chat-type"
	rtTagMessage  = "message"
	rtFieldUnread = "unread"
	rtPropCount   = "unreadCount"
)

func newReadTrackedType() handler.Type {
	return handler.Type{
		Id:   rtTypeId,
		Name: "RT Chat",
		Datasets: []handler.Dataset{{
			Name:        rtDataset,
			DataVersion: rtDataVersion,
			Handler:     handler.DefaultHandler{},
			Schema: handler.Schema{
				Dynamic: true,
				Fields: []handler.Field{
					{Id: rtFieldUnread, Name: "Unread", Schema: handler.Leaf(handler.PropertyKindBoolean), Scope: handler.ScopeLocal},
				},
			},
			ReadTracking: &handler.ReadTracking{
				Classify: func(_ *handler.ChangeCtx, rec *handler.RecordChange) handler.ReadClassification {
					for i := range rec.Ops {
						if rec.Ops[i].Type == handler.OpDelete {
							return handler.ReadClassification{}
						}
					}
					if rec.Upsert {
						return handler.ReadClassification{Track: true, Tags: []string{rtTagMessage}}
					}
					return handler.ReadClassification{}
				},
				Seed:          handler.ReadSeedAtFirstSight,
				RecordFlags:   map[string]string{rtTagMessage: rtFieldUnread},
				CounterFields: map[string]string{rtTagMessage: rtPropCount},
			},
		}},
		Properties: []handler.PropertyDecl{
			{Id: rtPropCount, Name: "Unread Count", Kind: handler.PropertyKindNumber, Scope: handler.ScopeLocal},
		},
	}
}

// TestE2E_ReadTracking drives the full read-tracking story over a live
// network: owner + member (distinct accounts) + a second member
// device.
//
//  1. Owner's own messages are born read (no unread on owner).
//  2. Member joins AFTER two messages exist → first-sight seed: both
//     start read on the member.
//  3. A new owner message goes unread on the member: the dirty-object
//     feed, UnreadSnapshot, per-record `unread` flag, and the
//     materialized `unreadCount` row property all agree.
//  4. The member's own reply stays read for the member but goes unread
//     for the owner (identity-based, not device-based).
//  5. A second member device converges to READ after device one calls
//     MarkReadUpTo("") — the frontier travels via the tech-space KV;
//     device two never acts.
//  6. Deleting an unread message clears it without reading.
//  7. A THIRD member device boots after device one published its read
//     state: it lands on the account's real read state via the synced
//     frontier (the new message stays unread) instead of first-sight
//     seeding everything read.
func TestE2E_ReadTracking(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("read-tracking e2e is slow (~3-5min); rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	open := func(name string, p *fixedSeedProvider) *anysyncsdk.SDK {
		sdk, err := anysyncsdk.Open(ctx, config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
			Types:   []handler.Type{newReadTrackedType()},
		}, p)
		require.NoError(t, err, "%s: Open", name)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk
	}

	ownerProvider := newFixedSeedProvider(t)
	memberProvider := newFixedSeedProvider(t)

	owner := open("owner", ownerProvider)

	// ---- Owner: space, object, two pre-join messages ----
	sp, err := owner.Spaces().Create(ctx, space.CreateRequest{Name: "ReadTracking"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("owner: Spaces().Create: %v", err)
	}
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{rtTypeId}})
	require.NoError(t, err)

	send := func(from space.Space, msgId, text string) {
		t.Helper()
		_, err := from.Modify(ctx, space.ModifyBatch{
			ObjectId: objId,
			Dataset:  rtDataset,
			Records: []space.RecordModify{{
				Id:     msgId,
				Upsert: true,
				Ops:    []space.Op{{Type: space.OpSet, Path: "text", Value: text}},
			}},
		})
		require.NoError(t, err, "send %s", msgId)
	}
	send(sp, "m1", "hello")
	send(sp, "m2", "world")

	// (1) Self-authored: nothing unread on the owner.
	counts, err := sp.ReadState().UnreadCounts(ctx, objId)
	require.NoError(t, err)
	require.Empty(t, counts, "owner: own messages are born read")

	// ---- Member joins (invite → request → accept → active) ----
	member := open("member", memberProvider)

	var inv space.Invite
	if !waitFor(ctx, 30*time.Second, time.Second, func() bool {
		inv, err = sp.ACL().CreateInvite(ctx)
		return err == nil
	}) {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on CreateInvite: %v", err)
		}
		t.Fatalf("owner: CreateInvite: %v", err)
	}
	token, err := space.EncodeInvite(inv)
	require.NoError(t, err)
	_, err = member.Spaces().Join(ctx, space.JoinRequest{Invite: token, Metadata: space.AccountMetadata{Name: "Member"}})
	require.True(t, errors.Is(err, spaceimpl.ErrJoinPending), "member: Join must return ErrJoinPending, got %v", err)

	var joinReq space.JoinRequestInfo
	if !waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = sp.SyncHeads(ctx)
		reqs, _ := sp.Members().JoinRequests(ctx)
		if len(reqs) > 0 {
			joinReq = reqs[0]
			return true
		}
		return false
	}) {
		t.Fatalf("owner never saw member's join request")
	}
	require.NoError(t, sp.ACL().AcceptRequest(ctx, joinReq.RecordId, space.PermissionWriter))

	var mSp space.Space
	if !waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = member.Spaces().SyncSpaceList(ctx)
		infos, listErr := member.Spaces().List(ctx)
		if listErr != nil {
			return false
		}
		for _, si := range infos {
			if si.Id == sp.Id() && si.Status == space.StatusActive {
				mSp, listErr = member.Spaces().Get(ctx, sp.Id())
				return listErr == nil
			}
		}
		return false
	}) {
		t.Fatalf("member's space never flipped to active")
	}

	// Helpers over the member's view.
	getMsg := func(from space.Space, msgId string) (found, unread bool) {
		doc, qerr := from.Query(objId, rtDataset).Filter(query.Key{
			Path:   []string{"id"},
			Filter: query.NewComp(query.CompOpEq, msgId),
		}).One(ctx)
		if qerr != nil {
			return false, false
		}
		return true, doc.GetBool(rtFieldUnread)
	}
	unreadCount := func(from space.Space) int {
		c, cerr := from.ReadState().UnreadCounts(ctx, objId)
		if cerr != nil {
			return -1
		}
		return c[rtTagMessage]
	}

	// (2) First-sight seed: the pre-join history lands read.
	if !waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = mSp.SyncHeads(ctx)
		f1, _ := getMsg(mSp, "m1")
		f2, _ := getMsg(mSp, "m2")
		return f1 && f2
	}) {
		t.Fatalf("member never received the pre-join messages")
	}
	require.Equal(t, 0, unreadCount(mSp), "member: pre-join history seeds read")
	snap, _, err := mSp.ReadState().UnreadSnapshot(ctx, objId)
	require.NoError(t, err)
	require.Empty(t, snap, "member: unread snapshot empty after seed")

	// (3) New owner message → unread on the member, everywhere.
	send(sp, "m3", "post-join")
	if !waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = mSp.SyncHeads(ctx)
		found, unread := getMsg(mSp, "m3")
		return found && unread && unreadCount(mSp) == 1
	}) {
		t.Fatalf("member: m3 never became unread (flag+counter)")
	}
	snap, _, err = mSp.ReadState().UnreadSnapshot(ctx, objId)
	require.NoError(t, err)
	require.Len(t, snap, 1)
	require.Equal(t, []string{"m3"}, snap[0].RecordIds)
	dirtyObjs, err := mSp.ReadState().ChangedSince(ctx, 0, 0)
	require.NoError(t, err)
	require.NotEmpty(t, dirtyObjs, "member: dirty-object feed has entries")
	// The materialized row property agrees.
	if !waitFor(ctx, 60*time.Second, time.Second, func() bool {
		row, rerr := mSp.Properties().Get(ctx, objId)
		return rerr == nil && row != nil && row.GetInt(rtTypeId, rtPropCount) == 1
	}) {
		t.Fatalf("member: unreadCount row property never reached 1")
	}

	// (4) The member's own reply: read for the member, unread for the
	// owner (identity, not device).
	send(mSp, "m4", "reply")
	require.Equal(t, 1, unreadCount(mSp), "member: own reply stays read")
	if !waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = sp.SyncHeads(ctx)
		found, unread := getMsg(sp, "m4")
		return found && unread && unreadCount(sp) == 1
	}) {
		t.Fatalf("owner: member's reply never became unread")
	}

	// ---- (5) Second member device: cross-device read sync ----
	member2 := open("member2", sameAccountFreshDevice(t, memberProvider))
	var m2Sp space.Space
	if !waitFor(ctx, 2*time.Minute, time.Second, func() bool {
		_ = member2.Spaces().SyncSpaceList(ctx)
		infos, listErr := member2.Spaces().List(ctx)
		if listErr != nil {
			return false
		}
		for _, si := range infos {
			if si.Id == sp.Id() && si.Status == space.StatusActive {
				m2Sp, listErr = member2.Spaces().Get(ctx, sp.Id())
				return listErr == nil
			}
		}
		return false
	}) {
		t.Fatalf("member device 2: space never active")
	}
	// Device 2 sees the full history (m1..m4); its first sight seeds
	// everything read locally — the divergence-from-device-1 window
	// closes at the next published mark below.
	if !waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = m2Sp.SyncHeads(ctx)
		found, _ := getMsg(m2Sp, "m4")
		return found
	}) {
		t.Fatalf("member device 2 never synced history")
	}

	// A fresh owner message goes unread on BOTH member devices.
	send(sp, "m5", "for both devices")
	for name, s := range map[string]space.Space{"m-dev1": mSp, "m-dev2": m2Sp} {
		s := s
		if !waitFor(ctx, 90*time.Second, time.Second, func() bool {
			_ = s.SyncHeads(ctx)
			found, unread := getMsg(s, "m5")
			return found && unread
		}) {
			t.Fatalf("%s: m5 never became unread", name)
		}
	}

	// Device 1 reads everything; device 2 must flip WITHOUT acting —
	// the frontier travels through the account's tech-space KV.
	require.NoError(t, mSp.ReadState().MarkReadUpTo(ctx, objId, ""))
	require.Equal(t, 0, unreadCount(mSp), "member dev1: local read-all")
	if !waitFor(ctx, 2*time.Minute, time.Second, func() bool {
		_, unread := getMsg(m2Sp, "m5")
		return !unread && unreadCount(m2Sp) == 0
	}) {
		t.Fatalf("member device 2 never converged to read via KV")
	}

	// (6) Deleting an unread message clears it without reading.
	send(sp, "m6", "to be deleted")
	if !waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = mSp.SyncHeads(ctx)
		return unreadCount(mSp) == 1
	}) {
		t.Fatalf("member: m6 never became unread")
	}
	_, err = sp.Delete(ctx, space.DeleteBatch{ObjectId: objId, Dataset: rtDataset, RecordIds: []string{"m6"}})
	require.NoError(t, err)
	if !waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = mSp.SyncHeads(ctx)
		return unreadCount(mSp) == 0
	}) {
		t.Fatalf("member: deleting m6 never cleared its unread state")
	}

	// ---- (7) Third member device: seeding consults the published
	// frontier. Device one read through m5 (step 5) and never reads
	// m7; a brand-new device must show exactly m7 unread — the
	// account's real state — not first-sight-seed it read.
	send(sp, "m7", "unread on fresh device")
	if !waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = mSp.SyncHeads(ctx)
		found, unread := getMsg(mSp, "m7")
		return found && unread
	}) {
		t.Fatalf("member dev1: m7 never became unread")
	}

	member3 := open("member3", sameAccountFreshDevice(t, memberProvider))
	var m3Sp space.Space
	if !waitFor(ctx, 2*time.Minute, time.Second, func() bool {
		_ = member3.Spaces().SyncSpaceList(ctx)
		infos, listErr := member3.Spaces().List(ctx)
		if listErr != nil {
			return false
		}
		for _, si := range infos {
			if si.Id == sp.Id() && si.Status == space.StatusActive {
				m3Sp, listErr = member3.Spaces().Get(ctx, sp.Id())
				return listErr == nil
			}
		}
		return false
	}) {
		t.Fatalf("member device 3: space never active")
	}
	if !waitFor(ctx, 2*time.Minute, time.Second, func() bool {
		_ = m3Sp.SyncHeads(ctx)
		f5, u5 := getMsg(m3Sp, "m5")
		f7, u7 := getMsg(m3Sp, "m7")
		return f5 && !u5 && f7 && u7 && unreadCount(m3Sp) == 1
	}) {
		t.Fatalf("member device 3 never landed on the account's real read state (m7 unread, rest read)")
	}
}
