package spaceimpl

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"
	"time"

	syncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/internal/components"
	"github.com/anyproto/any-sync-sdk/internal/kvimpl"
	"github.com/anyproto/any-sync-sdk/internal/objectimpl"

	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/commonspace/acl/aclclient"
	"github.com/anyproto/any-sync/commonspace/headsync/headstorage"
	"github.com/anyproto/any-sync/commonspace/object/acl/aclrecordproto"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/objecttreebuilder"
	"github.com/anyproto/any-sync/commonspace/syncstatus"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/util/crypto"
)

// SpaceImpl implements syncsdk.Space with lazy initialization.
type SpaceImpl struct {
	id           string
	spaceService commonspace.SpaceService
	coordClient  coordinatorclient.CoordinatorClient
	cfg          syncsdk.Config

	once    sync.Once
	initErr error
	cs      commonspace.Space

	kvOnce sync.Once
	kv     syncsdk.KeyValue

	mu                sync.Mutex
	objects           map[string]syncsdk.Object
	handlers          []syncsdk.Handler
	knownJoinRequests map[string]struct{}
	closed            bool
}

func New(id string, spaceService commonspace.SpaceService, cfg syncsdk.Config, coordClient coordinatorclient.CoordinatorClient) *SpaceImpl {
	return &SpaceImpl{
		id:                id,
		spaceService:      spaceService,
		coordClient:       coordClient,
		cfg:               cfg,
		objects:           make(map[string]syncsdk.Object),
		knownJoinRequests: make(map[string]struct{}),
	}
}

func (s *SpaceImpl) ensure(ctx context.Context) error {
	s.once.Do(func() {
		deps := commonspace.Deps{
			SyncStatus: syncstatus.NewNoOpSyncStatus(),
			TreeSyncer: components.NewTreeSyncer(),
		}
		cs, err := s.spaceService.NewSpace(ctx, s.id, deps)
		if err != nil {
			s.initErr = err
			return
		}
		if err = cs.Init(ctx); err != nil {
			s.initErr = err
			return
		}
		s.cs = cs

		// Capture existing join requests so they don't fire events,
		// then register as AclUpdater for future notifications.
		acl := cs.Acl()
		acl.RLock()
		initialJoinRecords, _ := acl.AclState().JoinRecords(false)
		for _, rec := range initialJoinRecords {
			s.knownJoinRequests[rec.RecordId] = struct{}{}
		}
		acl.RUnlock()
		acl.SetAclUpdater(s)
	})
	return s.initErr
}

func (s *SpaceImpl) ID() string {
	return s.id
}

func (s *SpaceImpl) GetObject(ctx context.Context, objectID string) (syncsdk.Object, error) {
	if err := s.ensure(ctx); err != nil {
		return nil, err
	}

	s.mu.Lock()
	if obj, ok := s.objects[objectID]; ok {
		s.mu.Unlock()
		return obj, nil
	}
	s.mu.Unlock()

	obj := objectimpl.NewObject(nil, s.id, s.cfg.SigningKey)
	tree, err := s.cs.TreeBuilder().BuildTree(ctx, objectID, objecttreebuilder.BuildTreeOpts{
		Listener: obj,
	})
	if err != nil {
		return nil, err
	}
	obj.SetTree(tree)

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.objects[objectID]; ok {
		tree.Close()
		return existing, nil
	}
	s.objects[objectID] = obj
	return obj, nil
}

func (s *SpaceImpl) CreateObject(ctx context.Context, opts ...syncsdk.ObjectCreateOption) (syncsdk.Object, error) {
	if err := s.ensure(ctx); err != nil {
		return nil, err
	}

	options := syncsdk.ResolveObjectCreateOptions(opts)

	seed := make([]byte, 32)
	if _, err := rand.Read(seed); err != nil {
		return nil, err
	}
	payload, err := s.cs.TreeBuilder().CreateTree(ctx, objecttree.ObjectTreeCreatePayload{
		PrivKey:    s.cfg.SigningKey,
		ChangeType: options.ChangeType,
		SpaceId:    s.id,
		Timestamp:  time.Now().Unix(),
		Seed:       seed,
	})
	if err != nil {
		return nil, err
	}

	obj := objectimpl.NewObject(nil, s.id, s.cfg.SigningKey)
	tree, err := s.cs.TreeBuilder().PutTree(ctx, payload, obj)
	if err != nil {
		return nil, err
	}
	obj.SetTree(tree)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[tree.Id()] = obj
	return obj, nil
}

func (s *SpaceImpl) DeriveObject(ctx context.Context, opts ...syncsdk.ObjectDeriveOption) (syncsdk.Object, error) {
	if err := s.ensure(ctx); err != nil {
		return nil, err
	}

	payload, err := s.cs.TreeBuilder().DeriveTree(ctx, objecttree.ObjectTreeDerivePayload{
		SpaceId: s.id,
	})
	if err != nil {
		return nil, err
	}

	obj := objectimpl.NewObject(nil, s.id, s.cfg.SigningKey)
	tree, err := s.cs.TreeBuilder().PutTree(ctx, payload, obj)
	if err != nil {
		if errors.Is(err, treestorage.ErrTreeExists) {
			// Tree already exists — open it instead.
			return s.GetObject(ctx, payload.RootRawChange.Id)
		}
		return nil, err
	}
	obj.SetTree(tree)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[tree.Id()] = obj
	return obj, nil
}

func (s *SpaceImpl) DeleteObject(ctx context.Context, objectID string) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	return s.cs.DeleteTree(ctx, objectID)
}

func (s *SpaceImpl) ListObjectIDs(ctx context.Context) ([]string, error) {
	if err := s.ensure(ctx); err != nil {
		return nil, err
	}
	// Build a set of internal IDs to exclude (ACL, settings, keyvalue).
	exclude := map[string]struct{}{
		s.cs.Acl().Id():                {},
		s.cs.KeyValue().DefaultStore().Id(): {},
	}
	stateStorage := s.cs.Storage().StateStorage()
	exclude[stateStorage.SettingsId()] = struct{}{}

	var ids []string
	err := s.cs.Storage().HeadStorage().IterateEntries(ctx, headstorage.IterOpts{}, func(entry headstorage.HeadsEntry) (bool, error) {
		if _, ok := exclude[entry.Id]; !ok {
			ids = append(ids, entry.Id)
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (s *SpaceImpl) KeyValue() syncsdk.KeyValue {
	s.kvOnce.Do(func() {
		if s.cs == nil {
			return
		}
		store := s.cs.KeyValue().DefaultStore()
		_ = store.Prepare()
		s.kv = kvimpl.NewKeyValue(store)
	})
	return s.kv
}

func (s *SpaceImpl) Subscribe(handler syncsdk.Handler) (unsubscribe func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers = append(s.handlers, handler)
	idx := len(s.handlers) - 1
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if idx < len(s.handlers) {
			s.handlers[idx] = nil
		}
	}
}

func (s *SpaceImpl) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	objects := make(map[string]syncsdk.Object, len(s.objects))
	for k, v := range s.objects {
		objects[k] = v
	}
	s.objects = nil
	s.mu.Unlock()

	for _, obj := range objects {
		_ = obj.Close()
	}

	if s.cs != nil {
		return s.cs.Close()
	}
	return nil
}

func (s *SpaceImpl) GenerateInvite(ctx context.Context, opts ...syncsdk.InviteOption) (string, error) {
	if err := s.ensure(ctx); err != nil {
		return "", err
	}
	resolved := syncsdk.ResolveInviteOptions(opts)

	if err := s.coordClient.SpaceMakeShareable(ctx, s.id); err != nil {
		return "", err
	}

	var inviteType aclrecordproto.AclInviteType
	var permissions list.AclPermissions
	if resolved.ApprovalRequired {
		inviteType = aclrecordproto.AclInviteType_RequestToJoin
		permissions = list.AclPermissionsNone
	} else {
		inviteType = aclrecordproto.AclInviteType_AnyoneCanJoin
		permissions = permissionToAcl(resolved.Permission)
	}

	result, err := s.cs.AclClient().ReplaceInvite(ctx, aclclient.InvitePayload{
		InviteType:  inviteType,
		Permissions: permissions,
	})
	if errors.Is(err, list.ErrDuplicateInvites) {
		// Same invite type already exists — re-read the existing invite key.
		// For simplicity, return the error; caller can re-generate with different opts.
		return "", err
	}
	if err != nil {
		return "", err
	}

	if err := s.cs.AclClient().AddRecord(ctx, result.InviteRec); err != nil {
		return "", err
	}

	return syncsdk.EncodeInvite(s.id, result.InviteKey, resolved.ApprovalRequired)
}

func (s *SpaceImpl) Members(ctx context.Context) ([]syncsdk.Member, error) {
	if err := s.ensure(ctx); err != nil {
		return nil, err
	}
	acl := s.cs.Acl()
	acl.RLock()
	defer acl.RUnlock()

	accounts := acl.AclState().CurrentAccounts()
	var members []syncsdk.Member
	for _, acc := range accounts {
		status := aclStatusToMemberStatus(acc.Status)
		perm := aclToPermission(acc.Permissions)
		// Skip accounts with no permissions that are not in joining/removing state
		if acc.Permissions.NoPermissions() && status != syncsdk.MemberStatusJoining && status != syncsdk.MemberStatusRemoving {
			continue
		}
		members = append(members, syncsdk.Member{
			Identity:    acc.PubKey,
			Permissions: perm,
			Status:      status,
		})
	}
	return members, nil
}

func (s *SpaceImpl) AddMember(ctx context.Context, identity crypto.PubKey, permissions syncsdk.Permission) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	return s.cs.AclClient().AddAccounts(ctx, list.AccountsAddPayload{
		Additions: []list.AccountAdd{
			{
				Identity:    identity,
				Permissions: permissionToAcl(permissions),
			},
		},
	})
}

func (s *SpaceImpl) RemoveMember(ctx context.Context, identity crypto.PubKey) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	readKey, err := crypto.NewRandomAES()
	if err != nil {
		return err
	}
	metadataKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return err
	}
	return s.cs.AclClient().RemoveAccounts(ctx, list.AccountRemovePayload{
		Identities: []crypto.PubKey{identity},
		Change: list.ReadKeyChangePayload{
			MetadataKey: metadataKey,
			ReadKey:     readKey,
		},
	})
}

func (s *SpaceImpl) ChangePermissions(ctx context.Context, identity crypto.PubKey, permissions syncsdk.Permission) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	return s.cs.AclClient().ChangePermissions(ctx, list.PermissionChangesPayload{
		Changes: []list.PermissionChangePayload{
			{
				Identity:    identity,
				Permissions: permissionToAcl(permissions),
			},
		},
	})
}

func (s *SpaceImpl) AcceptJoinRequest(ctx context.Context, identity crypto.PubKey, permissions syncsdk.Permission) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	acl := s.cs.Acl()
	acl.RLock()
	rec, err := acl.AclState().JoinRecord(identity, false)
	acl.RUnlock()
	if err != nil {
		return err
	}
	return s.cs.AclClient().AcceptRequest(ctx, list.RequestAcceptPayload{
		RequestRecordId: rec.RecordId,
		Permissions:     permissionToAcl(permissions),
	})
}

func (s *SpaceImpl) DeclineJoinRequest(ctx context.Context, identity crypto.PubKey) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	return s.cs.AclClient().DeclineRequest(ctx, identity)
}

func (s *SpaceImpl) RevokeInvite(ctx context.Context, inviteRecordID string) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	return s.cs.AclClient().RevokeInvite(ctx, inviteRecordID)
}

// UpdateAcl implements headupdater.AclUpdater. It is called by syncacl
// while the ACL lock is held; dispatch must be async to avoid deadlock.
func (s *SpaceImpl) UpdateAcl(aclList list.AclList) {
	joinRecords, _ := aclList.AclState().JoinRecords(false)
	var newEvents []syncsdk.Event
	current := make(map[string]struct{}, len(joinRecords))
	for _, rec := range joinRecords {
		current[rec.RecordId] = struct{}{}
		if _, known := s.knownJoinRequests[rec.RecordId]; !known {
			newEvents = append(newEvents, syncsdk.Event{
				Type:     syncsdk.JoinRequestReceived,
				SpaceID:  s.id,
				Identity: rec.RequestIdentity,
			})
		}
	}
	s.knownJoinRequests = current
	if len(newEvents) > 0 {
		go func() {
			for _, evt := range newEvents {
				s.dispatch(evt)
			}
		}()
	}
}

// dispatch sends an event to all registered space-level handlers.
func (s *SpaceImpl) dispatch(evt syncsdk.Event) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	subs := make([]syncsdk.Handler, len(s.handlers))
	copy(subs, s.handlers)
	s.mu.Unlock()
	for _, h := range subs {
		if h != nil {
			h(evt)
		}
	}
}

func (s *SpaceImpl) Push(ctx context.Context) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	state, err := s.cs.Storage().StateStorage().GetState(ctx)
	if err != nil {
		return err
	}
	_, err = s.coordClient.SpaceSign(ctx, coordinatorclient.SpaceSignPayload{
		SpaceId:     s.id,
		SpaceHeader: state.SpaceHeader,
	})
	return err
}

// CommonSpace returns the underlying commonspace.Space, initializing it lazily.
// This is used by other internal packages.
func (s *SpaceImpl) CommonSpace(ctx context.Context) (commonspace.Space, error) {
	if err := s.ensure(ctx); err != nil {
		return nil, err
	}
	return s.cs, nil
}

// permissionToAcl converts an SDK Permission to an any-sync AclPermissions.
// Values match directly since Permission constants mirror AclUserPermissions.
func permissionToAcl(p syncsdk.Permission) list.AclPermissions {
	return list.AclPermissions(p)
}

// aclToPermission converts an any-sync AclPermissions to an SDK Permission.
func aclToPermission(p list.AclPermissions) syncsdk.Permission {
	return syncsdk.Permission(p)
}

// aclStatusToMemberStatus maps any-sync AclStatus to the SDK MemberStatus.
func aclStatusToMemberStatus(s list.AclStatus) syncsdk.MemberStatus {
	switch s {
	case list.StatusActive:
		return syncsdk.MemberStatusActive
	case list.StatusJoining:
		return syncsdk.MemberStatusJoining
	case list.StatusRemoving:
		return syncsdk.MemberStatusRemoving
	default:
		return syncsdk.MemberStatusActive
	}
}
