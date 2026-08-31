package spaceimpl

import (
	"context"
	"fmt"
	"io"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/space"
)

// The tech-space handle offers reads, dataset declarations on bundle
// roots, derived-only bundles and generic record writes. Every other
// surface is served by the stubs below, each method returning
// space.ErrUnsupported.

func errUnsupported(op string) error {
	return fmt.Errorf("spaceimpl: %s on the tech space: %w", op, space.ErrUnsupported)
}

type unsupportedProperties struct{}

func (unsupportedProperties) Get(context.Context, string) (*anyenc.Value, error) {
	return nil, errUnsupported("Properties().Get")
}
func (unsupportedProperties) Set(context.Context, string, string, map[string]any) (space.ModifyResult, error) {
	return space.ModifyResult{}, errUnsupported("Properties().Set")
}
func (unsupportedProperties) AttachType(context.Context, string, string) (space.ModifyResult, error) {
	return space.ModifyResult{}, errUnsupported("Properties().AttachType")
}
func (unsupportedProperties) DetachType(context.Context, string, string) (space.ModifyResult, error) {
	return space.ModifyResult{}, errUnsupported("Properties().DetachType")
}

type unsupportedACL struct{}

func (unsupportedACL) CreateInvite(context.Context) (space.Invite, error) {
	return space.Invite{}, errUnsupported("ACL().CreateInvite")
}
func (unsupportedACL) RevokeInvite(context.Context, string) error {
	return errUnsupported("ACL().RevokeInvite")
}
func (unsupportedACL) RevokeAllInvites(context.Context) error {
	return errUnsupported("ACL().RevokeAllInvites")
}
func (unsupportedACL) AcceptRequest(context.Context, string, space.Permission) error {
	return errUnsupported("ACL().AcceptRequest")
}
func (unsupportedACL) DeclineRequest(context.Context, string) error {
	return errUnsupported("ACL().DeclineRequest")
}
func (unsupportedACL) ChangePermissions(context.Context, []space.PermissionChange) error {
	return errUnsupported("ACL().ChangePermissions")
}
func (unsupportedACL) RemoveAccounts(context.Context, []string) error {
	return errUnsupported("ACL().RemoveAccounts")
}
func (unsupportedACL) AddAccounts(context.Context, []space.MemberAdd) error {
	return errUnsupported("ACL().AddAccounts")
}
func (unsupportedACL) OwnershipChange(context.Context, string, space.Permission) error {
	return errUnsupported("ACL().OwnershipChange")
}
func (unsupportedACL) RequestSelfRemove(context.Context) error {
	return errUnsupported("ACL().RequestSelfRemove")
}
func (unsupportedACL) CancelJoinRequest(context.Context) error {
	return errUnsupported("ACL().CancelJoinRequest")
}
func (unsupportedACL) StopSharing(context.Context) error { return errUnsupported("ACL().StopSharing") }
func (unsupportedACL) CreateGuestKey(context.Context) (space.Invite, error) {
	return space.Invite{}, errUnsupported("ACL().CreateGuestKey")
}
func (unsupportedACL) RevokeGuestKey(context.Context) error {
	return errUnsupported("ACL().RevokeGuestKey")
}

type unsupportedMembers struct{}

func (unsupportedMembers) List(context.Context) ([]space.Member, error) {
	return nil, errUnsupported("Members().List")
}
func (unsupportedMembers) Get(context.Context, string) (space.Member, error) {
	return space.Member{}, errUnsupported("Members().Get")
}
func (unsupportedMembers) Me(context.Context) (space.Member, error) {
	return space.Member{}, errUnsupported("Members().Me")
}
func (unsupportedMembers) JoinRequests(context.Context) ([]space.JoinRequestInfo, error) {
	return nil, errUnsupported("Members().JoinRequests")
}
func (unsupportedMembers) Invites(context.Context) ([]space.InviteInfo, error) {
	return nil, errUnsupported("Members().Invites")
}
func (unsupportedMembers) Subscribe(func(space.MemberEvent)) func() { return func() {} }
func (unsupportedMembers) Query() space.Query {
	return unsupportedQuery{err: errUnsupported("Members().Query")}
}

// unsupportedQuery is a Query whose chain is inert and whose terminals
// return the refusal.
type unsupportedQuery struct{ err error }

func (q unsupportedQuery) Filter(any) space.Query                      { return q }
func (q unsupportedQuery) Sort(...any) space.Query                     { return q }
func (q unsupportedQuery) Limit(int) space.Query                       { return q }
func (q unsupportedQuery) Offset(int) space.Query                      { return q }
func (q unsupportedQuery) Projection(space.ProjectionOpts) space.Query { return q }
func (q unsupportedQuery) Iter(context.Context) (space.Iterator, error) {
	return nil, q.err
}
func (q unsupportedQuery) All(context.Context) ([]*anyenc.Value, error) { return nil, q.err }
func (q unsupportedQuery) One(context.Context) (*anyenc.Value, error)   { return nil, q.err }
func (q unsupportedQuery) Count(context.Context) (int, error)           { return 0, q.err }
func (q unsupportedQuery) Snapshot(context.Context, space.QueryOpts) (*space.QueryResult, error) {
	return nil, q.err
}
func (q unsupportedQuery) Subscribe(context.Context, space.QueryOpts) (*space.QueryResult, error) {
	return nil, q.err
}

type unsupportedFiles struct{}

func (unsupportedFiles) Attach(context.Context, string, io.Reader, space.AttachOpts) (space.FileInfo, error) {
	return space.FileInfo{}, errUnsupported("Files().Attach")
}
func (unsupportedFiles) Open(context.Context, string, space.Variant) (space.FileReader, error) {
	return nil, errUnsupported("Files().Open")
}
func (unsupportedFiles) Get(context.Context, string) (space.FileInfo, error) {
	return space.FileInfo{}, errUnsupported("Files().Get")
}
func (unsupportedFiles) Status(context.Context, string) (space.FileStatus, error) {
	return space.FileStatus{}, errUnsupported("Files().Status")
}
func (unsupportedFiles) SubscribeStatus(func(space.FileStatus)) func() { return func() {} }
func (unsupportedFiles) Stats(context.Context) (space.FileStats, error) {
	return space.FileStats{}, errUnsupported("Files().Stats")
}
func (unsupportedFiles) Pin(context.Context, string) error   { return errUnsupported("Files().Pin") }
func (unsupportedFiles) Retry(context.Context, string) error { return errUnsupported("Files().Retry") }
func (unsupportedFiles) Offload(context.Context, string) error {
	return errUnsupported("Files().Offload")
}
func (unsupportedFiles) Delete(context.Context, string) error {
	return errUnsupported("Files().Delete")
}
func (unsupportedFiles) List(context.Context, space.FileListOpts) ([]space.FileInfo, error) {
	return nil, errUnsupported("Files().List")
}
func (unsupportedFiles) Query(string) (space.Query, error) {
	return nil, errUnsupported("Files().Query")
}

type unsupportedHistory struct{}

func (unsupportedHistory) ListChanges(context.Context, string, space.HistoryFilter, int, string) (space.ChangeList, error) {
	return space.ChangeList{}, errUnsupported("History().ListChanges")
}
func (unsupportedHistory) ViewAt(context.Context, string, space.Version) (space.HistoricalView, error) {
	return nil, errUnsupported("History().ViewAt")
}
func (unsupportedHistory) RecordAt(context.Context, string, string, string, space.Version) (*anyenc.Value, error) {
	return nil, errUnsupported("History().RecordAt")
}
func (unsupportedHistory) Diff(context.Context, string, space.Version, space.Version, space.DiffFilter) (space.DiffResult, error) {
	return space.DiffResult{}, errUnsupported("History().Diff")
}

type unsupportedPayloads struct{}

func (unsupportedPayloads) ListObjects(context.Context) ([]string, error) {
	return nil, errUnsupported("Payloads().ListObjects")
}
func (unsupportedPayloads) ListRows(context.Context, string) ([]space.PayloadRow, error) {
	return nil, errUnsupported("Payloads().ListRows")
}

type unsupportedPubSub struct{}

func (unsupportedPubSub) Publish(context.Context, string, []byte) error {
	return errUnsupported("PubSub().Publish")
}
func (unsupportedPubSub) Subscribe(string, func(space.PubSubMessage)) (func(), error) {
	return nil, errUnsupported("PubSub().Subscribe")
}

type unsupportedReadState struct{}

func (unsupportedReadState) Subscribe(func(string, uint64)) func() { return func() {} }
func (unsupportedReadState) ChangedSince(context.Context, uint64, int) ([]space.ObjectReadState, error) {
	return nil, errUnsupported("ReadState().ChangedSince")
}
func (unsupportedReadState) UnreadSnapshot(context.Context, string) ([]space.UnreadChange, uint64, error) {
	return nil, 0, errUnsupported("ReadState().UnreadSnapshot")
}
func (unsupportedReadState) UnreadCounts(context.Context, string) (map[string]int, error) {
	return nil, errUnsupported("ReadState().UnreadCounts")
}
func (unsupportedReadState) MarkRead(context.Context, string, []string) error {
	return errUnsupported("ReadState().MarkRead")
}
func (unsupportedReadState) MarkReadUpTo(context.Context, string, crdt.VersionId) error {
	return errUnsupported("ReadState().MarkReadUpTo")
}
func (unsupportedReadState) Generation(context.Context) (string, error) {
	return "", errUnsupported("ReadState().Generation")
}

type unsupportedChanges struct{}

func (unsupportedChanges) MaxApplySeq(context.Context) (uint64, error) {
	return 0, errUnsupported("Changes().MaxApplySeq")
}
func (unsupportedChanges) ChangedSince(context.Context, uint64, int) ([]space.ObjectChange, error) {
	return nil, errUnsupported("Changes().ChangedSince")
}
func (unsupportedChanges) Subscribe(func(space.ObjectChange)) func() { return func() {} }
func (unsupportedChanges) Generation(context.Context) (string, error) {
	return "", errUnsupported("Changes().Generation")
}

var (
	_ space.PropertiesAPI  = unsupportedProperties{}
	_ space.ACL            = unsupportedACL{}
	_ space.MembersAPI     = unsupportedMembers{}
	_ space.Query          = unsupportedQuery{}
	_ space.Files          = unsupportedFiles{}
	_ space.HistoryAPI     = unsupportedHistory{}
	_ space.PayloadsView   = unsupportedPayloads{}
	_ space.PubSubAPI      = unsupportedPubSub{}
	_ space.ReadStateAPI   = unsupportedReadState{}
	_ space.ChangeIndexAPI = unsupportedChanges{}
)
