package spaceimpl

import (
	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/space"
)

// MembersCollection is the per-space any-store collection holding the
// materialised members view. One row per identity (active members,
// removed-member tombstones, and pending join requests). Kept in
// sync by the per-space memberWatcher; not user-writeable.
//
// Naming follows the existing per-space convention from
// spaceobjects.SpaceObjectsCollection: <spaceId>/<collection>.
const MembersCollection = "members"

// Member-row field names. All strings except where noted.
const (
	MemberFieldId          = "id" // == Member.Identity
	MemberFieldPermission  = "permission"
	MemberFieldStatus      = "status"
	MemberFieldName        = "name"
	MemberFieldDescription = "description"
	MemberFieldIcon        = "icon"
	MemberFieldRequestId   = "requestId" // pending join records only
)

// encodeMember writes a Member into a fresh anyenc object on the
// given arena. Only fields with content are emitted — empty strings
// stay absent so an upsert leaves prior values untouched (same
// convention as techspace.SpaceIndexRecord.EncodeCreate). Permission
// and status use the canonical wire labels (Permission.String /
// MemberStatus.String) so query filters read as cleanly as
// `{"permission": "writer"}`.
func encodeMember(a *anyenc.Arena, m space.Member) *anyenc.Value {
	obj := a.NewObject()
	obj.Set(MemberFieldId, a.NewString(m.Identity))
	obj.Set(MemberFieldPermission, a.NewString(m.Permission.String()))
	obj.Set(MemberFieldStatus, a.NewString(m.Status.String()))
	if m.Name != "" {
		obj.Set(MemberFieldName, a.NewString(m.Name))
	}
	if m.Description != "" {
		obj.Set(MemberFieldDescription, a.NewString(m.Description))
	}
	if m.IconCID != "" {
		obj.Set(MemberFieldIcon, a.NewString(m.IconCID))
	}
	if m.RequestRecordId != "" {
		obj.Set(MemberFieldRequestId, a.NewString(m.RequestRecordId))
	}
	return obj
}
