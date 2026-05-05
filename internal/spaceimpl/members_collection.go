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

// permissionString maps the SDK Permission enum to the on-disk
// string representation used in the members collection. Strings
// (rather than numbers) so query filters read as cleanly as
// `{"permission": "writer"}`.
func permissionString(p space.Permission) string {
	switch p {
	case space.PermissionNone:
		return "none"
	case space.PermissionReader:
		return "reader"
	case space.PermissionGuest:
		return "guest"
	case space.PermissionWriter:
		return "writer"
	case space.PermissionAdmin:
		return "admin"
	case space.PermissionOwner:
		return "owner"
	default:
		return "none"
	}
}

// memberStatusString maps the SDK MemberStatus enum to the on-disk
// string representation.
func memberStatusString(s space.MemberStatus) string {
	switch s {
	case space.MemberStatusJoining:
		return "joining"
	case space.MemberStatusActive:
		return "active"
	case space.MemberStatusRemoved:
		return "removed"
	case space.MemberStatusDeclined:
		return "declined"
	case space.MemberStatusRemoving:
		return "removing"
	case space.MemberStatusCanceled:
		return "canceled"
	default:
		return "unknown"
	}
}

// encodeMember writes a Member into a fresh anyenc object on the
// given arena. Only fields with content are emitted — empty strings
// stay absent so an upsert leaves prior values untouched (same
// convention as techspace.SpaceIndexRecord.EncodeCreate).
func encodeMember(a *anyenc.Arena, m space.Member) *anyenc.Value {
	obj := a.NewObject()
	obj.Set(MemberFieldId, a.NewString(m.Identity))
	obj.Set(MemberFieldPermission, a.NewString(permissionString(m.Permission)))
	obj.Set(MemberFieldStatus, a.NewString(memberStatusString(m.Status)))
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
