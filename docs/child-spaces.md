# Child Spaces

An **organization** is a space under which spaces are created: it is
their **parent**, and they are its **children**. A child is a full space
with its own ACL, read key and members: membership in the parent gives
no access to it. The parent's Owner and Admins govern a child without
reading it. The model and its rules are `docs/nested-spaces.md`; this
page is the SDK API.

Every method below is on `sdk.Spaces()` unless it names `ACL()`.

## The two types

| | Header type | Constant | Made by |
|---|---|---|---|
| Organization | `any.organization` | `space.SpaceTypeOrganization` | `Create` with that `CreateRequest.SpaceType` |
| Child | `any.organization.space` | `space.SpaceTypeOrganizationSpace` | `CreateChild` |

```go
org, err := sdk.Spaces().Create(ctx, space.CreateRequest{
	Name:      "Acme",
	SpaceType: space.SpaceTypeOrganization,
})
```

The type is in the immutable space header: a space is created as an
organization, and a space of another type never becomes one. Only an
organization has children, and only a child names a parent. Read the
type from `SpaceInfo.Type`, not from `SpaceInfo.SpaceType`: a derived
space carries there a free-text tag its application chooses.

An organization is otherwise a space like `any.space`: objects, files,
members and invites work the same. Two things differ: it has children,
and neither it nor a child has a guest role (*Members of a child*).

A device whose SDK does not know the two types lists, joins and loads an
organization and its children as ordinary spaces, under these type
strings. It creates no organization and no child.

## The parent link

`SpaceInfo.ParentSpaceId` is the parent of a child space, empty for a
top-level space. It is the `parentSpaceId` of the signed space header,
kept on the index row (`docs/tech-space.md § Space Index`):

- the creating device writes it with the row;
- a joined space carries it from its first load on, like `Type`;
- a device whose SDK does not know the two types fills `Type` alone:
  `List` and `Subscribe` show the child as `any.organization.space`
  with an empty `ParentSpaceId` until the first `Get` of the space on a
  device on this SDK fills it.

A member finds the children it belongs to in `List`, by
`ParentSpaceId`. The list of all children of a parent comes from the
coordinator (`Children`).

## Creating a child

```go
child, err := sdk.Spaces().CreateChild(ctx, space.CreateChildRequest{
	ParentSpaceId: parentId,
	Name:          "Design",
})
```

`CreateChild` works like `Create`: it is local and needs no network. The
child's v1 header carries the type `any.organization.space` and names
the parent, the child takes the parent's replication key, and nothing is
written to the parent. The caller owns the child.

Only `CreateChild` makes a space of that type: `Create` refuses it with
`ErrBadSpaceType`. `SpaceInfo.SpaceType` of a child is
`any.organization.space` too. The app-level space type is a tag of
derived spaces (`DeriveRequest.SpaceType`); a derived space is neither a
child nor a parent.

The parent is an organization of the account's index. `CreateChild`
refuses what the index already shows:

| Parent | Error |
|---|---|
| not a space id | `ErrBadSpaceId`, as from every call on this page for a malformed id |
| no row | `ErrSpaceUnknown` |
| deleted | `ErrSpaceDeleted` |
| join or invite not accepted yet | `ErrSpaceNotAccepted` |
| not an organization: a space of type `any.space` or of any other header type, a child, a derived space, a 1-1 space, the tech space | `ErrParentSpaceInvalid` |

A parent whose row has no type yet, a joined space the account has
loaded on no device, is not judged here. The coordinator refuses a child
of a space that is not an organization at its first push
(`RefusalParentInvalid`).

The caller's role in the parent is not checked locally. The coordinator
checks it when the child is first pushed: the account must then be a
Writer, an Admin or the Owner of the parent, and the parent's Owner below
its child spaces limit. Read the role from the parent's
`SpaceInfo.OwnRole` to decide whether to offer the action.

A child of a parent this account owns is sent to the network only after
the parent is, for example when both were created offline. Until then
`RetryPush` answers `ErrParentSpaceNotAvailable` and no refusal is
recorded: `Refusal` stays `RefusalNone`, and the SDK pushes the child by
itself as soon as a tree node holds the parent. A parent owned by another
account is on the network already, and its child is asked for at once.

## A refused child

A child the coordinator refuses to sign stays a local space: `Status` is
`StatusActive`, reads and writes work, and no node holds it.
`SpaceInfo.Refusal` names the failed check.

| `Refusal` | Label | Cause | What ends it |
|---|---|---|---|
| `RefusalParentNotAvailable` | `parent_not_available` | The parent does not exist on the network or is deleted. | Nothing, for a deleted parent |
| `RefusalParentInvalid` | `parent_invalid` | The parent is not an organization. | Nothing |
| `RefusalParentPermissionDenied` | `parent_permission_denied` | The account is below Writer in the parent. | A role of Writer or above in the parent |
| `RefusalChildLimitReached` | `child_limit_reached` | The parent's Owner is at its child spaces limit. | A higher limit of that account, or fewer children |

One more value is set on a deleted space, not on a child:
`RefusalHasChildren` (`has_children`), see *Deleting a parent*.

`Refusal` is on the synced index row: every device of the account reads
it, also one that has no copy of the child. The device the coordinator
answered writes it. It returns to `RefusalNone` when the coordinator
signs the space for any device that holds it, or reports it registered.

Each device that holds the child asks again by itself: a minute after a
refusal it got, then after two, four and so on, up to once an hour, and
once after a restart. No device asks again by itself after
`RefusalParentInvalid`. Two devices that hold the child each ask on their
own; the row keeps the last answer, and a device drops its wait when the
row reads `RefusalNone`. To ask at once:

```go
err := sdk.Spaces().RetryPush(ctx, childId)
```

| Result | Meaning |
|---|---|
| `nil` | Signed. `Refusal` is cleared and the space is pushed to the nodes in the background. |
| `ErrParentSpaceNotAvailable`, `ErrParentSpaceInvalid`, `ErrParentPermissionDenied`, `ErrChildSpacesLimitReached` | Refused again. `Refusal` names that check. |
| `ErrParentSpaceNotAvailable` with `Refusal` unchanged | The parent is the account's own and no tree node holds it yet. The coordinator was not asked; the SDK asks when one does. |
| `ErrOffline` | The coordinator could not be reached. Nothing changed. |
| `ErrNotChildSpace` | The space is not a child. |
| `ErrSpaceNotStored` | This device has no copy of the space. A device that holds it retries the push. |

A refused child is shared with nobody: no node holds it. On it
`ACL().CreateInvite` and `ACL().AddAccounts` ask the coordinator once
and, when it does not know the child, return the error of the recorded
refusal with no wait, the one `RetryPush` returns for it:
`ErrParentSpaceNotAvailable`, `ErrParentSpaceInvalid`,
`ErrParentPermissionDenied` or `ErrChildSpacesLimitReached`. When the
coordinator knows the child the calls go on, whatever the row still
reads; offline they fail as on any space.

A child with no refusal that the coordinator does not know yet is waited
for like any new space, up to 35 s: one whose own parent is not pushed
yet, for example. `ErrSpaceUnknown` when the push has not landed by
then.

A refused child that is no longer wanted is removed with `Delete`, on
any device of the account. A device asks for the child no more once its
own row reads deleted, also when it still holds a copy, whatever becomes
of the cause of the refusal. A sign already sent when the deletion lands
completes, and a device the deletion has not reached yet still asks: a
child signed that way is deleted on the network by the deletion
reconciler.

The child spaces limit belongs to the account that owns the parent and
counts the children of all the spaces it owns. That account reads its own
limit with `sdk.Account().Limits(ctx)`: `ChildSpaces.Limit`.

## Listing the children of a parent

```go
children, err := sdk.Spaces().Children(ctx, parentId)
for _, c := range children {
	// c.Id, c.Status: active or deletion_pending
}
```

The coordinator answers an Admin or the Owner of the parent with every
child, the ones the account is not a member of included. A deleted child
is listed as `deletion_pending` until the coordinator has finished
deleting it, and not at all after that. The list is read on every call
and kept nowhere. No row and no storage is created for a listed child,
and the account needs no row for the parent.

`ErrOffline` when the coordinator cannot be reached;
`ErrParentPermissionDenied` for any other caller; `ErrSpaceUnknown` for a
parent the coordinator does not know and `ErrSpaceDeleted` for one it
holds as deleted or in deletion.

The type of the space is not checked. An Admin or the Owner of a space
that is not an organization gets an empty list, so an empty list does
not say that the space can have children: `SpaceInfo.Type` does.

A parent the account has just created is not on the network at once.
Until it is pushed `Children` answers `ErrSpaceUnknown`, and then
`ErrParentSpaceNotAvailable` until the ACL of the parent has reached the
network as well. Both pass by themselves.

## Governing a child from the parent

These calls reach a child from outside it. The caller need not be a
member of the child, and the child is not added to the index, loaded or
synced. Each needs the network and returns `ErrOffline` without it.

| Call | The network accepts it from | Effect in the child |
|---|---|---|
| `Children(ctx, parentId)` | an Admin or the Owner of the parent | None: lists the children of the parent. |
| `ChildMembers(ctx, childId)`, `ChildRotationPending(ctx, childId)` | any account: the nodes serve the ACL of a space to whoever asks | None: the accounts of the child with role and status; whether a rotation is pending. |
| `RemoveChildMembers(ctx, childId, identities)` | an Admin or the Owner of the parent, and nobody else: no role in the child allows it | The members lose their permissions at once. The read key is not changed: see *Rotation pending*. The child's Owner cannot be removed, and an Admin of the parent does not remove an account that is an Admin or the Owner of the parent. |
| `ChangeChildOwner(ctx, childId, newOwner, oldOwnerPerm)` | the Owner of the parent; the child's own Owner | `newOwner`, an active member of the child and a member of the parent, becomes its Owner. The previous owner stays a member with `oldOwnerPerm`. |
| `DeleteChild(ctx, childId)` | the Owner of the parent; the child's own owner | The child is deleted on the network. |

The child's own Owner is accepted on the last two whatever its role in
the parent, as the owner of any space changes its owner and deletes it.

`ChildMembers` is how the parent side learns who is in a child: the
identities to remove, and the active members one of whom can become the
owner.

```go
members, err := sdk.Spaces().ChildMembers(ctx, childId)
for _, m := range members {
	// m.Identity, m.Permission, m.Status
}
```

It reads the child's ACL off the nodes. An ACL names accounts by
identity; names are not readable without the child's keys. The read is
not limited to children: for any space the account does not hold, a
top-level one included, `ChildMembers` and `ChildRotationPending`
answer, so whoever knows the id of a space reads its accounts and their
roles.

An account added with `ACL().AddAccounts` is an active member in the ACL
at once, whether it accepts the invite or declines it
(docs/direct-add-invites.md): `ChildMembers` does not tell the two
apart, and `ChangeChildOwner` hands the child to either. An owner who
has not accepted holds the child on no device until `AcceptInvite`.

`RemoveChildMembers` is for the parent's Admins and Owner only. An Admin
or the Owner of the child removes members with `ACL().RemoveAccounts`,
which changes the read key in the same record. The child's Owner
transfers the ownership with `ACL().OwnershipChange` as well.

`DeleteChild` and `Delete` are different calls. `Delete` is for a space
of the account's own list: it deletes on the network only a space the
account owns, and for a member of a child it removes the child from the
account alone, the parent's Owner included. `DeleteChild` deletes the
child for everyone. When the account also holds the child in its index,
the row then reads `StatusDeleted` and the local copy is removed.

Errors of these calls:

| Error | Meaning |
|---|---|
| `ErrOffline` | No node or coordinator could be reached. Nothing changed. |
| `ErrParentPermissionDenied` | The network does not accept the call from this account (the table above), or an Admin of the parent removes an account that is an Admin or the Owner of the parent. |
| `ErrNotParentMember` | The new owner is no member of the parent. |
| `ErrAccountNotFound` | An identity is no member of the child: one of the removed, or the new owner. |
| `ErrInsufficientPermissions` | The ACL forbids the record: the child's Owner or the caller among the removed identities; a new owner who is the Owner already; an `oldOwnerPerm` of `none`, `guest` or `owner`. |
| any other error from `RemoveChildMembers` or `ChangeChildOwner` | The record names the ACL head it was built on, and another record reached the child's ACL first (`records conflict`, `incorrect prev id of a record`). Nothing was written: call again. |
| `ErrSpaceUnknown` | The coordinator does not know the space. That includes a space this account created and has not pushed yet, a parent given to `Children` for example: the error passes once the space is pushed. |
| `ErrParentSpaceNotAvailable` | The coordinator cannot read the ACL of the parent. From `Children`: it knows the parent and the ACL is not on the network yet, which passes once the parent is pushed. From `RemoveChildMembers`, `ChangeChildOwner` and `DeleteChild`: the ACL of the child's parent. |
| `ErrSpaceDeleted` | The coordinator holds the space as deleted or in deletion. `DeleteChild` of such a child returns `nil`: it is done. The error is no proof that the space is gone for good: the coordinator lets the space's own owner take back a deletion that owner started. |
| `ErrNotChildSpace` | The space is not a child. The index shows it for a space the account holds whose row has a type and is not the row of a child, an organization, the tech space and a 1-1 space included, and nothing is sent. `DeleteChild` also returns it for an active top-level space of another account, an organization included, which the coordinator shows; `RemoveChildMembers` and `ChangeChildOwner` see only the ACL of such a space. The device refuses with `ErrInsufficientPermissions` when the record names the space's Owner or the caller, and with `ErrAccountNotFound` for an identity that is no account of it. For another member of the space the coordinator refuses the record, and that error carries no sentinel. Not covered: for the tech space of another account, or a 1-1 space the account does not hold, `DeleteChild` returns the coordinator's untyped error, and for a space of another account that is deleted or in deletion it returns `nil`, as for any space the coordinator holds so. |
| `ErrBadSpaceId`, `ErrBadIdentity` | Malformed input. |

## Rotation pending

`RemoveChildMembers` removes without the child's read key, so the removed
members keep the current key: they are members no longer, and read what
is written until the key changes.

```go
pending, err := child.ACL().RotationPending(ctx)
```

No call ends it. A device writes the read key change by itself when its
account is an Admin or the Owner of the child and the device holds the
child with its read key:

- when the SDK starts, with no `Get`: for a child the device holds whose
  row names the parent or has the type `any.organization.space`, and
  whose load at start succeeds. A headless SDK loads no space at start,
  and rotates from the first `Get` of the child;
- while it runs, when the removal reaches a child that is loaded: one
  the SDK loaded at start or the application opened since;
- with the network reachable; a change that could not be published is
  retried for as long as the rotation is pending, and at once when a
  tree node next answers a sync round of the space;
- once: when two devices write it at the same time, the network takes one
  record.

A device holds a child once it has loaded it: the device that created it
or accepted the invite or the join, and any other device of the account
after its first `Get` of the space. A device that has only the row of the
child does not rotate. With direct peer sync on, a device also pulls the
spaces of its index by itself; with it off, the first `Get` does.

The parent's Admins and Owner, who are no members of the child, read the
same state from the ACL the nodes serve, with nothing of the child
tracked or stored and the errors of `ChildMembers`:

```go
pending, err := sdk.Spaces().ChildRotationPending(ctx, childId)
```

It is `true` from the removal until the child's key is changed. A child
that stays pending has no device of an Admin or the Owner left to change
the key.

`RotationPending` reads `false` on a device once the change is applied
there. A child with no device of an Admin or the Owner left that holds
it stays pending; `DeleteChild` is what remains.

What the network enforces after a removal: any-sync checks a change
against the ACL record the change names. A change of the removed member
that names a record from before the removal is valid, and the other
members read it, as in any space. A device on the SDK names the head of
its own ACL, and stops writing once its own ACL shows the removal.

## Members of a child

Only a member of the parent becomes a member of a child, or its owner.
The coordinator checks every way in, and its refusal is
`ErrNotParentMember`:

| Call | Refused when |
|---|---|
| `ACL().AddAccounts` | one of the added accounts is not a member of the parent; nobody of the batch is added |
| `ACL().AcceptRequest` | the joiner is not a member of the parent any more: it left the parent after it asked |
| `Join` | this account is not a member of the parent: its join request is refused when it is posted, and no row is written |
| `ACL().OwnershipChange`, `ChangeChildOwner` | the new owner is not a member of the parent |

The same calls return `ErrParentSpaceNotAvailable` when the coordinator
cannot read the ACL of the parent.

### No guest role

An organization and its children have no guest role: no account is a
guest in one, and none has a guest key. A read-only member is a Reader.
In both types these calls return `ErrNoGuestRole`:

| Call | Refused |
|---|---|
| `ACL().CreateGuestKey`, `ACL().RevokeGuestKey` | always |
| `ACL().AddAccounts` | when an account is added with `PermissionGuest`; nobody of the batch is added |
| `ACL().ChangePermissions` | when a change is to `PermissionGuest`; no change of the batch is made |
| `ACL().AcceptRequest` | with `PermissionGuest` |

The type of the space decides (`SpaceInfo.Type`), for any caller and
before its role is checked, and nothing is sent. A space of type
`any.space` keeps its guest key and its guests.

`ErrNoGuestRole` is the coordinator's own refusal of a record that makes
an account a guest in an organization or a child. A device whose SDK
does not know the two types sends such a record and gets that refusal
from the network, as an error its SDK has no sentinel for.

## Deleting a parent

The coordinator does not delete an organization that still has children,
and a deleted child is one until the coordinator has finished deleting
it. So delete the children, and delete the parent once `Children` lists
none:

```go
children, err := sdk.Spaces().Children(ctx, parentId)
for _, c := range children {
	if c.Status == space.ChildStatusActive {
		err = sdk.Spaces().DeleteChild(ctx, c.Id)
	}
}

// Later, when Children(ctx, parentId) returns no child:
err = sdk.Spaces().Delete(ctx, parentId)
```

`Delete` of an organization the account owns settles this before it
writes anything:

- a child in the account's index whose row names the parent:
  `ErrSpaceHasChildren`, with no network;
- the coordinator in reach: the signed delete is sent on the call path.
  `ErrSpaceHasChildren` while `Children` still lists a child, the ones
  the account is not a member of and the ones in deletion included.
  Nothing is written then, and nothing repeats the call: the application
  calls `Delete` again once `Children` lists none.

With no coordinator in reach `Delete` stays offline-first: the space
leaves the account at once and the network delete is sent later. When the
coordinator then refuses it for children, the deleted row shows it:

```go
// in List: info.Status == space.StatusDeleted
if info.Refusal == space.RefusalHasChildren {
	// Children(info.Id), then DeleteChild each active one
}
```

`Children` and `DeleteChild` work on a deleted parent. The SDK sends the
waiting delete again by itself, on every pass of the deletion reconciler
and after each accepted `DeleteChild`. It goes through once the last
child is finally deleted, and `Refusal` returns to `RefusalNone`.

A deletion the coordinator refused changes nothing on the network: the
parent stays an active space there. Only an account that is a Writer or
above in the parent and holds it as an active space creates children
under it, and each new child is one more the deletion waits for.

Only an organization has children, so `Delete` of a space whose row
carries another type asks nothing first (docs/space.md § Space
Lifecycle). A device whose SDK does not know the two types deletes an
organization the same way: the row reads deleted at once, and the
network delete is refused until the children are gone.

## Without the network, and when the process is killed

| Flow | Offline | Killed |
|---|---|---|
| `CreateChild` | Works. The child is pushed when the network is back. | As `Create`: a child whose row was not written yet is not in the list. |
| Refusal | Nothing is asked; the recorded reason stays. | The reason is on the row. The next push asks once, then the waits start again; `RefusalParentInvalid` is not asked about. |
| `RetryPush` | `ErrOffline`, nothing changes. | The coordinator's answer is recorded before the call returns; a signed space is pushed by the next sync round. |
| `Children` | `ErrOffline`. | Nothing is stored. |
| `ChildMembers`, `ChildRotationPending` | `ErrOffline`. | Nothing is stored. |
| `RemoveChildMembers`, `ChangeChildOwner` | `ErrOffline`. | One ACL record: accepted by the network or not written. |
| `DeleteChild` | `ErrOffline`, nothing changes. | After the coordinator accepted: the deletion reconciler marks the account's row deleted and removes the local copy on its next pass. |
| Rotation | Retried until published. | The pending state is ACL state: the next start of the SDK sees it. |
| `Delete` of a parent | `ErrSpaceHasChildren` from the index alone for a child whose row names the parent; otherwise deferred, and a later refusal is `RefusalHasChildren` on the deleted row. | After the coordinator accepted: the deletion reconciler marks the row deleted and removes the local copy on its next pass. After the row was marked: the reconciler sends the delete. |
