# Nested Spaces

A space can be created under a **parent** space. The child is a full space
with its own ACL, read key and members. The parent's owner is the child's
**legalOwner**: the child counts against that owner's limit, and that owner
keeps control over the child without being able to read it.

This is the design of the first version (V1). It is not implemented yet.

"Organization" is application vocabulary. any-sync and the SDK speak only
of parent, child and legalOwner, and the word does not appear in their
names, types or APIs. An application that calls a parent space an
organization, a team or a workspace builds that on this primitive.

## Model

- **The parent link is in the child's header.** `parentSpaceId` is a
  first-class signed field of `SpaceHeader`, not part of the opaque header
  payload. It is bound into the child's id and never changes. An empty
  value is a top-level space.
- **The parent is an ordinary space.** It is a regular top-level space. A
  tech space or a 1-1 space cannot be a parent, and a child cannot be a
  parent: nesting has one level.
- **The child is a regular space** with the V1 header, which binds its ACL
  root into the signed header.
- **Nothing else records the relation.** Nothing is written to the parent
  when a child is created, and the child's ACL stores nothing about the
  parent's owner.
- **A child inherits the parent's replicationKey**, so a parent and its
  children live on one node partition and a client syncs them over one set
  of connections. This is an optimization, never a correctness dependency.
  Its costs are a possible hotspot for a very large parent and a visible
  shared suffix in the ids of a parent's children.

## Who can do what

| Action | Who |
|---|---|
| Create a child | A Writer, an Admin or the Owner of the parent |
| Add a member to a child | The child's Admins and Owner; only members of the parent can be added |
| Read or write a child's data | Members of the child |
| List a parent's children | The parent's Admins and Owner |
| Remove a member of a child | The child's Admins and Owner; the parent's Admins and Owner, without the child's read key |
| Change a child's owner | The child's Owner; the parent's Owner |
| Delete a child | The child's Owner; the parent's Owner |

Membership in the parent gives no access to its children. The parent's
Owner and Admins govern a child but read it only when they are members of
it, and they cannot add themselves or anyone else to a child they do not
administer.

## legalOwner

The legalOwner of a child is the parent's current owner. The coordinator
reads the parent's ACL on every decision and accepts a legalOwner action
only from the parent's current owner.

- The legalOwner key is the account key of the parent's owner. The role
  has no key of its own. A parent whose owner must not be tied to one
  person is owned by a dedicated account.
- When the parent's ownership is transferred, the billing of its children
  moves to the new owner and the new owner acts in the children at once.

The legalOwner has no read key of a child it is not a member of. It cannot
read or write the child's data, add members to it or rotate its keys.

## Trust model

The coordinator is the authority for everything that depends on the
parent. It holds the ACLs of both spaces.

On a network, a client does not re-check the content of an ACL record. It
verifies the network's signature on the record, and the network signs a
record only after the coordinator validated it. Authority that comes from
the parent is therefore checked in one place.

Two kinds of record are written in a child by an account that is not its
member: a removal by the parent's Admins or Owner, and an ownership
transfer by the parent's Owner. The coordinator resolves the author's role
in the parent and gives the allowed authors to ACL validation. Code that
re-validates a stored ACL without the network accepts these records as
they are.

## Creating a child

1. The creator is a Writer, an Admin or the Owner of the parent. No
   approval is needed.
2. The child is created locally, like any other space. Its header carries
   `parentSpaceId` and the parent's replicationKey.
3. At the child's first push the coordinator's `SpaceSign` checks that:
   - the parent exists and is not deleted;
   - the parent is a regular top-level space;
   - the caller is a Writer or above in the parent at that moment;
   - the legalOwner's limit is not exceeded.
4. The creator pushes the child with the receipt.

Steps 1 and 2 work offline. The child exists on the network only after the
push. A child the coordinator refuses stays a local space, and a creator
who loses the role before the first push is refused.

The creation role is fixed. A parent has no setting for it.

## Billing

A child counts against the shared-spaces limit of the legalOwner, not of
its creator. The limit is one number, counted across the children of every
parent the same account owns. There is no cap per member or per parent.

## Membership

A child's own Admins and Owner manage its members, mainly by direct-add
(`docs/direct-add-invites.md`).

Only members of the parent can be added to a child. The coordinator checks
this on every admission path: accepted join requests, direct-add batches
and invite joins.

## The list of children

The coordinator stores the parent of every child and serves the list of a
parent's children to the parent's Admins and Owner. Other members see only
the children they belong to, in their own space list, where each child
carries its parent id.

This restricts the list, not the link: a child's header names its parent,
and its id shares the parent's replication key.

## Removing a member without the read key

The parent's Admins and Owner can remove a member of a child they cannot
read.

A normal removal rotates the read key in the same record, which needs the
key. The keyless removal is therefore its own record,
`AclAccountRemoveNoRotate`, and the removal has two steps:

1. **Removal.** The record takes effect at once: the removed member has no
   permissions and their writes are no longer accepted. The child is now
   in a "rotation pending" state.
2. **Rotation.** An Admin or the Owner of the child, who holds the read
   key, writes the standard read-key rotation. The SDK does this by itself
   on a device of such a member, and exposes the pending state.

Between the two steps the removed member still holds the old key and can
read content written with it.

A keyless removal cannot remove the child's owner. When no key-holder is
left in a child, the rotation cannot happen and deleting the child is the
only lever.

## Changing a child's owner

The parent's Owner can hand a child to any active member of that child and
names the role the previous owner keeps. The new owner is already a member
and already holds the read key. The previous owner stays a member; removing
them is a separate step.

## Deleting a child

The parent's Owner can delete a child without access to its data. The
coordinator accepts the parent's current owner in place of the child's
owner.

## SDK surface

The SDK exposes:

- creating a child of a parent;
- listing a parent's children;
- removing a member of a child as an Admin or the Owner of the parent;
- changing a child's owner as the parent's Owner;
- deleting a child as the parent's Owner;
- the "rotation pending" state of a space.

`SpaceInfo` carries the parent id.

## Children as access scopes

A parent can be split into children by team, project or level of
confidentiality. Each child has its own members and read key, and a
member's access is the union of the children they belong to.

Spaces stay isolated: an object lives in one space, and objects are not
shared or linked across spaces. Combining a member's children into one view
is the client's work.

## Outside V1

- **External members.** Paid seats that admit accounts which are not
  members of the parent.
- **A configurable creation role.** The parent's ACL options can hold it;
  a ceiling set by a network or a license belongs on the coordinator.
- **Deleting a parent.** Its children are meant to be deleted with it. V1
  does not handle a parent that is deleted while it has children.
- **More than one level** of nesting.

## Open questions

1. **Removing a member of the parent.** The intended rule is that a member
   removed from the parent is removed from all its children: the remover
   takes the list of children from the coordinator and writes a keyless
   removal in each child the person is a member of. Unspecified:
   - a removed member who owns children. The ownership first goes to
     another member, which only the parent's Owner can do. A child whose
     only member is the removed person can only be deleted;
   - how the removal is finished when it stops halfway;
   - a member who leaves the parent by themselves;
   - whether a parent's Admin can remove from a child an Admin or the
     Owner of the parent.
2. **Limits.** Whether a child's member limits and file storage are charged
   to the legalOwner, as the number of children is.
3. **Protocol version.** Child spaces are expected to need a new any-sync
   protocol version, which clients on older versions do not open. The
   version and the point where an old client is refused are not chosen.
