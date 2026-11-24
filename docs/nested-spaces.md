# Nested Spaces

A space can be created under a **parent** space. The child is a full space
with its own ACL, read key and members. The parent's owner is the child's
**legalOwner**: the child counts against that owner's limit, and that owner
keeps control over the child without being able to read it.

This is the design of the first version (V1).

The parent is an **organization**: a space type of its own. The protocol
and the APIs name the relation parent and child; "organization" names the
two space types.

## Model

- **The parent link is in the child's header.** `parentSpaceId` is a
  first-class signed field of `SpaceHeader`, not part of the opaque header
  payload. It is bound into the child's id and never changes. An empty
  value is a top-level space.
- **The parent is an organization.** It is a top-level space of the type
  `any.organization`, created as one: a space of another type never
  becomes a parent. Apart from having children and having no guest role it
  is a full space, with its own content, members and invites.
- **The child is a space of an organization.** It carries the type
  `any.organization.space` and the V1 header, which binds its ACL root
  into the signed header. Nesting has one level.
- **The type and the parent id agree.** A space of the type
  `any.organization.space` names a parent that is an organization, and a
  space of no other type names a parent. A derived space is neither a
  parent nor a child.
- **The type tells the network what a space is.** The coordinator stores
  the type of every space, so limits and payments can treat organizations
  and their spaces apart from other spaces.
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
| Change a child's owner | The child's Owner; the parent's Owner. The new owner is a member of the child and of the parent |
| Delete a child | The child's Owner; the parent's Owner |
| Delete a parent | The parent's Owner, once it has no children |

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
in the parent and gives the allowed authors to ACL validation. A top-level
space gets no such authority, so it never admits these records.

A stored ACL that is rebuilt accepts these records as they are: the child
stores nothing about the parent, so a rebuild cannot check them again.
Without a network there is no coordinator and no authority from a parent,
and a client that validates in full refuses these records when it
receives them.

The coordinator reads the parent's ACL from its cache, which can lag the
network by seconds. In that window an account just removed from the parent
can still be admitted to a child or act on one.

## Creating a child

1. The creator is a Writer, an Admin or the Owner of the parent. No
   approval is needed.
2. The child is created locally, like any other space. Its header carries
   `parentSpaceId` and the parent's replicationKey.
3. At the child's first push the coordinator's `SpaceSign` checks that:
   - the parent exists and is not deleted or in deletion;
   - the parent is an organization, the child is a space of an
     organization, and the child's header is V1;
   - the caller is a Writer or above in the parent at that moment;
   - the legalOwner's limit is not exceeded.
4. The creator pushes the child with the receipt.

Steps 1 and 2 work offline. The child exists on the network only after the
push. A creator who loses the role before the first push is refused. A
child of a parent that the same account created offline is sent after the
parent.

Only the sign that registers a child is checked this way. A registered
child is signed again without the parent checks, so a later change in the
parent never turns a working child into a refused one. A child needs no
step to become shareable.

A child the coordinator refuses is kept as a local space with a status
that names the failed check: the parent is not available, the parent or
the child cannot be nested, the creator's role in the parent is too low,
or the limit is reached. The status is synced across the account's
devices. The client can retry the push or remove the space, and decides
what to offer the user for each reason.

The creation role is fixed. A parent has no setting for it.

## Billing

A child counts against the legalOwner's limit on child spaces, not against
its creator. This limit is separate from the shared-spaces limit: a child
uses nobody's shared-spaces limit. It is one number per account, counted
across the children of every parent that account owns. There is no cap per
member or per parent.

A child counts until its deletion is final. A network that configures no
limit allows no children. An account's stored limit of zero means the
network's default.

The limit does not hold back a transfer of a parent: a parent can go to an
owner whose limit its children exceed, and that owner creates no more
children until it is under the limit.

Only the number of children is charged to the legalOwner. A child's member
limits and file storage are accounted as for any other space, and a child
counts toward its creator's cap on the total number of spaces.

An organization that is shared takes a place in its owner's shared-spaces
limit, like any other space.

## Membership

A child's own Admins and Owner manage its members, mainly by direct-add
(`docs/direct-add-invites.md`).

Only members of the parent can be added to a child. The coordinator checks
this on every admission path: a join request when it is posted and when
it is accepted, direct-add batches and invite joins. The new owner of a
child must be a member of the parent too.

An organization and its spaces have no guest role. The coordinator refuses
a record that gives an account that role, whichever way it does: a guest
key, a direct add, an accepted request or a change of permissions. A
read-only member is a Reader.

## The list of children

The coordinator stores the parent of every child and serves the list of a
parent's children to the parent's Admins and Owner. Other members see only
the children they belong to, in their own space list, where each child
carries its parent id.

This restricts the list, not the link: a child's header names its parent,
and its id shares the parent's replication key.

With a child's id, the parent's Admins and Owner can read its accounts and
their roles from the child's ACL. Names need the child's key and are not
readable. The network serves the ACL of a space to any account that knows
its id, so this read is not restricted by role: the list of children is
what gives the ids.

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
   on a device of such a member that holds the child, also after a
   restart. The pending state is readable by the child's members and by
   the parent's Admins and Owner.

Between the two steps the removed member still holds the old key and can
read content written with it.

A keyless removal cannot remove the child's owner. When no key-holder is
left in a child, the rotation cannot happen and deleting the child is the
only lever.

An Admin of the parent cannot remove from a child an account it could not
remove from the parent itself: the parent's Owner or another Admin. The
parent's Owner can remove anyone but the child's owner.

## Removing a member of the parent

The network does not remove a member of the parent from its children. A
member who is removed from the parent, or who leaves it, stays a member of
the children they are in, and a child's owner stays its owner.

Bringing the children in line is the responsibility of the legalOwner.
With the list of children from the coordinator, the parent's Owner or an
Admin writes a keyless removal in each child the person is a member of.
The tool that does this for an organization, a service of its own or a
part of the application, is outside V1.

## Changing a child's owner

The parent's Owner can hand a child to any active member of that child and
names the role the previous owner keeps. The new owner is already a member
and already holds the read key. The previous owner stays a member; removing
them is a separate step.

The new owner must also be a member of the parent.

## Deleting a child

The parent's Owner can delete a child without access to its data. The
coordinator accepts the parent's current owner in place of the child's
owner. The child's owner cannot undo a deletion made by the parent's
Owner.

## Deleting a parent

A parent that still has children cannot be deleted: the coordinator
refuses the deletion. A child counts until its deletion is final. The
parent's Owner deletes the children first, and deletes the parent once its
list of children is empty.

The deletion of an account is refused the same way while a space it owns
has a child of another account.

## Protocol version

The any-sync change that adds child spaces raises the protocol version to
14. A node on an older version skips a keyless removal and applies an
ownership change by the parent's Owner wrongly, so every tree node and
file node runs the new version before a coordinator admits these records.
No gate refuses a client on an older version for a child space.

## SDK surface

The SDK exposes:

- creating an organization;
- creating a child of a parent;
- listing a parent's children;
- reading a child's accounts and its "rotation pending" state as an Admin
  or the Owner of the parent;
- removing a member of a child as an Admin or the Owner of the parent;
- changing a child's owner as the parent's Owner;
- deleting a child as the parent's Owner;
- the "rotation pending" state of a space;
- the status of a refused child, with retry and removal;
- the limit on child spaces in the account limits.

`SpaceInfo` carries the type and the parent id. The API is described in
`docs/child-spaces.md`.

## Children as access scopes

A parent can be split into children by team, project or level of
confidentiality. Each child has its own members and read key, and a
member's access is the union of the children they belong to.

Spaces stay isolated: an object lives in one space, and objects are not
shared or linked across spaces. Combining a member's children into one view
is the client's work.

## Outside V1

- **External members.** Paid seats that admit accounts which are not
  members of the parent, and a guest role that gives read-only access from
  outside it. In V1 nobody outside the parent is admitted to a child.
- **Removal from the children with the parent.** The legalOwner's tool
  that removes from the children a member who was removed from the parent
  or left it.
- **A configurable creation role.** The parent's ACL options can hold it;
  a ceiling set by a network or a license belongs on the coordinator.
- **Deleting a parent together with its children.** In V1 the children
  are deleted first.
- **More than one level** of nesting.
- **Other clients.** Organizations exist only for applications built on
  the SDK. Anytype clients neither create nor open them; support there
  comes later. A device on an SDK that does not know the two types opens
  an organization and its spaces as ordinary spaces and creates neither.

## Open questions

1. **Removing a member of the parent from its children.** Unspecified for
   the tool that does it:
   - a removed member who owns children. The ownership first goes to
     another member, which only the parent's Owner can do. A child whose
     only member is the removed person can only be deleted;
   - how the removal is finished when it stops halfway;
   - whether a parent's Admin can remove from a child an Admin or the
     Owner of the parent. V1 refuses it.
2. **Limits and payments.** The payment service does not set the limit on
   child spaces yet, so every account takes the network's default. Whether
   a child's member limits and file storage are charged to the legalOwner
   is decided with it.
3. **A parent handed to an account in deletion.** The ownership change does
   not look at the new owner's account, so a parent can end up owned by a
   deleted account, with nobody to govern its children.
