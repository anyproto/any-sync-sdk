# Nested Spaces (parent / child) — TODO / DESIGN DRAFT

> **Status: TODO.** This is a grooming/research document, not an implemented
> feature. It records the design direction, the required any-sync and
> coordinator changes, the SDK surface, and the open questions. Nothing here is
> built yet. Decisions dated below are grooming decisions, subject to revision
> when implementation starts.

## Naming

At the any-sync / SDK level there is **no "organization"**. The primitive is a
**parent ↔ child** relationship between spaces ("nested spaces"). A space may
declare a `parentSpaceId`; the parent is an ordinary space.

The word **"organization" must not appear in any-sync or SDK names, types, or
APIs.** It is strictly an **application-level** concept layered on the
parent/child + legalOwner primitive. The product's **organization feature is the
first consumer** of this primitive — it is the motivating use case and drives
the requirements — but the SDK only ever speaks parent/child/legalOwner; whether
a client calls a parent space an "organization", a "team", or a "workspace" is
the client's vocabulary, not ours.

The rest of this doc says **org space** for "the parent" only as a readability
shorthand for the reader — it is **not** a name that appears in code.

## Vision

A member of a parent space can create **child spaces** that are governed by the
parent:

- The child's resource limits (member caps, shared-space count, file storage)
  are charged to the parent's **legalOwner** identity — not to the personal
  quota of the member who created the child.
- The parent's legalOwner retains authority over the child — it can **remove
  members** and **delete the child** — *even if it never had read access to the
  child's data*.
- A member with sufficient rights in the parent can create children
  **self-service**: no per-child human approval step. The only hard gate is the
  coordinator's validation at space-sign time (limits + parent settings +
  authority checks).

This is the substrate for: company/team accounts where the company governs
membership and lifecycle but employees own their data; delegated space creation
under a shared quota; compliance/recovery authority over a set of spaces; and —
the next planned consumer — **fine-grained ACL via compartments**: scoping
access to subsets of an org's data by splitting it into child spaces, each with
its own membership and read key (see "ACL granularity via compartments" below).

### Driving requirements

1. **Parent-governed limits.** Children consume the **legalOwner's** quota,
   computed and enforced by the coordinator at sign time. (Grooming answer:
   limit bucket = legalOwner identity.)
2. **legalOwner authority without data access.** legalOwner can remove members
   and delete a child without holding the child's read key. When it lacks the
   key, remaining key-holding members complete the cryptographic half (read-key
   rotation) on observing the state. (Requirement 3 in the brief.)
3. **Self-service creation, coordinator-gated.** Any parent member at/above a
   configurable permission threshold (default **Admin**) registers a child by
   appending one ACL record to the parent; the coordinator validates and signs.
   No owner approval record in the loop. (Grooming answer: self-service for
   admins+.)
4. **legalOwner = the parent's real owner.** The child's declared legalOwner
   must be the actual owner of `parentSpaceId`; the coordinator verifies this at
   sign time. When the org itself creates the child, the org is **real owner and
   legalOwner simultaneously** (and may join the child's ACL as
   reader/writer/admin to gain real data access).

## The flow (end to end)

```
member of orgSpace (Admin+)                    coordinator                 nodes
        │                                            │                       │
 1. create child header                              │                       │
    SpaceHeader{ parentSpaceId = orgId,              │                       │
                 identity = me, ... }  (signed by me)│                       │
    + child AclRoot{ legalOwner = orgOwner,          │                       │
                     parentAclRef = <set in step 2> }│                       │
        │                                            │                       │
 2. register child in org ACL  ──────────────────────┼──> (parent ACL sync)  │
    append AclChildRegister{ childSpaceId,           │                       │
        childAclRootCid, permissionsGranted }        │                       │
    authored by me, requires Admin+ in orgSpace      │                       │
        │  (record id = parentAclRecordId)           │                       │
        │                                            │                       │
 3. SpaceSign(childId, header, parentAclRecordId) ──>│  validates:           │
        │                                            │   • parentSpaceId real │
        │                                            │   • legalOwner == owner│
        │                                            │     of parentSpaceId   │
        │                                            │   • registration record│
        │                                            │     exists in parent   │
        │                                            │     ACL, references    │
        │                                            │     childId, authored  │
        │                                            │     by Admin+          │
        │                                            │   • legalOwner limits  │
        │                                            │     (sharedSpaces,     │
        │                                            │      members) not over │
        │                                            │   • parent settings    │
        │                                            │     allow child create │
        │<───────────────── SpaceReceipt (signed) ───│                       │
        │                                            │                       │
 4. SpacePush(child, receipt) ───────────────────────┼──────────────────────>│
        │                                            │   child stored,        │
        │                                            │   charged to legalOwner│
```

Steps 1–2 are local + parent-ACL writes (offline-capable). Step 3 needs the
coordinator. Step 4 pushes to nodes. The child does not exist on the network
until step 4; before that it is a local draft plus one record in the parent
ACL.

## What any-sync gives us today vs. what must change

### Already present (reused, not reinvented)

- **Space header signing & CID id** (`commonspace/spacepayloads`): the child id
  is the CID of its signed header, exactly as for any space.
- **Coordinator `SpaceSign` → `SpaceReceipt`** (`coordinator.proto`): the
  existing authorization-to-push handshake. We extend its request and its
  server-side validation; the response shape (`SpaceReceiptWithSignature`) is
  unchanged.
- **Identity-keyed limits** (`AccountLimitsSetRequest.identity`,
  `AccountLimits.sharedSpacesLimit`, `SpaceLimits.readMembers/writeMembers`):
  limits are already charged to an **identity**. "Charge the legalOwner" is a
  retargeting of an existing accounting unit, not a new accounting model.
- **ACL record `oneof`** (`AclContentValue` in `aclrecordproto`): new record
  types (child-register, legalOwner-set/change) are added variants, the
  established extension pattern.
- **`AclRoot` carries space-scoped extras already** (`oneToOneInfo`, `options`):
  a `legalOwner` + `parentAclRef` field on `AclRoot` follows that precedent.
- **`AclReadKeyChange`** record + read-key rotation: the existing mechanism the
  key-regeneration flow (below) reuses verbatim — we do not invent rotation.
- **`AclAccountRemove`** record: legalOwner removal reuses this record type with
  a relaxed authorization rule (below).
- **`AclSpaceOptions.deleteRestricted`** + coordinator `SpaceDelete`: the delete
  path legalOwner deletion plugs into.

### Required any-sync changes (TODO — tracked here)

1. **`SpaceHeader.parentSpaceId` — a first-class header field.**
   `parentSpaceId` is **NOT** stuffed into the opaque `spaceHeaderPayload`
   (field 6). any-sync routes, validates, syncs, and cascades nested spaces
   differently from flat spaces, so it must discriminate at the header level
   without parsing an opaque blob. Add an explicit field to the `SpaceHeader`
   message, e.g.:

   ```proto
   message SpaceHeader {
     bytes  identity = 1;
     int64  timestamp = 2;
     string spaceType = 3;
     uint64 replicationKey = 4;
     bytes  seed = 5;
     bytes  spaceHeaderPayload = 6;
     bytes  aclPayload = 7;
     bytes  settingPayload = 8;
     string parentSpaceId = 9;   // NEW — empty for top-level spaces
     SpaceHeaderVersion version = 100;
   }
   ```
   The field is part of the signed header, so the parent link is immutable and
   bound into the child's id. Empty ⇒ ordinary top-level space (full backward
   compat). (A `SpaceNesting` sub-message can be used instead if more nesting
   metadata is needed later; one string is enough for v1.)

2. **`AclRoot` gains `legalOwner` + `parentAclRef`.**
   ```proto
   message AclRoot {
     // ... existing fields 1..11 ...
     bytes  legalOwner = 12;     // identity that bears limits + retains authority; may lack readKey
     AclParentRef parentRef = 13; // pointer to the registration record in the parent ACL
   }
   message AclParentRef {
     string parentSpaceId   = 1; // == SpaceHeader.parentSpaceId (redundant, lets ACL validate standalone)
     string parentAclRecordId = 2; // the AclChildRegister record id in the parent ACL
   }
   ```
   For a top-level space both are empty and everything behaves as today.

3. **New ACL record types** (`AclContentValue` oneof):
   ```proto
   // appended to the PARENT (org) ACL — registers a child under it
   message AclChildRegister {
     string childSpaceId    = 1;
     bytes  childAclRootCid = 2;   // binds the registration to a specific child ACL root
     AclUserPermissions orgPermission = 3; // permission the org grants ITSELF in the child, if any (None = no data access)
   }
   message AclChildRegisterRevoke { string childSpaceId = 1; } // optional: de-list a child

   // sets / changes the legalOwner of THIS space (owner-only)
   message AclLegalOwnerChange { bytes legalOwner = 1; }
   ```
   `AclChildRegister` is authored in the **parent**; `AclLegalOwnerChange` (if we
   allow post-root changes) and the `legalOwner` field live in the **child**.

4. **Coordinator `SpaceSign` extension.** Request carries a pointer to the
   parent registration record; server-side validation does the nested checks
   (next section). Minimal proto delta:
   ```proto
   message SpaceSignRequest {
     string spaceId = 1;
     bytes  header = 2;
     // 3,4 deprecated
     bool   forceRequest = 5;
     string parentAclRecordId = 6; // NEW — the AclChildRegister record in the parent ACL
   }
   ```
   (`parentSpaceId` is already inside the signed `header`, so it need not be
   repeated; the coordinator reads it from the parsed header.)

5. **Coordinator limit accounting retargeted to legalOwner.** At child sign and
   at child member-add, the coordinator charges/checks the **legalOwner's**
   `AccountLimits` (sharedSpacesLimit, spaceMembersRead/Write) instead of the
   creator's. Top-level spaces keep charging the owner. New error reuse:
   `SpaceLimitReached` / `Forbidden`.

6. **Coordinator `SpaceDelete` accepts a legalOwner-signed deletion** for a
   child, in addition to the owner-signed path. (See "legalOwner delete" below.)

> The coordinator lives in the separate `any-sync-coordinator` repo; only the
> proto (in `any-sync`) is visible here. All server-side validation/accounting
> work is a coordinator-repo task, tracked from this doc.

## Coordinator validation at child `SpaceSign`

This is the trust gate; nothing else enforces the parent's authority. On
`SpaceSign(childId, header, parentAclRecordId)` the coordinator MUST verify, in
addition to the normal space-sign checks:

1. **Parent exists & is healthy.** `header.parentSpaceId` resolves to a real,
   non-deleted space the coordinator knows.
2. **legalOwner is the parent's real owner.** The child `AclRoot.legalOwner`
   equals the current owner identity of `parentSpaceId` (read from the parent
   ACL head). *(Grooming answer — "validate legalOwner is real owner of parent
   space".)*
3. **Registration record is real and consistent.** `parentAclRecordId` exists in
   the parent ACL, is an `AclChildRegister`, has `childSpaceId == childId`, and
   its `childAclRootCid` matches the child's actual ACL root CID.
4. **Registrant has authority.** The author of that registration record holds
   **Admin+** (or the parent-settings-configured threshold) in the parent ACL at
   the record's position. *(Grooming answer — "validate real space owner /
   registrant has enough rights".)*
5. **Limits.** legalOwner's `sharedSpacesLimit` not exceeded by adding this
   child; child's declared member caps within legalOwner's allowance.
6. **Parent settings allow it.** Parent-level toggles (e.g. "members may create
   children", child-creation permission threshold, max children) permit this
   creation. These live in the parent (ACL options or a settings object — see
   open questions).

Only if all pass does the coordinator emit the `SpaceReceipt`. A member cannot
forge parent governance: every check reads the **parent's** signed ACL, which
the member cannot author beyond its real permission level.

## legalOwner — authority model

`legalOwner` is an identity named in the child's `AclRoot`. It is the parent's
real owner. Its powers:

| Power | Holds child readKey? | Mechanism |
|---|---|---|
| Bears all limits | not required | coordinator charges legalOwner quota |
| Remove a member | not required | authors `AclAccountRemove`; key rotation deferred to a key-holder (below) |
| Delete the child | not required | coordinator `SpaceDelete` accepts legalOwner signature |
| Read/write data | **only if** added to child ACL as Reader/Writer/Admin | normal membership; org opts in via `AclChildRegister.orgPermission` or a later join |

Two configurations fall out of the grooming answers:

- **Member-created child:** org = legalOwner only. Org has no read key unless it
  added itself (`orgPermission >= Reader`). This is the "company governs but
  doesn't read employee data" case.
- **Org-created child:** the org creates the child itself ⇒ org is **real owner
  and legalOwner at once**, holds all keys, full data access. No special keyless
  path is exercised.

### Keyless removal + key regeneration (requirement 3, the novel part)

Problem: removing a member from an any-sync space normally requires rotating the
read key (`AclReadKeyChange`) so the removed member loses **future** read
access. If legalOwner holds no read key, it cannot perform that rotation.

Design:

1. **Removal is authoritative immediately, rotation is deferred.** legalOwner
   authors an `AclAccountRemove` for the target **without** a new read key
   (an "unsealed" removal). The ACL + coordinator honor the membership change at
   once: the removed member loses write acceptance and limit attribution
   immediately. *(Authorization rule change in any-sync's ACL state machine:
   the space's `legalOwner` may author `AclAccountRemove` even though it is not
   a normal Admin/Owner of the child ACL.)*
2. **Key-holders complete the cut-off.** The space now sits in an observable
   state: *a removal whose read key was never rotated.* Any remaining member who
   **does** hold the read key (an Admin, or the org itself if it joined) detects
   this and performs the standard `AclReadKeyChange`, re-encrypting a fresh read
   key for the reduced member set. This is the regeneration: existing mechanism,
   triggered by observing the unsealed-removal state.
3. **Window semantics.** Between (1) and (2) the removed member keeps the *old*
   read key and can still decrypt data encrypted under it — exactly the normal
   any-sync property (rotation only protects *future* writes; it never reaches
   back). Membership/limits are correct immediately; forward secrecy is restored
   when any key-holder rotates. We surface the "rotation pending" state so a
   client can prompt/auto-rotate.
4. **No key-holder left.** If every key-holding member is gone and only the
   keyless legalOwner remains, no one can rotate or read. legalOwner's remaining
   lever is **delete** (below) — the recovery escape hatch.

Open: whether the SDK auto-rotates on detecting an unsealed removal (preferred)
or requires an explicit client call. See open questions.

### legalOwner delete

legalOwner can delete a child it cannot read. Coordinator `SpaceDelete` is
extended to accept a deletion signed by the child's `legalOwner` (verified
against `AclRoot.legalOwner`), not only the owner. Interplay with
`AclSpaceOptions.deleteRestricted`: legalOwner deletion **overrides**
`deleteRestricted` (legalOwner is the governance authority; the restriction
guards against member-initiated deletes, not the legalOwner). The local offload
path is the existing one (`docs/03-space.md` deletion lifecycle); only the
authorizing identity differs.

## ACL granularity via compartments (child-space-per-scope) — TODO

> Next planned consumer of the nested-spaces primitive. Same mechanism, used to
> get **finer-grained access control than a single space's one-ACL-one-read-key**
> allows.

### The problem this solves

any-sync's unit of access control is the **space**: one ACL, one read key, one
permission level per member for the *whole* space. There is no per-object or
per-collection ACL — property *scopes* (`synced/account/local`, see
`docs/scoped-properties-proposal.md`) govern write-route and version domain, not
*who can read*. So "team members see project A but not project B", or
"contractors see only the shared docs, not finance", cannot be expressed inside
one space.

### The approach: compartments

A **compartment is a child space** dedicated to one access scope (a team, a
project, a confidentiality tier). It has its own ACL, its own read key, its own
membership — and it hangs off the org via the parent/child + legalOwner
machinery above. A member's **effective access is the union of the compartments
they belong to**, composed client-side; granularity is achieved by *composition
of spaces*, not by a new in-space mechanism.

```
                    orgSpace (parent)
                    ACL: all org members ; legalOwner = orgOwner
                    holds: org-wide shared data (everyone-in-org tier)
                      │
        ┌─────────────┼──────────────────┐
        ▼             ▼                   ▼
   child: "eng"   child: "finance"   child: "design"
   ACL: {a,b,c}   ACL: {a,d}         ACL: {b,e}
   readKey_eng    readKey_fin        readKey_des
   legalOwner=orgOwner (governs all three; reads none unless added)

   member b's effective view = orgSpace ∪ eng ∪ design   (NOT finance)
```

This deliberately trades a **coarser unit** (a whole sub-space, not an object)
for **zero new crypto**: it reuses the entire shipped ACL, sync, key-rotation,
and members-collection machinery per compartment. Per-object ACL would require
per-object keys + per-object membership — a far larger change — and is **not**
what "use nested spaces for granularity" means.

### Rules (from grooming, 2026-06-24)

1. **Compartments are need-to-know (no access cascade).** The org
   owner/admin/legalOwner **governs** every compartment (keyless member-remove,
   delete — per the legalOwner model) but is **not** an automatic member and
   **cannot read** a compartment's data unless explicitly added to its ACL as
   Reader+. An org can therefore host compartments it cannot read — legal
   separation by default.
2. **Each compartment self-governs its membership.** A compartment's own Admins
   run its invites/accepts/removes via the normal space ACL surface
   (`docs/03-space.md`). Org admins who are not members of a compartment cannot
   add themselves or others into it; their only reach into a compartment is the
   legalOwner powers (keyless remove, delete). This keeps compartments
   autonomous.
3. **Compartment membership ⊆ org membership, by default.** Joining a
   compartment requires being an org member (the org is the identity boundary).
   The parent ACL MAY widen this per the external-seats rule below.
4. **Externals via paid seats, controlled by the parent ACL.** A compartment may
   admit an identity that is **not** an org member only when the parent ACL
   grants an **external-seat** allowance, enforced by the coordinator against the
   legalOwner's limits (a billable resource — "external seats are paid"). This
   ties external collaborators to a quota the org pays for, rather than an
   unbounded free-for-all. Requires a new limit field (e.g.
   `AccountLimits.externalSeatsLimit`) and a coordinator check when a child adds
   a member who is not in the parent ACL.
5. **Existence and membership of compartments are visible; only content is
   gated.** Compartments are registered in the parent ACL (`AclChildRegister`)
   and exist on the network with their own member lists — all of which org
   members (and the network) can see. What a non-member **cannot** do is read the
   compartment's *data*, which is encrypted under the compartment's own read key.
   No hidden compartments in v1 (hiding existence would need encrypted
   registration metadata — a separate later feature).

### Consequences to design around

- **Data is partitioned, not overlapping.** An object lives in exactly one space
  (`docs/04-object.md`), so an object in compartment A is **not** also in
  compartment B. To make data visible to a *broader* audience, it must live in a
  *broader* space (the org space for everyone-in-org; a parent compartment for a
  super-group). Choosing the right tier at creation is the core modeling act.
- **No cross-compartment object sharing in v1.** Two compartments that need the
  same object must either duplicate it, or put it in a common ancestor space, or
  (future) use a cross-space *reference* primitive — but a reference is only
  resolvable by a reader who holds the target compartment's read key, so it does
  not bypass the ACL. Cross-space references are out of scope for v1; flagged as
  an open question.
- **Granularity has a quota cost.** Every compartment is a child space charging
  the legalOwner's `sharedSpacesLimit` + its own `SpaceLimits` member caps.
  Fine-grained compartmentalization multiplies the org's space count; the
  coordinator caps it (max-children / shared-spaces limit). This is a real
  pressure toward *coarse* compartments (a handful of tiers), not per-document
  spaces.
- **Two-dimensional permissions.** A member's capability is (which compartment) ×
  (level within it). Admin of `eng`, Reader of the org space, absent from
  `finance` — all independent. The members collection per space already
  expresses the per-space half; the cross-space composition is the client's.
- **Independent key rotation.** Each compartment rotates its read key
  independently; the legalOwner keyless-removal + key-regeneration flow (above)
  applies per compartment and never touches a sibling.

### SDK surface (mostly reuse)

Granularity adds **no new core primitive** — it is `CreateChildSpace` +
per-child normal ACL operations, used as a pattern. The only genuinely new
surface:

- **Compartment creation** = `CreateChildSpace(orgId, …)` with `orgPermission =
  None` (org governs but takes no seat/read access in the compartment).
- **Compartment membership** = the existing per-space ACL methods
  (`docs/03-space.md`) operated on the child by the child's own admins.
- **`Children(orgId)`** lists compartments (registration records); a member sees
  all compartment ids + member lists, content only for the ones they hold a key
  to.
- **External seats** (phase later): an org-settings allowance + a member-add that
  flags an external (non-org) identity, gated by the coordinator against
  `externalSeatsLimit`.

Effective-access composition ("show me everything member X can reach across the
org") is a **client concern** over `Children` + per-compartment membership, not
an SDK method in v1.

## SDK surface (sketch — names TODO)

```go
// CreateChildSpace creates a space nested under parentSpaceId. The caller must
// hold Admin+ (or the parent-configured threshold) in the parent. Performs the
// full flow: build child header with parentSpaceId, append the AclChildRegister
// record to the parent ACL, request the coordinator SpaceSign, push. legalOwner
// is taken as the parent's current owner. orgPermission grants the org itself a
// role in the child (None = org gets no data access / keyless governance).
CreateChildSpace(ctx, parentSpaceId string, p ChildSpacePayload) (Space, error)

// Children lists the child spaces registered under parentSpaceId (read from the
// parent ACL's AclChildRegister records).
Children(ctx, parentSpaceId string) ([]ChildRef, error)

// RemoveMemberAsLegalOwner removes target from a child the caller is legalOwner
// of, even without the child's read key. Writes the unsealed AclAccountRemove;
// a key-holder (or the SDK auto-rotation) completes the read-key rotation.
RemoveMemberAsLegalOwner(ctx, childSpaceId, targetIdentity string) error

// DeleteChildAsLegalOwner deletes a child the caller is legalOwner of, with no
// read access required. Overrides deleteRestricted.
DeleteChildAsLegalOwner(ctx, childSpaceId string) error
```

The members collection (`docs/03-space.md`) gains a way to surface the
"rotation pending after legalOwner removal" state so clients (or the SDK's own
rotation worker) can act on it. `SpaceInfo` may gain `ParentSpaceId` /
`LegalOwner` fields and a child/parent filter on `List`.

## Security & abuse

- **Forged parent authority is impossible.** Every authority check at sign time
  reads the **parent's signed ACL**; a member cannot grant itself rights it does
  not hold. The registration record is signed by its author at its real
  permission level.
- **Child-id ↔ parent binding is signed.** `parentSpaceId` is inside the signed
  header (bound into the child id); `parentAclRef` is inside the signed ACL root.
  Neither can be swapped post-hoc.
- **Quota exhaustion / child spam.** Children charge legalOwner's quota and are
  capped by parent settings (max children, creation threshold). A malicious
  admin can burn the org's quota but cannot exceed it; the coordinator is the
  hard ceiling. Mitigation knobs live in parent settings.
- **legalOwner over-reach.** legalOwner can remove members and delete — by
  design. It **cannot** silently read data it was never keyed into: gaining read
  access requires an ACL record (`orgPermission`/join) that is visible to all
  members. The keyless-removal window is the only subtlety and matches normal
  any-sync rotation semantics.
- **Orphaned children on parent delete.** Deleting the parent must define child
  fate (cascade delete? re-parent? detach to standalone?). Open question.

## Open questions

1. **Org space owner identity (deferred from the brief, point 1).** "Who owns
   the org space — maybe a separate identity?" Out of scope for this draft; the
   org space is a normal space whose owner is its creator. A dedicated org
   identity is a future refinement that would make legalOwner = that identity
   cleanly. Revisit before implementation.
2. **Parent settings location.** Child-creation threshold, max-children,
   "members may create children" toggle — `AclSpaceOptions` (signed, owner-only,
   coordinator-readable) vs. a parent settings object. Coordinator must read it
   at sign time, which favors `AclSpaceOptions`.
3. **Editors creating children.** The brief lists "owner/admins/editors"; ACL
   record authorship in any-sync is Admin+. Either (a) keep registration Admin+
   and route editor intent through an admin, or (b) add a "may register child"
   capability grantable to writers/editors via parent settings. Default to (a)
   for v1.
4. **Auto-rotation policy.** Does the SDK auto-perform `AclReadKeyChange` on
   detecting an unsealed legalOwner removal, or expose it for the client to
   trigger? Auto is safer (forward secrecy restored without UI), but needs a
   key-holder online. Lean auto, with a surfaced "pending" state as fallback.
5. **Parent deletion → child fate.** Cascade delete all children, detach them to
   standalone (drop parent link — but it's signed into the id, so this means
   tombstone + re-create), or block parent delete while children exist. Likely
   block-or-cascade; needs a decision.
6. **Multi-level nesting.** Is a child allowed to be a parent (grandchildren)?
   Nothing in the design forbids it, but limit accounting and the registration
   chain need to be defined if so. Default v1: single level (children cannot
   have children), enforced by the coordinator (reject a child whose
   `parentSpaceId` itself has a non-empty `parentSpaceId`).
7. **legalOwner change after root.** Allow `AclLegalOwnerChange` (e.g. org
   ownership transfer must re-target every child's legalOwner)? Cross-space
   fan-out — likely phase 2.
8. **Cross-compartment references / shared objects.** Objects are space-bound, so
   compartments partition data with no overlap. Do we need a cross-space
   reference primitive (link an object in compartment A from compartment B,
   resolvable only by holders of A's read key), or is "put shared data in a
   common ancestor space" sufficient? Out of scope for v1; revisit when a product
   need appears.
9. **External-seat accounting.** Exact shape of `externalSeatsLimit` (per-org
   total vs per-compartment), how an external identity is flagged at child
   member-add, and how the coordinator distinguishes an org-member add (free,
   counts vs member caps) from an external add (counts vs external seats).
10. **Hidden compartments.** v1 leaks compartment existence + membership to all
    org members. If a product needs compartments whose *existence* is hidden from
    non-members, that requires encrypted/redacted registration metadata in the
    parent ACL — a separate feature, not in this draft.
11. **Effective-access query.** Whether the SDK ever offers a server-free
    "everything identity X can reach across this org" helper, or leaves the
    union-of-compartments composition entirely to clients (v1 default: clients).

## Phasing (proposed)

1. **Nested spaces, org-as-legalOwner-with-keys.** `parentSpaceId` header field,
   `AclChildRegister`, `AclRoot.legalOwner`/`parentAclRef`, coordinator
   `SpaceSign` validation (checks 1–6), limit retargeting to legalOwner,
   `CreateChildSpace` / `Children`. Covers the org-creates-child case where org
   holds keys — no keyless path yet. e2e: org creates a child, child charged to
   org quota, member caps enforced, forged-authority rejected.
2. **Keyless legalOwner governance.** Member-created children where org is
   legalOwner-only (no key); unsealed `AclAccountRemove` by legalOwner +
   key-holder rotation (auto + pending-state surfacing); legalOwner
   `SpaceDelete`; `deleteRestricted` override. e2e: member creates child under
   org quota with org keyless; org removes a member, a key-holding admin
   rotates; org deletes the child without ever reading it.
3. **Settings, lifecycle, multi-level (as needed).** Parent-settings toggles
   (Q2/Q3), parent-deletion child fate (Q5), legalOwner change fan-out (Q7),
   optional multi-level (Q6).
4. **Compartments / ACL granularity.** Builds directly on phases 1–2: compartment
   = `CreateChildSpace(orgPermission=None)`, self-governed per-child membership,
   need-to-know (no cascade), existence-visible/content-gated. Mostly a usage
   pattern + docs + an effective-access example; no new core primitive. e2e: org
   with three compartments, a member in two of them reads exactly those, org
   owner governs (removes/deletes) a compartment it cannot read.
5. **External seats.** `externalSeatsLimit`, parent-ACL-controlled external
   admission, coordinator accounting for non-org members of compartments
   (Q9). Paid-collaborator path; layered on top of phase 4.

## Grooming decisions (2026-06-24)

1. **Naming: parent/child / nested spaces at the any-sync level**; "organization"
   is application vocabulary only.
2. **`parentSpaceId` is a first-class signed `SpaceHeader` field**, not embedded
   in the opaque `spaceHeaderPayload` — any-sync handles nested spaces
   differently and must discriminate at the header level. (User correction.)
3. **Limit bucket = legalOwner identity.** Coordinator charges legalOwner's
   `AccountLimits`; creator's personal quota is untouched.
4. **legalOwner = the parent's real owner**, verified by the coordinator at
   child `SpaceSign` against the parent ACL. Registrant must hold Admin+ in the
   parent.
5. **Child registration is self-service for Admin+**, gated only by coordinator
   validation — no per-child human approval record.
6. **Org may opt into data access** by taking a Reader/Writer/Admin role in the
   child (`AclChildRegister.orgPermission` or a later join). When the org
   creates the child itself, it is real owner + legalOwner at once.
7. **Org space owner-as-separate-identity is deferred** (brief point 1); org
   space is a normal space for now.
8. **ACL granularity = compartments (child-space-per-scope).** Reuse nested
   spaces for fine-grained access; effective access = union of compartments,
   composed client-side. No per-object ACL.
9. **Compartments are need-to-know (no access cascade).** Org owner/legalOwner
   governs every compartment but reads none unless explicitly added.
10. **Each compartment self-governs membership**; the org reaches in only via
    legalOwner powers (keyless remove, delete).
11. **Compartment membership ⊆ org membership by default; externals only via a
    paid, parent-ACL-controlled external-seat allowance** enforced by the
    coordinator.
12. **Compartment existence + membership are visible to org members; only data
    is key-gated.** No hidden compartments in v1.
