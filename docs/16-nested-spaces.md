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
4. **legalOwner = the parent's current owner — hybrid: derived where
   infrastructure enforces, stored where clients validate.** For powers the
   coordinator enforces anyway (child deletion, quota attribution, SpaceSign
   validation) legalOwner is resolved **fresh** from the parent ACL head at
   decision time. For powers exercised as child-ACL records that every client
   must validate (keyless removal) the child ACL pins the legalOwner **public
   key** at genesis and advances it via `AclLegalOwnerUpdate`, a record
   carrying a signature-induction proof (below). A parent ownership transfer
   (`AclOwnershipChange` — shipped in any-sync v0.13) is honored by the
   coordinator immediately and by each child lazily; the window is
   fail-closed (see the authority model). When the org itself creates the
   child, the org is **real owner and legalOwner simultaneously** (and may
   join the child's ACL as reader/writer/admin to gain real data access).

## The flow (end to end)

```
member of orgSpace (Admin+)                    coordinator                 nodes
        │                                            │                       │
 1. create child header                              │                       │
    SpaceHeader{ parentSpaceId = orgId,              │                       │
                 identity = me, ... }  (signed by me)│                       │
    + child AclRoot{ parentRef = <set in step 2>,    │                       │
                     legalOwner = orgOwner pubkey }  │                       │
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
        │                                            │     of parent (fresh)  │
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
  types (child-register, keyless remove) are added variants, the
  established extension pattern.
- **`AclRoot` carries space-scoped extras already** (`oneToOneInfo`, `options`):
  a `parentRef` field on `AclRoot` follows that precedent.
- **`AclReadKeyChange`** record + read-key rotation: the existing mechanism the
  key-regeneration flow (below) reuses verbatim — we do not invent rotation.
- **`AclAccountRemove`** record: legalOwner removal reuses this record type with
  a relaxed authorization rule (below).
- **`AclSpaceOptions.deleteRestricted`** + coordinator `SpaceDelete`: the delete
  path legalOwner deletion plugs into.
- **`AclSpaceOptionsChange`** (owner-only record, shipped in v0.13): the update
  mechanism the new parent-settings toggles ride on — no new settings machinery.
- **`AclOwnershipChange`** (shipped in v0.13): parent ownership transfer is a
  real record; the hybrid legalOwner rule below consumes it directly — the
  coordinator observes it immediately, and its raw signed payload doubles as
  the offline proof inside `AclLegalOwnerUpdate`.
- **Coordinator `aclService.OwnerPubKey(spaceId)`** (any-sync-coordinator):
  resolves any space's current owner from live ACL state; already used in
  production to track ownership transfers. The derivation half of the hybrid
  is a call to an existing API.

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
     SpaceFileProtoVersion fileprotoVersion = 9;  // taken by files v2
     string parentSpaceId = 10;  // NEW — empty for top-level spaces
     SpaceHeaderVersion version = 100;
   }
   ```
   The field is part of the signed header, so the parent link is immutable and
   bound into the child's id. Empty ⇒ ordinary top-level space (full backward
   compat). (A `SpaceNesting` sub-message can be used instead if more nesting
   metadata is needed later; one string is enough for v1.)

2. **`AclRoot` gains `parentRef` + a pinned legalOwner public key.**
   ```proto
   message AclRoot {
     // ... existing fields 1..11 (through oneToOneInfo=10, options=11) ...
     AclParentRef parentRef = 12; // pointer to the registration record in the parent ACL
     bytes legalOwner = 13;       // pubkey of the parent's owner AT GENESIS; advanced via AclLegalOwnerUpdate
   }
   message AclParentRef {
     string parentSpaceId   = 1; // == SpaceHeader.parentSpaceId (redundant, binds the ACL to the header)
     string parentAclRecordId = 2; // the AclChildRegister record id in the parent ACL
   }
   ```
   The stored key serves **client-side validation only** — it is the trust
   anchor of the signature-induction chain (change 3), letting any child
   member (including external seats, which never replicate the parent)
   validate legalOwner-authored records against the child's own causal
   timeline, with no cross-ACL lookup. The coordinator never trusts it: at
   SpaceSign it verifies the pinned key equals the parent's *current* owner
   (fresh read), and every later infra decision re-derives. For a top-level
   space both fields are empty and everything behaves as today.

3. **New ACL record types** (`AclContentValue` oneof — variants 1..16 are
   taken as of v0.13 (`accountsAdd`, `inviteJoin`, `ownershipChange`,
   `spaceOptionsChange`, …); new ones land at 17+):
   ```proto
   // appended to the PARENT (org) ACL — registers a child under it
   message AclChildRegister {
     string childSpaceId    = 1;
     bytes  childAclRootCid = 2;   // binds the registration to a specific child ACL root
     AclUserPermissions orgPermission = 3; // permission the org grants ITSELF in the child, if any (None = no data access)
   }
   message AclChildRegisterRevoke { string childSpaceId = 1; } // optional: de-list a child

   // authored in the CHILD by the parent's NEW owner after an ownership
   // transfer — advances the child's stored legalOwner by signature induction
   message AclLegalOwnerUpdate {
     repeated bytes ownershipChanges = 1; // raw signed AclOwnershipChange record(s) from the parent, in order
   }
   ```
   The register records are authored in the **parent**; `AclLegalOwnerUpdate`
   in the **child**. Validating the update is pure **signature induction**:
   the first embedded parent `AclOwnershipChange` must be signed by the
   child's currently stored legalOwner key; each subsequent one by the owner
   the previous one named; the last one names the new stored legalOwner.
   Intermediate parent history (rotations, invites, joins) never alters
   signing authority, so it is skipped entirely — the proof is O(number of
   transfers), ~300 bytes each, verifiable fully offline. Replay guard: the
   child remembers the CIDs of consumed `AclOwnershipChange` payloads (the
   CID is recomputable from the embedded bytes) and rejects re-use, so cycled
   ownership (A→B→A) cannot be replayed by a malicious ex-owner. Updates are
   **lazy**: the new parent owner pushes one only when it actually needs to
   exercise a keyless power in that child.

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
   `SpaceLimitReached` / `Forbidden`. (Verified 2026-07-06: limits are
   identity-keyed in the coordinator's store and the key is swappable, but
   there is **no sign-time `sharedSpacesLimit` check today** — it runs only in
   the make-shareable path. The child-sign limit check is net-new logic, not
   a retarget of an existing one.)

6. **Coordinator `SpaceDelete` accepts a legalOwner-signed deletion** for a
   child, in addition to the owner-signed path. (See "legalOwner delete" below.)

7. **Child placement: inherit the parent's replicationKey.** Node placement
   consistent-hashes only the replicationKey suffix of the spaceId
   (`nodeconf.ReplKey`), and nothing enforces key uniqueness (1-1 spaces
   already share deterministically derived keys) — so a child that copies its
   parent's `ReplicationKey` into its header co-locates on the parent's node
   partition with **zero routing changes** (verified). Chosen to minimize the
   number of node connections a client needs to sync an org: one partition
   serves the parent and all its compartments. Validation does NOT depend on
   this — nodes skip ACL content validation entirely (acceptor-trust), the
   coordinator has global ACL access, and clients validate via the stored key
   + signature induction. Accepted costs, to monitor: (a) a very large org
   concentrates its whole load on one partition — a hard-to-rebalance hotspot
   if a whale outgrows it; (b) all of an org's spaceIds share a visible
   suffix, making org membership/size publicly linkable. If either bites,
   later children can switch to independent keys without breaking anything —
   co-location is an optimization, never a correctness dependency.

> The coordinator lives in the separate `any-sync-coordinator` repo; only the
> proto (in `any-sync`) is visible here. All server-side validation/accounting
> work is a coordinator-repo task, tracked from this doc.

### Feasibility spot-checks (2026-07-06, against the live repos)

- **Header compat:** verification runs over the raw stored header bytes (never
  re-marshals) and vtproto preserves unknown fields — `parentSpaceId = 10` is
  fully backward-compatible on the wire; old peers store and forward it inert.
- **Placement:** `nodeconf` routes purely on the replicationKey suffix of the
  spaceId, with no uniqueness enforcement — replicationKey inheritance
  (change 7) needs zero routing code.
- **Where ACL content validation actually runs:** exactly two places — the
  coordinator submit path (full validator + a post-validation hook, with
  `OwnerPubKey(anySpaceId)` available) and clients. Sync nodes skip content
  validation and verify only the consensus acceptor signature; consensus
  nodes are head-CAS log stores with no ACL state machine at all. The
  keyless-removal validation therefore only needs the coordinator (has
  cross-ACL access) and clients (self-contained via the stored key).
- **Coordinator:** `SpaceSign` has a clean insertion point after header
  verification; the modern `SpaceDelete` path authorizes via an identity
  predicate (localized change to accept the parent's owner); limits are
  identity-keyed and the billed identity is swappable — but a sign-time
  `sharedSpacesLimit` check is net-new (today it runs only at
  make-shareable).

## Coordinator validation at child `SpaceSign`

This is the trust gate; nothing else enforces the parent's authority. On
`SpaceSign(childId, header, parentAclRecordId)` the coordinator MUST verify, in
addition to the normal space-sign checks:

1. **Parent exists & is healthy.** `header.parentSpaceId` resolves to a real,
   non-deleted space the coordinator knows.
2. **legalOwner check.** The coordinator resolves the current owner identity
   of `parentSpaceId` **fresh** from the parent ACL head (bypassing any owner
   cache) and verifies the child `AclRoot.legalOwner` pins exactly that key.
   (Verified: the coordinator's `aclService.OwnerPubKey(spaceId)` already
   exists and is used in production to track ownership transfers.)
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

legalOwner ≙ the parent's current owner, tracked through a **hybrid** split by
trust domain. Powers enforced by infrastructure (delete, quota, sign) are
checked by the coordinator against a **fresh derivation** from the parent ACL
head. Powers exercised as child-ACL records that clients must validate (the
keyless removal) are checked against the **stored legalOwner key** in the
child ACL, advanced by `AclLegalOwnerUpdate`. The window between a parent
ownership transfer and a child's update is **fail-closed**: the ex-owner still
passes client validation but the coordinator (fresh derivation) rejects her
records at submit; the new owner passes the coordinator but must first prove
himself to clients via the update record. Neither can act unilaterally. Its
powers:

| Power | Holds child readKey? | Mechanism |
|---|---|---|
| Bears all limits | not required | coordinator charges legalOwner quota |
| Remove a member | not required | authors `AclAccountRemoveNoRotate`; key rotation deferred to a key-holder (below) |
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

This is the **one hard part** of the whole feature — verified against any-sync.
Today `AclAccountRemove` is *welded* to a read-key rotation at three layers:
its proto carries a non-optional `readKeyChange`
(`aclrecordproto`: `AclAccountRemove.readKeyChange`); `applyAccountRemove`
unconditionally calls `applyReadKeyChange`, which dereferences the new key
(`list/aclstate.go` — a nil rotation **panics**); and `ValidateAccountRemove`
requires the new key re-encrypted for *every* remaining member
(`list/validator.go`). A keyless legalOwner can satisfy none of that. So the
removal must be a **new, separate record type**, never a nil-`readKeyChange`
`AclAccountRemove` (which would crash older clients).

Design — a two-phase split:

1. **Phase A — keyless removal (legalOwner).** legalOwner authors a **new**
   `AclAccountRemoveNoRotate { identities }` record — removal with **no** read
   key. The ACL + coordinator honor the membership change at once: the removed
   member drops to `None`, loses write acceptance and limit attribution
   immediately. This needs three additions in any-sync:
   - the new record variant in the `AclContentValue` oneof (additive; old
     clients hit the tolerant unknown-content default and skip it, so they
     don't crash — but they also keep treating the member as present until
     phase B lands, which is the inherent window below);
   - a **keyless-authorization track** in `AclState`, populated from the
     child's stored legalOwner key (`AclRoot.legalOwner` + any
     `AclLegalOwnerUpdate` records) — fully self-contained, no cross-ACL
     lookup — so the validator admits a legalOwner-authored removal even
     though `Permissions(legalOwner)` is `None` (it is not a normal member);
   - a **"removed-but-not-yet-rotated"** state marker the next phase keys on.
2. **Phase B — a key-holder completes the cut-off.** The space now sits in the
   observable *removed-but-not-rotated* state. A remaining member who **holds the
   read key** authors a standard `AclReadKeyChange`, re-encrypting a fresh read
   key for the reduced set. This composes cleanly with existing code: the removed
   account is already `None`, so the standard rotation naturally excludes it —
   **no change to the rotation record itself.** Who may author it:
   - **Admins/Owner: already allowed, zero any-sync change.** Standalone
     `AclReadKeyChange` is gated by `CanManageAccounts()` today
     (`list/aclrecordbuilder.go` build path + `list/validator.go` validate path),
     true for Admin/Owner. So "an admin observes the keyless removal and rotates"
     works out of the box. **This is the default rotation responsibility.**
   - **Editors (Writers): crypto already works; only the permission predicate
     blocks them.** Writers *do* hold the read key (every rotation re-encrypts to
     all non-`None` accounts, Writers included), so an editor can produce a valid
     rotation — they are blocked solely by the `CanManageAccounts()` check.
     Admitting them is a **scoped predicate relaxation** (also accept
     `CanWrite()`) that **must be gated on "this rotation completes a pending
     keyless removal"** — otherwise any editor could force-rotate the read key at
     will (privilege-escalation / DoS). Treat editor-rotation as an opt-in for
     orgs whose compartments may have no admin online, not the default.
3. **Window semantics.** Between phase A and B the removed member keeps the *old*
   read key and can still decrypt data encrypted under it — exactly the normal
   any-sync property (rotation only protects *future* writes; it never reaches
   back). Membership/limits are correct immediately; forward secrecy is restored
   when a key-holder rotates. The SDK surfaces the "rotation pending" state so a
   client (or the SDK's own worker) can act.
4. **No key-holder left.** If every key-holding member is gone and only the
   keyless legalOwner remains, no one can rotate or read. legalOwner's remaining
   lever is **delete** (below) — the recovery escape hatch.

**Effort (verified):** phase A (`AclAccountRemoveNoRotate` + keyless-auth track +
pending-rotation state) is **L** — bounded surgery, but it breaks the
removal↔rotation coupling, a security-critical invariant, so it is the real cost
of the feature. `AclLegalOwnerUpdate` (signature-induction validation +
consumed-CID replay guard) adds **S–M** on top, and everything stays inside
the child's own validator — no cross-ACL plumbing (verified: the core content
validator has no external-context hook, and with the stored key it doesn't
need one). Phase B is **free for admins**, **S–M for editors** (predicate
relaxation + the pending-removal guard). The rotation record itself is
unchanged.

Open: whether the SDK auto-rotates on detecting a pending keyless removal
(preferred — restores forward secrecy without UI, needs a key-holder online) or
requires an explicit client call. See open questions.

### legalOwner delete

legalOwner can delete a child it cannot read. Coordinator `SpaceDelete` is
extended to accept a deletion signed by the child's legalOwner (the coordinator
resolves the child's `parentSpaceId` and checks the signer is the parent's
current owner), not only the owner. Interplay with
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
   run its membership via **direct-add invites** (`ACL().AddAccounts`,
   `docs/15-direct-add-invites.md`) as the primary path — an org colleague is
   added by identity, one ACL record, approval is the receiver's local
   materialization gate, no token round-trip. Token invites and join requests
   (`docs/03-space.md`) remain available for edge flows. Org admins who are
   not members of a compartment cannot
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
   a member who is not in the parent ACL — gating **every** admission path:
   join-request accepts, direct-add `AclAccountsAdd` batches, and invite-joins.
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
- **Compartment membership** = **direct-add** (`ACL().AddAccounts`,
  `docs/15-direct-add-invites.md`) as the primary path — org members added by
  identity, local-gate approval — operated on the child by the child's own
  admins; the full token-invite/join-request surface (`docs/03-space.md`)
  stays available.
- **`Children(orgId)`** lists compartments (registration records); a member sees
  all compartment ids + member lists, content only for the ones they hold a key
  to.
- **External seats** (phase later): an org-settings allowance + a member-add that
  flags an external (non-org) identity, gated by the coordinator against
  `externalSeatsLimit`. Discovery for externals ("which compartments am I in")
  is a dedicated **coordinator endpoint** backed by its limits/billing data —
  externals never read the parent ACL. The coordinator gains only DoS power
  here: a forged entry fails the client's own ACL validation of the
  compartment, and an omitted one is indistinguishable from a network failure.

Effective-access composition ("show me everything member X can reach across the
org") is a **client concern** over `Children` + per-compartment membership, not
an SDK method in v1.

## SDK surface (sketch — names TODO)

```go
// CreateChildSpace creates a space nested under parentSpaceId. The caller must
// hold Admin+ (or the parent-configured threshold) in the parent. Performs the
// full flow: build child header with parentSpaceId, append the AclChildRegister
// record to the parent ACL, request the coordinator SpaceSign, push. legalOwner
// is implicit — always the parent's current owner, never passed or stored.
// orgPermission grants the org itself a role in the child (None = org gets no
// data access / keyless governance).
CreateChildSpace(ctx, parentSpaceId string, p ChildSpacePayload) (Space, error)

// Children lists the child spaces registered under parentSpaceId (read from the
// parent ACL's AclChildRegister records).
Children(ctx, parentSpaceId string) ([]ChildRef, error)

// RemoveMemberAsLegalOwner removes target from a child the caller is legalOwner
// of, even without the child's read key. Writes the AclAccountRemoveNoRotate
// record; a key-holder (or the SDK auto-rotation) completes the read-key
// rotation.
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
  header (bound into the child id); `parentRef` is inside the signed ACL root.
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
2. **Parent settings location — RESOLVED (2026-07-06): split by kind.**
   Behavioral toggles ("members may create children", child-creation
   permission threshold) go into **`AclSpaceOptions`** as new fields, changed
   via the shipped owner-only `AclSpaceOptionsChange` — signed, in the parent
   ACL the coordinator already reads at sign time. Numeric limits (max
   children, member caps, external seats) are **coordinator-side state** keyed
   to the legalOwner's `AccountLimits` — the coordinator validates every space
   push against the network and owns current limits, so quotas live where they
   are enforced, not in the ACL.
3. **Editors creating children.** The brief lists "owner/admins/editors"; ACL
   record authorship in any-sync is Admin+. Either (a) keep registration Admin+
   and route editor intent through an admin, or (b) add a "may register child"
   capability grantable to writers/editors via parent settings. Default to (a)
   for v1.
4. **Auto-rotation policy & responsibility.** Resolved on responsibility:
   **admins/owner are the default rotators** (no any-sync change), with
   **editor-rotation an opt-in behind a "completes a pending keyless removal"
   guard** (predicate relaxation, S–M). Still open: does the SDK
   **auto**-perform the `AclReadKeyChange` on detecting a pending keyless removal,
   or expose it for the client to trigger? Auto is safer (forward secrecy
   restored without UI) but needs a key-holder online. Lean auto, with a surfaced
   "rotation pending" state as fallback.
5. **Parent deletion → child fate.** Cascade delete all children, detach them to
   standalone (drop parent link — but it's signed into the id, so this means
   tombstone + re-create), or block parent delete while children exist. Likely
   block-or-cascade; needs a decision.
6. **Multi-level nesting.** Is a child allowed to be a parent (grandchildren)?
   Nothing in the design forbids it, but limit accounting and the registration
   chain need to be defined if so. Default v1: single level (children cannot
   have children), enforced by the coordinator (reject a child whose
   `parentSpaceId` itself has a non-empty `parentSpaceId`).
7. **legalOwner change after root — RESOLVED (2026-07-06): hybrid.** The
   coordinator always derives fresh, so infra-enforced powers re-target the
   moment the parent's `AclOwnershipChange` lands. The child's stored key is
   advanced lazily by `AclLegalOwnerUpdate` with a signature-induction proof;
   the interim window is fail-closed (authority model). No proactive fan-out
   is ever required.
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
12. **Cross-ACL resolution — RESOLVED (2026-07-06) by the hybrid model.**
    Clients (external seats included) validate legalOwner-authored records
    against the child's stored key + `AclLegalOwnerUpdate` signature
    induction — no parent replication needed. Nodes were never a problem:
    they skip ACL content validation and trust acceptor signatures
    (verified). The coordinator derives fresh. replicationKey inheritance
    (required change 7) co-locates the org on one partition purely as a
    client connection-count optimization, never a validation dependency.

## Phasing (proposed)

1. **Nested spaces, org-as-legalOwner-with-keys.** `parentSpaceId` header field,
   `AclChildRegister`, `AclRoot.parentRef`, coordinator
   `SpaceSign` validation (checks 1–6), limit retargeting to legalOwner,
   `CreateChildSpace` / `Children`. Covers the org-creates-child case where org
   holds keys — no keyless path yet. e2e: org creates a child, child charged to
   org quota, member caps enforced, forged-authority rejected.
2. **Keyless legalOwner governance.** Member-created children where org is
   legalOwner-only (no key); `AclAccountRemoveNoRotate` by legalOwner +
   key-holder rotation (auto + pending-state surfacing); `AclLegalOwnerUpdate`
   with signature induction + replay guard; legalOwner
   `SpaceDelete`; `deleteRestricted` override. e2e: member creates child under
   org quota with org keyless; org removes a member, a key-holding admin
   rotates; org deletes the child without ever reading it.
3. **Settings, lifecycle, multi-level (as needed).** Parent-settings toggles
   (per resolved Q2: `AclSpaceOptions` fields + coordinator-side limits; Q3),
   parent-deletion child fate (Q5), optional multi-level (Q6). (Q7 is resolved
   by derivation — no fan-out phase needed.)
4. **Compartments / ACL granularity.** Builds directly on phases 1–2: compartment
   = `CreateChildSpace(orgPermission=None)`, self-governed per-child membership
   via direct-add invites (`docs/15-direct-add-invites.md`),
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
   parent. *(Refined by decision 14: hybrid — coordinator derives fresh,
   clients validate the child's stored key.)*
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
13. **Keyless removal = a new `AclAccountRemoveNoRotate` record** (never a
    nil-`readKeyChange` `AclAccountRemove` — that crashes old clients), plus a
    keyless-auth track in `AclState` and a pending-rotation marker. **Rotation is
    completed by a key-holder: admins/owner by default (no any-sync change),
    editors opt-in behind a "completes a pending keyless removal" guard.**
    (Feasibility-verified against any-sync, 2026-06-25.)

## Grooming decisions (2026-07-06 — re-groom against v0.13 + feasibility review)

14. **legalOwner is hybrid: coordinator derives fresh, clients validate a
    stored key.** Infra-enforced powers (delete, quota, sign) always resolve
    the parent's current owner live (`OwnerPubKey` exists). Client-validated
    powers (keyless removal) check the child's pinned
    `AclRoot.legalOwner` key, advanced by `AclLegalOwnerUpdate` — a record
    embedding the raw signed parent `AclOwnershipChange` payload(s), verified
    by signature induction from the pinned key (O(transfers), ~300 B each,
    offline-verifiable, consumed-CID replay guard), pushed lazily. The
    transfer window is fail-closed: ex-owner passes clients but the
    coordinator rejects; new owner passes the coordinator but must prove
    itself to clients first. (Supersedes the same-day pure-derivation
    decision after the feasibility review + external design consult: pure
    derivation had no workable client-side story for external seats, and pure
    storage would need proactive fan-out; hash-chain proofs were rejected —
    content-addressed links need full record preimages, megabytes for an old
    org.)
15. **Parent settings split by kind.** Behavioral toggles →
    `AclSpaceOptions` fields (changed via the shipped, owner-only
    `AclSpaceOptionsChange`); numeric limits (max children, member caps,
    external seats) → coordinator-side, keyed to the legalOwner's
    `AccountLimits`. The coordinator validates every push to the network and
    owns current limits — quotas live where they are enforced.
16. **Direct-add invites (`docs/15-direct-add-invites.md`) are the primary
    compartment membership path.** Compartment admins add org colleagues by
    identity via `ACL().AddAccounts`; approval is the receiver's local gate.
    The external-seat coordinator check gates every admission path,
    `AclAccountsAdd` batches included.
17. **Proto drift fixed against v0.13:** `parentSpaceId` is `SpaceHeader`
    field **10** (9 is now `fileprotoVersion`); new `AclContentValue` variants
    start at **17** (11–16 landed: `accountsAdd`, `inviteJoin`,
    `inviteChange`, `ownershipChange`, `spaceOptionsChange`, …); doc
    renumbered `14-` → `16-` (`14-identities.md`, `15-direct-add-invites.md`
    now exist).
18. **Children inherit the parent's replicationKey.** Verified trivial (node
    placement hashes only the spaceId suffix, uniqueness unenforced). Chosen
    to minimize client connection count: one node partition serves the whole
    org. Accepted, monitored costs: whale-org partition hotspot + public
    linkability of an org's spaceIds. Escape hatch: co-location is never a
    correctness dependency, so later children can switch to independent keys.
19. **External-seat compartment discovery = coordinator endpoint** backed by
    limits/billing data; externals never read the parent ACL. Infra gains
    only DoS power — forged entries fail client-side ACL validation.
20. **Feasibility verified across the four repos (2026-07-06).** Coordinator:
    `OwnerPubKey` exists, `SpaceSign` has a clean insertion point, sign-time
    `sharedSpacesLimit` is net-new, `SpaceDelete` auth is a localized
    predicate change. any-sync: header field is wire-compatible (raw-bytes
    verification), `AclRoot` 12/13 free, removal↔rotation weld confirmed.
    Nodes: sync nodes skip ACL content validation (acceptor-trust), consensus
    nodes have no ACL state machine — the keyless-removal check only needs
    the coordinator + clients.
