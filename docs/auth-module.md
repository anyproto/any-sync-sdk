# Auth Module

## Vision
Auth module is a pluggable provider of two private keys that any-sync needs to operate:
1. **Account private key** — identity, signing, encryption. The "who you are".
2. **Device private key** — per-device peer identity (libp2p PeerId). The "which device you are".

The auth module does **not** know about spaces, trees, or sync — it just produces keys. How keys are obtained is an implementation detail (mnemonic derivation, server-side auth, hardware token, etc.).

## What any-sync Requires

any-sync needs exactly:
- **Account PrivKey** (Ed25519) — used for signing changes, deriving tech space, ACL identity
- **Device PrivKey** (Ed25519) — used as libp2p peer identity, unique per installation

Everything else (symmetric keys, space keys, read keys) is derived internally by any-sync from these two.

## Auth Strategies (pluggable)

### Strategy 1: Mnemonic (v1)
1. **BIP-39 mnemonic** (12-24 words) → seed (512 bits via PBKDF2/SHA-512)
2. **SLIP-10** HD derivation at path `m/44'/2046'/[index]'` → `MasterKey`
3. Sub-path `…/0'` → `Identity` key (Ed25519) — this is the **account private key**
4. Optional: **Ethereum** path `m/44'/60'/0'/0/[index]` → ECDSA key for Any Naming Service

Account index semantics: **index 0 is anytype's**, `auth.DefaultAccountIndex` (1) is the `any` product's — one phrase yields a distinct account per product. `FileProviderConfig.Index` applies on wallet creation (fresh generation included) and is pinned in the wallet file thereafter; an existing wallet's stored index always wins, and a restore whose mnemonic+index disagree with the stored wallet fails with `ErrMnemonicMismatch`. Restoring an anytype-derived account means passing index 0 explicitly. The index selects the account identity only — all derived spaces (tech space included) use `any.*` types regardless of index.

### Strategy 2: Server-side auth (future)
Server returns account private key after authentication. Same downstream result.

### Strategy N: Any other
As long as the result is an Ed25519 private key, any-sync doesn't care how it was obtained.

## Device Key
- One per installation, generated on first launch
- Persisted locally (survives app restarts, does not sync)
- Different from account key — two devices on the same account have different peer IDs

## Key Storage
SDK does **not** manage key persistence. The caller is responsible for storing keys or mnemonic in their system's secure storage (OS keychain, encrypted config, etc.) and providing them on every SDK init.

## Key Types (any-sync crypto)
| Type | Algo | Usage |
|------|------|-------|
| `Ed25519PrivKey / PubKey` | Ed25519 | Account identity, signing |
| `AESKey` | AES-256-GCM | Symmetric encryption (internal to any-sync) |
| `X25519` | Curve25519 | DH key exchange (internal to any-sync) |

## Key Encoding (StrKey)
Base58 + CRC-16 checksum with version byte:
- `A…` (0x5b) — account address
- `S…` (0xff) — account seed
- `D…` (0x7d) — device seed
- `N…` (0xd3) — network address

## Source files
- `any-sync/util/crypto/mnemonic.go` — mnemonic generation & seed derivation
- `any-sync/util/crypto/key.go` — Key/PrivKey/PubKey/SymKey interfaces
- `any-sync/util/strkey/` — StrKey encoding
- `any-sync/commonspace/object/accountdata/accountdata.go` — AccountKeys struct
- `any-sync/accountservice/accountservice.go` — Config, account service interface

## Grooming Questions

### Auth Interface
1. What should the auth interface look like? Something like `AuthProvider` with `AccountKey() PrivKey` + `DeviceKey() PrivKey`?
2. Should SDK ship built-in mnemonic provider, or only define the interface and let callers implement?
3. If built-in mnemonic provider — should it also expose `GenerateMnemonic()` as a helper?

### Device Key
4. Who generates the device key — SDK on first launch, or the caller provides it?
5. Where is the device key persisted? SDK manages storage, or caller is responsible?
6. Device key rotation — ever needed? (e.g., app reinstall = new device identity?)

### Abstraction Boundary
7. Does the SDK caller ever need anything beyond `AccountKey()` and `DeviceKey()`? (e.g., MasterKey, EthereumIdentity, account address)
8. Should StrKey encoding (human-readable account/device addresses) be part of the public API?
9. Do we keep the Ethereum key derivation path for SDK v1, or defer it?

### Dependencies
10. Auth runs before any-sync init — if auth fails, SDK doesn't start. No runtime swapping. Anything else to consider?
