# Auth Module

Package `auth` supplies the two private keys any-sync runs on. It knows
nothing about spaces, trees or sync.

```go
type Provider interface {
    AccountKey(ctx context.Context) ([]byte, error)
    DeviceKey(ctx context.Context) ([]byte, error)
}
```

`sdk.Open(ctx, cfg, provider)` decodes both keys before starting
any-sync; a provider error or an undecodable key fails `Open`. Keys are
raw Ed25519 private keys in any-sync's `PrivKey.Raw()` form.

| Key | Identity it yields | Used for |
|-----|--------------------|----------|
| Account key | account id (`A…`) | signing changes, ACL identity, deriving the tech space |
| Device key | peer id | transport identity, one per installation |

Two devices of one account share the account key and have different
device keys, so they have different peer ids. Every other key any-sync
uses (space read keys, symmetric keys) is derived from these two or
encrypted to the account key.

How a provider obtains the keys is its own concern: mnemonic
derivation, server-side auth or a hardware token all work as long as
the result is an Ed25519 key.

## Mnemonic derivation

1. BIP-39 mnemonic (12 words when generated) → 512-bit seed.
2. SLIP-10 master node at `m/44'/2046'/[index]'`.
3. Account key at `m/44'/2046'/[index]'/0'`.

The index selects the account: index 0 is anytype's,
`auth.DefaultAccountIndex` (1) is the `any` product's, so one phrase
yields a distinct account per product. Restoring an anytype-derived
account means passing index 0 explicitly. The index changes only the
account identity; every space the SDK derives, the tech space included,
uses `any.*` types regardless.

## Built-in providers

### `FileProvider`

`auth.NewFileProvider(FileProviderConfig{Path, Passkey, Mnemonic, Index})`
keeps a JSON wallet file holding the mnemonic, the device key and the
account index.

- **No file at `Path`:** creates one. The mnemonic is `Mnemonic` when
  set, otherwise freshly generated; the device key is always fresh;
  `Index` is pinned in the file. `Created()` reports true so the caller
  can show `Mnemonic()` once for backup.
- **File exists:** loads it; the stored index wins. A set `Mnemonic`
  that differs from the stored phrase, or matches it at a different
  `Index`, fails with `ErrMnemonicMismatch`.
- **Passkey:** when set, the payload is encrypted with AES-256-GCM
  keyed by PBKDF2-HMAC-SHA256 (600k iterations, 16-byte salt). Opening
  an encrypted wallet without a passkey returns `ErrPasskeyRequired`;
  a wrong passkey or corrupted file returns `ErrWrongPasskey`.
- An invalid phrase returns `ErrInvalidMnemonic`.

Restoring from a mnemonic on a new installation produces a new device
key, and therefore a new device of the same account.

### `MnemonicProvider`

`auth.NewMnemonicProvider(MnemonicConfig{Mnemonic, Index, DeviceKey})`
derives the account key from the phrase and returns `DeviceKey` as
given. The caller owns device-key storage: generate it once with
`auth.GenerateDeviceKey`, keep it in secure storage, pass it on every
start.

### Helpers

- `auth.GenerateMnemonic()` returns a fresh 12-word phrase.
- `auth.AccountId(mnemonic, index)` returns the `A…` account id without
  touching disk or starting the SDK; it equals `SDK.Account().Id()` for
  the same wallet. Callers use it to address per-account storage before
  a wallet exists.

## Key encoding (StrKey)

Base58 with a version byte and CRC-16 checksum:

| Prefix | Version byte | Meaning |
|--------|--------------|---------|
| `A…` | `0x5b` | account address |
| `S…` | `0xff` | account seed |
| `D…` | `0x7d` | device seed |
| `N…` | `0xd3` | network address |

## any-sync sources

- `util/crypto/mnemonic.go` — mnemonic generation and key derivation
- `util/crypto/key.go` — `PrivKey` / `PubKey` / `SymKey`
- `util/strkey/` — StrKey encoding
- `commonspace/object/accountdata/accountdata.go` — `AccountKeys`
