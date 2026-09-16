package techspace

import (
	"context"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// The identities collection — one account-global directory of every
// account identity this account has encountered, keyed by the account
// address. One row mixes two sync classes on purpose (see docs/one-to-one-spaces.md):
//
//   - symKey  (ScopeSynced)  — the contact's metadata symkey, the secret
//     a device needs to decrypt their identityRepo profile. It MUST cross
//     devices, so it rides the tech-space DAG.
//   - name / description / iconCID / spaceIds (ScopeLocal) — derived,
//     device-local cache: the last resolved profile and the set of spaces
//     where we've seen this identity. Persistent on disk but never synced
//     — any device re-derives these from the synced symkey + identityRepo
//     + its own membership, so syncing them would only add staleness.
//
// "Sync the key, derive the value." Exposed to consumers via the public
// Identities API.

const (
	// IdentitiesDataset name on the space-index tree. Piggybacks on the
	// space-index tree like ProfileDataset — one extra handler reg.
	IdentitiesDataset = "identities"

	// IdentitiesHandlerVersion stamped on every change.
	IdentitiesHandlerVersion = "identitiesHandler-v1"

	// FieldIdentitySymKey holds the contact's metadata symkey (synced).
	FieldIdentitySymKey = "symKey"
	// FieldIdentityName / Description / Icon hold the cached identityRepo
	// profile (device-local).
	FieldIdentityName        = "name"
	FieldIdentityDescription = "description"
	FieldIdentityIcon        = "iconCID"
	// FieldIdentitySpaceIds is the set of spaces where we've seen this
	// identity (device-local array of space ids).
	FieldIdentitySpaceIds = "spaceIds"
)

// IdentitiesSchema declares the synced symkey field plus the device-local
// profile/sightings cache.
func IdentitiesSchema() schema.Dataset {
	str := func() *schema.Schema { return schema.Leaf(schema.KindString) }
	return schema.Dataset{Fields: []schema.Field{
		{Id: FieldIdentitySymKey, Name: "Sym key", Schema: str(), Scope: schema.ScopeSynced,
			Description: "Key that decrypts the contact's published profile."},
		{Id: FieldIdentityName, Name: "Name", Schema: str(), Scope: schema.ScopeLocal,
			Description: "Cached profile name; empty until the key arrives.", XFormat: map[string]any{"type": "text"}},
		{Id: FieldIdentityDescription, Name: "Description", Schema: str(), Scope: schema.ScopeLocal,
			Description: "Cached profile description.", XFormat: map[string]any{"type": "longtext"}},
		{Id: FieldIdentityIcon, Name: "Icon", Schema: str(), Scope: schema.ScopeLocal,
			Description: "Cached profile icon CID."},
		{Id: FieldIdentitySpaceIds, Name: "Space ids", Schema: &schema.Schema{Kind: schema.KindArray, Items: str()}, Scope: schema.ScopeLocal,
			Description: "Spaces this identity was seen in on this device."},
	}}
}

// IdentityRecord is the typed view of one identities row.
type IdentityRecord struct {
	Identity    string
	SymKey      string
	Name        string
	Description string
	IconCID     string
	SpaceIds    []string
}

// IdentitiesIndexes backs by-space queries ("which identities are in
// space X") — an array (multi-key) index on the spaceIds set, used by
// RemoveSpaceFromIdentities and consumer queries.
func IdentitiesIndexes() []anystore.IndexInfo {
	return []anystore.IndexInfo{
		{Name: "idx_identity_spaceIds", Fields: []string{FieldIdentitySpaceIds}},
	}
}

// IdentitiesHandler validates ops on the identities dataset: any
// non-empty id (the account address), no deletes (the row is a durable
// directory entry; profile/sightings are overwritten in place).
type IdentitiesHandler struct{}

func (IdentitiesHandler) Init(_ context.Context) error { return nil }

func (IdentitiesHandler) BeforeCreate(_ *crdt.ChangeCtx, rec *crdt.RecordChange, _ *crdt.Sink) error {
	if rec.Id == "" {
		return crdt.ErrValidation
	}
	return nil
}

func (IdentitiesHandler) BeforeModify(_ *crdt.ChangeCtx, rec *crdt.RecordChange, _ *crdt.Op, _ *crdt.Sink) error {
	if rec.Id == "" {
		return crdt.ErrValidation
	}
	return nil
}

func (IdentitiesHandler) BeforeDelete(_ *crdt.ChangeCtx, _ *crdt.RecordChange, _ *crdt.Sink) error {
	return crdt.ErrValidation
}

// DecodeIdentityRecord lifts an anyenc value (as returned by
// Controller.Get / Records) into an IdentityRecord. The row id (the
// account identity) is read from the CRDT-stamped "id" field.
func DecodeIdentityRecord(v *anyenc.Value) IdentityRecord {
	if v == nil {
		return IdentityRecord{}
	}
	r := IdentityRecord{Identity: v.GetString("id")}
	r.SymKey = v.GetString(FieldIdentitySymKey)
	r.Name = v.GetString(FieldIdentityName)
	r.Description = v.GetString(FieldIdentityDescription)
	r.IconCID = v.GetString(FieldIdentityIcon)
	if arr := v.GetArray(FieldIdentitySpaceIds); len(arr) > 0 {
		r.SpaceIds = make([]string, 0, len(arr))
		for _, e := range arr {
			if s := e.GetStringBytes(); len(s) > 0 {
				r.SpaceIds = append(r.SpaceIds, string(s))
			}
		}
	}
	return r
}
