package spaceimpl

import (
	"context"
	"fmt"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/subscribe"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

// identitiesAPI implements space.IdentitiesAPI over the tech-space
// `identities` directory dataset. Account-global, device-local reads; the
// Subscribe stream reuses the same SubEngine as the space list.
type identitiesAPI struct {
	tsp    *techspace.Service
	parent *Service
}

// NewIdentitiesAPI builds the account-global identities directory surface
// over the tech space; parent supplies the byte layer behind OpenIcon.
func NewIdentitiesAPI(tsp *techspace.Service, parent *Service) space.IdentitiesAPI {
	return &identitiesAPI{tsp: tsp, parent: parent}
}

// OpenIcon opens the avatar the identity's cached profile names. See
// space.IdentitiesAPI. The account's own avatar is a tech-space file
// read through the member path; a contact's is fetched by (space,
// rootCid) and decrypted with the key from the profile, under a ref
// named after the identity so the cached bytes outlive any one read.
func (a *identitiesAPI) OpenIcon(ctx context.Context, identity string) (space.FileReader, error) {
	if identity == "" {
		return nil, fmt.Errorf("identities: identity required: %w", space.ErrNotFound)
	}
	if keys := a.parent.app.AccountKeys(); keys != nil && identity == keys.SignKey.GetPublic().Account() {
		rec, ok := a.tsp.GetProfile(ctx)
		if !ok || rec.IconFileId == "" {
			return nil, fmt.Errorf("identities: no avatar: %w", space.ErrNotFound)
		}
		return a.parent.OpenTechFile(ctx, rec.IconFileId)
	}
	r, ok := a.tsp.GetIdentity(ctx, identity)
	if !ok {
		return nil, fmt.Errorf("identities: %s: %w", identity, space.ErrNotFound)
	}
	return a.parent.openIdentityIcon(ctx, identity, r.IdentityProfile)
}

func (a *identitiesAPI) List(ctx context.Context) ([]space.IdentityInfo, error) {
	recs := a.tsp.ListIdentities(ctx)
	out := make([]space.IdentityInfo, 0, len(recs))
	for _, r := range recs {
		out = append(out, identityInfo(r))
	}
	return out, nil
}

func (a *identitiesAPI) Get(ctx context.Context, identity string) (space.IdentityInfo, bool, error) {
	r, ok := a.tsp.GetIdentity(ctx, identity)
	if !ok {
		return space.IdentityInfo{}, false, nil
	}
	return identityInfo(r), true, nil
}

func (a *identitiesAPI) Subscribe(cb func(space.IdentityListEvent)) (cancel func()) {
	sub, err := a.tsp.SubEngine().Subscribe(subscribe.SubConfig{
		Scope: subscribe.Scope{ObjectId: a.tsp.IndexObjectId(), Dataset: techspace.IdentitiesDataset},
	}, func(func(id string, doc *anyenc.Value)) error { return nil })
	if err != nil {
		return func() {}
	}
	ctx, cancelCtx := context.WithCancel(context.Background())
	go func() {
		events := sub.Events()
		for {
			evs, werr := events.Wait(ctx)
			if werr != nil {
				return
			}
			for _, ev := range evs {
				if out, ok := toIdentityListEvent(ev); ok {
					cb(out)
				}
			}
		}
	}()
	return func() {
		cancelCtx()
		_ = sub.Close()
	}
}

// identityInfo maps the internal record to the public view (drops the
// synced symkey — consumers never see decryption keys).
func identityInfo(r techspace.IdentityRecord) space.IdentityInfo {
	return space.IdentityInfo{
		Identity:    r.Identity,
		Name:        r.Name,
		Description: r.Description,
		IconCID:     r.IconCID,
		SpaceIds:    r.SpaceIds,
	}
}

func toIdentityListEvent(ev space.SubscriptionEvent) (space.IdentityListEvent, bool) {
	var out space.IdentityListEvent
	for _, rec := range ev.Added {
		if rec.Doc != nil {
			out.Added = append(out.Added, identityInfo(techspace.DecodeIdentityRecord(rec.Doc)))
		}
	}
	for _, rec := range ev.Updated {
		if rec.Doc != nil {
			out.Updated = append(out.Updated, identityInfo(techspace.DecodeIdentityRecord(rec.Doc)))
		}
	}
	for _, rec := range ev.Removed {
		out.Removed = append(out.Removed, rec.Id)
	}
	if len(out.Added) == 0 && len(out.Updated) == 0 && len(out.Removed) == 0 {
		return space.IdentityListEvent{}, false
	}
	return out, true
}
