package spaceimpl

import (
	"context"
	"fmt"
	"io"

	"github.com/anyproto/any-sync-sdk/internal/payloads"
	"github.com/anyproto/any-sync-sdk/space"
)

// Tech-space files. The account avatar is a files-v2 object bound to
// the tech index object: it rides the owner's quota, syncs to the
// owner's devices like any tech-space row, and its ciphertext is
// fetchable by rootCid through the public read path — which is how
// contacts, who never join the tech space, render it. The restricted
// tech handle withholds Files(); the account API reaches the bytes
// through these accessors only.

// implFor resolves the spaceImpl behind spaceId: the tech space's inner
// implementation for the tech space id, the loaded space otherwise. The
// byte-layer workers (queue jobs, status dispatch, GC) resolve through
// it so a tech-space file gets the same service as any other.
func (s *Service) implFor(ctx context.Context, spaceId string) (*spaceImpl, error) {
	if s.tsp != nil && spaceId == s.tsp.SpaceId() {
		return s.techHandle().inner, nil
	}
	sp, err := s.Get(ctx, spaceId)
	if err != nil {
		return nil, err
	}
	impl, ok := sp.(*spaceImpl)
	if !ok {
		return nil, fmt.Errorf("spaceimpl: %s has no file surface", spaceId)
	}
	return impl, nil
}

func (s *Service) techFiles() *filesAPI { return &filesAPI{s: s.techHandle().inner} }

// TechSpaceId is the id of the tech space, the space every tech file
// belongs to.
func (s *Service) TechSpaceId() string { return s.tsp.SpaceId() }

// AttachTechFile stores r as a file of the tech index object, always on
// the content-addressed tier: a file under the inline cutoff would ride
// inside the sealed row, with no rootCid for a non-member to fetch.
func (s *Service) AttachTechFile(ctx context.Context, r io.Reader, opts space.AttachOpts) (space.FileInfo, error) {
	return s.techFiles().attach(ctx, s.tsp.IndexObjectId(), r, opts, true)
}

// TechFileRow returns the unsealed row of one tech-space file: its
// rootCid, file key and custody receipt.
func (s *Service) TechFileRow(ctx context.Context, fileId string) (payloads.Row, error) {
	return s.techHandle().inner.PayloadsInternal().FindRow(ctx, fileId)
}

// OpenTechFile returns a reader over one tech-space file's plaintext.
func (s *Service) OpenTechFile(ctx context.Context, fileId string) (space.FileReader, error) {
	return s.techFiles().Open(ctx, fileId, space.VariantOriginal)
}

// DeleteTechFile removes one tech-space file (row, queue jobs, local ref).
func (s *Service) DeleteTechFile(ctx context.Context, fileId string) error {
	return s.techFiles().Delete(ctx, fileId)
}

// SubscribeTechFileStatus observes the durability transitions of the
// tech space's files.
func (s *Service) SubscribeTechFileStatus(cb func(space.FileStatus)) (unsubscribe func()) {
	return s.techFiles().SubscribeStatus(cb)
}
