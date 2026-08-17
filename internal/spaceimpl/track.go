package spaceimpl

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/ipfs/go-cid"

	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

// Track registers a foreign spaceId in the tech-space index so a later
// Get can open it. The row is the Create/Join registry shape minus the
// membership semantics — Type unknown until the first load backfills
// it from the header, remote+local status active, no metadata (nothing
// is known about a space we hold no keys to; the name/icon mirror only
// runs for materialized spaceIndex trees).
//
// No network round trip happens here: Get is what triggers any-sync's
// storage bootstrap (SpacePull from the responsible nodes when local
// storage is missing). Idempotent; an existing row of any status —
// own, joined, tracked, or deleted — is left untouched.
func (s *Service) Track(ctx context.Context, spaceId string) error {
	if err := validateSpaceId(spaceId); err != nil {
		return err
	}
	if spaceId == s.tsp.SpaceId() {
		return fmt.Errorf("spaceimpl: Track: %w", space.ErrIsTechSpace)
	}
	if _, ok := s.tsp.Get(ctx, spaceId); ok {
		return nil
	}
	if _, err := s.tsp.Add(ctx, techspace.SpaceIndexRecord{
		Id:           spaceId,
		RemoteStatus: techspace.StatusActive,
	}); err != nil {
		return fmt.Errorf("spaceimpl: Track: write index entry: %w", err)
	}
	// Add() can't write localStatus (device-local field; see Create).
	// Stamp it active so the row reports StatusActive consistently.
	if _, err := s.tsp.SetLocalStatus(ctx, spaceId, techspace.StatusActive); err != nil {
		return fmt.Errorf("spaceimpl: Track: set local status: %w", err)
	}
	return nil
}

// validateSpaceId checks the any-sync spaceId shape —
// `<cid>.<repKey-base36>` — before the id enters the index. A malformed
// id would otherwise sit in the registry and fail every Get with an
// opaque remote error; catching it here gives the caller an immediate,
// attributable failure.
func validateSpaceId(spaceId string) error {
	dot := strings.LastIndexByte(spaceId, '.')
	if dot <= 0 || dot == len(spaceId)-1 {
		return fmt.Errorf("spaceimpl: %w: %q: want <cid>.<replication key>", space.ErrBadSpaceId, spaceId)
	}
	if _, err := cid.Decode(spaceId[:dot]); err != nil {
		return fmt.Errorf("spaceimpl: %w: %q: bad cid: %w", space.ErrBadSpaceId, spaceId, err)
	}
	if _, err := strconv.ParseUint(spaceId[dot+1:], 36, 64); err != nil {
		return fmt.Errorf("spaceimpl: %w: %q: bad replication key: %w", space.ErrBadSpaceId, spaceId, err)
	}
	return nil
}
