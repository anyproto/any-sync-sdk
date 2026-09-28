package techspace

import "context"

// Test seams for the CRDT version mark.

func (s *Service) OnCRDTVersionForTest() func(int) { return s.onCRDTVersion }

func CRDTVersionVerdictForTest(stored, supported int) (bool, error) {
	return crdtVersionVerdict(stored, supported)
}

// BeforeCreateForTest drives the handler's admission of one version
// through its hook without a controller.
func (h CRDTVersionHandler) BeforeCreateForTest(version int) { h.notify(version) }

// LockClaimsForTest takes the ClaimActive slot.
func (s *Service) LockClaimsForTest(ctx context.Context) (func(), error) { return s.lockClaims(ctx) }
