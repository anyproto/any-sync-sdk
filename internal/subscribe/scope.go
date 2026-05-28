package subscribe

import "github.com/anyproto/any-sync-sdk/internal/eventbus"

// Scope locates which events a sub cares about.
//
// Shared=true means "every event whose dataset == eventbus.ObjectsDataset
// across every object in the space" — the property firehose. The
// ObjectId/Dataset fields are ignored in this case.
//
// Shared=false means "events whose ObjectId/Dataset match exactly".
type Scope struct {
	Shared   bool
	ObjectId string
	Dataset  string
}

// scopeIndex groups querySubs by scope so OnApply can route an event
// to just the relevant subs in O(1) per bucket without scanning every
// sub. Shared-scope and per-(object, dataset) buckets are disjoint at
// register time — a single sub lives in exactly one bucket.
type scopeIndex struct {
	shared map[uint64]*querySub
	perObj map[string]map[string]map[uint64]*querySub
}

func newScopeIndex() scopeIndex {
	return scopeIndex{
		shared: map[uint64]*querySub{},
		perObj: map[string]map[string]map[uint64]*querySub{},
	}
}

func (idx *scopeIndex) add(s *querySub) {
	if s.cfg.Scope.Shared {
		idx.shared[s.id] = s
		return
	}
	perObj, ok := idx.perObj[s.cfg.Scope.ObjectId]
	if !ok {
		perObj = map[string]map[uint64]*querySub{}
		idx.perObj[s.cfg.Scope.ObjectId] = perObj
	}
	perDs, ok := perObj[s.cfg.Scope.Dataset]
	if !ok {
		perDs = map[uint64]*querySub{}
		perObj[s.cfg.Scope.Dataset] = perDs
	}
	perDs[s.id] = s
}

func (idx *scopeIndex) remove(s *querySub) {
	if s.cfg.Scope.Shared {
		delete(idx.shared, s.id)
		return
	}
	perObj, ok := idx.perObj[s.cfg.Scope.ObjectId]
	if !ok {
		return
	}
	perDs := perObj[s.cfg.Scope.Dataset]
	delete(perDs, s.id)
	if len(perDs) == 0 {
		delete(perObj, s.cfg.Scope.Dataset)
	}
	if len(perObj) == 0 {
		delete(idx.perObj, s.cfg.Scope.ObjectId)
	}
}

// matches appends every querySub whose scope covers ev. Shared-scope
// subs fire for every event whose dataset is ObjectsDataset; explicit
// subs fire only on exact-object+dataset match. The two sets are
// disjoint by construction, so callers can iterate the union without
// dedup.
func (idx *scopeIndex) matches(ev eventbus.Event, out []*querySub) []*querySub {
	if ev.Dataset == eventbus.ObjectsDataset {
		for _, s := range idx.shared {
			out = append(out, s)
		}
	}
	if perObj, ok := idx.perObj[ev.ObjectId]; ok {
		if perDs, ok := perObj[ev.Dataset]; ok {
			for _, s := range perDs {
				out = append(out, s)
			}
		}
	}
	return out
}
