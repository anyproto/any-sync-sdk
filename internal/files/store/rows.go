package store

import "sync"

// rowState is the single live in-memory state of one stored file,
// shared by every open Handle and every store-level mutator (Offload,
// Delete, Finalize, CreateSparse). The metadata row persists it; this
// struct is the runtime authority while anything holds a reference, so
// two handles can never diverge on the bitmap and a mutation (offload,
// delete) is immediately visible to open handles.
type rowState struct {
	mu     sync.Mutex
	loaded bool
	state  string // StatePartial / StateComplete / StateOffload / stateGone
	have   bitmap
}

// stateGone marks a row deleted while referenced — never persisted.
const stateGone = "gone"

// rowRegistry hands out refcounted rowState entries keyed by row id.
type rowRegistry struct {
	mu sync.Mutex
	m  map[string]*rowEntry
}

type rowEntry struct {
	rowState
	refs int
}

func newRowRegistry() *rowRegistry {
	return &rowRegistry{m: map[string]*rowEntry{}}
}

// acquire returns the shared state for key, creating an unloaded entry
// if nothing references it. Pair with release.
func (r *rowRegistry) acquire(key string) *rowEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.m[key]
	if !ok {
		e = &rowEntry{}
		r.m[key] = e
	}
	e.refs++
	return e
}

func (r *rowRegistry) release(key string, e *rowEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e.refs--
	if e.refs == 0 {
		delete(r.m, key)
	}
}

// withRow runs fn holding the locked shared state for key, acquiring
// and releasing a reference around it — the store-level mutation
// pattern (Offload, Delete, Finalize, CreateSparse).
func (s *Store) withRow(key string, fn func(rs *rowState) error) error {
	e := s.rows.acquire(key)
	defer s.rows.release(key, e)
	e.mu.Lock()
	defer e.mu.Unlock()
	return fn(&e.rowState)
}
