// Package store is the local content-addressed payload store of the
// files subsystem (SYN-25): one CARv2 per file keyed by its root cid
// (byte-identical to the uploaded S3 object), plus an any-store
// metadata collection holding the small hot state — download bitmap,
// state, per-file refs, the per-space content-dedup index and LRU
// access times. Blob bytes never enter the DB; evicting a file is one
// unlink.
//
// Layout under the store root:
//
//	<root>/tmp/<rand>.car              — in-progress upload builds (swept on open)
//	<root>/<spaceId>/<shard>/<rootCid>.car
//
// The per-space directory keeps space deletion a recursive remove and
// matches the per-space dedup/GC scope (rootCids never repeat across
// spaces — per-file random keys).
package store

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/ipfs/go-cid"
)

const (
	// FilesCollection holds one row per stored CAR:
	// {id: "<spaceId>/<rootCid>", sp, root, st, sz, secs, have?, refs, la}.
	FilesCollection = "files_local"
	// DedupCollection is the per-space whole-file dedup index:
	// {id: "<spaceId>/<sha256 hex>", root, fileId}.
	DedupCollection = "files_dedup"
	// KVCollection holds small files-subsystem state ({id, v}) — e.g.
	// the persisted publicReadBaseUrl per network.
	KVCollection = "files_kv"
)

// Row states.
const (
	StatePartial  = "partial"   // sparse skeleton, bitmap tracks arrived sections
	StateComplete = "complete"  // every section present and verified
	StateOffload  = "offloaded" // row kept, bytes dropped (refetchable)
)

// Row field keys.
const (
	fieldSpaceId  = "sp"
	fieldRoot     = "root"
	fieldState    = "st"
	fieldSize     = "sz"
	fieldSections = "secs"
	fieldHave     = "have"
	fieldRefs     = "refs"
	fieldAccess   = "la"
	fieldFileId   = "fileId"
	fieldOwner    = "owner"
	idField       = "id"
)

var (
	// ErrNotFound — no row for (spaceId, rootCid).
	ErrNotFound = errors.New("filestore: not found")
	// ErrNoBytes — the row exists but local bytes are gone (offloaded,
	// or the file vanished). Refetch via CreateSparse.
	ErrNoBytes = errors.New("filestore: bytes not local")
	// ErrBlockMissing — a wanted block's section hasn't arrived (or
	// failed verification). The fetch path resolves it and calls
	// Handle.WriteBlock.
	ErrBlockMissing = errors.New("filestore: block missing")
)

// Store is the local payload store. Safe for concurrent use.
type Store struct {
	root  string
	db    anystore.DB
	files anystore.Collection
	dedup anystore.Collection
	kv    anystore.Collection
	rows  *rowRegistry
}

// New opens (creating if needed) the store rooted at rootDir with
// metadata in db, and sweeps abandoned build temps.
func New(ctx context.Context, rootDir string, db anystore.DB) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(rootDir, "tmp"), 0o755); err != nil {
		return nil, err
	}
	files, err := db.Collection(ctx, FilesCollection)
	if err != nil {
		return nil, fmt.Errorf("filestore: open %s: %w", FilesCollection, err)
	}
	if err = files.EnsureIndex(ctx, anystore.IndexInfo{
		Name:   "idx_files_local_sp",
		Fields: []string{fieldSpaceId},
		Sparse: true,
	}); err != nil {
		return nil, err
	}
	dedup, err := db.Collection(ctx, DedupCollection)
	if err != nil {
		return nil, fmt.Errorf("filestore: open %s: %w", DedupCollection, err)
	}
	kv, err := db.Collection(ctx, KVCollection)
	if err != nil {
		return nil, fmt.Errorf("filestore: open %s: %w", KVCollection, err)
	}
	s := &Store{root: rootDir, db: db, files: files, dedup: dedup, kv: kv, rows: newRowRegistry()}
	s.sweepTmp()
	return s, nil
}

// sweepTmp removes build temps left by a crash. Any live Build was
// created after this ran (New is called once at SDK open).
func (s *Store) sweepTmp() {
	dir := filepath.Join(s.root, "tmp")
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range ents {
		_ = os.Remove(filepath.Join(dir, e.Name()))
	}
}

// TmpDir is the store's scratch directory (same filesystem as the
// CARs, swept on open). The upload spool spills oversized inputs here.
func (s *Store) TmpDir() string {
	return filepath.Join(s.root, "tmp")
}

// carPath is the final location of a stored file.
func (s *Store) carPath(spaceId string, root cid.Cid) string {
	name := root.String()
	shard := name[len(name)-2:]
	return filepath.Join(s.root, spaceId, shard, name+".car")
}

func rowId(spaceId string, root cid.Cid) string {
	return spaceId + "/" + root.String()
}

// Info is the metadata view of one stored file.
type Info struct {
	SpaceId    string
	Root       cid.Cid
	State      string
	Size       int64 // total CAR size in bytes (the S3 object size)
	Sections   int   // total blocks in the CAR
	Present    int   // arrived blocks (== Sections when complete)
	Refs       []string
	LastAccess time.Time
}

// Info returns the metadata row for (spaceId, root).
func (s *Store) Info(ctx context.Context, spaceId string, root cid.Cid) (Info, error) {
	doc, err := s.files.FindId(ctx, rowId(spaceId, root))
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return Info{}, ErrNotFound
		}
		return Info{}, err
	}
	return infoFromValue(doc.Value())
}

// ListSpace returns metadata for every stored file of a space (GC,
// status and eviction scans).
func (s *Store) ListSpace(ctx context.Context, spaceId string) ([]Info, error) {
	filter := query.Key{Path: []string{fieldSpaceId}, Filter: query.NewComp(query.CompOpEq, spaceId)}
	iter, err := s.files.Find(filter).Iter(ctx)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []Info
	for iter.Next() {
		doc, derr := iter.Doc()
		if derr != nil {
			return nil, derr
		}
		info, ierr := infoFromValue(doc.Value())
		if ierr != nil {
			return nil, ierr
		}
		out = append(out, info)
	}
	return out, nil
}

func infoFromValue(v *anyenc.Value) (Info, error) {
	root, err := cid.Decode(string(v.GetStringBytes(fieldRoot)))
	if err != nil {
		return Info{}, fmt.Errorf("filestore: bad root in row %s: %w", v.GetStringBytes(idField), err)
	}
	info := Info{
		SpaceId:    string(v.GetStringBytes(fieldSpaceId)),
		Root:       root,
		State:      string(v.GetStringBytes(fieldState)),
		Size:       int64(v.GetFloat64(fieldSize)),
		Sections:   v.GetInt(fieldSections),
		LastAccess: time.Unix(int64(v.GetInt(fieldAccess)), 0),
	}
	for _, r := range v.GetArray(fieldRefs) {
		info.Refs = append(info.Refs, string(r.GetStringBytes()))
	}
	switch info.State {
	case StateComplete:
		info.Present = info.Sections
	case StatePartial:
		info.Present = decodeBitmap(v).count(info.Sections)
	}
	return info, nil
}

// Touch bumps the LRU access time.
func (s *Store) Touch(ctx context.Context, spaceId string, root cid.Cid) error {
	return s.updateExisting(ctx, rowId(spaceId, root), func(a *anyenc.Arena, v *anyenc.Value) {
		v.Set(fieldAccess, a.NewNumberInt(int(time.Now().Unix())))
	})
}

// AddRefs registers fileIds as users of this stored file (set union).
func (s *Store) AddRefs(ctx context.Context, spaceId string, root cid.Cid, fileIds ...string) error {
	if len(fileIds) == 0 {
		return nil
	}
	return s.updateExisting(ctx, rowId(spaceId, root), func(a *anyenc.Arena, v *anyenc.Value) {
		addRefs(a, v, fileIds)
	})
}

// RemoveRef drops one fileId ref; returns the remaining ref count so
// GC can see the file go unreferenced.
func (s *Store) RemoveRef(ctx context.Context, spaceId string, root cid.Cid, fileId string) (remaining int, err error) {
	err = s.updateExisting(ctx, rowId(spaceId, root), func(a *anyenc.Arena, v *anyenc.Value) {
		arr := v.GetArray(fieldRefs)
		out := a.NewArray()
		n := 0
		for _, r := range arr {
			if s := string(r.GetStringBytes()); s != fileId {
				out.SetArrayItem(n, a.NewString(s))
				n++
			}
		}
		v.Set(fieldRefs, out)
		remaining = n
	})
	return remaining, err
}

// Offload drops the local bytes but keeps the row (state=offloaded).
// Open handles observe the flip through the shared row state and stop
// mutating. The caller is responsible for the durability gate (only
// files with a verified networkSign may be offloaded).
func (s *Store) Offload(ctx context.Context, spaceId string, root cid.Cid) error {
	id := rowId(spaceId, root)
	return s.withRow(id, func(rs *rowState) error {
		if err := s.updateExisting(ctx, id, func(a *anyenc.Arena, v *anyenc.Value) {
			v.Set(fieldState, a.NewString(StateOffload))
			v.Del(fieldHave)
		}); err != nil {
			return err
		}
		rs.state = StateOffload
		rs.have = nil
		rs.loaded = true
		return removeCar(s.carPath(spaceId, root))
	})
}

// Delete removes the file and its row entirely (the owning rows are
// gone). Dedup entries pointing at the root stay: a later BIND reuses
// remote content, not local bytes.
func (s *Store) Delete(ctx context.Context, spaceId string, root cid.Cid) error {
	id := rowId(spaceId, root)
	return s.withRow(id, func(rs *rowState) error {
		if err := s.files.DeleteId(ctx, id); err != nil && !errors.Is(err, anystore.ErrDocNotFound) {
			return err
		}
		rs.state = stateGone
		rs.have = nil
		rs.loaded = true
		return removeCar(s.carPath(spaceId, root))
	})
}

// DeleteSpace drops every row and the whole per-space directory.
func (s *Store) DeleteSpace(ctx context.Context, spaceId string) error {
	for _, coll := range []anystore.Collection{s.files, s.dedup} {
		filter := query.Key{Path: []string{fieldSpaceId}, Filter: query.NewComp(query.CompOpEq, spaceId)}
		iter, err := coll.Find(filter).Iter(ctx)
		if err != nil {
			return err
		}
		var ids []string
		for iter.Next() {
			doc, derr := iter.Doc()
			if derr != nil {
				iter.Close()
				return derr
			}
			ids = append(ids, string(doc.Value().GetStringBytes(idField)))
		}
		if err = iter.Close(); err != nil {
			return err
		}
		for _, id := range ids {
			if coll == s.files {
				if err = s.withRow(id, func(rs *rowState) error {
					rs.state = stateGone
					rs.loaded = true
					err := coll.DeleteId(ctx, id)
					if errors.Is(err, anystore.ErrDocNotFound) {
						return nil
					}
					return err
				}); err != nil {
					return err
				}
				continue
			}
			if err = coll.DeleteId(ctx, id); err != nil && !errors.Is(err, anystore.ErrDocNotFound) {
				return err
			}
		}
	}
	return os.RemoveAll(filepath.Join(s.root, spaceId))
}

// ContentRef is one dedup-index entry: the file already registered
// for a plaintext sha256 in this space. OwnerId locates the payloads
// row (the BIND path needs the donor row's wrapped key + networkSign).
type ContentRef struct {
	Root    cid.Cid
	FileId  string
	OwnerId string
}

// RecordContent stores the per-space sha256 → (root, fileId, ownerId)
// dedup mapping consulted by the upload BIND path. Dedup is strictly
// per-space: a cross-space hit would share a rootCid across spaces and
// break per-space GC on the node.
func (s *Store) RecordContent(ctx context.Context, spaceId string, sha256 []byte, root cid.Cid, fileId, ownerId string) error {
	id := spaceId + "/" + fmt.Sprintf("%x", sha256)
	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		v.Set(fieldSpaceId, a.NewString(spaceId))
		v.Set(fieldRoot, a.NewString(root.String()))
		v.Set(fieldFileId, a.NewString(fileId))
		v.Set(fieldOwner, a.NewString(ownerId))
		return v, true, nil
	})
	_, err := s.dedup.UpsertId(ctx, id, mod)
	return err
}

// LookupContent resolves a plaintext sha256 to an already-registered
// file in this space.
func (s *Store) LookupContent(ctx context.Context, spaceId string, sha256 []byte) (ref ContentRef, ok bool, err error) {
	doc, err := s.dedup.FindId(ctx, spaceId+"/"+fmt.Sprintf("%x", sha256))
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return ContentRef{}, false, nil
		}
		return ContentRef{}, false, err
	}
	v := doc.Value()
	root, err := cid.Decode(string(v.GetStringBytes(fieldRoot)))
	if err != nil {
		return ContentRef{}, false, err
	}
	return ContentRef{
		Root:    root,
		FileId:  string(v.GetStringBytes(fieldFileId)),
		OwnerId: string(v.GetStringBytes(fieldOwner)),
	}, true, nil
}

// SetKV persists one small files-subsystem value.
func (s *Store) SetKV(ctx context.Context, key, value string) error {
	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		v.Set("v", a.NewString(value))
		return v, true, nil
	})
	_, err := s.kv.UpsertId(ctx, key, mod)
	return err
}

// GetKV reads one value; ok is false when the key was never set.
func (s *Store) GetKV(ctx context.Context, key string) (value string, ok bool, err error) {
	doc, err := s.kv.FindId(ctx, key)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return "", false, nil
		}
		return "", false, err
	}
	return string(doc.Value().GetStringBytes("v")), true, nil
}

// updateExisting applies fn to an existing row in one atomic UpdateId;
// ErrNotFound if the row is absent — it never creates or resurrects a
// row, so a mutation racing a Delete cannot bring the row back.
func (s *Store) updateExisting(ctx context.Context, id string, fn func(a *anyenc.Arena, v *anyenc.Value)) error {
	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		fn(a, v)
		return v, true, nil
	})
	if _, err := s.files.UpdateId(ctx, id, mod); err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return ErrNotFound
		}
		return err
	}
	return nil
}

// upsertRow writes a full row (create or replace semantics for the
// managed fields).
func (s *Store) upsertRow(ctx context.Context, spaceId string, root cid.Cid, fn func(a *anyenc.Arena, v *anyenc.Value)) error {
	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		v.Set(fieldSpaceId, a.NewString(spaceId))
		v.Set(fieldRoot, a.NewString(root.String()))
		v.Set(fieldAccess, a.NewNumberInt(int(time.Now().Unix())))
		fn(a, v)
		return v, true, nil
	})
	_, err := s.files.UpsertId(ctx, rowId(spaceId, root), mod)
	return err
}

func addRefs(a *anyenc.Arena, v *anyenc.Value, fileIds []string) {
	seen := map[string]bool{}
	out := a.NewArray()
	n := 0
	add := func(id string) {
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		out.SetArrayItem(n, a.NewString(id))
		n++
	}
	for _, r := range v.GetArray(fieldRefs) {
		add(string(r.GetStringBytes()))
	}
	for _, id := range fileIds {
		add(id)
	}
	v.Set(fieldRefs, out)
}

func removeCar(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func decodeBitmap(v *anyenc.Value) bitmap {
	b, err := base64.StdEncoding.DecodeString(string(v.GetStringBytes(fieldHave)))
	if err != nil {
		return nil
	}
	return b
}

func sizeValue(a *anyenc.Arena, size int64) *anyenc.Value {
	// float64 keeps 2 GiB+ sizes exact on 32-bit platforms (anyenc
	// numbers are float64; int would truncate at the platform width).
	return a.NewNumberFloat64(float64(size))
}
