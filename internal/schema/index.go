package schema

import (
	"fmt"
	"strings"
)

// Index is a declared secondary index of a records dataset: an ordered
// list of field paths any-store keeps sorted, so a filter or sort on a
// prefix of them is a range read.
type Index struct {
	// Key is the index's slug, unique within its dataset.
	Key string
	// Fields are the indexed paths in order; a "-" prefix keeps that
	// path descending. Each names a declared scalar field, or one of
	// the protocol paths (IndexProtocolPaths).
	Fields []string
	// Sparse keeps a record out of the index unless it carries every
	// indexed field.
	Sparse bool
}

const (
	// MaxIndexFields bounds the paths of one index — what any-store
	// plans a compound index over.
	MaxIndexFields = 4
	// MaxDatasetIndexes bounds the indexes of one dataset: every index
	// is written on every record write.
	MaxDatasetIndexes = 8
	// IndexStorePrefix starts the any-store name of every declared
	// index, apart from the handler and built-in ones.
	IndexStorePrefix = "dx_"

	// MaxIndexFieldLen bounds the key of an indexed field, so the store
	// name of the widest index stays a valid any-store name.
	MaxIndexFieldLen = 48

	// IndexPathCreated orders records by creation within one object.
	IndexPathCreated = "_ver.id"
	// IndexPathObject is the owning object of a shared dataset's record.
	IndexPathObject = "_objectId"
)

// IndexFieldPath splits one Fields entry into its path and direction.
func IndexFieldPath(entry string) (path string, desc bool) {
	if strings.HasPrefix(entry, "-") {
		return entry[1:], true
	}
	return entry, false
}

// ValidateIndexShape checks what an index definition says on its own:
// a slug key and one to MaxIndexFields distinct, non-empty paths.
func ValidateIndexShape(idx Index) error {
	if err := ValidateSlug("index", idx.Key); err != nil {
		return err
	}
	if len(idx.Fields) == 0 {
		return fmt.Errorf("%w: index %q names no fields", ErrDecl, idx.Key)
	}
	if len(idx.Fields) > MaxIndexFields {
		return fmt.Errorf("%w: index %q names %d fields, at most %d", ErrDecl, idx.Key, len(idx.Fields), MaxIndexFields)
	}
	seen := make(map[string]struct{}, len(idx.Fields))
	for _, entry := range idx.Fields {
		path, _ := IndexFieldPath(entry)
		if path == "" || strings.HasPrefix(path, "-") {
			return fmt.Errorf("%w: index %q: invalid field %q", ErrDecl, idx.Key, entry)
		}
		if _, dup := seen[path]; dup {
			return fmt.Errorf("%w: index %q names %q twice", ErrDecl, idx.Key, path)
		}
		seen[path] = struct{}{}
	}
	return nil
}

// ValidateIndexDecl checks an index against the dataset it belongs to:
// its shape, and that every path is a declared field of a scalar kind
// (string, number, boolean, datetime) with an indexable key, or a
// protocol path the dataset carries. shared says the dataset keeps
// every object's records in one collection, which is where
// IndexPathObject exists.
func ValidateIndexDecl(ds Dataset, idx Index, shared bool) error {
	if err := ValidateIndexShape(idx); err != nil {
		return err
	}
	for _, entry := range idx.Fields {
		path, _ := IndexFieldPath(entry)
		switch path {
		case IndexPathCreated:
			continue
		case IndexPathObject:
			if !shared {
				return fmt.Errorf("%w: index %q: %s exists on a shared dataset only", ErrDecl, idx.Key, IndexPathObject)
			}
			continue
		}
		if !indexableKey(path) {
			return fmt.Errorf("%w: index %q: field %q: an indexed field's key is letters, digits and \"_\", starts with a letter and is at most %d bytes",
				ErrDecl, idx.Key, path, MaxIndexFieldLen)
		}
		var field *Field
		for i := range ds.Fields {
			if ds.Fields[i].Id == path {
				field = &ds.Fields[i]
				break
			}
		}
		if field == nil {
			return fmt.Errorf("%w: index %q: %q is not a declared field", ErrDecl, idx.Key, path)
		}
		if field.Schema == nil || !indexableKind(field.Schema.Kind) {
			return fmt.Errorf("%w: index %q: field %q is not a string, number, boolean or datetime", ErrDecl, idx.Key, path)
		}
	}
	return nil
}

// indexableKey admits the field keys an index may name. The store name
// joins the paths with "," and marks direction and sparseness with "-"
// and "~": keys free of those, and of any-store's reserved "$", keep
// every name valid and distinct per shape.
func indexableKey(key string) bool {
	if key == "" || len(key) > MaxIndexFieldLen {
		return false
	}
	for i := 0; i < len(key); i++ {
		c := key[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case i > 0 && (c >= '0' && c <= '9' || c == '_'):
		default:
			return false
		}
	}
	return true
}

func indexableKind(k Kind) bool {
	switch k {
	case KindString, KindNumber, KindBoolean, KindDatetime:
		return true
	}
	return false
}

// StoreName is the any-store name of the index, derived from what it
// indexes: two definitions of one shape are one index, and a changed
// definition is another index, never a conflict on a name.
func (idx Index) StoreName() string {
	name := IndexStorePrefix + strings.Join(idx.Fields, ",")
	if idx.Sparse {
		name += "~sparse"
	}
	return name
}
