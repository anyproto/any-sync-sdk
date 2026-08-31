// The dataset-definitions dataset — the third built-in on type objects
// (next to `properties` and `shortIds`). Holds a type's runtime-defined
// dataset schemas as CRDT records: one head record per dataset plus one
// record per field, so concurrent edits by different members merge as
// distinct records (the properties model). The catalog compiler
// (types.CompileDatasetDefs) folds the records into schema.Dataset
// declarations enforced by the generic schema handler.

package typetype

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// DatasetDefs is the dataset on a type object holding its dataset
// definition records. On disk the collection is `<typeId>_datasets`.
const DatasetDefs = "datasets"

// DatasetDefsHandlerVersion is the DataVersion stamped on changes to
// the DatasetDefs dataset itself (hardcoded, like HandlerVersion —
// definition writes are never gated on their own schema state).
//
// Known mixed-fleet limitation: this string is deliberately
// NOT parseable by the spaceobjects DataVersion gate (legacy
// handler-version strings pass through unconstrained — see gateFor),
// so bumping it CANNOT park new wire forms on old replicas. A peer
// that predates the array text form sheds an array-form `search.text`
// $set (its handler admits only scalar strings) and never replays
// it, diverging the head record until re-written. Accepted
// pre-release; a real fix needs a version pair the gate parses (and
// old drainers can satisfy), which is a protocol change.
const DatasetDefsHandlerVersion = "typeDatasetHandler-v1"

// DatasetDefsLocalVersion is this handler's LOCAL logic version
// (HandlerReg.Version): bumped when a validation change means an
// already-materialized set of dataset definitions would come out
// different, so the SDK replays the type object's tree. Like the
// property handler's, it is also the recovery path for definitions an
// older build dropped because it could not validate them.
//
// v2: `datetime` is a kind, so a stamped time field validates.
const DatasetDefsLocalVersion = 2

// Discriminator values of the pinned `def` field.
const (
	DefKindDataset = "dataset" // head record: one per defined dataset
	DefKindField   = "field"   // field record: one per dataset field
)

// Dataset-def record field names. Head records use the Def* set; field
// records reuse FieldKey/FieldKind/FieldName/FieldDescription/FieldItems/
// FieldProperties from the property-record vocabulary plus the Field*
// behavioral set below.
const (
	DefFieldDef         = "def"         // discriminator, pinned
	DefFieldName        = "collection"  // dataset collection name (head), pinned — distinct from the mutable display `name`
	DefFieldDynamic     = "dynamic"     // bool (head), pinned
	DefFieldIdRule      = "idRule"      // "auto"/"user" (head), pinned ("id" is a reserved head)
	DefFieldIdPattern   = "idPattern"   // RE2 (head), pinned
	DefFieldIdMaxLen    = "idMaxLen"    // number (head), pinned
	DefFieldDeleteBy    = "deleteBy"    // "anyone"/"author" (head), pinned
	DefFieldSkipHistory = "skipHistory" // bool (head), pinned
	DefFieldSearch      = "search"      // {title,text,scope} (head); leaves mutable, text string-or-array
	DefFieldDisplayName = "displayName" // human label (head), mutable
	DefFieldDataset     = "dataset"     // owning head record id (field), pinned
	DefFieldStamp       = "stamp"       // "creator"/"createTime"/"modifyTime" (field), pinned
	DefFieldRequired    = "required"    // bool (field), pinned
	DefFieldMutableBy   = "mutableBy"   // "never"/"author"/"any" (field), pinned
)

// Sub-keys of the head `search` object — mutable leaves (the
// format.ui/filter model: broad `search` replaces are pinned, the
// leaves mutate freely). `title` and `scope` are scalar strings; `text`
// is a bare field key or a non-empty array of field keys.
const (
	SearchKeyTitle = "title"
	SearchKeyText  = "text"
	SearchKeyScope = "scope"
)

// reservedDatasetNames are collection names a user dataset-def may not
// claim: the built-in datasets every controller registers. Kept local
// (the store's builtinDataVersions map lives a layer up); the catalog
// compiler re-checks against the live reg set anyway — this create-time
// gate just fails fast on the obvious collisions.
var reservedDatasetNames = map[string]struct{}{
	"objects":    {},
	"properties": {},
	"shortIds":   {},
	"datasets":   {},
	"payloads":   {},
	"bundles":    {},
}

// datasetDefMutableTop are the head-level fields freely mutable on an
// existing def record (either record kind; `name` doubles as the field
// record's display label).
var datasetDefMutableTop = map[string]struct{}{
	FieldName:           {},
	FieldDescription:    {},
	DefFieldDisplayName: {},
}

// ErrBadDatasetDef indicates a structurally invalid dataset-definition
// record. Wraps crdt.ErrValidation.
var ErrBadDatasetDef = errors.New("typetype: invalid dataset definition record")

// isDatasetDefPinnedPath reports whether a write to `path` touches
// pinned dataset-def state. Everything is pinned except the mutable
// display fields and the search.title / search.text string leaves.
func isDatasetDefPinnedPath(path []string) bool {
	if len(path) == 0 {
		return false
	}
	if _, mutable := datasetDefMutableTop[path[0]]; mutable {
		return false
	}
	if path[0] == DefFieldSearch {
		if len(path) != 2 {
			return true // broad `search` replace or a deeper path
		}
		return path[1] != SearchKeyTitle && path[1] != SearchKeyText && path[1] != SearchKeyScope
	}
	return true
}

// IsDatasetDefPinnedPath is the exported form for client-side
// preflights (PatchDataset), mirroring IsPinnedPath.
func IsDatasetDefPinnedPath(path []string) bool { return isDatasetDefPinnedPath(path) }

// DatasetDefsHandler validates ops on a type object's `datasets`
// dataset and projects shortId rows on every important change (def
// added / removed), so the existing DataVersion gate parks data changes
// written against schema state this replica hasn't applied yet.
//
// All validation is record-local and stateless (see PropertyHandler for
// the convergence rationale): cross-record consistency — orphan field
// records, author rules without a creator stamp — is resolved
// deterministically at catalog compile, never here.
type DatasetDefsHandler struct{}

func (DatasetDefsHandler) Init(_ context.Context) error { return nil }

// BeforeCreate validates a definition record's creation shape and
// projects the "added" shortId row.
func (DatasetDefsHandler) BeforeCreate(ctx *crdt.ChangeCtx, rec *crdt.RecordChange, sink *crdt.Sink) error {
	defKind, ok := extractField(rec.Ops, DefFieldDef)
	if !ok {
		return fmt.Errorf("%w: %w: missing `def` discriminator", crdt.ErrValidation, ErrBadDatasetDef)
	}
	switch defKind {
	case DefKindDataset:
		if err := validateHeadCreate(rec.Ops); err != nil {
			return err
		}
	case DefKindField:
		if err := validateFieldCreate(rec.Ops); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: %w: unknown `def` %q", crdt.ErrValidation, ErrBadDatasetDef, defKind)
	}
	sink.Project(ShortIdsDataset, datasetShortIdRow(ctx.Change.ChangeId, rec.Id))
	return nil
}

// ValidateDatasetName checks a user dataset's collection name: non-empty,
// no "_" prefix (reserved for internal collections), no path/separator
// characters, not a built-in name. Shared by the create-time handler gate
// and the AddDataset preflight.
func ValidateDatasetName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: dataset name must be non-empty", ErrBadDatasetDef)
	}
	if strings.HasPrefix(name, "_") || strings.ContainsAny(name, "./:") {
		return fmt.Errorf("%w: invalid dataset name %q", ErrBadDatasetDef, name)
	}
	if _, reserved := reservedDatasetNames[name]; reserved {
		return fmt.Errorf("%w: dataset name %q is reserved", ErrBadDatasetDef, name)
	}
	return nil
}

// validateHeadCreate checks a dataset head record: collection name
// sanity and parseable behavioral labels.
func validateHeadCreate(ops []crdt.Op) error {
	name, _ := extractField(ops, DefFieldName)
	if err := ValidateDatasetName(name); err != nil {
		return fmt.Errorf("%w: %w", crdt.ErrValidation, err)
	}
	if label, present := extractField(ops, DefFieldIdRule); present {
		if _, ok := schema.ParseIdRule(label); !ok {
			return fmt.Errorf("%w: %w: unknown idRule %q", crdt.ErrValidation, ErrBadDatasetDef, label)
		}
	}
	if label, present := extractField(ops, DefFieldDeleteBy); present {
		if _, ok := schema.ParseDeletePolicy(label); !ok {
			return fmt.Errorf("%w: %w: unknown deleteBy %q", crdt.ErrValidation, ErrBadDatasetDef, label)
		}
	}
	if pattern, present := extractField(ops, DefFieldIdPattern); present {
		probe := schema.Dataset{IdRule: schema.IdUser, IdPattern: pattern}
		if _, _, err := schema.CompileIdPattern(probe); err != nil {
			return fmt.Errorf("%w: %w: %v", crdt.ErrValidation, ErrBadDatasetDef, err)
		}
	}
	return nil
}

// validateFieldCreate checks a dataset field record: owning head ref,
// key sanity, and parseable kind / scope / stamp / mutableBy labels.
// The stamp/required/mutableBy exclusivity rules mirror
// schema.ValidateDatasetDecl so a locally-authored bad field fails at
// write time, not silently at compile.
func validateFieldCreate(ops []crdt.Op) error {
	if headId, ok := extractField(ops, DefFieldDataset); !ok || headId == "" {
		return fmt.Errorf("%w: %w: field record requires its owning `dataset` id", crdt.ErrValidation, ErrBadDatasetDef)
	}
	key, ok := extractField(ops, FieldKey)
	if !ok || key == "" {
		return fmt.Errorf("%w: %w: field record requires a non-empty `key`", crdt.ErrValidation, ErrBadDatasetDef)
	}
	if key == "id" || strings.HasPrefix(key, "_") || strings.Contains(key, ".") {
		return fmt.Errorf("%w: %w: invalid field key %q", crdt.ErrValidation, ErrBadDatasetDef, key)
	}
	kindLabel, ok := extractKind(ops)
	if !ok {
		return fmt.Errorf("%w: %w", crdt.ErrValidation, ErrMissingKind)
	}
	if _, ok := schema.ParseKind(kindLabel); !ok {
		return fmt.Errorf("%w: unsupported kind %q", crdt.ErrValidation, kindLabel)
	}
	stampLabel, hasStamp := extractField(ops, DefFieldStamp)
	var stamp schema.Stamp
	if hasStamp {
		s, ok := schema.ParseStamp(stampLabel)
		if !ok {
			return fmt.Errorf("%w: %w: unknown stamp %q", crdt.ErrValidation, ErrBadDatasetDef, stampLabel)
		}
		stamp = s
	}
	if scopeLabel, present := extractField(ops, FieldScope); present {
		sc, known := schema.ParseScope(scopeLabel)
		if !known {
			return fmt.Errorf("%w: %w (got %q)", crdt.ErrValidation, ErrBadScope, scopeLabel)
		}
		if sc == schema.ScopeDerived && stamp == schema.StampNone {
			return fmt.Errorf("%w: %w (got %q)", crdt.ErrValidation, ErrBadScope, scopeLabel)
		}
		if stamp != schema.StampNone && sc != schema.ScopeDerived {
			return fmt.Errorf("%w: %w: a stamped field is derived-scope", crdt.ErrValidation, ErrBadDatasetDef)
		}
	}
	if label, present := extractField(ops, DefFieldMutableBy); present {
		if _, ok := schema.ParseMutability(label); !ok {
			return fmt.Errorf("%w: %w: unknown mutableBy %q", crdt.ErrValidation, ErrBadDatasetDef, label)
		}
		if stamp != schema.StampNone && label != "never" {
			return fmt.Errorf("%w: %w: a stamped field cannot declare mutableBy", crdt.ErrValidation, ErrBadDatasetDef)
		}
	}
	return nil
}

// BeforeModify rejects edits to pinned dataset-def state; the mutable
// display fields pass through and the search leaves must keep their
// wire shapes (scalar strings; `text` also accepts a key array).
func (DatasetDefsHandler) BeforeModify(_ *crdt.ChangeCtx, _ *crdt.RecordChange, op *crdt.Op, _ *crdt.Sink) error {
	if len(op.Path) == 0 {
		return rejectIfMultiFieldTouchesDefPinned(op)
	}
	if isDatasetDefPinnedPath(op.Path) {
		return fmt.Errorf("%w: %q is pinned after first write", crdt.ErrValidation, strings.Join(op.Path, "."))
	}
	if op.Path[0] == DefFieldSearch {
		return checkSearchLeafOp(op.Type, op.Path, op.Payload)
	}
	return nil
}

// checkSearchLeafOp validates a mutation of a search.* leaf: $unset
// always passes, $set must carry a valid leaf value
// (CheckSearchLeafValue), other ops are rejected — the
// checkFormatLeafOp contract with the string-or-array `text` form.
func checkSearchLeafOp(opType crdt.OpType, path []string, payload *anyenc.Value) error {
	switch opType {
	case crdt.OpUnset:
		return nil
	case crdt.OpSet:
		if err := CheckSearchLeafValue(path, payload); err != nil {
			return fmt.Errorf("%w: %w: %v", crdt.ErrValidation, ErrBadDatasetDef, err)
		}
		return nil
	default:
		return fmt.Errorf("%w: %w: op %s not allowed on %q", crdt.ErrValidation, ErrBadDatasetDef, opType, strings.Join(path, "."))
	}
}

// CheckSearchLeafValue validates a search.* leaf's $set payload:
// `title`/`scope` must be scalar strings, `text` a bare string or a
// non-empty array of unique non-empty string keys (both wire forms).
// Shared with the client-side PatchDataset preflight; returns
// a plain error — callers wrap with their own sentinel.
//
// The array branch deliberately re-checks entry types instead of
// decoding through schema.SearchTextFromAnyenc: the parser is
// tolerance-biased (a non-string entry becomes "" for the compile fold
// to flag), while a write gate owes the author the precise "entries
// must be strings". A bare "" stays admissible here — older
// handlers accepted it, so rejecting it at apply time would diverge
// on old-authored changes; the PatchDataset preflight is where the
// empty spellings get refused.
func CheckSearchLeafValue(path []string, payload *anyenc.Value) error {
	if len(path) > 0 && path[len(path)-1] == SearchKeyText {
		if payload != nil && payload.Type() == anyenc.TypeString {
			return nil
		}
		if payload != nil && payload.Type() == anyenc.TypeArray {
			arr, _ := payload.Array()
			keys := make([]string, 0, len(arr))
			for _, e := range arr {
				if e == nil || e.Type() != anyenc.TypeString {
					return fmt.Errorf("%q entries must be strings", strings.Join(path, "."))
				}
				keys = append(keys, string(e.GetStringBytes()))
			}
			return schema.ValidateSearchText(keys)
		}
		return fmt.Errorf("%q must be a string or an array of field keys", strings.Join(path, "."))
	}
	if payload == nil || payload.Type() != anyenc.TypeString {
		return fmt.Errorf("%q must be a string", strings.Join(path, "."))
	}
	return nil
}

// rejectIfMultiFieldTouchesDefPinned is rejectIfMultiFieldTouchesPinned
// for the dataset-def pinning rules.
func rejectIfMultiFieldTouchesDefPinned(op *crdt.Op) error {
	if op.Payload == nil || op.Payload.Type() != anyenc.TypeObject {
		return nil
	}
	obj, _ := op.Payload.Object()
	var hit error
	obj.Visit(func(k []byte, v *anyenc.Value) {
		if hit != nil {
			return
		}
		path := strings.Split(string(k), ".")
		if isDatasetDefPinnedPath(path) {
			hit = fmt.Errorf("%w: %q is pinned after first write", crdt.ErrValidation, string(k))
			return
		}
		if path[0] == DefFieldSearch {
			hit = checkSearchLeafOp(op.Type, path, v)
		}
	})
	return hit
}

// BeforeDelete classifies the removal as an important change (removed
// definitions must gate stale writers) and projects the removal row.
// Record data under a removed dataset is not cleaned up in v1 — the
// RemoveProperty stance.
func (DatasetDefsHandler) BeforeDelete(ctx *crdt.ChangeCtx, rec *crdt.RecordChange, sink *crdt.Sink) error {
	sink.Project(ShortIdsDataset, datasetRemovalShortIdRow(ctx.Change.ChangeId, rec.Id))
	return nil
}
