package history

import (
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
)

// Shared plumbing for every history walk (scratch replay, record fast
// path, index backfill): one decode conversion and one envelope-stamp
// rule, so a field added to the apply envelope cannot silently reach
// only some of the walks.

// decodeConvert returns the ChangeConvertFunc all history walks share:
// skip the root change (no CRDT payload) and empty payloads, tolerate
// non-CRDT payloads (settings changes, foreign data types) by skipping
// rather than failing the iteration. The codec must be private to the
// walk — its parser arena is reused across Decodes.
func decodeConvert(codec *object.Codec, rootId string) objecttree.ChangeConvertFunc {
	return func(ch *objecttree.Change, decrypted []byte) (any, error) {
		if ch.Id == rootId || len(decrypted) == 0 {
			return nil, nil
		}
		decoded, decodeErr := codec.Decode(decrypted)
		if decodeErr != nil {
			return nil, nil
		}
		return &decoded, nil
	}
}

// rootMeta extracts the object-constant envelope fields from the tree
// root: ObjectAuthor (root signer) and ObjectCreatedAt.
func rootMeta(tree objecttree.ReadableObjectTree) (author string, createdAt int64) {
	root := tree.Root()
	if root == nil {
		return "", 0
	}
	createdAt = root.Timestamp
	if root.Identity != nil {
		author = root.Identity.Account()
	}
	return author, createdAt
}

// stampEnvelope fills the decoded change's envelope from its tree
// change and the object-constant metadata — the same fields the live
// apply pipeline stamps, so scratch projections and index rows agree
// with the live projection. Live OrderIds are preserved by history
// trees, so _ver maps come out identical to what the live projection
// held when only these changes existed (proposal §4.1).
func stampEnvelope(decoded *crdt.Change, ch *objecttree.Change, objectId, objectAuthor string, objectCreatedAt int64) {
	decoded.ObjectId = objectId
	decoded.ChangeId = ch.Id
	decoded.AddSeq = ch.AddSeq
	decoded.Timestamp = ch.Timestamp
	decoded.VersionId = crdt.VersionId(ch.OrderId)
	decoded.PrevIds = ch.PreviousIds
	decoded.ObjectAuthor = objectAuthor
	decoded.ObjectCreatedAt = objectCreatedAt
	if ch.Identity != nil {
		decoded.Creator = ch.Identity.Account()
	}
}
