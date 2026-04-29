package spaceimpl

import (
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/space"
)

// modifyResultFromWrite converts the internal object.WriteResult
// into the public space.ModifyResult, translating per-op rejections
// from the crdt-package shape to the public-API shape (string Reason
// for callers that just want to render, plus ReasonErr for those
// who want errors.Is).
func modifyResultFromWrite(w object.WriteResult) space.ModifyResult {
	return space.ModifyResult{
		VersionId:  w.VersionId,
		ChangeId:   w.ChangeId,
		RecordIds:  w.RecordIds,
		Rejections: convertRejections(w.Rejections),
	}
}

func convertRejections(rs []crdt.OpRejection) []space.OpRejection {
	if len(rs) == 0 {
		return nil
	}
	out := make([]space.OpRejection, len(rs))
	for i, r := range rs {
		reason := ""
		if r.Err != nil {
			reason = r.Err.Error()
		}
		out[i] = space.OpRejection{
			RecordIndex: r.RecordIndex,
			RecordId:    r.RecordId,
			OpIndex:     r.OpIndex,
			Reason:      reason,
			ReasonErr:   r.Err,
		}
	}
	return out
}
