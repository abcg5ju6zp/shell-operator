package object_patch

import (
	sdkpkg "github.com/deckhouse/module-sdk/pkg"
	"k8s.io/apimachinery/pkg/types"
)

// NeedsPreImage reports whether safe verification of an ambiguous send
// requires capturing the object content BEFORE the request with an extra Get.
//
// Plain Merge and RFC6902 JSON patches issue a single PATCH without reading
// the object first, so their pre-image must be fetched explicitly. JSONPatch
// may be non-idempotent; merge patches need the pre-image to distinguish a
// request that never arrived from a concurrent write. Filter and
// create-or-update paths read the object internally and attach it to the
// OperationOutcome, so no extra Get is needed.
func NeedsPreImage(op sdkpkg.PatchCollectorOperation) bool {
	p, ok := op.(*patchOperation)
	if !ok {
		return false
	}
	if p.hasFilterFn() {
		return false
	}
	return p.patchType == types.JSONPatchType || p.patchType == types.MergePatchType
}
