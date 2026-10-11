package object_patch

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	sdkpkg "github.com/deckhouse/module-sdk/pkg"
	jsonpatch "github.com/evanphx/json-patch/v5"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

// VerifyState is the verdict of re-checking an operation against the live
// object after an ambiguous API response.
type VerifyState string

const (
	// VerifyApplied: live object content proves the operation already took
	// effect; the operation must not be sent again.
	VerifyApplied VerifyState = "Applied"
	// VerifyNotApplied: live object proves the operation had no effect; it is
	// safe to send it again.
	VerifyNotApplied VerifyState = "NotApplied"
	// VerifyIncompatible: live content is incompatible with the operation
	// intent (object was changed by a different writer or rejects the patch).
	// The coordinator treats it as a terminal, queryable conflict.
	VerifyIncompatible VerifyState = "Incompatible"
	// VerifyUndecidable: intent cannot be checked (e.g. non-idempotent
	// JSONPatch without a recorded pre-image). Resending blindly is unsafe,
	// so it is handled like an incompatibility, with a distinct reason.
	VerifyUndecidable VerifyState = "Undecidable"
	// VerifyGetUncertain: the verification Get itself failed ambiguously; no
	// decision is possible yet, retry later.
	VerifyGetUncertain VerifyState = "GetUncertain"
)

// Terminal reports whether the verdict ends the operation without resending.
func (s VerifyState) Terminal() bool {
	return s == VerifyIncompatible || s == VerifyUndecidable
}

// VerifyEvidence holds what was recorded at send time and is needed to verify.
type VerifyEvidence struct {
	// PreImage is the object content captured before the send. Required to
	// prove non-idempotent JSONPatch application; optional otherwise.
	PreImage *unstructured.Unstructured
	// PostResourceVersion is the RV of the object returned by the write call.
	PostResourceVersion string
}

// VerifyResult carries the verdict plus the live object RV observed while
// verifying (useful for journaling).
type VerifyResult struct {
	State           VerifyState
	ResourceVersion string
	Reason          string
}

// OperationVerifier re-reads the target object and decides whether an
// operation after an ambiguous outcome was actually applied.
type OperationVerifier struct {
	patcher *ObjectPatcher
}

func NewOperationVerifier(patcher *ObjectPatcher) *OperationVerifier {
	return &OperationVerifier{patcher: patcher}
}

// VerifyOperation re-checks a single operation against the live object.
func (v *OperationVerifier) VerifyOperation(ctx context.Context, op sdkpkg.PatchCollectorOperation, evidence VerifyEvidence) *VerifyResult {
	if op == nil {
		return &VerifyResult{State: VerifyApplied}
	}

	live, err := v.patcher.GetOperationObject(ctx, op)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return v.verifyMissing(op)
		}
		state := VerifyGetUncertain
		if classifyRequestError(err).Terminal() {
			// A deterministic rejection on Get (e.g. malformed GVK) is terminal.
			state = VerifyUndecidable
		}
		return &VerifyResult{State: state, Reason: err.Error()}
	}

	res := &VerifyResult{ResourceVersion: live.GetResourceVersion()}

	// RV fast path: the object did not change since the write response.
	if evidence.PostResourceVersion != "" && live.GetResourceVersion() == evidence.PostResourceVersion {
		res.State = VerifyApplied
		res.Reason = "object resourceVersion matches the write response"
		return res
	}

	switch o := op.(type) {
	case *createOperation:
		return v.verifyCreate(o, live, res)
	case *deleteOperation:
		// Object still exists; foreground deletion may already be in progress.
		if live.GetDeletionTimestamp() != nil {
			res.State = VerifyApplied
			res.Reason = "object is being deleted"
			return res
		}
		res.State = VerifyNotApplied
		res.Reason = "object still exists without deletionTimestamp"
		return res
	case *patchOperation:
		if o.hasFilterFn() {
			return v.verifyFilter(o, live, evidence, res)
		}
		if o.patchType == types.JSONPatchType {
			return v.verifyJSONPatch(o, live, evidence, res)
		}
		return v.verifyMergePatch(o, live, evidence, res)
	}

	res.State = VerifyUndecidable
	res.Reason = fmt.Sprintf("unsupported operation type %T", op)
	return res
}

// verifyMissing decides when the live object is absent.
func (v *OperationVerifier) verifyMissing(op sdkpkg.PatchCollectorOperation) *VerifyResult {
	switch o := op.(type) {
	case *deleteOperation:
		// Absence is the desired end state.
		return &VerifyResult{State: VerifyApplied, Reason: "object is absent"}
	case *createOperation:
		if o.ignoreIfExists {
			// CreateIfNotExists only writes when the object is absent: with
			// the object now missing, re-issuing it provably cannot create a
			// duplicate or overwrite anything.
			return &VerifyResult{State: VerifyNotApplied, Reason: "object absent: create-if-not-exists is safe to resend"}
		}
		return &VerifyResult{State: VerifyNotApplied, Reason: "object does not exist"}
	case *patchOperation:
		if o.ignoreMissingObject {
			return &VerifyResult{State: VerifyApplied, Reason: "object missing with IgnoreMissingObject"}
		}
		return &VerifyResult{State: VerifyNotApplied, Reason: "object does not exist"}
	}
	return &VerifyResult{State: VerifyNotApplied, Reason: "object does not exist"}
}

func (v *OperationVerifier) verifyCreate(op *createOperation, live *unstructured.Unstructured, res *VerifyResult) *VerifyResult {
	if op.ignoreIfExists {
		// CreateIfNotExists: any existing object satisfies the intent.
		res.State = VerifyApplied
		res.Reason = "object exists (create-if-not-exists)"
		return res
	}

	intended, err := toUnstructured(op.object)
	if err != nil {
		res.State = VerifyUndecidable
		res.Reason = err.Error()
		return res
	}

	if mapSubset(intended.Object, live.Object) {
		res.State = VerifyApplied
		res.Reason = "existing object contains the intended content"
		return res
	}
	res.State = VerifyIncompatible
	res.Reason = "existing object content differs from the create intent"
	return res
}

func (v *OperationVerifier) verifyMergePatch(op *patchOperation, live *unstructured.Unstructured, evidence VerifyEvidence, res *VerifyResult) *VerifyResult {
	patchBytes, err := convertPatchToBytes(op.patch)
	if err != nil {
		res.State = VerifyUndecidable
		res.Reason = fmt.Sprintf("encode merge patch: %v", err)
		return res
	}
	var mergeDoc map[string]any
	if err := json.Unmarshal(patchBytes, &mergeDoc); err != nil {
		res.State = VerifyUndecidable
		res.Reason = fmt.Sprintf("decode merge patch: %v", err)
		return res
	}

	if mergePatchSatisfied(mergeDoc, live.Object) {
		res.State = VerifyApplied
		res.Reason = "live object contains every merge-patch leaf value"
		return res
	}

	// The intended leaves are absent from live. With a pre-image we can tell
	// "the request never landed" (live still equals the pre-image on every
	// patched path; resending an idempotent merge patch is safe) apart from
	// "another writer changed these paths" (resending would overwrite newer
	// content -> terminal conflict).
	var pre map[string]any
	if evidence.PreImage != nil {
		pre = evidence.PreImage.Object
	}
	if pre == nil {
		res.State = VerifyUndecidable
		res.Reason = "merge patch cannot be proven safe to resend without a pre-image"
		return res
	}
	if mergePathsUnchanged(mergeDoc, pre, live.Object) {
		res.State = VerifyNotApplied
		res.Reason = "merge-patch paths are unchanged since the pre-image"
		return res
	}
	res.State = VerifyIncompatible
	res.Reason = "merge-patch paths were changed by another writer"
	return res
}

func (v *OperationVerifier) verifyJSONPatch(op *patchOperation, live *unstructured.Unstructured, evidence VerifyEvidence, res *VerifyResult) *VerifyResult {
	if evidence.PreImage == nil {
		// Without a pre-image a non-idempotent RFC6902 patch can never be
		// proven applied or safe to resend; stay conservative.
		res.State = VerifyUndecidable
		res.Reason = "JSONPatch cannot be verified without a pre-image"
		return res
	}

	patchBytes, err := convertPatchToBytes(op.patch)
	if err != nil {
		res.State = VerifyUndecidable
		res.Reason = fmt.Sprintf("encode json patch: %v", err)
		return res
	}
	decoded, err := jsonpatch.DecodePatch(patchBytes)
	if err != nil {
		res.State = VerifyUndecidable
		res.Reason = fmt.Sprintf("decode json patch: %v", err)
		return res
	}

	preBytes, err := json.Marshal(evidence.PreImage.Object)
	if err != nil {
		res.State = VerifyUndecidable
		res.Reason = fmt.Sprintf("marshal pre-image: %v", err)
		return res
	}
	expectedBytes, err := decoded.Apply(preBytes)
	if err != nil {
		res.State = VerifyUndecidable
		res.Reason = fmt.Sprintf("replay json patch on pre-image: %v", err)
		return res
	}
	var expected map[string]any
	if err := json.Unmarshal(expectedBytes, &expected); err != nil {
		res.State = VerifyUndecidable
		res.Reason = fmt.Sprintf("decode expected object: %v", err)
		return res
	}

	// Verify exactly the leaf changes the patch performs, not the whole
	// object: the apiserver/admission chain legitimately mutates untouched
	// paths (defaulting, injected labels/annotations, server metadata) on
	// every accepted write, so a full-object comparison would false-report a
	// successful write as an irreconcilable conflict.
	return verifyChangedLeaves(
		"json patch replayed on the pre-image",
		evidence.PreImage.Object, expected, live.Object, res)
}

func (v *OperationVerifier) verifyFilter(op *patchOperation, live *unstructured.Unstructured, evidence VerifyEvidence, res *VerifyResult) *VerifyResult {
	if op.filterFunc == nil {
		res.State = VerifyUndecidable
		res.Reason = "FilterFunc is nil"
		return res
	}

	// With a pre-image, recompute the filter against the pre-write object:
	// this stays correct for non-idempotent filters (e.g. list-append JQ),
	// whose output never equals the live object by recomputation alone.
	if evidence.PreImage != nil {
		intended, err := op.filterFunc(evidence.PreImage)
		if err != nil || intended == nil {
			res.State = VerifyGetUncertain
			res.Reason = fmt.Sprintf("recompute filter on pre-image: %v", err)
			return res
		}
		// Verify only the leaves the filter changes against live; unrelated
		// admission/defaulting mutations must not turn a success into a
		// conflict.
		return verifyChangedLeaves(
			"filter applied to the pre-image",
			evidence.PreImage.Object, intended.Object, live.Object, res)
	}

	// No pre-image (older journals, best-effort capture failure): fall back to
	// the conservative idempotent-recompute check. A difference here may mean
	// "never landed" or "moved by another writer" and cannot be resolved, so it
	// stays terminal.
	filtered, err := op.filterFunc(live)
	if err != nil {
		res.State = VerifyIncompatible
		res.Reason = fmt.Sprintf("recompute filter on live object: %v", err)
		return res
	}
	if equality.Semantic.DeepEqual(live, filtered) {
		res.State = VerifyApplied
		res.Reason = "recomputed filter output equals the live object"
		return res
	}
	res.State = VerifyIncompatible
	res.Reason = "live object differs from the recomputed filter output"
	return res
}

// mergePatchSatisfied walks a JSON merge patch document and asserts every
// leaf value (null means "key absent") is present in the live object content.
func mergePatchSatisfied(patchDoc, live map[string]any) bool {
	for key, intended := range patchDoc {
		switch sub := intended.(type) {
		case nil:
			if _, ok := live[key]; ok {
				return false
			}
		case map[string]any:
			liveSub, ok := live[key].(map[string]any)
			if !ok {
				return false
			}
			if !mergePatchSatisfied(sub, liveSub) {
				return false
			}
		default:
			if !jsonEqual(intended, live[key]) {
				return false
			}
		}
	}
	return true
}

// mapSubset reports whether every value present in intended (recursively for
// maps) is equal in actual. Server-filled metadata fields absent from intended
// are ignored this way.
func mapSubset(intended, actual any) bool {
	switch i := intended.(type) {
	case map[string]any:
		a, ok := actual.(map[string]any)
		if !ok {
			return false
		}
		for k, val := range i {
			if !mapSubset(val, a[k]) {
				return false
			}
		}
		return true
	default:
		return jsonEqual(intended, actual)
	}
}

// jsonLeafIntent is one changed leaf (at an RFC6901 JSON pointer) derived by
// diffing the pre-image against the intended post-write object. Delete means
// the pointer must be absent; otherwise Value must be present and equal.
type jsonLeafIntent struct {
	Value  any
	Delete bool
}

// verifyChangedLeaves decides Applied/NotApplied/Incompatible from the leaf
// changes between the pre-image and the intended object only:
//
//   - Applied: live satisfies every changed leaf (the write landed); leaves
//     outside the change set are ignored, so server/admission mutations of
//     untouched paths cannot force a false conflict.
//   - NotApplied: live is identical to the pre-image on every changed leaf;
//     replaying the original (never-executed) operation is then safe even for
//     non-idempotent ops.
//   - Incompatible: a changed leaf was modified by another writer.
func verifyChangedLeaves(intentLabel string, pre, intended, live map[string]any, res *VerifyResult) *VerifyResult {
	changes := leafChanges(pre, intended)
	if len(changes) == 0 {
		res.State = VerifyApplied
		res.Reason = intentLabel + " performs no content changes"
		return res
	}
	if changedLeavesSatisfied(live, changes) {
		res.State = VerifyApplied
		res.Reason = "live object satisfies every changed leaf of the " + intentLabel
		return res
	}
	if changedLeavesUnchanged(pre, live, changes) {
		res.State = VerifyNotApplied
		res.Reason = "live object is unchanged at every changed leaf since the pre-image"
		return res
	}
	res.State = VerifyIncompatible
	res.Reason = "a changed leaf of the " + intentLabel + " was modified by another writer"
	return res
}

// leafChanges returns every JSON-pointer leaf whose value differs between
// pre and intended. A key present in pre but absent in intended becomes a
// deletion intent. Arrays (and scalars) are compared atomically.
func leafChanges(pre, intended map[string]any) map[string]jsonLeafIntent {
	changes := map[string]jsonLeafIntent{}
	collectLeafChanges("", pre, intended, changes)
	return changes
}

func collectLeafChanges(ptr string, pre, intended any, out map[string]jsonLeafIntent) {
	preMap, preIsMap := pre.(map[string]any)
	intMap, intIsMap := intended.(map[string]any)
	switch {
	case preIsMap && intIsMap:
		keys := map[string]struct{}{}
		for k := range preMap {
			keys[k] = struct{}{}
		}
		for k := range intMap {
			keys[k] = struct{}{}
		}
		for k := range keys {
			child := joinJSONPointer(ptr, k)
			preVal, preOK := preMap[k]
			intVal, intOK := intMap[k]
			if !intOK {
				// Key absent in the intended document: deletion of the
				// subtree/key, satisfied when the pointer is gone from live.
				out[child] = jsonLeafIntent{Delete: true}
				continue
			}
			if preOK {
				collectLeafChanges(child, preVal, intVal, out)
			} else {
				collectLeafChanges(child, nil, intVal, out)
			}
		}
	case !preIsMap && intIsMap:
		// An object subtree is added (or a node changed type into an object):
		// descend against an empty pre-image so each newly set leaf gets its
		// own path instead of comparing the whole subtree atomically.
		collectLeafChanges(ptr, map[string]any{}, intended, out)
	default:
		// Scalar/slice/null (including subtree replaced by a scalar): the
		// value is compared atomically. An explicit null is a set-to-null,
		// distinct from key deletion handled in the map branch.
		out[ptr] = jsonLeafIntent{Value: intended}
	}
}

// changedLeavesSatisfied reports whether live fulfills every leaf intent.
func changedLeavesSatisfied(live map[string]any, changes map[string]jsonLeafIntent) bool {
	for ptr, intent := range changes {
		got, present := jsonPointerValue(live, ptr)
		if intent.Delete {
			if present {
				return false
			}
			continue
		}
		if !present || !jsonEqual(intent.Value, got) {
			return false
		}
	}
	return true
}

// changedLeavesUnchanged reports whether live still equals the pre-image at
// every changed leaf, i.e. the write provably never touched those paths.
func changedLeavesUnchanged(pre, live map[string]any, changes map[string]jsonLeafIntent) bool {
	for ptr := range changes {
		preVal, prePresent := jsonPointerValue(pre, ptr)
		liveVal, livePresent := jsonPointerValue(live, ptr)
		if prePresent != livePresent {
			return false
		}
		if prePresent && !jsonEqual(preVal, liveVal) {
			return false
		}
	}
	return true
}

// joinJSONPointer appends an escaped key to an RFC6901 pointer.
func joinJSONPointer(ptr, key string) string {
	key = strings.ReplaceAll(key, "~", "~0")
	key = strings.ReplaceAll(key, "/", "~1")
	return ptr + "/" + key
}

// jsonPointerValue resolves an RFC6901 pointer against generic JSON content.
// The empty pointer addresses the document root.
func jsonPointerValue(doc any, pointer string) (any, bool) {
	if pointer == "" {
		return doc, true
	}
	cur := doc
	for _, raw := range strings.Split(pointer, "/")[1:] {
		seg := strings.ReplaceAll(raw, "~1", "/")
		seg = strings.ReplaceAll(seg, "~0", "~")
		switch node := cur.(type) {
		case map[string]any:
			v, ok := node[seg]
			if !ok {
				return nil, false
			}
			cur = v
		case []any:
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 || idx >= len(node) {
				return nil, false
			}
			cur = node[idx]
		default:
			return nil, false
		}
	}
	return cur, true
}

// mergePathsUnchanged reports whether every path touched by patchDoc has the
// same content in live as in the pre-image (i.e. the merge patch provably had
// no effect). It only descends into paths the patch touches, so unrelated
// concurrent modifications elsewhere do not force a conflict verdict.
func mergePathsUnchanged(patchDoc, pre, live map[string]any) bool {
	for key, intended := range patchDoc {
		switch sub := intended.(type) {
		case nil:
			// A null leaf deletes the key. Unchanged means equal presence AND,
			// when the key survives in both images, equal value: a concurrent
			// writer that replaced the value must not have its value deleted.
			preVal, prePresent := pre[key]
			liveVal, livePresent := live[key]
			if prePresent != livePresent {
				return false
			}
			if prePresent && !jsonEqual(preVal, liveVal) {
				return false
			}
		case map[string]any:
			preSub, preOK := pre[key].(map[string]any)
			liveSub, liveOK := live[key].(map[string]any)
			if preOK != liveOK || (preOK && !mergePathsUnchanged(sub, preSub, liveSub)) {
				return false
			}
		default:
			if !jsonEqual(pre[key], live[key]) {
				return false
			}
		}
	}
	return true
}

func jsonEqual(a, b any) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return string(ab) == string(bb)
}
