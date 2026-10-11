package object_patch

import (
	"context"
	"testing"
	"time"

	sdkpkg "github.com/deckhouse/module-sdk/pkg"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/flant/kube-client/fake"
)

const verifierNS = "default"

func verifierFor(t *testing.T, objects ...string) (*OperationVerifier, *ObjectPatcher, *fake.Cluster) {
	t.Helper()
	cluster := newFakeClusterWithNamespaceAndObjects(t, verifierNS, objects...)
	patcher := NewObjectPatcher(cluster.Client, nopLogger())
	return NewOperationVerifier(patcher), patcher, cluster
}

func cmManifest(name string, kv ...string) string {
	data := ""
	for i := 0; i+1 < len(kv); i += 2 {
		data += "  " + kv[i] + ": \"" + kv[i+1] + "\"\n"
	}
	return `
apiVersion: v1
kind: ConfigMap
metadata:
  namespace: default
  name: ` + name + `
data:
` + data
}

func Test_Verifier_MergePatch(t *testing.T) {
	ctx := context.Background()

	t.Run("intent present is Applied", func(t *testing.T) {
		v, _, _ := verifierFor(t, cmManifest("testcm", "foo", "bar", "baz", "quux"))
		op := NewMergePatchOperation(`{"data":{"baz":"quux"}}`, "v1", "ConfigMap", verifierNS, "testcm")
		res := v.VerifyOperation(ctx, op, VerifyEvidence{})
		require.Equal(t, VerifyApplied, res.State, res.Reason)
	})

	t.Run("intent absent without pre-image is Undecidable", func(t *testing.T) {
		v, _, _ := verifierFor(t, cmManifest("testcm", "foo", "bar"))
		op := NewMergePatchOperation(`{"data":{"baz":"quux"}}`, "v1", "ConfigMap", verifierNS, "testcm")
		res := v.VerifyOperation(ctx, op, VerifyEvidence{})
		require.Equal(t, VerifyUndecidable, res.State, res.Reason)
	})

	t.Run("intent absent and paths unchanged is NotApplied", func(t *testing.T) {
		pre := cmManifest("testcm", "foo", "bar")
		v, _, _ := verifierFor(t, cmManifest("testcm", "foo", "bar"))
		op := NewMergePatchOperation(`{"data":{"baz":"quux"}}`, "v1", "ConfigMap", verifierNS, "testcm")
		res := v.VerifyOperation(ctx, op, VerifyEvidence{PreImage: mustManifest(t, pre).Unstructured()})
		require.Equal(t, VerifyNotApplied, res.State, res.Reason)
	})

	t.Run("intent absent and paths changed is Incompatible", func(t *testing.T) {
		// A different writer already touched the patched leaf with another value.
		pre := cmManifest("testcm", "baz", "old")
		v, _, _ := verifierFor(t, cmManifest("testcm", "baz", "other"))
		op := NewMergePatchOperation(`{"data":{"baz":"quux"}}`, "v1", "ConfigMap", verifierNS, "testcm")
		res := v.VerifyOperation(ctx, op, VerifyEvidence{PreImage: mustManifest(t, pre).Unstructured()})
		require.Equal(t, VerifyIncompatible, res.State, res.Reason)
	})

	t.Run("unrelated concurrent change still verifies NotApplied", func(t *testing.T) {
		// Another writer changed an untouched sibling leaf; patched paths are
		// unchanged -> resending the idempotent merge is safe.
		pre := cmManifest("testcm", "foo", "bar")
		v, _, _ := verifierFor(t, cmManifest("testcm", "foo", "bar", "other", "x"))
		op := NewMergePatchOperation(`{"data":{"baz":"quux"}}`, "v1", "ConfigMap", verifierNS, "testcm")
		res := v.VerifyOperation(ctx, op, VerifyEvidence{PreImage: mustManifest(t, pre).Unstructured()})
		require.Equal(t, VerifyNotApplied, res.State, res.Reason)
	})

	t.Run("null delete satisfied when key gone", func(t *testing.T) {
		v, _, _ := verifierFor(t, cmManifest("testcm", "baz", "quux"))
		op := NewMergePatchOperation(`{"data":{"foo":null}}`, "v1", "ConfigMap", verifierNS, "testcm")
		res := v.VerifyOperation(ctx, op, VerifyEvidence{})
		require.Equal(t, VerifyApplied, res.State, res.Reason)
	})

	t.Run("null delete with key remaining is NotApplied when unchanged since pre-image", func(t *testing.T) {
		pre := cmManifest("testcm", "foo", "bar")
		v, _, _ := verifierFor(t, cmManifest("testcm", "foo", "bar"))
		op := NewMergePatchOperation(`{"data":{"foo":null}}`, "v1", "ConfigMap", verifierNS, "testcm")
		res := v.VerifyOperation(ctx, op, VerifyEvidence{PreImage: mustManifest(t, pre).Unstructured()})
		require.Equal(t, VerifyNotApplied, res.State, res.Reason)
	})

	t.Run("null delete against a concurrent value replacement is Incompatible", func(t *testing.T) {
		// The merge patch deletes data.foo; the pre-image had foo="bar", but
		// another writer replaced it with foo="other" while the send was
		// ambiguous. Replaying the null leaf would erase the newer value, so
		// presence alone must not count as "unchanged": this is a conflict.
		pre := cmManifest("testcm", "foo", "bar")
		v, _, _ := verifierFor(t, cmManifest("testcm", "foo", "other"))
		op := NewMergePatchOperation(`{"data":{"foo":null}}`, "v1", "ConfigMap", verifierNS, "testcm")
		res := v.VerifyOperation(ctx, op, VerifyEvidence{PreImage: mustManifest(t, pre).Unstructured()})
		require.Equal(t, VerifyIncompatible, res.State, res.Reason)
	})

	t.Run("top-level null delete against concurrent replacement is Incompatible", func(t *testing.T) {
		pre := `
apiVersion: v1
kind: ConfigMap
metadata:
  namespace: default
  name: testcm
data:
  foo: "bar"
`
		live := `
apiVersion: v1
kind: ConfigMap
metadata:
  namespace: default
  name: testcm
data:
  foo: "other"
`
		v, _, _ := verifierFor(t, live)
		op := NewMergePatchOperation(`{"data":null}`, "v1", "ConfigMap", verifierNS, "testcm")
		res := v.VerifyOperation(ctx, op, VerifyEvidence{PreImage: mustManifest(t, pre).Unstructured()})
		require.Equal(t, VerifyIncompatible, res.State, res.Reason)
	})

	t.Run("missing object tolerated is Applied", func(t *testing.T) {
		v, _, _ := verifierFor(t, cmManifest("testcm"))
		op := NewMergePatchOperation(`{"data":{"baz":"quux"}}`, "v1", "ConfigMap", verifierNS, "ghost", WithIgnoreMissingObject())
		res := v.VerifyOperation(ctx, op, VerifyEvidence{})
		require.Equal(t, VerifyApplied, res.State, res.Reason)
	})

	t.Run("missing object plain is NotApplied", func(t *testing.T) {
		v, _, _ := verifierFor(t, cmManifest("testcm"))
		op := NewMergePatchOperation(`{"data":{"baz":"quux"}}`, "v1", "ConfigMap", verifierNS, "ghost")
		res := v.VerifyOperation(ctx, op, VerifyEvidence{})
		require.Equal(t, VerifyNotApplied, res.State, res.Reason)
	})
}

func Test_Verifier_JSONPatch(t *testing.T) {
	ctx := context.Background()
	jsonOp := func() sdkpkg.PatchCollectorOperation {
		return NewJSONPatchOperation([]any{
			map[string]any{"op": "add", "path": "/data/baz", "value": "quux"},
		}, "v1", "ConfigMap", verifierNS, "testcm")
	}

	t.Run("replay matches live is Applied", func(t *testing.T) {
		pre := cmManifest("testcm", "foo", "bar")
		v, _, _ := verifierFor(t, cmManifest("testcm", "foo", "bar", "baz", "quux"))
		res := v.VerifyOperation(ctx, jsonOp(), VerifyEvidence{PreImage: mustManifest(t, pre).Unstructured()})
		require.Equal(t, VerifyApplied, res.State, res.Reason)
	})

	t.Run("replay differs is Incompatible", func(t *testing.T) {
		pre := cmManifest("testcm", "foo", "bar")
		v, _, _ := verifierFor(t, cmManifest("testcm", "foo", "bar", "baz", "other"))
		res := v.VerifyOperation(ctx, jsonOp(), VerifyEvidence{PreImage: mustManifest(t, pre).Unstructured()})
		require.Equal(t, VerifyIncompatible, res.State, res.Reason)
	})

	t.Run("applied despite server-managed metadata changes", func(t *testing.T) {
		// Real apiserver bumps resourceVersion/uid/generation/managedFields on
		// every accepted write; user content matches the replayed patch.
		pre := cmManifest("testcm", "foo", "bar")
		live := `
apiVersion: v1
kind: ConfigMap
metadata:
  namespace: default
  name: testcm
  uid: 7a1b-9c2d
  resourceVersion: "9999"
  generation: 3
  creationTimestamp: "2025-01-01T00:00:00Z"
  managedFields:
  - manager: kube-apiserval
    operation: Update
data:
  foo: "bar"
  baz: "quux"
`
		v, _, _ := verifierFor(t, live)
		res := v.VerifyOperation(ctx, jsonOp(), VerifyEvidence{PreImage: mustManifest(t, pre).Unstructured()})
		require.Equal(t, VerifyApplied, res.State, res.Reason)
	})

	t.Run("unchanged since pre-image is NotApplied", func(t *testing.T) {
		pre := cmManifest("testcm", "foo", "bar")
		v, _, _ := verifierFor(t, cmManifest("testcm", "foo", "bar"))
		res := v.VerifyOperation(ctx, jsonOp(), VerifyEvidence{PreImage: mustManifest(t, pre).Unstructured()})
		require.Equal(t, VerifyNotApplied, res.State, res.Reason)
	})

	t.Run("applied despite admission/defaulting mutations on untouched paths", func(t *testing.T) {
		// The write landed (data.baz=quux), but the admission chain also
		// injected a label and a defaulted field and the apiserver bumped
		// metadata: verification follows the patched leaves only and must
		// still verdict Applied instead of a false terminal conflict.
		pre := cmManifest("testcm", "foo", "bar")
		live := `
apiVersion: v1
kind: ConfigMap
metadata:
  namespace: default
  name: testcm
  labels:
    sidecar.istio.io/inject: "true"
  resourceVersion: "77"
data:
  foo: "bar"
  baz: "quux"
  injected-by-admission: "default"
`
		v, _, _ := verifierFor(t, live)
		res := v.VerifyOperation(ctx, jsonOp(), VerifyEvidence{PreImage: mustManifest(t, pre).Unstructured()})
		require.Equal(t, VerifyApplied, res.State, res.Reason)
	})

	t.Run("not applied even with admission mutations when patched leaf absent", func(t *testing.T) {
		// Admission touched the object but the patched leaf never landed:
		// resending the original (never-executed) op is safe.
		pre := cmManifest("testcm", "foo", "bar")
		live := `
apiVersion: v1
kind: ConfigMap
metadata:
  namespace: default
  name: testcm
  labels:
    injected: "yes"
data:
  foo: "bar"
  unrelated: "x"
`
		v, _, _ := verifierFor(t, live)
		res := v.VerifyOperation(ctx, jsonOp(), VerifyEvidence{PreImage: mustManifest(t, pre).Unstructured()})
		require.Equal(t, VerifyNotApplied, res.State, res.Reason)
	})

	t.Run("no pre-image is Undecidable", func(t *testing.T) {
		v, _, _ := verifierFor(t, cmManifest("testcm", "foo", "bar", "baz", "quux"))
		res := v.VerifyOperation(ctx, jsonOp(), VerifyEvidence{})
		require.Equal(t, VerifyUndecidable, res.State, res.Reason)
	})
}

func Test_Verifier_Create(t *testing.T) {
	ctx := context.Background()
	intended := cmManifest("testcm", "foo", "bar")

	t.Run("plain create existing matching subset is Applied", func(t *testing.T) {
		v, _, _ := verifierFor(t, cmManifest("testcm", "foo", "bar", "extra", "server"))
		op := NewCreateOperation(mustManifest(t, intended).Unstructured())
		res := v.VerifyOperation(ctx, op, VerifyEvidence{})
		require.Equal(t, VerifyApplied, res.State, res.Reason)
	})

	t.Run("plain create existing mismatch is Incompatible", func(t *testing.T) {
		v, _, _ := verifierFor(t, cmManifest("testcm", "foo", "different"))
		op := NewCreateOperation(mustManifest(t, intended).Unstructured())
		res := v.VerifyOperation(ctx, op, VerifyEvidence{})
		require.Equal(t, VerifyIncompatible, res.State, res.Reason)
	})

	t.Run("create-if-not-exists tolerates any object", func(t *testing.T) {
		v, _, _ := verifierFor(t, cmManifest("testcm", "whatever", "x"))
		op := NewCreateIfNotExistsOperation(mustManifest(t, intended).Unstructured())
		res := v.VerifyOperation(ctx, op, VerifyEvidence{})
		require.Equal(t, VerifyApplied, res.State, res.Reason)
	})

	t.Run("create missing is NotApplied", func(t *testing.T) {
		v, _, _ := verifierFor(t, cmManifest("other"))
		op := NewCreateOperation(mustManifest(t, intended).Unstructured())
		res := v.VerifyOperation(ctx, op, VerifyEvidence{})
		require.Equal(t, VerifyNotApplied, res.State, res.Reason)
	})
}

func Test_Verifier_Delete(t *testing.T) {
	ctx := context.Background()

	t.Run("absent is Applied", func(t *testing.T) {
		v, _, _ := verifierFor(t, cmManifest("other"))
		op := NewDeleteOperation("v1", "ConfigMap", verifierNS, "testcm")
		res := v.VerifyOperation(ctx, op, VerifyEvidence{})
		require.Equal(t, VerifyApplied, res.State)
	})

	t.Run("existing plain is NotApplied", func(t *testing.T) {
		v, patcher, _ := verifierFor(t, cmManifest("testcm", "foo", "bar"))
		op := NewDeleteOperation("v1", "ConfigMap", verifierNS, "testcm")
		res := v.VerifyOperation(ctx, op, VerifyEvidence{})
		require.Equal(t, VerifyNotApplied, res.State, res.Reason)
		// sanity: patcher is wired
		require.NotNil(t, patcher)
	})

	t.Run("deletionTimestamp set is Applied", func(t *testing.T) {
		cluster := newFakeClusterWithNamespaceAndObjects(t, verifierNS, cmManifest("testcm", "foo", "bar"))
		fdc := cluster.Client.Dynamic().(*fakedynamic.FakeDynamicClient)
		ts := metav1.NewTime(time.Now())
		fdc.PrependReactor("get", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
			u := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"namespace":         verifierNS,
					"name":              "testcm",
					"deletionTimestamp": ts.Format(time.RFC3339),
				},
			}}
			return true, u, nil
		})
		v := NewOperationVerifier(NewObjectPatcher(cluster.Client, nopLogger()))
		op := NewDeleteOperation("v1", "ConfigMap", verifierNS, "testcm")
		res := v.VerifyOperation(ctx, op, VerifyEvidence{})
		require.Equal(t, VerifyApplied, res.State, res.Reason)
	})
}

func Test_Verifier_Filter(t *testing.T) {
	ctx := context.Background()
	addBaz := func(u *unstructured.Unstructured) (*unstructured.Unstructured, error) {
		res := u.DeepCopy()
		data, _ := res.Object["data"].(map[string]any)
		if data == nil {
			data = map[string]any{}
		}
		data["baz"] = "quux"
		res.Object["data"] = data
		return res, nil
	}

	t.Run("recompute equals live is Applied", func(t *testing.T) {
		v, _, _ := verifierFor(t, cmManifest("testcm", "foo", "bar", "baz", "quux"))
		op := NewPatchWithMutatingFuncOperation(addBaz, "v1", "ConfigMap", verifierNS, "testcm")
		res := v.VerifyOperation(ctx, op, VerifyEvidence{})
		require.Equal(t, VerifyApplied, res.State, res.Reason)
	})

	t.Run("recompute differs is Incompatible", func(t *testing.T) {
		v, _, _ := verifierFor(t, cmManifest("testcm", "foo", "bar"))
		op := NewPatchWithMutatingFuncOperation(addBaz, "v1", "ConfigMap", verifierNS, "testcm")
		res := v.VerifyOperation(ctx, op, VerifyEvidence{})
		require.Equal(t, VerifyIncompatible, res.State, res.Reason)
	})

	// Non-idempotent filter (e.g. a JQ that appends to a list): plain
	// recomputation against live can never prove application, so the verdict
	// must be derived against the captured pre-image.
	appendItem := func(u *unstructured.Unstructured) (*unstructured.Unstructured, error) {
		res := u.DeepCopy()
		items, _ := res.Object["items"].([]any)
		items = append(items, "x")
		res.Object["items"] = items
		return res, nil
	}
	objManifest := func(items ...string) string {
		l := ""
		for _, it := range items {
			l += "  - " + it + "\n"
		}
		return `
apiVersion: v1
kind: ConfigMap
metadata:
  namespace: default
  name: testcm
items:
` + l
	}

	t.Run("non-idempotent filter applied matches pre-image rework", func(t *testing.T) {
		pre := objManifest()
		v, _, _ := verifierFor(t, objManifest("x"))
		op := NewPatchWithMutatingFuncOperation(appendItem, "v1", "ConfigMap", verifierNS, "testcm")
		res := v.VerifyOperation(ctx, op, VerifyEvidence{PreImage: mustManifest(t, pre).Unstructured()})
		require.Equal(t, VerifyApplied, res.State, res.Reason)
	})

	t.Run("non-idempotent filter unchanged is NotApplied", func(t *testing.T) {
		pre := objManifest("a")
		v, _, _ := verifierFor(t, objManifest("a"))
		op := NewPatchWithMutatingFuncOperation(appendItem, "v1", "ConfigMap", verifierNS, "testcm")
		res := v.VerifyOperation(ctx, op, VerifyEvidence{PreImage: mustManifest(t, pre).Unstructured()})
		require.Equal(t, VerifyNotApplied, res.State, res.Reason)
	})

	t.Run("non-idempotent filter moved by another writer is Incompatible", func(t *testing.T) {
		pre := objManifest()
		v, _, _ := verifierFor(t, objManifest("y", "z"))
		op := NewPatchWithMutatingFuncOperation(appendItem, "v1", "ConfigMap", verifierNS, "testcm")
		res := v.VerifyOperation(ctx, op, VerifyEvidence{PreImage: mustManifest(t, pre).Unstructured()})
		require.Equal(t, VerifyIncompatible, res.State, res.Reason)
	})

	t.Run("filter applied despite admission-injected sibling fields", func(t *testing.T) {
		// The filter appended "x"; admission also injected an unrelated label.
		// Only the filter's changed leaves determine the verdict.
		pre := objManifest()
		live := `
apiVersion: v1
kind: ConfigMap
metadata:
  namespace: default
  name: testcm
  labels:
    injected: "yes"
items:
  - x
`
		v, _, _ := verifierFor(t, live)
		op := NewPatchWithMutatingFuncOperation(appendItem, "v1", "ConfigMap", verifierNS, "testcm")
		res := v.VerifyOperation(ctx, op, VerifyEvidence{PreImage: mustManifest(t, pre).Unstructured()})
		require.Equal(t, VerifyApplied, res.State, res.Reason)
	})
}

func Test_Verifier_CreateIfNotExistsMissingIsNotApplied(t *testing.T) {
	// Ambiguous CreateIfNotExists, object now absent: resending can neither
	// duplicate nor overwrite (it only writes when missing).
	v, _, _ := verifierFor(t, cmManifest("other"))
	op := NewCreateIfNotExistsOperation(mustManifest(t, cmManifest("ghost", "foo", "bar")).Unstructured())
	res := v.VerifyOperation(context.Background(), op, VerifyEvidence{})
	require.Equal(t, VerifyNotApplied, res.State, res.Reason)
}

func Test_Verifier_RVFastPath(t *testing.T) {
	ctx := context.Background()
	cluster := newFakeClusterWithNamespaceAndObjects(t, verifierNS, cmManifest("testcm", "foo", "bar"))
	fdc := patchDynamicFake(t, cluster)
	// The simple fake client does not stamp RVs on stored objects, so emulate
	// an apiserver that does: every Get returns the object with a fixed RV.
	fdc.PrependReactor("get", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		u := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"namespace":       verifierNS,
				"name":            "testcm",
				"resourceVersion": "4242",
			},
			"data": map[string]any{"foo": "bar"},
		}}
		return true, u, nil
	})
	v := NewOperationVerifier(NewObjectPatcher(cluster.Client, nopLogger()))

	// Intent is absent from live, but post-RV match proves the object has not
	// changed since the write response: verdict must be Applied.
	op := NewMergePatchOperation(`{"data":{"baz":"quux"}}`, "v1", "ConfigMap", verifierNS, "testcm")
	res := v.VerifyOperation(ctx, op, VerifyEvidence{PostResourceVersion: "4242"})
	require.Equal(t, VerifyApplied, res.State, res.Reason)

	// A different post-RV falls through to content comparison. The pre-image
	// had baz="srv" while live (reactor above returns data.foo only) lost that
	// leaf to another writer: patched paths changed → Incompatible.
	pre := `
apiVersion: v1
kind: ConfigMap
metadata:
  namespace: default
  name: testcm
data:
  foo: "bar"
  baz: "srv"
`
	res = v.VerifyOperation(ctx, op, VerifyEvidence{PostResourceVersion: "9999", PreImage: mustManifest(t, pre).Unstructured()})
	require.Equal(t, VerifyIncompatible, res.State, res.Reason)
}

func Test_Verifier_GetUncertain(t *testing.T) {
	ctx := context.Background()

	t.Run("timeout on get is GetUncertain", func(t *testing.T) {
		cluster := newFakeClusterWithNamespaceAndObjects(t, verifierNS, cmManifest("testcm", "foo", "bar"))
		fdc := patchDynamicFake(t, cluster)
		fdc.PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewTimeoutError("slow", 0)
		})
		v := NewOperationVerifier(NewObjectPatcher(cluster.Client, nopLogger()))
		op := NewMergePatchOperation(`{"data":{"baz":"quux"}}`, "v1", "ConfigMap", verifierNS, "testcm")
		res := v.VerifyOperation(ctx, op, VerifyEvidence{})
		require.Equal(t, VerifyGetUncertain, res.State, res.Reason)
	})
}
