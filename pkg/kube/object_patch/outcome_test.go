package object_patch

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"

	"github.com/deckhouse/deckhouse/pkg/log"
	sdkpkg "github.com/deckhouse/module-sdk/pkg"
	"github.com/flant/kube-client/fake"
	"github.com/flant/kube-client/manifest"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

func cmGR() schema.GroupResource {
	return schema.GroupResource{Resource: "configmaps"}
}

func nopLogger() *log.Logger {
	return log.NewNop()
}

func mustManifest(t *testing.T, yamlDoc string) manifest.Manifest {
	t.Helper()
	m, err := manifest.NewFromYAML(yamlDoc)
	require.NoError(t, err)
	return m
}

// patchDynamicFake fetches the fake dynamic client backing the fake cluster so
// reactors can be installed.
func patchDynamicFake(t *testing.T, cluster *fake.Cluster) *fakedynamic.FakeDynamicClient {
	t.Helper()
	fdc, ok := cluster.Client.Dynamic().(*fakedynamic.FakeDynamicClient)
	require.True(t, ok, "fake cluster must back a FakeDynamicClient")
	return fdc
}

func Test_ClassifyRequestError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want OutcomeState
	}{
		{"nil", nil, OutcomeApplied},
		{"conflict", apierrors.NewConflict(cmGR(), "x", errors.New("conflict")), OutcomeConflict},
		{"invalid", apierrors.NewInvalid(schema.GroupKind{Kind: "ConfigMap"}, "x", nil), OutcomeRejected},
		{"bad request", apierrors.NewBadRequest("nope"), OutcomeRejected},
		{"timeout", apierrors.NewTimeoutError("slow", 0), OutcomeUncertain},
		{"server timeout", apierrors.NewServerTimeout(cmGR(), "patch", 0), OutcomeUncertain},
		{"service unavailable", apierrors.NewServiceUnavailable("down"), OutcomeUncertain},
		{"internal error", apierrors.NewInternalError(errors.New("oops")), OutcomeUncertain},
		{"too many requests", apierrors.NewTooManyRequests("slow down", 429), OutcomeRetryable},
		{"not found", apierrors.NewNotFound(cmGR(), "x"), OutcomeRetryable},
		{"context canceled", context.Canceled, OutcomeUncertain},
		{"deadline exceeded", context.DeadlineExceeded, OutcomeUncertain},
		{"connection refused", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, OutcomeRetryable},
		{"connection reset", &net.OpError{Op: "write", Err: syscall.ECONNRESET}, OutcomeUncertain},
		{"plain error", errors.New("plain"), OutcomeRetryable},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, classifyRequestError(tc.err))
		})
	}
}

func Test_ExecuteOperationCtx_ErrorClassification(t *testing.T) {
	const ns = "default"
	cmYAML := `
apiVersion: v1
kind: ConfigMap
metadata:
  namespace: default
  name: testcm
data:
  foo: "bar"
`

	t.Run("successful merge patch is Applied", func(t *testing.T) {
		cluster := newFakeClusterWithNamespaceAndObjects(t, ns, cmYAML)
		patcher := NewObjectPatcher(cluster.Client, nopLogger())
		op := NewMergePatchOperation(`{"data":{"baz":"quux"}}`, "v1", "ConfigMap", ns, "testcm")

		outcome := patcher.ExecuteOperationCtx(context.Background(), op)
		require.Equal(t, OutcomeApplied, outcome.State)
		require.NoError(t, outcome.Err)
		require.True(t, outcome.Sent)
		// PostResourceVersion is populated from the write response; the fake
		// dynamic client happens not to stamp RVs on patch, so only assert the
		// capture does not error (real apiservers always return an RV).
	})

	t.Run("timeout is Uncertain", func(t *testing.T) {
		cluster := newFakeClusterWithNamespaceAndObjects(t, ns, cmYAML)
		fdc := patchDynamicFake(t, cluster)
		fdc.PrependReactor("patch", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewTimeoutError("api server timeout", 0)
		})
		patcher := NewObjectPatcher(cluster.Client, nopLogger())
		op := NewMergePatchOperation(`{"data":{"baz":"quux"}}`, "v1", "ConfigMap", ns, "testcm")

		outcome := patcher.ExecuteOperationCtx(context.Background(), op)
		require.Equal(t, OutcomeUncertain, outcome.State)
		require.Error(t, outcome.Err)
	})

	t.Run("409 conflict is terminal Conflict", func(t *testing.T) {
		cluster := newFakeClusterWithNamespaceAndObjects(t, ns, cmYAML)
		fdc := patchDynamicFake(t, cluster)
		fdc.PrependReactor("patch", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewConflict(cmGR(), "testcm", errors.New("conflict"))
		})
		patcher := NewObjectPatcher(cluster.Client, nopLogger())
		op := NewMergePatchOperation(`{"data":{"baz":"quux"}}`, "v1", "ConfigMap", ns, "testcm")

		outcome := patcher.ExecuteOperationCtx(context.Background(), op)
		require.Equal(t, OutcomeConflict, outcome.State)
		require.True(t, outcome.State.Terminal())
	})

	t.Run("422 invalid is terminal Rejected", func(t *testing.T) {
		cluster := newFakeClusterWithNamespaceAndObjects(t, ns, cmYAML)
		fdc := patchDynamicFake(t, cluster)
		fdc.PrependReactor("patch", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewInvalid(schema.GroupKind{Kind: "ConfigMap"}, "testcm",
				field.ErrorList{field.Invalid(field.NewPath("data"), "x", "bad")})
		})
		patcher := NewObjectPatcher(cluster.Client, nopLogger())
		op := NewMergePatchOperation(`{"data":{"baz":"quux"}}`, "v1", "ConfigMap", ns, "testcm")

		outcome := patcher.ExecuteOperationCtx(context.Background(), op)
		require.Equal(t, OutcomeRejected, outcome.State)
		require.True(t, outcome.State.Terminal())
	})

	t.Run("missing object without ignore is Retryable", func(t *testing.T) {
		cluster := newFakeClusterWithNamespaceAndObjects(t, ns, cmYAML)
		patcher := NewObjectPatcher(cluster.Client, nopLogger())
		op := NewMergePatchOperation(`{"data":{"baz":"quux"}}`, "v1", "ConfigMap", ns, "missing-object")

		outcome := patcher.ExecuteOperationCtx(context.Background(), op)
		require.Equal(t, OutcomeRetryable, outcome.State)
		require.Error(t, outcome.Err)
	})

	t.Run("missing object with ignore is Ignored", func(t *testing.T) {
		cluster := newFakeClusterWithNamespaceAndObjects(t, ns, cmYAML)
		patcher := NewObjectPatcher(cluster.Client, nopLogger())
		op := NewMergePatchOperation(`{"data":{"baz":"quux"}}`, "v1", "ConfigMap", ns, "missing-object", WithIgnoreMissingObject())

		outcome := patcher.ExecuteOperationCtx(context.Background(), op)
		require.Equal(t, OutcomeIgnored, outcome.State)
		require.NoError(t, outcome.Err)
	})

	t.Run("in-flight cancel surfaces as Uncertain", func(t *testing.T) {
		// The fake client does not propagate ctx into reactors, so an
		// in-flight cancellation is simulated by the error the transport
		// returns when ctx is canceled while a write is outstanding.
		cluster := newFakeClusterWithNamespaceAndObjects(t, ns, cmYAML)
		fdc := patchDynamicFake(t, cluster)
		fdc.PrependReactor("patch", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, context.Canceled
		})
		patcher := NewObjectPatcher(cluster.Client, nopLogger())
		op := NewMergePatchOperation(`{"data":{"baz":"quux"}}`, "v1", "ConfigMap", ns, "testcm")

		outcome := patcher.ExecuteOperationCtx(context.Background(), op)
		require.Equal(t, OutcomeUncertain, outcome.State)
		require.ErrorIs(t, outcome.Err, context.Canceled)
	})

	t.Run("plain create of an existing object is Uncertain for verification", func(t *testing.T) {
		cluster := newFakeClusterWithNamespaceAndObjects(t, ns, cmYAML)
		patcher := NewObjectPatcher(cluster.Client, nopLogger())
		op := NewCreateOperation(mustManifest(t, cmYAML).Unstructured())

		outcome := patcher.ExecuteOperationCtx(context.Background(), op)
		require.Equal(t, OutcomeUncertain, outcome.State)
	})

	t.Run("create ignoreIfExists of an existing object is Ignored", func(t *testing.T) {
		cluster := newFakeClusterWithNamespaceAndObjects(t, ns, cmYAML)
		patcher := NewObjectPatcher(cluster.Client, nopLogger())
		op := NewCreateIfNotExistsOperation(mustManifest(t, cmYAML).Unstructured())

		outcome := patcher.ExecuteOperationCtx(context.Background(), op)
		require.Equal(t, OutcomeIgnored, outcome.State)
	})
}

func Test_ExecuteOperationsCtx_LegacyAggregation(t *testing.T) {
	const ns = "default"
	cmYAML := `
apiVersion: v1
kind: ConfigMap
metadata:
  namespace: default
  name: testcm
data:
  foo: "bar"
`
	cluster := newFakeClusterWithNamespaceAndObjects(t, ns, cmYAML)
	fdc := patchDynamicFake(t, cluster)
	calls := 0
	// Fail the second patch invocation.
	fdc.PrependReactor("patch", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		if calls == 2 {
			return true, nil, apierrors.NewTimeoutError("boom", 0)
		}
		return false, nil, nil
	})
	patcher := NewObjectPatcher(cluster.Client, nopLogger())

	ops := []sdkpkg.PatchCollectorOperation{
		NewMergePatchOperation(`{"data":{"a":"1"}}`, "v1", "ConfigMap", ns, "testcm"),
		NewMergePatchOperation(`{"data":{"b":"2"}}`, "v1", "ConfigMap", ns, "testcm"),
		NewMergePatchOperation(`{"data":{"c":"3"}}`, "v1", "ConfigMap", ns, "testcm"),
	}

	seen := make([]OutcomeState, 0, len(ops))
	err := patcher.ExecuteOperationsCtx(context.Background(), ops, func(_ int, outcome *OperationOutcome) {
		seen = append(seen, outcome.State)
	})
	require.Error(t, err)
	require.Equal(t, 3, calls, "all operations must be attempted despite the mid failure")
	require.Equal(t, []OutcomeState{OutcomeApplied, OutcomeUncertain, OutcomeApplied}, seen)
}
