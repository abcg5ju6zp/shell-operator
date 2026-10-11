package object_patch

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/deckhouse/deckhouse/pkg/log"
	sdkpkg "github.com/deckhouse/module-sdk/pkg"
	"github.com/hashicorp/go-multierror"
	gerror "github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"

	"github.com/flant/shell-operator/pkg"
)

type ObjectPatcher struct {
	kubeClient KubeClient
	logger     *log.Logger
}

type KubeClient interface {
	kubernetes.Interface
	Dynamic() dynamic.Interface
	GroupVersionResource(apiVersion string, kind string) (schema.GroupVersionResource, error)
}

func NewObjectPatcher(kubeClient KubeClient, logger *log.Logger) *ObjectPatcher {
	return &ObjectPatcher{
		kubeClient: kubeClient,
		logger:     logger.With(pkg.LogKeyOperatorComponent, "KubernetesObjectPatcher"),
	}
}

// ExecuteOperations applies all operations, continuing after errors and
// aggregating them. It is preserved for compatibility (and for hook-error
// status patching); new commit-pipeline code should drive operations
// one-by-one via ExecuteOperationCtx and inspect OperationOutcome.
func (o *ObjectPatcher) ExecuteOperations(ops []sdkpkg.PatchCollectorOperation) error {
	return o.ExecuteOperationsCtx(context.Background(), ops, nil)
}

// ExecuteOperationsCtx applies all operations in order and never stops on a
// single failure, mirroring the historical ExecuteOperations semantics:
// perOp (when non-nil) observes every outcome and errors are aggregated.
func (o *ObjectPatcher) ExecuteOperationsCtx(ctx context.Context, ops []sdkpkg.PatchCollectorOperation, perOp func(index int, outcome *OperationOutcome)) error {
	log.Debug("Starting execute operations process")
	defer log.Debug("Finished execute operations process")

	applyErrors := &multierror.Error{}
	for i, op := range ops {
		log.Debug("Applying operation", slog.String(pkg.LogKeyName, op.Description()))
		outcome := o.ExecuteOperationCtx(ctx, op)
		if perOp != nil {
			perOp(i, outcome)
		}
		if outcome.Err != nil && outcome.State != OutcomeIgnored {
			applyErrors = multierror.Append(applyErrors, gerror.WithMessage(outcome.Err, outcome.Description))
		}
	}

	return applyErrors.ErrorOrNil()
}

// ExecuteOperation keeps the legacy error-only API.
func (o *ObjectPatcher) ExecuteOperation(operation sdkpkg.PatchCollectorOperation) error {
	outcome := o.ExecuteOperationCtx(context.Background(), operation)
	if outcome == nil {
		return nil
	}
	return outcome.Err
}

// ExecuteOperationCtx applies a single operation and returns a structured
// outcome describing what happened, including object ResourceVersion evidence
// for later uncertain-response verification.
func (o *ObjectPatcher) ExecuteOperationCtx(ctx context.Context, operation sdkpkg.PatchCollectorOperation) *OperationOutcome {
	if operation == nil {
		return &OperationOutcome{State: OutcomeApplied}
	}

	switch v := operation.(type) {
	case *createOperation:
		return o.executeCreateOperationCtx(ctx, v)
	case *deleteOperation:
		return o.executeDeleteOperationCtx(ctx, v)
	case *patchOperation:
		if v.hasFilterFn() {
			return o.executeFilterOperationCtx(ctx, v)
		}

		return o.executePatchOperationCtx(ctx, v)
	}

	return newOutcome(OutcomeApplied, operation, nil)
}

// GetOperationObject reads the object addressed by an operation. It is used
// both for best-effort pre-image capture (needed to verify non-idempotent
// JSONPatch attempts after ambiguous responses) and by the verifier.
func (o *ObjectPatcher) GetOperationObject(ctx context.Context, operation sdkpkg.PatchCollectorOperation) (*unstructured.Unstructured, error) {
	switch v := operation.(type) {
	case *createOperation:
		object, err := toUnstructured(v.object)
		if err != nil {
			return nil, err
		}
		gvk, err := o.kubeClient.GroupVersionResource(object.GetAPIVersion(), object.GetKind())
		if err != nil {
			return nil, err
		}
		return o.kubeClient.Dynamic().
			Resource(gvk).
			Namespace(object.GetNamespace()).
			Get(ctx, object.GetName(), metav1.GetOptions{}, generateSubresources(v.subresource)...)
	case *deleteOperation:
		gvk, err := o.kubeClient.GroupVersionResource(v.apiVersion, v.kind)
		if err != nil {
			return nil, err
		}
		return o.kubeClient.Dynamic().
			Resource(gvk).
			Namespace(v.namespace).
			Get(ctx, v.name, metav1.GetOptions{}, generateSubresources(v.subresource)...)
	case *patchOperation:
		gvk, err := o.kubeClient.GroupVersionResource(v.apiVersion, v.kind)
		if err != nil {
			return nil, err
		}
		return o.kubeClient.Dynamic().
			Resource(gvk).
			Namespace(v.namespace).
			Get(ctx, v.name, metav1.GetOptions{}, generateSubresources(v.subresource)...)
	}
	return nil, fmt.Errorf("operation %T does not address a single object", operation)
}

func (o *ObjectPatcher) executeCreateOperationCtx(ctx context.Context, op *createOperation) *OperationOutcome {
	if op.object == nil {
		return newOutcome(OutcomeRejected, op, fmt.Errorf("cannot create empty object"))
	}

	// Convert object from any.
	object, err := toUnstructured(op.object)
	if err != nil {
		return newOutcome(OutcomeRejected, op, err)
	}

	apiVersion := object.GetAPIVersion()
	kind := object.GetKind()

	wrapErr := func(e error) error {
		objectID := fmt.Sprintf("%s/%s/%s/%s", apiVersion, kind, object.GetNamespace(), object.GetName())
		return gerror.WithMessage(e, objectID)
	}

	gvk, err := o.kubeClient.GroupVersionResource(apiVersion, kind)
	if err != nil {
		return newOutcome(classifyRequestError(err), op, wrapErr(err))
	}

	log.Debug("Started Create API call")
	created, err := o.kubeClient.Dynamic().
		Resource(gvk).
		Namespace(object.GetNamespace()).
		Create(ctx, object, pkg.DefaultCreateOptions(), generateSubresources(op.subresource)...)
	log.Debug("Finished Create API call")

	objectExists := errors.IsAlreadyExists(err)

	if objectExists && op.ignoreIfExists {
		log.Debug("resource already exists, exiting without error")
		return newOutcome(OutcomeIgnored, op, nil)
	}

	if objectExists && op.updateIfExists {
		log.Debug("Object already exists, attempting to Update it with optimistic lock")

		outcome := newOutcome(OutcomeApplied, op, nil)
		outcome.Sent = true
		retryErr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			log.Debug("Started Get API call")
			existingObj, getErr := o.kubeClient.Dynamic().
				Resource(gvk).
				Namespace(object.GetNamespace()).
				Get(ctx, object.GetName(), metav1.GetOptions{}, generateSubresources(op.subresource)...)
			log.Debug("Finished Get API call")
			if getErr != nil {
				return wrapErr(getErr)
			}
			outcome.PreResourceVersion = existingObj.GetResourceVersion()
			outcome.PreImage = existingObj.DeepCopy()

			objCopy := object.DeepCopy()
			objCopy.SetResourceVersion(existingObj.GetResourceVersion())

			log.Debug("Started Update API call")
			updated, updateErr := o.kubeClient.Dynamic().
				Resource(gvk).
				Namespace(objCopy.GetNamespace()).
				Update(ctx, objCopy, pkg.DefaultUpdateOptions(), generateSubresources(op.subresource)...)
			log.Debug("Finished Update API call")
			if updateErr != nil {
				return wrapErr(updateErr)
			}
			if updated != nil {
				outcome.PostResourceVersion = updated.GetResourceVersion()
			}
			return nil
		})
		if retryErr != nil {
			outcome.State = classifyRequestError(retryErr)
			outcome.Err = retryErr
		}
		return outcome
	}

	if objectExists {
		// The object may be the product of an earlier attempt of the same
		// operation: verify by content before deciding to fail.
		return newOutcome(OutcomeUncertain, op, wrapErr(err))
	}

	if err != nil {
		outcome := newOutcome(classifyRequestError(err), op, wrapErr(err))
		outcome.Sent = true
		return outcome
	}

	outcome := newOutcome(OutcomeApplied, op, nil)
	outcome.Sent = true
	if created != nil {
		outcome.PostResourceVersion = created.GetResourceVersion()
	}
	return outcome
}

// executePatchOperationCtx applies a patch to the specified object using API call Patch.
//
// There 2 types of patches:
// - Merge — use Patch API call with MergePatchType.
// - JSON — use Patch API call with JSONPatchType.
//
// Other options:
// - WithSubresource — a subresource argument for Patch or Update API call.
// - IgnoreMissingObject — do not return error if the specified object is missing.
// - IgnoreHookError — allows applying patches for a Status subresource even if the hook fails
func (o *ObjectPatcher) executePatchOperationCtx(ctx context.Context, op *patchOperation) *OperationOutcome {
	if op.patchType == types.MergePatchType {
		log.Debug("Started MergePatchObject")
		defer log.Debug("Finished MergePatchObject")
	}
	if op.patchType == types.JSONPatchType {
		log.Debug("Started JSONPatchObject")
		defer log.Debug("Finished JSONPatchObject")
	}

	patchBytes, err := convertPatchToBytes(op.patch)
	if err != nil {
		return newOutcome(OutcomeRejected, op,
			fmt.Errorf("encode %s patch for %s/%s/%s/%s: %v", op.patchType, op.apiVersion, op.kind, op.namespace, op.name, err))
	}
	if patchBytes == nil {
		return newOutcome(OutcomeRejected, op,
			fmt.Errorf("%s patch is nil for %s/%s/%s/%s", op.patchType, op.apiVersion, op.kind, op.namespace, op.name))
	}

	gvk, err := o.kubeClient.GroupVersionResource(op.apiVersion, op.kind)
	if err != nil {
		return newOutcome(classifyRequestError(err), op, err)
	}

	log.Debug("Started Patch API call")
	patched, err := o.kubeClient.Dynamic().
		Resource(gvk).
		Namespace(op.namespace).
		Patch(ctx, op.name, op.patchType, patchBytes, pkg.DefaultPatchOptions(), generateSubresources(op.subresource)...)
	log.Debug("Finished Patch API call")

	if op.ignoreMissingObject && errors.IsNotFound(err) {
		return newOutcome(OutcomeIgnored, op, nil)
	}
	if err != nil {
		outcome := newOutcome(classifyRequestError(err), op, err)
		outcome.Sent = true
		return outcome
	}

	outcome := newOutcome(OutcomeApplied, op, nil)
	outcome.Sent = true
	if patched != nil {
		outcome.PostResourceVersion = patched.GetResourceVersion()
	}
	return outcome
}

// executeFilterOperationCtx retrieves a specified object, modifies it with
// filterFunc and calls update.
//
// Other options:
// - WithSubresource — a subresource argument for Patch or Update API call.
// - IgnoreMissingObject — do not return error if the specified object is missing.
// - IgnoreHookError — allows applying patches for a Status subresource even if the hook fails
func (o *ObjectPatcher) executeFilterOperationCtx(ctx context.Context, op *patchOperation) *OperationOutcome {
	if op.filterFunc == nil {
		return newOutcome(OutcomeRejected, op, fmt.Errorf("FilterFunc is nil"))
	}

	gvk, err := o.kubeClient.GroupVersionResource(op.apiVersion, op.kind)
	if err != nil {
		return newOutcome(classifyRequestError(err), op, err)
	}

	outcome := newOutcome(OutcomeApplied, op, nil)

	retryErr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		log.Debug("Started Get API call")
		obj, err := o.kubeClient.Dynamic().
			Resource(gvk).
			Namespace(op.namespace).
			Get(ctx, op.name, metav1.GetOptions{})
		log.Debug("Finished Get API call")
		if op.ignoreMissingObject && errors.IsNotFound(err) {
			outcome.State = OutcomeIgnored
			return nil
		}
		if err != nil {
			return err
		}
		outcome.PreResourceVersion = obj.GetResourceVersion()
		outcome.PreImage = obj.DeepCopy()

		log.Debug("Started filtering object")
		filteredObj, err := op.filterFunc(obj)
		log.Debug("Finished filtering object")
		if err != nil {
			return err
		}

		if equality.Semantic.DeepEqual(obj, filteredObj) {
			outcome.State = OutcomeIgnored
			return nil
		}

		var filteredObjBuf bytes.Buffer
		err = unstructured.UnstructuredJSONScheme.Encode(filteredObj, &filteredObjBuf)
		if err != nil {
			return err
		}

		log.Debug("Started Update API call")
		updated, err := o.kubeClient.Dynamic().
			Resource(gvk).
			Namespace(op.namespace).
			Update(ctx, filteredObj, pkg.DefaultUpdateOptions(), generateSubresources(op.subresource)...)
		log.Debug("Finished Update API call")
		if err != nil {
			return err
		}

		if updated != nil {
			outcome.PostResourceVersion = updated.GetResourceVersion()
		}
		outcome.Sent = true
		return nil
	})

	if retryErr != nil {
		outcome.State = classifyRequestError(retryErr)
		outcome.Err = retryErr
	}
	return outcome
}

func (o *ObjectPatcher) executeDeleteOperationCtx(ctx context.Context, op *deleteOperation) *OperationOutcome {
	gvk, err := o.kubeClient.GroupVersionResource(op.apiVersion, op.kind)
	if err != nil {
		return newOutcome(classifyRequestError(err), op, err)
	}

	log.Debug("Started Delete API call")
	err = o.kubeClient.Dynamic().
		Resource(gvk).
		Namespace(op.namespace).
		Delete(ctx, op.name, metav1.DeleteOptions{PropagationPolicy: &op.deletionPropagation}, op.subresource)

	log.Debug("Finished Delete API call")
	if errors.IsNotFound(err) {
		return newOutcome(OutcomeIgnored, op, nil)
	}

	if err != nil {
		outcome := newOutcome(classifyRequestError(err), op, err)
		outcome.Sent = true
		return outcome
	}

	outcome := newOutcome(OutcomeApplied, op, nil)
	outcome.Sent = true

	if op.deletionPropagation != metav1.DeletePropagationForeground {
		return outcome
	}

	log.Debug("Waiting for object deletion")

	err = wait.PollUntilContextTimeout(ctx, time.Second, 20*time.Second, false, func(ctx context.Context) (bool, error) {
		log.Debug("Started Get API call")
		_, err := o.kubeClient.Dynamic().
			Resource(gvk).
			Namespace(op.namespace).
			Get(ctx, op.name, metav1.GetOptions{})

		log.Debug("Finished Get API call")
		if errors.IsNotFound(err) {
			return true, nil
		}

		return false, err
	})
	if err != nil {
		// Timed out while foreground-deleting: the delete itself was accepted,
		// verification decides whether the object is actually gone.
		outcome.State = OutcomeUncertain
		outcome.Err = err
	}
	return outcome
}
