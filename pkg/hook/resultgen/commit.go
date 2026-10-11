package resultgen

import (
	"context"

	metricsop "github.com/deckhouse/deckhouse/pkg/metrics-storage/operation"
	sdkpkg "github.com/deckhouse/module-sdk/pkg"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	objectpatch "github.com/flant/shell-operator/pkg/kube/object_patch"
)

// FrozenGenerationView is the read-only snapshot handed to external value
// committers (e.g. addon-operator).
type FrozenGenerationView struct {
	ID          GenerationID
	Seq         Seq
	HookName    string
	TaskID      string
	QueueName   string
	BindingType string
}

func newView(rec *GenerationRecord) FrozenGenerationView {
	return FrozenGenerationView{
		ID:          rec.ID,
		Seq:         rec.Seq,
		HookName:    rec.HookName,
		TaskID:      rec.TaskID,
		QueueName:   rec.QueueName,
		BindingType: rec.BindingType,
	}
}

// PatchExecutor sends patch operations and reads objects. The production
// implementation is *objectpatch.ObjectPatcher.
type PatchExecutor interface {
	ExecuteOperationCtx(ctx context.Context, op sdkpkg.PatchCollectorOperation) *objectpatch.OperationOutcome
	GetOperationObject(ctx context.Context, op sdkpkg.PatchCollectorOperation) (*unstructured.Unstructured, error)
}

// PatchVerifier re-checks an ambiguous operation against the live object.
// The production implementation is *objectpatch.OperationVerifier.
type PatchVerifier interface {
	VerifyOperation(ctx context.Context, op sdkpkg.PatchCollectorOperation, evidence objectpatch.VerifyEvidence) *objectpatch.VerifyResult
}

// ValueCommitter publishes configuration values for a frozen generation.
// shell-operator registers no committer (values stage is Skipped); extenders
// such as addon-operator provide their own implementation.
//
// Idempotency contract: in-process retries never re-invoke a committed
// stage, but a process crash in the Applying->Committed window can lead to a
// single redelivery after restart. Implementations must therefore commit
// idempotently, using view.ID (the stable result-generation identity) and
// view.Seq as dedup keys.
type ValueCommitter interface {
	CommitValues(ctx context.Context, view FrozenGenerationView) error
}

// HookMetricsApplier applies hook metric operations. metricsstorage.Storage
// satisfies this interface.
type HookMetricsApplier interface {
	ApplyBatchOperations(ops []metricsop.MetricOperation, commonLabels map[string]string) error
}

// CommitDecisionKind is the queue-level conclusion of a commit pass.
type CommitDecisionKind string

const (
	// DecisionSuccess: all required stages committed; acknowledge the queue.
	DecisionSuccess CommitDecisionKind = "Success"
	// DecisionFail: a retryable stage failure; the task is retried with
	// backoff, committed stages and confirmed operations are never replayed.
	DecisionFail CommitDecisionKind = "Fail"
	// DecisionRepeat: the context was canceled (shutdown/task cancel); do not
	// start new stages and retry very soon.
	DecisionRepeat CommitDecisionKind = "Repeat"
	// DecisionDeadLetter: a terminal failure; the task must leave the queue
	// without a success acknowledgment and the failure record is queryable.
	DecisionDeadLetter CommitDecisionKind = "DeadLetter"
	// DecisionSuperseded: a newer generation of the same hook took over; the
	// task is acknowledged as replaced and the stale journal is retired.
	DecisionSuperseded CommitDecisionKind = "Superseded"
)

// CommitDecision is returned by Coordinator.Commit.
type CommitDecision struct {
	Kind    CommitDecisionKind
	Err     error
	Failure *FailureRecord
	Record  *GenerationRecord
}

// CommitInput carries the frozen hook result and its execution coordinates.
type CommitInput struct {
	HookName    string
	TaskID      string
	QueueName   string
	BindingType string

	// Queued distinguishes queue tasks (queue stage required) from
	// synchronous webhook executions (conclusion is the HTTP response).
	Queued bool

	// Freeze payload.
	PatchBytes             []byte
	Metrics                []FrozenMetricOp
	AdmissionResponseJSON  []byte
	ConversionResponseJSON []byte
	CommonMetricLabels     map[string]string

	// ParsedPatchCount is the number of operations parsed from PatchBytes.
	// ParseError holds a deterministic parse failure (terminal dead letter).
	ParsedPatchCount int
	ParseError       string
}

// FreezeInput converts the commit input into the store's freeze payload. The
// binding hash and fingerprint are computed by the caller from the binding
// contexts and frozen outputs.
func (in CommitInput) FreezeInput(bindingHash, fingerprint string) FreezeInput {
	return FreezeInput{
		HookName:               in.HookName,
		TaskID:                 in.TaskID,
		QueueName:              in.QueueName,
		BindingType:            in.BindingType,
		BindingHash:            bindingHash,
		Fingerprint:            fingerprint,
		Queued:                 in.Queued,
		PatchBytes:             in.PatchBytes,
		Metrics:                in.Metrics,
		AdmissionResponseJSON:  in.AdmissionResponseJSON,
		ConversionResponseJSON: in.ConversionResponseJSON,
		PatchOpCount:           in.ParsedPatchCount,
		ParseError:             in.ParseError,
	}
}

// stageOutcome enumerates what a stage run concluded.
type stageOutcomeKind string

const (
	stageDone       stageOutcomeKind = "done"
	stageRetryable  stageOutcomeKind = "retryable"
	stageCanceled   stageOutcomeKind = "canceled"
	stageStale      stageOutcomeKind = "stale"
	stageTerminalFL stageOutcomeKind = "terminal"
)

type stageOutcome struct {
	kind    stageOutcomeKind
	err     error
	failure *FailureRecord
}

func doneOutcome() stageOutcome { return stageOutcome{kind: stageDone} }

func canceledOutcome() stageOutcome { return stageOutcome{kind: stageCanceled} }

func staleOutcome() stageOutcome { return stageOutcome{kind: stageStale} }

func retryableOutcome(err error) stageOutcome { return stageOutcome{kind: stageRetryable, err: err} }

func terminalOutcome(failure *FailureRecord) stageOutcome {
	return stageOutcome{kind: stageTerminalFL, failure: failure}
}
