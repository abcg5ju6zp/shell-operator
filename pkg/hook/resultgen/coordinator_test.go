package resultgen

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/deckhouse/deckhouse/pkg/log"
	metricsop "github.com/deckhouse/deckhouse/pkg/metrics-storage/operation"
	sdkpkg "github.com/deckhouse/module-sdk/pkg"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	objectpatch "github.com/flant/shell-operator/pkg/kube/object_patch"
)

// ---- fakes -----------------------------------------------------------------

type scriptedExecutor struct {
	mu       sync.Mutex
	sends    map[string]int
	outcomes map[string]*objectpatch.OperationOutcome
	gets     map[string]int
	// hook allows a test to mutate state during a send (e.g. cancel context).
	hook func(name string)
}

func newScriptedExecutor() *scriptedExecutor {
	return &scriptedExecutor{
		sends:    map[string]int{},
		outcomes: map[string]*objectpatch.OperationOutcome{},
		gets:     map[string]int{},
	}
}

func (e *scriptedExecutor) ExecuteOperationCtx(_ context.Context, op sdkpkg.PatchCollectorOperation) *objectpatch.OperationOutcome {
	name := op.Description()
	e.mu.Lock()
	e.sends[name]++
	o, hasOutcome := e.outcomes[name]
	hook := e.hook
	e.mu.Unlock()
	if hook != nil {
		hook(name)
	}
	if hasOutcome {
		// Return a fresh copy with an error preserved.
		cp := *o
		return &cp
	}
	return &objectpatch.OperationOutcome{State: objectpatch.OutcomeApplied}
}

func (e *scriptedExecutor) GetOperationObject(_ context.Context, op sdkpkg.PatchCollectorOperation) (*unstructured.Unstructured, error) {
	e.mu.Lock()
	e.gets[op.Description()]++
	e.mu.Unlock()
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "x", "resourceVersion": "1"},
	}}, nil
}

func (e *scriptedExecutor) sendCount(name string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.sends[name]
}

type scriptedVerifier struct {
	mu      sync.Mutex
	calls   map[string]int
	verdict map[string]objectpatch.VerifyState
}

func newScriptedVerifier() *scriptedVerifier {
	return &scriptedVerifier{calls: map[string]int{}, verdict: map[string]objectpatch.VerifyState{}}
}

func (v *scriptedVerifier) VerifyOperation(_ context.Context, op sdkpkg.PatchCollectorOperation, _ objectpatch.VerifyEvidence) *objectpatch.VerifyResult {
	name := op.Description()
	v.mu.Lock()
	v.calls[name]++
	state := v.verdict[name]
	v.mu.Unlock()
	return &objectpatch.VerifyResult{State: state, Reason: "scripted"}
}

func (v *scriptedVerifier) callCount(name string) int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.calls[name]
}

type fakeValueCommitter struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (f *fakeValueCommitter) CommitValues(_ context.Context, _ FrozenGenerationView) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.err
}

type fakeMetricsApplier struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (f *fakeMetricsApplier) ApplyBatchOperations(_ []metricsop.MetricOperation, _ map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.err
}

// ---- helpers ---------------------------------------------------------------

func threeOpPatchBytes() []byte {
	return []byte(`
---
operation: MergePatch
apiVersion: v1
kind: ConfigMap
namespace: default
name: cm-1
mergePatch:
  data:
    a: "1"
---
operation: MergePatch
apiVersion: v1
kind: ConfigMap
namespace: default
name: cm-2
mergePatch:
  data:
    a: "1"
---
operation: MergePatch
apiVersion: v1
kind: ConfigMap
namespace: default
name: cm-3
mergePatch:
  data:
    a: "1"
`)
}

func commitInput(taskID string, patchBytes []byte, opCount int) CommitInput {
	return CommitInput{
		HookName:           "hooks/test.sh",
		TaskID:             taskID,
		QueueName:          "main",
		BindingType:        "kubernetes",
		Queued:             true,
		PatchBytes:         patchBytes,
		ParsedPatchCount:   opCount,
		CommonMetricLabels: map[string]string{"hook": "hooks/test.sh"},
		Metrics: FrozenMetricOpsFromExternal([]metricsop.MetricOperation{
			{Name: "m", Action: metricsop.ActionCounterAdd, Value: floatPtr(1)},
		}),
	}
}

func newTestCoordinator(t *testing.T, opts ...Option) (*Coordinator, *scriptedExecutor, *scriptedVerifier, *fakeMetricsApplier) {
	t.Helper()
	store, _ := newTestStore(t)
	reg := NewRegistry(store, testLogger())
	exec := newScriptedExecutor()
	ver := newScriptedVerifier()
	met := &fakeMetricsApplier{}
	return NewCoordinator(reg, exec, ver, met, testLogger(), opts...), exec, ver, met
}

// ---- tests -----------------------------------------------------------------

func Test_Coordinator_FullSuccess(t *testing.T) {
	c, exec, _, met := newTestCoordinator(t)
	in := commitInput("task-1", threeOpPatchBytes(), 3)

	d := c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionSuccess, d.Kind)
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-1"))
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-2"))
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-3"))
	require.Equal(t, 1, met.calls)

	// Journal archived.
	_, err := c.Registry().Store().Get(d.Record.ID)
	require.Error(t, err)
}

func Test_Coordinator_PartialFailureResume(t *testing.T) {
	c, exec, _, met := newTestCoordinator(t)
	in := commitInput("task-1", threeOpPatchBytes(), 3)

	// op cm-2 fails on the first pass.
	exec.outcomes["Filter object v1/ConfigMap/default/cm-2"] = &objectpatch.OperationOutcome{
		State: objectpatch.OutcomeRetryable,
		Err:   errTemp("api is slow"),
	}

	d := c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionFail, d.Kind)
	require.Error(t, d.Err)
	require.Equal(t, 0, met.calls, "metrics must not run before patches finish")

	// Retry: remove the injected failure and re-run the SAME task.
	delete(exec.outcomes, "Filter object v1/ConfigMap/default/cm-2")
	d = c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionSuccess, d.Kind)

	// cm-1/cm-3 were Confirmed on pass one: zero extra sends. cm-2 sends twice.
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-1"))
	require.Equal(t, 2, exec.sendCount("Filter object v1/ConfigMap/default/cm-2"))
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-3"))
	require.Equal(t, 1, met.calls)
}

func Test_Coordinator_UncertainVerifiedApplied(t *testing.T) {
	c, exec, ver, _ := newTestCoordinator(t)
	in := commitInput("task-1", threeOpPatchBytes(), 3)
	name := "Filter object v1/ConfigMap/default/cm-2"
	exec.outcomes[name] = &objectpatch.OperationOutcome{State: objectpatch.OutcomeUncertain, Err: errTemp("timeout")}
	ver.verdict[name] = objectpatch.VerifyApplied

	d := c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionSuccess, d.Kind)
	require.Equal(t, 1, exec.sendCount(name), "verified ops must not be resent")
	require.Equal(t, 1, ver.callCount(name))
}

func Test_Coordinator_UncertainVerifiedNotAppliedResendsOnce(t *testing.T) {
	c, exec, ver, _ := newTestCoordinator(t)
	in := commitInput("task-1", threeOpPatchBytes(), 3)
	name := "Filter object v1/ConfigMap/default/cm-2"
	exec.outcomes[name] = &objectpatch.OperationOutcome{State: objectpatch.OutcomeUncertain, Err: errTemp("reset")}
	ver.verdict[name] = objectpatch.VerifyNotApplied
	// The ambiguity happens only on the first send; the resend succeeds.
	exec.hook = func(got string) {
		if got == name && exec.sendCount(name) == 1 {
			delete(exec.outcomes, name)
		}
	}

	d := c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionSuccess, d.Kind)
	require.Equal(t, 2, exec.sendCount(name), "not-applied uncertain op is sent one more time")
	require.Equal(t, 1, ver.callCount(name))
}

func Test_Coordinator_RepeatedAmbiguityBoundsOneResendPerTick(t *testing.T) {
	c, exec, ver, met := newTestCoordinator(t)
	in := commitInput("task-1", threeOpPatchBytes(), 3)
	name := "Filter object v1/ConfigMap/default/cm-2"
	// Every send is ambiguous, verification always says "not applied": the
	// coordinator must not loop within a tick.
	exec.outcomes[name] = &objectpatch.OperationOutcome{State: objectpatch.OutcomeUncertain, Err: errTemp("reset")}
	ver.verdict[name] = objectpatch.VerifyNotApplied

	d := c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionFail, d.Kind)
	require.LessOrEqual(t, exec.sendCount(name), 2, "at most one resend per tick")
	require.Equal(t, 0, met.calls)
}

func Test_Coordinator_UncertainGetPending(t *testing.T) {
	c, exec, ver, met := newTestCoordinator(t)
	in := commitInput("task-1", threeOpPatchBytes(), 3)
	name := "Filter object v1/ConfigMap/default/cm-2"
	exec.outcomes[name] = &objectpatch.OperationOutcome{State: objectpatch.OutcomeUncertain, Err: errTemp("reset")}
	ver.verdict[name] = objectpatch.VerifyGetUncertain

	d := c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionFail, d.Kind)
	require.Equal(t, 1, exec.sendCount(name))
	require.Equal(t, 0, met.calls)
	require.Equal(t, 1, ver.callCount(name))

	// Next tick verification succeeds.
	ver.verdict[name] = objectpatch.VerifyApplied
	d = c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionSuccess, d.Kind)
	require.Equal(t, 1, exec.sendCount(name), "no resend after Applied verdict")
	require.Equal(t, 2, ver.callCount(name))
}

func Test_Coordinator_TerminalConflictDeadLetter(t *testing.T) {
	c, exec, _, met := newTestCoordinator(t)
	in := commitInput("task-1", threeOpPatchBytes(), 3)
	name := "Filter object v1/ConfigMap/default/cm-2"
	exec.outcomes[name] = &objectpatch.OperationOutcome{State: objectpatch.OutcomeConflict, Err: errTemp("409")}

	d := c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionDeadLetter, d.Kind)
	require.NotNil(t, d.Failure)
	require.Equal(t, "Conflict", d.Failure.Reason)
	require.Equal(t, 1, d.Failure.OpIndex)
	require.Equal(t, 0, met.calls)
	require.Equal(t, 1, exec.sendCount(name), "terminal conflict is never retried")

	// cm-1 stays confirmed, cm-3 never sent: the record is dead-lettered.
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-1"))
	require.Equal(t, 0, exec.sendCount("Filter object v1/ConfigMap/default/cm-3"))

	// Failure is queryable from the registry/journal.
	got, err := c.Registry().GetFailure(d.Failure.GenerationID)
	require.NoError(t, err)
	require.Equal(t, "hooks/test.sh", got.HookName)
}

func Test_Coordinator_MetricsFailureNoRollbackNoReplay(t *testing.T) {
	values := &fakeValueCommitter{}
	c, exec, _, met := newTestCoordinator(t, WithValueCommitter(values))
	in := commitInput("task-1", threeOpPatchBytes(), 3)

	met.err = errTemp("invalid metric op")
	d := c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionFail, d.Kind)
	require.Equal(t, 1, values.calls)

	// Retry: patches not re-sent, values not re-invoked, metrics succeed.
	met.err = nil
	d = c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionSuccess, d.Kind)
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-1"))
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-2"))
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-3"))
	require.Equal(t, 1, values.calls, "committed values are never invoked again")
	require.Equal(t, 2, met.calls)
}

func Test_Coordinator_ValuesFailureStopsMetrics(t *testing.T) {
	values := &fakeValueCommitter{err: errTemp("values backend down")}
	c, _, _, met := newTestCoordinator(t, WithValueCommitter(values))
	in := commitInput("task-1", threeOpPatchBytes(), 3)

	d := c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionFail, d.Kind)
	require.Equal(t, 0, met.calls)

	values.err = nil
	d = c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionSuccess, d.Kind)
	require.Equal(t, 2, values.calls)
	require.Equal(t, 1, met.calls)
}

func Test_Coordinator_CancelStopsNewStagesAndResumes(t *testing.T) {
	c, exec, _, met := newTestCoordinator(t)
	in := commitInput("task-1", threeOpPatchBytes(), 3)

	ctx, cancel := context.WithCancel(context.Background())
	// Shutdown arrives right after the first send completes.
	exec.hook = func(string) { cancel() }

	d := c.Commit(ctx, "bh", "fp", in)
	require.Equal(t, DecisionRepeat, d.Kind)
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-1"))
	require.Equal(t, 0, exec.sendCount("Filter object v1/ConfigMap/default/cm-2"))
	require.Equal(t, 0, met.calls)

	// Resume with a live context: only the remaining two ops are sent.
	exec.hook = nil
	d = c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionSuccess, d.Kind)
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-1"))
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-2"))
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-3"))
	require.Equal(t, 1, met.calls)
}

func Test_Coordinator_StaleGenerationFenced(t *testing.T) {
	store, _ := newTestStore(t)
	reg := NewRegistry(store, testLogger())
	exec := newScriptedExecutor()
	ver := newScriptedVerifier()
	met := &fakeMetricsApplier{}
	c := NewCoordinator(reg, exec, ver, met, testLogger())

	in := commitInput("task-1", threeOpPatchBytes(), 3)

	// After the first send, a newer generation of the same hook is frozen.
	exec.hook = func(string) {
		newer := commitInput("task-2", threeOpPatchBytes(), 3)
		_, _, err := reg.BeginOrResume(newer.FreezeInput("bh", "fp-2"))
		require.NoError(t, err)
	}

	d := c.Commit(context.Background(), "bh", "fp-1", in)
	require.Equal(t, DecisionSuperseded, d.Kind)
	require.Equal(t, 0, met.calls)
	// Only op1 was sent before the fence tripped at the next op boundary.
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-1"))
	require.Equal(t, 0, exec.sendCount("Filter object v1/ConfigMap/default/cm-2"))

	// Stale journal was retired.
	_, err := reg.Store().Get(d.Record.ID)
	require.Error(t, err)
}

func Test_Coordinator_SyncWebhookSkipsQueueStage(t *testing.T) {
	c, _, _, met := newTestCoordinator(t)
	in := commitInput("task-1", nil, 0)
	in.Queued = false
	in.PatchBytes = nil

	d := c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionSuccess, d.Kind)
	// Metrics still apply on the synchronous path; only the queue conclusion
	// is skipped (the HTTP response is the conclusion).
	require.Equal(t, 1, met.calls)
	// No ValueCommitter registered in shell-operator: the values stage is
	// Skipped (not Applying/Failed); synchronous conclusion is Skipped too.
	require.Equal(t, StageSkipped, d.Record.Stages[StageValues].State)
	require.Equal(t, StageSkipped, d.Record.Stages[StageQueue].State)
}

func Test_Coordinator_InvalidPatchDeadLetter(t *testing.T) {
	c, _, _, _ := newTestCoordinator(t)
	in := commitInput("task-1", []byte("{not-yaml"), 0)
	in.ParseError = "validate kubernetes patch spec: bad"

	d := c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionDeadLetter, d.Kind)
	require.Equal(t, "InvalidPatch", d.Failure.Reason)
}

func Test_Coordinator_RestartSameFingerprintNoReplay(t *testing.T) {
	dir := filepathJoinTemp(t)

	// Process one: op2 fails.
	s1, err := NewFileStore(dir, testLogger())
	require.NoError(t, err)
	reg1 := NewRegistry(s1, testLogger())
	exec := newScriptedExecutor()
	ver := newScriptedVerifier()
	met := &fakeMetricsApplier{}
	c1 := NewCoordinator(reg1, exec, ver, met, testLogger())
	in1 := commitInput("old-task", threeOpPatchBytes(), 3)
	exec.outcomes["Filter object v1/ConfigMap/default/cm-2"] = &objectpatch.OperationOutcome{
		State: objectpatch.OutcomeRetryable, Err: errTemp("slow"),
	}
	d := c1.Commit(context.Background(), "bh", "fp", in1)
	require.Equal(t, DecisionFail, d.Kind)

	// Process two: Recover, new task ID but identical frozen result.
	s2, err := NewFileStore(dir, testLogger())
	require.NoError(t, err)
	reg2 := NewRegistry(s2, testLogger())
	_, _, err = reg2.Recover()
	require.NoError(t, err)
	c2 := NewCoordinator(reg2, exec, ver, met, testLogger())
	in2 := commitInput("new-task", threeOpPatchBytes(), 3)
	delete(exec.outcomes, "Filter object v1/ConfigMap/default/cm-2")

	d = c2.Commit(context.Background(), "bh", "fp", in2)
	require.Equal(t, DecisionSuccess, d.Kind)
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-1"))
	require.Equal(t, 2, exec.sendCount("Filter object v1/ConfigMap/default/cm-2"))
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-3"))
	require.Equal(t, 1, met.calls)
}

// flakyStore wraps a Store and fails Save calls matching a predicate, to
// emulate a disk write failure after an external apply already succeeded.
type flakyStore struct {
	Store
	skipSavesFor func(rec *GenerationRecord) bool
	failed       bool
}

func (f *flakyStore) Save(rec *GenerationRecord) error {
	if !f.failed && f.skipSavesFor != nil && f.skipSavesFor(rec) {
		f.failed = true
		return errTemp("disk write failed")
	}
	return f.Store.Save(rec)
}

func newCoordinatorWithStore(t *testing.T, store Store) (*Coordinator, *scriptedExecutor, *scriptedVerifier, *fakeMetricsApplier) {
	t.Helper()
	reg := NewRegistry(store, testLogger())
	exec := newScriptedExecutor()
	ver := newScriptedVerifier()
	met := &fakeMetricsApplier{}
	return NewCoordinator(reg, exec, ver, met, testLogger()), exec, ver, met
}

func Test_Coordinator_CancelDuringUncertainRequestSkipsVerification(t *testing.T) {
	store, _ := newTestStore(t)
	c, exec, ver, met := newCoordinatorWithStore(t, store)
	in := commitInput("task-1", threeOpPatchBytes(), 3)
	name := "Filter object v1/ConfigMap/default/cm-1"
	exec.outcomes[name] = &objectpatch.OperationOutcome{State: objectpatch.OutcomeUncertain, Err: errTemp("timeout")}

	ctx, cancel := context.WithCancel(context.Background())
	// Cancellation arrives while the in-flight request is finishing.
	exec.hook = func(string) { cancel() }

	d := c.Commit(ctx, "bh", "fp", in)
	require.Equal(t, DecisionRepeat, d.Kind)
	require.Equal(t, 1, exec.sendCount(name))
	require.Equal(t, 0, ver.callCount(name), "no verification Get may start after cancellation")
	require.Equal(t, 0, exec.sendCount("Filter object v1/ConfigMap/default/cm-2"))
	require.Equal(t, 0, met.calls)

	// Resume: verification proves the ambiguous op was applied; the remaining
	// ops run and metrics apply once.
	exec.hook = nil
	ver.verdict[name] = objectpatch.VerifyApplied
	d = c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionSuccess, d.Kind)
	require.Equal(t, 1, exec.sendCount(name), "verified op is never resent")
	require.Equal(t, 1, ver.callCount(name))
	require.Equal(t, 1, met.calls)
}

func Test_Coordinator_ConflictVerifiedApplied(t *testing.T) {
	c, exec, ver, met := newTestCoordinator(t)
	in := commitInput("task-1", threeOpPatchBytes(), 3)
	name := "Filter object v1/ConfigMap/default/cm-2"
	exec.outcomes[name] = &objectpatch.OperationOutcome{State: objectpatch.OutcomeConflict, Err: errTemp("409")}
	// A concurrent writer persisted exactly the intended content.
	ver.verdict[name] = objectpatch.VerifyApplied

	d := c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionSuccess, d.Kind)
	require.Equal(t, 1, exec.sendCount(name), "verified conflict must not be resent")
	require.Equal(t, 1, ver.callCount(name))
	require.Equal(t, 1, met.calls)
}

func Test_Coordinator_ConflictVerifiedIncompatibleDeadLetter(t *testing.T) {
	c, exec, ver, met := newTestCoordinator(t)
	in := commitInput("task-1", threeOpPatchBytes(), 3)
	name := "Filter object v1/ConfigMap/default/cm-2"
	exec.outcomes[name] = &objectpatch.OperationOutcome{State: objectpatch.OutcomeConflict, Err: errTemp("409")}
	ver.verdict[name] = objectpatch.VerifyIncompatible

	d := c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionDeadLetter, d.Kind)
	require.Equal(t, "Conflict", d.Failure.Reason)
	require.Equal(t, 1, exec.sendCount(name))
	require.Equal(t, 1, ver.callCount(name))
	require.Equal(t, 0, met.calls)
}

func Test_Coordinator_MetricsGuardAvoidsInProcessDoubleAdd(t *testing.T) {
	base, _ := newTestStore(t)
	store := &flakyStore{
		Store: base,
		skipSavesFor: func(rec *GenerationRecord) bool {
			return rec.Stages[StageMetrics] != nil && rec.Stages[StageMetrics].State == StageCommitted
		},
	}
	c, exec, _, met := newCoordinatorWithStore(t, store)
	in := commitInput("task-1", threeOpPatchBytes(), 3)

	// First pass: patches finish, metrics apply, but their Committed record
	// write is lost -> the task fails and will be retried in-process.
	d := c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionFail, d.Kind)
	require.Equal(t, 1, met.calls)

	// Retry reloads stage state Applying from disk; the in-process guard makes
	// the coordinator adopt the applied batch without re-adding counters.
	d = c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionSuccess, d.Kind)
	require.Equal(t, 1, met.calls, "metrics must be applied exactly once in-process")
	require.True(t, store.failed)
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-1"))
}

func Test_Coordinator_ValuesGuardAvoidsInProcessDoubleCommit(t *testing.T) {
	base, _ := newTestStore(t)
	store := &flakyStore{
		Store: base,
		skipSavesFor: func(rec *GenerationRecord) bool {
			return rec.Stages[StageValues] != nil && rec.Stages[StageValues].State == StageCommitted
		},
	}
	reg := NewRegistry(store, testLogger())
	exec := newScriptedExecutor()
	values := &fakeValueCommitter{}
	c := NewCoordinator(reg, exec, newScriptedVerifier(), &fakeMetricsApplier{}, testLogger(), WithValueCommitter(values))
	in := commitInput("task-1", threeOpPatchBytes(), 3)

	d := c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionFail, d.Kind)
	require.Equal(t, 1, values.calls)

	d = c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionSuccess, d.Kind)
	require.Equal(t, 1, values.calls, "value committer must not be invoked again in-process")
}

func Test_Coordinator_ConfirmedOpGuardAvoidsResendAfterLostSave(t *testing.T) {
	base, _ := newTestStore(t)
	store := &flakyStore{
		Store: base,
		skipSavesFor: func(rec *GenerationRecord) bool {
			return len(rec.Ops) > 0 && rec.Ops[0].State == OpConfirmed
		},
	}
	c, exec, _, met := newCoordinatorWithStore(t, store)
	in := commitInput("task-1", threeOpPatchBytes(), 3)

	// cm-2 fails on the first pass; cm-1's confirmation record is lost.
	exec.outcomes["Filter object v1/ConfigMap/default/cm-2"] = &objectpatch.OperationOutcome{
		State: objectpatch.OutcomeRetryable, Err: errTemp("slow"),
	}
	d := c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionFail, d.Kind)

	delete(exec.outcomes, "Filter object v1/ConfigMap/default/cm-2")
	d = c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionSuccess, d.Kind)

	// cm-1's send was confirmed in-process and the lost journal record must
	// not cause a second send; cm-2 did fail and is sent once more.
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-1"))
	require.Equal(t, 2, exec.sendCount("Filter object v1/ConfigMap/default/cm-2"))
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-3"))
	require.Equal(t, 1, met.calls)
}

func Test_Coordinator_StructuredLogsCarryCoordinates(t *testing.T) {
	// Terminal path: dead-letter log must carry hook/generation/seq/opIndex.
	var deadBuf bytes.Buffer
	deadLogger := log.NewLogger(log.WithOutput(&deadBuf))
	store, _ := newTestStore(t)
	reg := NewRegistry(store, deadLogger)
	exec := newScriptedExecutor()
	ver := newScriptedVerifier()
	c := NewCoordinator(reg, exec, ver, &fakeMetricsApplier{}, deadLogger)
	in := commitInput("task-1", threeOpPatchBytes(), 3)
	name := "Filter object v1/ConfigMap/default/cm-2"
	exec.outcomes[name] = &objectpatch.OperationOutcome{State: objectpatch.OutcomeConflict, Err: errTemp("409")}
	ver.verdict[name] = objectpatch.VerifyIncompatible

	d := c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionDeadLetter, d.Kind)
	logText := deadBuf.String()
	require.Contains(t, logText, `"hook":"hooks/test.sh"`)
	require.Contains(t, logText, `"seq":1`)
	require.Contains(t, logText, `"opIndex":1`)
	require.Contains(t, logText, `"generation":"`+string(d.Failure.GenerationID)+`"`)
	require.Contains(t, logText, "dead-letter")

	// Success path: stage commit logs carry the same coordinates.
	var okBuf bytes.Buffer
	okLogger := log.NewLogger(log.WithOutput(&okBuf))
	store2, _ := newTestStore(t)
	c2 := NewCoordinator(NewRegistry(store2, okLogger), newScriptedExecutor(), newScriptedVerifier(),
		&fakeMetricsApplier{}, okLogger)
	d2 := c2.Commit(context.Background(), "bh", "fp", commitInput("task-9", threeOpPatchBytes(), 3))
	require.Equal(t, DecisionSuccess, d2.Kind)
	okText := okBuf.String()
	require.Contains(t, okText, "object patches stage committed")
	require.Contains(t, okText, "metrics stage committed")
	require.Contains(t, okText, `"metrics":1`)
	require.Contains(t, okText, `"hook":"hooks/test.sh"`)
}

func Test_FrozenMetricOps_RoundTrip(t *testing.T) {
	v := 3.5
	ops := []metricsop.MetricOperation{
		{Name: "c", Action: metricsop.ActionCounterAdd, Value: &v, Labels: map[string]string{"x": "y"}},
		{Name: "g", Group: "grp", Action: metricsop.ActionExpireMetrics},
	}
	frozen := FrozenMetricOpsFromExternal(ops)
	require.Equal(t, "CounterAdd", frozen[0].Action)
	require.Equal(t, "ExpireMetrics", frozen[1].Action)
	require.Equal(t, metricsop.ActionCounterAdd, frozen[0].ToExternal().Action)
	require.Equal(t, metricsop.ActionExpireMetrics, frozen[1].ToExternal().Action)
}

// Test_Coordinator_ConflictAfterVerifiedMissingBoundsOneResendPerTick covers
// the chain: ambiguous send -> verification proves "not applied" -> the single
// resend allowed in this tick gets a 409. The conflict must NOT become an
// immediate dead letter (live content is still unexamined) and the tick must
// not loop verify->send; the next tick verifies before any terminal decision.
func Test_Coordinator_ConflictAfterVerifiedMissingBoundsOneResendPerTick(t *testing.T) {
	c, exec, ver, met := newTestCoordinator(t)
	in := commitInput("task-1", threeOpPatchBytes(), 3)
	name := "Filter object v1/ConfigMap/default/cm-2"

	// Send #1 is ambiguous; the hook swaps in a conflict for every later
	// send (outcomes are looked up by the fake before the hook runs).
	exec.outcomes[name] = &objectpatch.OperationOutcome{State: objectpatch.OutcomeUncertain, Err: errTemp("connection reset")}
	exec.hook = func(got string) {
		if got == name && exec.sendCount(name) == 1 {
			exec.outcomes[name] = &objectpatch.OperationOutcome{State: objectpatch.OutcomeConflict, Err: errTemp("the object has been modified (409)")}
		}
	}
	ver.verdict[name] = objectpatch.VerifyNotApplied

	d := c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionFail, d.Kind, "unexamined conflict after a verified-missing resend is retryable, not terminal")
	require.Equal(t, 2, exec.sendCount(name), "at most one resend per tick")
	require.Equal(t, 1, ver.callCount(name), "only the post-ambiguity verification happens this tick")
	require.Equal(t, 0, exec.sendCount("Filter object v1/ConfigMap/default/cm-3"))
	require.Equal(t, 0, met.calls)
	// The op stays Uncertain: on the next tick live content must be verified
	// BEFORE another send (a 409 proves another writer touched the object).
	require.Equal(t, OpUncertain, d.Record.Ops[1].State)

	// Next tick: no blind resend happens; verification against live content
	// decides. Incompatible content produces the dead letter with zero extra
	// sends.
	ver.verdict[name] = objectpatch.VerifyIncompatible
	d = c.Commit(context.Background(), "bh", "fp", in)
	require.Equal(t, DecisionDeadLetter, d.Kind)
	require.Equal(t, "Conflict", d.Failure.Reason)
	require.Equal(t, 2, exec.sendCount(name), "verification precedes any further send")
	require.Equal(t, 2, ver.callCount(name))
}

// guardKeysFor returns the in-process applied-guard entries belonging to one
// generation (same-package white-box assertion).
func guardKeysFor(c *Coordinator, id GenerationID) []string {
	c.guardMu.Lock()
	defer c.guardMu.Unlock()
	var keys []string
	for key := range c.guard {
		if strings.Contains(key, string(id)) {
			keys = append(keys, key)
		}
	}
	return keys
}

// Test_Coordinator_GuardMapEvictedOnTerminalStates ensures the in-process
// guard map (which may hold captured pre-images) does not accumulate entries
// once a generation reaches a terminal state: success archives it, conflicts
// dead-letter it and fence-offs supersede it.
func Test_Coordinator_GuardMapEvictedOnTerminalStates(t *testing.T) {
	// Success: op and stage guards are populated mid-flight and dropped after.
	c, exec, _, _ := newTestCoordinator(t)
	var sawGuardsMidFlight bool
	exec.hook = func(got string) {
		if got == "Filter object v1/ConfigMap/default/cm-2" {
			c.guardMu.Lock()
			sawGuardsMidFlight = len(c.guard) > 0
			c.guardMu.Unlock()
		}
	}
	d := c.Commit(context.Background(), "bh", "fp", commitInput("task-ok", threeOpPatchBytes(), 3))
	require.Equal(t, DecisionSuccess, d.Kind)
	require.True(t, sawGuardsMidFlight, "guard must hold confirmed ops before the generation finishes")
	require.Empty(t, guardKeysFor(c, d.Record.ID))

	// Dead letter: the first op was confirmed and guarded; evicted on terminal.
	c2, exec2, ver2, _ := newTestCoordinator(t)
	name2 := "Filter object v1/ConfigMap/default/cm-2"
	exec2.outcomes[name2] = &objectpatch.OperationOutcome{State: objectpatch.OutcomeConflict, Err: errTemp("409")}
	ver2.verdict[name2] = objectpatch.VerifyIncompatible
	d2 := c2.Commit(context.Background(), "bh", "fp", commitInput("task-bad", threeOpPatchBytes(), 3))
	require.Equal(t, DecisionDeadLetter, d2.Kind)
	require.Empty(t, guardKeysFor(c2, d2.Record.ID))

	// Superseded: op1 was guarded before the fence tripped; evicted on retire.
	store3, _ := newTestStore(t)
	reg3 := NewRegistry(store3, testLogger())
	exec3 := newScriptedExecutor()
	ver3 := newScriptedVerifier()
	c3 := NewCoordinator(reg3, exec3, ver3, &fakeMetricsApplier{}, testLogger())
	exec3.hook = func(string) {
		newer := commitInput("task-new", threeOpPatchBytes(), 3)
		_, _, err := reg3.BeginOrResume(newer.FreezeInput("bh", "fp-new"))
		require.NoError(t, err)
	}
	d3 := c3.Commit(context.Background(), "bh", "fp-old", commitInput("task-old", threeOpPatchBytes(), 3))
	require.Equal(t, DecisionSuperseded, d3.Kind)
	require.Empty(t, guardKeysFor(c3, d3.Record.ID))
}

// Test_Coordinator_SyncFailureMarksOrphanedAndResumesByFingerprint covers the
// synchronous webhook path: an unfinished generation has no in-memory owner
// task, so it must be marked orphaned (durable) and resumed by frozen-result
// fingerprint on the next HTTP retry instead of leaking or starting over.
func Test_Coordinator_SyncFailureMarksOrphanedAndResumesByFingerprint(t *testing.T) {
	c, exec, _, met := newTestCoordinator(t)
	in := commitInput("webhook-call-1", threeOpPatchBytes(), 3)
	in.Queued = false
	name := "Filter object v1/ConfigMap/default/cm-2"
	exec.outcomes[name] = &objectpatch.OperationOutcome{State: objectpatch.OutcomeRetryable, Err: errTemp("api is slow")}

	d := c.Commit(context.Background(), "bh", "fp-webhook", in)
	require.Equal(t, DecisionFail, d.Kind)
	genID := d.Record.ID

	// The owner task is gone: the active ref must be flagged orphaned so it
	// becomes resumable by fingerprint (and retired by a newer generation)...
	fs := c.Registry().Store().(*FileStore)
	fs.mu.Lock()
	refs := fs.index.Hooks["hooks/test.sh"].Active
	require.Len(t, refs, 1)
	require.True(t, refs[0].Orphaned)
	require.Equal(t, genID, refs[0].ID)
	fs.mu.Unlock()
	// ...which is how the next (identical) HTTP retry correlates with it.
	orphan := c.Registry().Store().ResumeableByFingerprint("hooks/test.sh", "fp-webhook")
	require.NotNil(t, orphan)
	require.Equal(t, genID, orphan.ID)

	// The orphan flag is durable (a restart would recover the same state).
	raw, err := os.ReadFile(filepath.Join(fs.dir, indexFileName))
	require.NoError(t, err)
	require.Contains(t, string(raw), `"orphaned": true`)

	// Next HTTP retry: new one-shot task ID, identical frozen result.
	delete(exec.outcomes, name)
	in2 := commitInput("webhook-call-2", threeOpPatchBytes(), 3)
	in2.Queued = false
	d2 := c.Commit(context.Background(), "bh", "fp-webhook", in2)
	require.Equal(t, DecisionSuccess, d2.Kind)
	require.Equal(t, genID, d2.Record.ID, "the retry resumes the original generation instead of freezing a new one")

	// cm-1 was confirmed during the failed call: zero replay. cm-2 retried.
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-1"))
	require.Equal(t, 2, exec.sendCount(name))
	require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-3"))
	require.Equal(t, 1, met.calls)
}

// Test_Coordinator_TerminalIndexFaultDoesNotRefreezeOrReplay is the
// end-to-end guard for the final-transition crash window: if the journal
// index write fails while finishing a generation, the retry must complete
// the SAME generation without re-sending any patch or re-adding metrics —
// freezing a fresh generation here would double-apply non-idempotent
// operations and double-count counters.
func Test_Coordinator_TerminalIndexFaultDoesNotRefreezeOrReplay(t *testing.T) {
	t.Run("success finalization fault is completed in place", func(t *testing.T) {
		dir := filepathJoinTemp(t)
		// Index writes: #1 Freeze, #2 Succeed (fails), #3 completing replay.
		store, err := NewFileStore(dir, testLogger(), withIndexSaveFaultAtCall(2))
		require.NoError(t, err)
		c, exec, _, met := newCoordinatorWithStore(t, store)
		in := commitInput("task-1", threeOpPatchBytes(), 3)

		d := c.Commit(context.Background(), "bh", "fp", in)
		require.Equal(t, DecisionFail, d.Kind)
		require.Error(t, d.Err)
		genID := d.Record.ID
		require.NotNil(t, store.ByTask("hooks/test.sh", "task-1"))

		d = c.Commit(context.Background(), "bh", "fp", in)
		require.Equal(t, DecisionSuccess, d.Kind)
		require.Equal(t, genID, d.Record.ID, "the original generation is completed, not refrozen")

		require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-1"))
		require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-2"))
		require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-3"))
		require.Equal(t, 1, met.calls, "metrics are never applied twice")

		_, err = store.Get(genID)
		require.Error(t, err, "journal is archived after the completed replay")
	})

	t.Run("dead-letter finalization fault is completed in place", func(t *testing.T) {
		dir := filepathJoinTemp(t)
		store, err := NewFileStore(dir, testLogger(), withIndexSaveFaultAtCall(2))
		require.NoError(t, err)
		c, exec, ver, _ := newCoordinatorWithStore(t, store)
		in := commitInput("task-1", threeOpPatchBytes(), 3)
		name := "Filter object v1/ConfigMap/default/cm-2"
		exec.outcomes[name] = &objectpatch.OperationOutcome{State: objectpatch.OutcomeConflict, Err: errTemp("409")}
		ver.verdict[name] = objectpatch.VerifyIncompatible

		d := c.Commit(context.Background(), "bh", "fp", in)
		require.Equal(t, DecisionFail, d.Kind, "the failed final index write surfaces as a retryable failure")
		genID := d.Record.ID

		d = c.Commit(context.Background(), "bh", "fp", in)
		require.Equal(t, DecisionDeadLetter, d.Kind)
		require.Equal(t, genID, d.Record.ID, "dead letter completes the original generation")
		require.Equal(t, 1, exec.sendCount(name), "terminal conflict is never resent")

		failures, err := store.ListFailures()
		require.NoError(t, err)
		require.Len(t, failures, 1)
		require.Equal(t, genID, failures[0].GenerationID)
	})

	t.Run("synchronous retry completes an interrupted success in place", func(t *testing.T) {
		// Synchronous webhook calls use a fresh task ID per HTTP request; the
		// only correlation channel is the frozen-result fingerprint. The
		// interrupted terminal record is orphaned and resumed by fingerprint.
		dir := filepathJoinTemp(t)
		store, err := NewFileStore(dir, testLogger(), withIndexSaveFaultAtCall(2))
		require.NoError(t, err)
		c, exec, _, met := newCoordinatorWithStore(t, store)
		in := commitInput("http-call-1", threeOpPatchBytes(), 3)
		in.Queued = false

		d := c.Commit(context.Background(), "bh", "fp-sync", in)
		require.Equal(t, DecisionFail, d.Kind)
		genID := d.Record.ID
		require.NotNil(t, store.ResumeableByFingerprint("hooks/test.sh", "fp-sync"),
			"the finalized but unacknowledged sync generation stays resumable by fingerprint")

		in2 := commitInput("http-call-2", threeOpPatchBytes(), 3)
		in2.Queued = false
		d = c.Commit(context.Background(), "bh", "fp-sync", in2)
		require.Equal(t, DecisionSuccess, d.Kind)
		require.Equal(t, genID, d.Record.ID, "no new generation: terminal transition completed in place")
		require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-1"))
		require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-2"))
		require.Equal(t, 1, exec.sendCount("Filter object v1/ConfigMap/default/cm-3"))
		require.Equal(t, 1, met.calls)
	})

	t.Run("synchronous retry completes an interrupted dead letter in place", func(t *testing.T) {
		// Same window for the dead-letter path: the next call must surface the
		// original terminal verdict, not freeze a fresh generation that could
		// overturn the conflict decision into success.
		dir := filepathJoinTemp(t)
		store, err := NewFileStore(dir, testLogger(), withIndexSaveFaultAtCall(2))
		require.NoError(t, err)
		c, exec, ver, _ := newCoordinatorWithStore(t, store)
		in := commitInput("http-call-1", threeOpPatchBytes(), 3)
		in.Queued = false
		name := "Filter object v1/ConfigMap/default/cm-2"
		exec.outcomes[name] = &objectpatch.OperationOutcome{State: objectpatch.OutcomeConflict, Err: errTemp("409")}
		ver.verdict[name] = objectpatch.VerifyIncompatible

		d := c.Commit(context.Background(), "bh", "fp-sync", in)
		require.Equal(t, DecisionFail, d.Kind)
		genID := d.Record.ID

		in2 := commitInput("http-call-2", threeOpPatchBytes(), 3)
		in2.Queued = false
		d = c.Commit(context.Background(), "bh", "fp-sync", in2)
		require.Equal(t, DecisionDeadLetter, d.Kind, "the terminal verdict is not overturned")
		require.Equal(t, genID, d.Record.ID)
		require.Equal(t, 1, exec.sendCount(name), "conflicting patch is never replayed")

		failures, err := store.ListFailures()
		require.NoError(t, err)
		require.Len(t, failures, 1)
		require.Equal(t, genID, failures[0].GenerationID)
	})
}
