package shell_operator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/deckhouse/deckhouse/pkg/log"
	metricsop "github.com/deckhouse/deckhouse/pkg/metrics-storage/operation"
	sdkpkg "github.com/deckhouse/module-sdk/pkg"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/flant/shell-operator/pkg/app"
	bindingcontext "github.com/flant/shell-operator/pkg/hook/binding_context"
	"github.com/flant/shell-operator/pkg/hook/controller"
	"github.com/flant/shell-operator/pkg/hook/resultgen"
	"github.com/flant/shell-operator/pkg/hook/task_metadata"
	htypes "github.com/flant/shell-operator/pkg/hook/types"
	kemtypes "github.com/flant/shell-operator/pkg/kube_events_manager/types"
	objectpatch "github.com/flant/shell-operator/pkg/kube/object_patch"
	"github.com/flant/shell-operator/pkg/task"
	"github.com/flant/shell-operator/pkg/task/queue"
)

// --- scripted commit dependencies (minimal copies of the resultgen
// package-internal test fakes) ----------------------------------------------

type opExecutor struct {
	mu      sync.Mutex
	sends   map[string]int
	outcome *objectpatch.OperationOutcome
}

func (e *opExecutor) ExecuteOperationCtx(_ context.Context, op sdkpkg.PatchCollectorOperation) *objectpatch.OperationOutcome {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sends[op.Description()]++
	if e.outcome != nil {
		cp := *e.outcome
		return &cp
	}
	return &objectpatch.OperationOutcome{State: objectpatch.OutcomeApplied}
}

func (e *opExecutor) GetOperationObject(_ context.Context, _ sdkpkg.PatchCollectorOperation) (*unstructured.Unstructured, error) {
	return &unstructured.Unstructured{Object: map[string]any{}}, nil
}

type opVerifier struct{ state objectpatch.VerifyState }

func (v *opVerifier) VerifyOperation(_ context.Context, _ sdkpkg.PatchCollectorOperation, _ objectpatch.VerifyEvidence) *objectpatch.VerifyResult {
	return &objectpatch.VerifyResult{State: v.state}
}

type opMetrics struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (m *opMetrics) ApplyBatchOperations(_ []metricsop.MetricOperation, _ map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	return m.err
}

// --- fixtures ---------------------------------------------------------------

const hookConfig = `if [[ $1 == "--config" ]] ; then
cat <<'CFGEOF'
configVersion: v1
onStartup: 1
CFGEOF
else
`

const successHookScript = `#!/usr/bin/env bash
set -e
` + hookConfig + `
if [[ -n "$KUBERNETES_PATCH_PATH" ]]; then
  cat > "$KUBERNETES_PATCH_PATH" <<'EOF'
---
operation: MergePatch
apiVersion: v1
kind: ConfigMap
namespace: default
name: cm-1
mergePatch:
  data:
    a: "1"
EOF
fi
if [[ -n "$METRICS_PATH" ]]; then
  echo '{"name":"hook_metric","action":"add","value":1,"labels":{}}' > "$METRICS_PATH"
fi
fi
exit 0
`

const failingHookScript = `#!/usr/bin/env bash
` + hookConfig + `
echo "boom" >&2
exit 1
fi
`

const metricsOnlyHookScript = `#!/usr/bin/env bash
` + hookConfig + `
if [[ -n "$METRICS_PATH" ]]; then
  echo '{"name":"hook_metric","action":"set","value":2,"labels":{}}' > "$METRICS_PATH"
fi
fi
exit 0
`

func writeHook(t *testing.T, hooksDir, name, script string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(hooksDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(hooksDir, name), []byte(script), 0o755))
}

func newOperatorForHook(t *testing.T, name, script string) (*ShellOperator, string) {
	t.Helper()
	dir := t.TempDir()
	hooksDir := filepath.Join(dir, "hooks")
	writeHook(t, hooksDir, name, script)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "tmp"), 0o755))

	op := newBareShellOperator(context.Background(), WithLogger(log.NewNop()))
	op.SetupEventManagers()
	op.setupHookManagers(app.NewConfig(), hooksDir, filepath.Join(dir, "tmp"))
	require.NoError(t, op.initHookManager())
	return op, dir
}

func hookTask(name string) task.Task {
	return task.NewTask("hook-run").
		WithMetadata(task_metadata.HookMetadata{
			HookName:       name,
			BindingType:    htypes.OnKubernetesEvent,
			BindingContext: []bindingcontext.BindingContext{{}},
		})
}

func runHandle(t *testing.T, op *ShellOperator, tsk task.Task, ctx context.Context) (queue.TaskStatus, error) {
	t.Helper()
	meta, ok := task_metadata.HookMetadataAccessor(tsk)
	require.True(t, ok)
	taskHook := op.HookManager.GetHook(meta.HookName)
	require.NotNil(t, taskHook)
	return op.handleRunHook(ctx, tsk, taskHook, meta, log.NewNop(),
		map[string]string{}, map[string]string{})
}

// --- tests ------------------------------------------------------------------

func Test_handleRunHook_StagedCommitSuccess(t *testing.T) {
	op, dir := newOperatorForHook(t, "hook01.sh", successHookScript)

	store, err := resultgen.NewFileStore(filepath.Join(dir, "rg"), log.NewNop())
	require.NoError(t, err)
	registry := resultgen.NewRegistry(store, log.NewNop())
	exec := &opExecutor{sends: map[string]int{}}
	met := &opMetrics{}
	op.ResultCommitter = resultgen.NewCoordinator(registry, exec, &opVerifier{}, met, log.NewNop())

	status, err := runHandle(t, op, hookTask("hook01.sh"), context.Background())
	require.NoError(t, err)
	require.Equal(t, queue.Success, status)
	require.Equal(t, 1, exec.sends["Filter object v1/ConfigMap/default/cm-1"])
	require.Equal(t, 1, met.calls)
}

func Test_handleRunHook_ConflictIsDeadLetterAndQueryable(t *testing.T) {
	op, dir := newOperatorForHook(t, "hook01.sh", successHookScript)

	store, err := resultgen.NewFileStore(filepath.Join(dir, "rg"), log.NewNop())
	require.NoError(t, err)
	registry := resultgen.NewRegistry(store, log.NewNop())
	exec := &opExecutor{
		sends:   map[string]int{},
		outcome: &objectpatch.OperationOutcome{State: objectpatch.OutcomeConflict, Err: &stringErr{"409 conflict"}},
	}
	met := &opMetrics{}
	op.ResultCommitter = resultgen.NewCoordinator(registry, exec, &opVerifier{}, met, log.NewNop())

	tsk := hookTask("hook01.sh")
	status, err := runHandle(t, op, tsk, context.Background())
	require.NoError(t, err, "dead letter is a status, not a handler error")
	require.Equal(t, queue.DeadLetter, status)
	require.Equal(t, 0, met.calls, "metrics never run after a terminal conflict")
	require.Contains(t, tsk.GetFailureMessage(), "dead-lettered")

	failures, err := registry.ListFailures()
	require.NoError(t, err)
	require.Len(t, failures, 1)
	require.Equal(t, "Conflict", failures[0].Reason)
	require.Equal(t, "hook01.sh", failures[0].HookName)
}

// Test_taskHandleHookRun_DeadLetterNotSwallowedByAllowFailure: AllowFailure
// only covers hook execution failures; a terminal commit conflict must leave
// the task as DeadLetter even when the hook is allowed to fail.
func Test_taskHandleHookRun_DeadLetterNotSwallowedByAllowFailure(t *testing.T) {
	op, dir := newOperatorForHook(t, "hook01.sh", successHookScript)
	store, err := resultgen.NewFileStore(filepath.Join(dir, "rg"), log.NewNop())
	require.NoError(t, err)
	registry := resultgen.NewRegistry(store, log.NewNop())
	exec := &opExecutor{
		sends:   map[string]int{},
		outcome: &objectpatch.OperationOutcome{State: objectpatch.OutcomeConflict, Err: &stringErr{"409"}},
	}
	op.ResultCommitter = resultgen.NewCoordinator(registry, exec,
		&opVerifier{state: objectpatch.VerifyIncompatible}, &opMetrics{}, log.NewNop())

	tsk := hookTask("hook01.sh")
	meta, ok := task_metadata.HookMetadataAccessor(tsk)
	require.True(t, ok)
	meta.AllowFailure = true
	tsk.UpdateMetadata(meta)

	res := op.taskHandleHookRun(context.Background(), tsk)
	require.Equal(t, queue.DeadLetter, res.Status, "AllowFailure must not turn a terminal conflict into success")
}

func Test_handleRunHook_HookExplicitFailureUnchanged(t *testing.T) {
	op, _ := newOperatorForHook(t, "hook01.sh", failingHookScript)

	status, err := runHandle(t, op, hookTask("hook01.sh"), context.Background())
	require.Error(t, err)
	require.Equal(t, queue.Fail, status)
}

func Test_handleRunHook_CancelBeforeStagesRepeats(t *testing.T) {
	op, dir := newOperatorForHook(t, "hook01.sh", successHookScript)

	store, err := resultgen.NewFileStore(filepath.Join(dir, "rg"), log.NewNop())
	require.NoError(t, err)
	registry := resultgen.NewRegistry(store, log.NewNop())
	exec := &opExecutor{sends: map[string]int{}}
	met := &opMetrics{}
	op.ResultCommitter = resultgen.NewCoordinator(registry, exec, &opVerifier{}, met, log.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	status, err := runHandle(t, op, hookTask("hook01.sh"), ctx)
	require.NoError(t, err)
	require.Equal(t, queue.Repeat, status)
	require.Equal(t, 0, exec.sends["Filter object v1/ConfigMap/default/cm-1"])
	require.Equal(t, 0, met.calls)
}

func Test_handleRunHook_LegacyMetricsOnlyPath(t *testing.T) {
	// Bare operator (no ResultCommitter, no ObjectPatcher): a metrics-only
	// hook keeps succeeding through the legacy direct path.
	op, _ := newOperatorForHook(t, "hook02.sh", metricsOnlyHookScript)

	status, err := runHandle(t, op, hookTask("hook02.sh"), context.Background())
	require.NoError(t, err)
	require.Equal(t, queue.Success, status)
}

func Test_handleRunHook_StagedCommitFailureIsMarkedAndRetryable(t *testing.T) {
	op, dir := newOperatorForHook(t, "hook01.sh", successHookScript)

	store, err := resultgen.NewFileStore(filepath.Join(dir, "rg"), log.NewNop())
	require.NoError(t, err)
	registry := resultgen.NewRegistry(store, log.NewNop())
	exec := &opExecutor{
		sends:   map[string]int{},
		outcome: &objectpatch.OperationOutcome{State: objectpatch.OutcomeRetryable, Err: &stringErr{"api down"}},
	}
	met := &opMetrics{}
	op.ResultCommitter = resultgen.NewCoordinator(registry, exec, &opVerifier{}, met, log.NewNop())

	status, err := runHandle(t, op, hookTask("hook01.sh"), context.Background())
	require.Equal(t, queue.Fail, status)
	require.Error(t, err)
	// The failure must be recognizable as a staged-commit failure so
	// AllowFailure cannot swallow it as a successful hook execution.
	var se *stagedCommitError
	require.ErrorAs(t, err, &se)
	require.Equal(t, 0, met.calls)
}

func Test_handleRunHook_HookExecutionFailureIsNotStagedError(t *testing.T) {
	op, _ := newOperatorForHook(t, "hook01.sh", failingHookScript)

	status, err := runHandle(t, op, hookTask("hook01.sh"), context.Background())
	require.Equal(t, queue.Fail, status)
	require.Error(t, err)
	var se *stagedCommitError
	require.False(t, errors.As(err, &se), "hook execution errors stay AllowFailure-eligible")
}

func Test_synchronousHookExecutionMarker(t *testing.T) {
	require.False(t, isSynchronousHookExecution(context.Background()))
	syncCtx := withSynchronousHookExecution(context.Background())
	require.True(t, isSynchronousHookExecution(syncCtx))
}

// fakeKubeBindingsController records event-unlock calls; the embedded nil
// interface guarantees any other method call would panic (only unlock is
// expected on these paths).
type fakeKubeBindingsController struct {
	controller.KubernetesBindingsController
	mu       sync.Mutex
	unlocked []string
}

func (f *fakeKubeBindingsController) UnlockEventsFor(monitorID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unlocked = append(f.unlocked, monitorID)
}

func (f *fakeKubeBindingsController) SnapshotsInfo() []string { return nil }

func (f *fakeKubeBindingsController) SnapshotsFor(string) []kemtypes.ObjectAndFilterResult {
	return nil
}

func (f *fakeKubeBindingsController) unlockedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.unlocked...)
}

func synchronizationTask(name string) task.Task {
	syncCtx := bindingcontext.BindingContext{Type: kemtypes.TypeSynchronization}
	syncCtx.Metadata.Version = "v1"
	syncCtx.Metadata.BindingType = htypes.OnKubernetesEvent
	return task.NewTask("hook-run").
		WithMetadata(task_metadata.HookMetadata{
			HookName:                 name,
			BindingType:              htypes.OnKubernetesEvent,
			BindingContext:           []bindingcontext.BindingContext{syncCtx},
			MonitorIDs:               []string{"monitor-1", "monitor-2"},
			ExecuteOnSynchronization: true,
		})
}

// Test_taskHandleHookRun_SynchronizationUnlockedOnDeadLetter: a dead-lettered
// Synchronization leaves the queue and is never retried, so its monitor event
// callbacks must be released — otherwise the binding stays deaf until restart.
// A retryable Fail keeps the historical behavior (locked until success).
func Test_taskHandleHookRun_SynchronizationUnlockedOnDeadLetter(t *testing.T) {
	t.Run("dead letter releases monitor locks", func(t *testing.T) {
		op, dir := newOperatorForHook(t, "hook01.sh", successHookScript)
		store, err := resultgen.NewFileStore(filepath.Join(dir, "rg"), log.NewNop())
		require.NoError(t, err)
		registry := resultgen.NewRegistry(store, log.NewNop())
		exec := &opExecutor{
			sends:   map[string]int{},
			outcome: &objectpatch.OperationOutcome{State: objectpatch.OutcomeConflict, Err: &stringErr{"409"}},
		}
		op.ResultCommitter = resultgen.NewCoordinator(registry, exec,
			&opVerifier{state: objectpatch.VerifyIncompatible}, &opMetrics{}, log.NewNop())

		fakeCtrl := &fakeKubeBindingsController{}
		op.HookManager.GetHook("hook01.sh").HookController.KubernetesController = fakeCtrl

		res := op.taskHandleHookRun(context.Background(), synchronizationTask("hook01.sh"))
		require.Equal(t, queue.DeadLetter, res.Status)
		require.Equal(t, []string{"monitor-1", "monitor-2"}, fakeCtrl.unlockedIDs())
	})

	t.Run("retryable failure keeps monitor locked", func(t *testing.T) {
		op, dir := newOperatorForHook(t, "hook01.sh", successHookScript)
		store, err := resultgen.NewFileStore(filepath.Join(dir, "rg"), log.NewNop())
		require.NoError(t, err)
		registry := resultgen.NewRegistry(store, log.NewNop())
		exec := &opExecutor{
			sends:   map[string]int{},
			outcome: &objectpatch.OperationOutcome{State: objectpatch.OutcomeRetryable, Err: &stringErr{"slow"}},
		}
		op.ResultCommitter = resultgen.NewCoordinator(registry, exec, &opVerifier{}, &opMetrics{}, log.NewNop())

		fakeCtrl := &fakeKubeBindingsController{}
		op.HookManager.GetHook("hook01.sh").HookController.KubernetesController = fakeCtrl

		res := op.taskHandleHookRun(context.Background(), synchronizationTask("hook01.sh"))
		require.Equal(t, queue.Fail, res.Status)
		require.Empty(t, fakeCtrl.unlockedIDs(), "the task stays queued and unlocks after a future success")
	})

	t.Run("success releases monitor locks as before", func(t *testing.T) {
		op, dir := newOperatorForHook(t, "hook01.sh", successHookScript)
		store, err := resultgen.NewFileStore(filepath.Join(dir, "rg"), log.NewNop())
		require.NoError(t, err)
		registry := resultgen.NewRegistry(store, log.NewNop())
		op.ResultCommitter = resultgen.NewCoordinator(registry, &opExecutor{sends: map[string]int{}},
			&opVerifier{}, &opMetrics{}, log.NewNop())

		fakeCtrl := &fakeKubeBindingsController{}
		op.HookManager.GetHook("hook01.sh").HookController.KubernetesController = fakeCtrl

		res := op.taskHandleHookRun(context.Background(), synchronizationTask("hook01.sh"))
		require.Equal(t, queue.Success, res.Status)
		require.Equal(t, []string{"monitor-1", "monitor-2"}, fakeCtrl.unlockedIDs())
	})
}

type stringErr struct{ s string }

func (e *stringErr) Error() string { return e.s }
