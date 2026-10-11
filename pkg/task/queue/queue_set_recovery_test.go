package queue

import (
	"context"
	"os"
	"testing"
	"time"

	metricsstorage "github.com/deckhouse/deckhouse/pkg/metrics-storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/flant/shell-operator/pkg/hook/task_metadata"
	"github.com/flant/shell-operator/pkg/task"
)

// TestQueueSet_GracefulShutdownAndRecovery drives the public TaskQueueSet API:
// a main and a hook queue are created with a shared checkpoint directory,
// Shutdown persists their boundaries, and a fresh set restores them.
func TestQueueSet_GracefulShutdownAndRecovery(t *testing.T) {
	dir := t.TempDir()

	build := func() *TaskQueueSet {
		tqs := NewTaskQueueSet().
			WithMetricStorage(metricsstorage.NewMetricStorage(metricsstorage.WithNewRegistry())).
			WithCheckpointDir(dir)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tqs.WithContext(ctx)
		return tqs
	}

	tqs := build()
	require.NoError(t, tqs.NewNamedQueue(MainQueueName, nil, WithCompactableTypes(task_metadata.HookRun)))
	require.NoError(t, tqs.NewNamedQueue("kube-hooks", nil, WithCompactableTypes(task_metadata.HookRun)))

	main := tqs.GetMain()
	hooks := tqs.GetByName("kube-hooks")
	main.AddLast(serviceTaskWithID("m1"), serviceTaskWithID("m2"))
	hooks.AddLast(hookTaskWithID("h1", "hook-h", "x"))

	// Graceful shutdown: admission stops, boundary persists.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, tqs.Shutdown(shutdownCtx))
	assert.False(t, main.IsAccepting())

	// Restart into a fresh set backed by the same directory.
	tqs2 := build()
	require.NoError(t, tqs2.NewNamedQueue(MainQueueName, nil, WithCompactableTypes(task_metadata.HookRun)))
	require.NoError(t, tqs2.NewNamedQueue("kube-hooks", nil, WithCompactableTypes(task_metadata.HookRun)))

	assert.Equal(t, []string{"m1", "m2"}, queueIDs(tqs2.GetMain()))
	assert.Equal(t, []string{"h1"}, queueIDs(tqs2.GetByName("kube-hooks")))

	// Checkpoint file naming must keep different queues apart.
	assert.FileExists(t, mustStorePath(t, dir, MainQueueName))
	assert.FileExists(t, mustStorePath(t, dir, "kube-hooks"))

	shutdownCtx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	require.NoError(t, tqs2.Shutdown(shutdownCtx2))
}

// TestQueueSet_CorruptCheckpointSurfacesError: NewNamedQueue returns the
// corruption error and the queue is present but refuses to run.
func TestQueueSet_CorruptCheckpointSurfacesError(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileCheckpointStore(dir, MainQueueName)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(store.Path(), []byte("not-json"), 0o600))

	tqs := NewTaskQueueSet().
		WithMetricStorage(metricsstorage.NewMetricStorage(metricsstorage.WithNewRegistry())).
		WithCheckpointDir(dir)
	tqs.WithContext(context.Background())

	err = tqs.NewNamedQueue(MainQueueName, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCorruptCheckpoint)

	q := tqs.GetMain()
	require.NotNil(t, q)
	assert.False(t, q.IsAccepting())
	assert.Equal(t, err, q.RestoreError())
}

// TestQueueSet_NoPersistenceStaysCompatible: without a checkpoint directory
// queues behave exactly as before (NewNamedQueue succeeds, no files, shutdown
// plain).
func TestQueueSet_NoPersistenceStaysCompatible(t *testing.T) {
	tqs := NewTaskQueueSet().
		WithMetricStorage(metricsstorage.NewMetricStorage(metricsstorage.WithNewRegistry()))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tqs.WithContext(ctx)

	require.NoError(t, tqs.NewNamedQueue(MainQueueName, func(_ context.Context, _ task.Task) TaskResult {
		return TaskResult{Status: Success}
	}))
	tqs.GetMain().AddLast(serviceTaskWithID("a"))

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	require.NoError(t, tqs.Shutdown(shutdownCtx))
}

func mustStorePath(t *testing.T, dir, queueName string) string {
	t.Helper()
	store, err := NewFileCheckpointStore(dir, queueName)
	require.NoError(t, err)
	return store.Path()
}
