package queue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	metricsstorage "github.com/deckhouse/deckhouse/pkg/metrics-storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkg "github.com/flant/shell-operator/pkg"
	bindingcontext "github.com/flant/shell-operator/pkg/hook/binding_context"
	"github.com/flant/shell-operator/pkg/hook/task_metadata"
	"github.com/flant/shell-operator/pkg/hook/types"
	"github.com/flant/shell-operator/pkg/task"
)

func newRecoverableQueue(t *testing.T, dir, name string, store CheckpointStore) *TaskQueue {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if store == nil {
		var err error
		store, err = NewFileCheckpointStore(dir, name)
		require.NoError(t, err)
	}

	q := NewTasksQueue(name,
		metricsstorage.NewMetricStorage(metricsstorage.WithNewRegistry()),
		WithContext(ctx),
		WithCheckpointStore(store),
		WithCompactableTypes(task_metadata.HookRun),
	)
	q.WaitLoopCheckInterval = 5 * time.Millisecond
	q.DelayOnQueueIsEmpty = 5 * time.Millisecond
	q.DelayOnRepeat = 5 * time.Millisecond
	require.NoError(t, q.InitPersistence(ctx))
	t.Cleanup(func() {
		if q.cpWriter != nil && q.CheckpointError() == nil {
			_ = q.cpWriter.Close()
		}
	})
	return q
}

func reopenRecoverableQueue(t *testing.T, dir, name string) *TaskQueue {
	t.Helper()
	return newRecoverableQueue(t, dir, name, nil)
}

func hookTaskWithID(id, hookName string, binding string) *task.BaseTask {
	bc := bindingcontext.BindingContext{Binding: binding}
	bc.Metadata.BindingType = types.Schedule
	t := task.NewTask(task_metadata.HookRun).
		WithMetadata(task_metadata.HookMetadata{
			HookName:       hookName,
			BindingType:    types.Schedule,
			Binding:        binding,
			BindingContext: []bindingcontext.BindingContext{bc},
		}).
		WithQueueName("main").
		WithCompactionID(hookName)
	t.Id = id
	t.LogLabels[pkg.LogKeyTaskDotID] = id
	return t
}

func serviceTaskWithID(id string) *task.BaseTask {
	t := task.NewTask("Service")
	t.Id = id
	t.QueueName = "main"
	t.LogLabels[pkg.LogKeyTaskDotID] = id
	return t
}

func queueIDs(q *TaskQueue) []string {
	ids := make([]string, 0)
	q.IterateSnapshot(func(t task.Task) { ids = append(ids, t.GetId()) })
	return ids
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !fn() {
		t.Fatalf("condition not met within %s", timeout)
	}
}

// TestRecovery_EmptyQueueStart ensures a missing checkpoint is a valid
// empty-queue start and that the first mutation creates the file.
func TestRecovery_EmptyQueueStart(t *testing.T) {
	dir := t.TempDir()
	q := newRecoverableQueue(t, dir, "main", nil)

	assert.False(t, q.WasRestored())
	assert.Equal(t, 0, q.Length())

	q.AddLast(serviceTaskWithID("s1"))
	waitFor(t, time.Second, func() bool {
		store, err := NewFileCheckpointStore(dir, "main")
		require.NoError(t, err)
		cp, err := store.Load(context.Background())
		return err == nil && len(cp.Tasks) == 1
	})

	require.NoError(t, q.PersistShutdownBoundary(context.Background()))
}

// TestRecovery_RestoreOrderAndReplay: tasks survive an ordered shutdown;
// the claimed-but-unfinished head task comes back at its original position and
// is re-runnable, while completed tasks are absent.
func TestRecovery_ClaimedTaskReplaysFromPosition(t *testing.T) {
	dir := t.TempDir()
	q := newRecoverableQueue(t, dir, "main", nil)

	claimed := make(chan struct{})
	q.Handler = func(ctx context.Context, t task.Task) TaskResult {
		if t.GetId() == "block" {
			close(claimed)
			<-ctx.Done() // killed mid-flight by shutdown
			return TaskResult{Status: Repeat}
		}
		return TaskResult{Status: Success}
	}

	q.AddLast(
		hookTaskWithID("block", "hook-a", "b0"),
		hookTaskWithID("b", "hook-b", "b1"),
		hookTaskWithID("c", "hook-c", "b2"),
	)

	q.Start(q.ctx)
	waitFor(t, time.Second, func() bool {
		head := q.GetFirst()
		return head != nil && head.IsProcessing()
	})

	q.BeginShutdown()
	waitFor(t, time.Second, func() bool { return q.GetStatusType() == QueueStatusStop })
	require.NoError(t, q.PersistShutdownBoundary(context.Background()))

	// Restart into a fresh queue backed by the same file.
	q2 := reopenRecoverableQueue(t, dir, "main")
	assert.True(t, q2.WasRestored())
	assert.Equal(t, []string{"block", "b", "c"}, queueIDs(q2))

	head := q2.GetFirst()
	require.NotNil(t, head)
	assert.False(t, head.IsProcessing(), "claimed unfinished task must be restored as unclaimed")
	// Restored tasks carry the previous incarnation epoch.
	assert.Equal(t, int64(1), head.GetBirthEpoch())

	// A task enqueued after restart belongs to the new epoch.
	q2.AddLast(hookTaskWithID("d", "hook-d", "b3"))
	assert.Equal(t, int64(2), q2.Get("d").GetBirthEpoch())

	require.NoError(t, q2.PersistShutdownBoundary(context.Background()))
}

// TestRecovery_CompletedTasksNotReplayed: successfully handled tasks are
// removed from the checkpoint and never come back after restart.
func TestRecovery_CompletedTasksNotReplayed(t *testing.T) {
	dir := t.TempDir()
	q := newRecoverableQueue(t, dir, "main", nil)
	q.Handler = func(_ context.Context, _ task.Task) TaskResult {
		return TaskResult{Status: Success}
	}

	q.AddLast(hookTaskWithID("a", "hook-a", "x"), hookTaskWithID("b", "hook-b", "x"))
	q.Start(q.ctx)
	waitFor(t, 2*time.Second, func() bool { return q.Length() == 0 })

	q.BeginShutdown()
	waitFor(t, time.Second, func() bool { return q.GetStatusType() == QueueStatusStop })
	require.NoError(t, q.PersistShutdownBoundary(context.Background()))

	q2 := reopenRecoverableQueue(t, dir, "main")
	assert.True(t, q2.WasRestored())
	assert.Equal(t, 0, q2.Length(), "completed tasks must not be replayed")
	require.NoError(t, q2.PersistShutdownBoundary(context.Background()))
}

// TestRecovery_CompactionEpochBarrier: a compacted task restored after restart
// keeps representing only its original binding contexts and cannot merge with
// post-restart events; post-restart events compact among themselves normally.
func TestRecovery_CompactionEpochBarrier(t *testing.T) {
	dir := t.TempDir()
	q := newRecoverableQueue(t, dir, "main", nil)

	// Pre-restart: two hook-a tasks compacted into one, plus a service task.
	a1 := hookTaskWithID("a1", "hook-a", "ctx-a1")
	a2 := hookTaskWithID("a2", "hook-a", "ctx-a2")
	svc := serviceTaskWithID("svc")
	q.AddLast(a1, a2, svc)
	q.compaction(nil)

	assert.Equal(t, []string{"a1", "svc"}, queueIDs(q))
	meta1, _ := task_metadata.HookMetadataAccessor(q.Get("a1"))
	require.Len(t, meta1.BindingContext, 2, "a1 should hold both pre-restart contexts")

	require.NoError(t, q.PersistShutdownBoundary(context.Background()))

	// Restart.
	q2 := reopenRecoverableQueue(t, dir, "main")
	assert.Equal(t, []string{"a1", "svc"}, queueIDs(q2))
	assert.Equal(t, int64(1), q2.Get("a1").GetBirthEpoch())

	// New events for the same hook arrive after the restart.
	c1 := hookTaskWithID("c1", "hook-a", "ctx-c1")
	c2 := hookTaskWithID("c2", "hook-a", "ctx-c2")
	q2.AddLast(c1, c2)
	q2.compaction(nil)

	// Restored compacted task must not absorb the new-generation tasks.
	assert.Equal(t, []string{"a1", "svc", "c1"}, queueIDs(q2))

	oldMeta, _ := task_metadata.HookMetadataAccessor(q2.Get("a1"))
	assert.Len(t, oldMeta.BindingContext, 2, "restored compacted task keeps its original contexts only")

	newMeta, _ := task_metadata.HookMetadataAccessor(q2.Get("c1"))
	assert.Len(t, newMeta.BindingContext, 2, "post-restart tasks compact among themselves")

	require.NoError(t, q2.PersistShutdownBoundary(context.Background()))
}

// TestRecovery_BootstrapMarkerPersisted: after bootstrapping, a restart must
// be able to recognize that initial seeding already happened.
func TestRecovery_BootstrapMarkerPersisted(t *testing.T) {
	dir := t.TempDir()
	q := newRecoverableQueue(t, dir, "main", nil)
	q.MarkSeeded("onStartup:hook-a", "enableKubernetes:hook-a", "enableSchedule:hook-b")
	q.MarkBootstrapped()
	require.NoError(t, q.PersistShutdownBoundary(context.Background()))

	q2 := reopenRecoverableQueue(t, dir, "main")
	assert.True(t, q2.IsBootstrapped())
	assert.True(t, q2.WasRestored())
	assert.True(t, q2.IsSeeded("onStartup:hook-a"))
	assert.True(t, q2.IsSeeded("enableSchedule:hook-b"))
	assert.False(t, q2.IsSeeded("onStartup:hook-new"))
	require.NoError(t, q2.PersistShutdownBoundary(context.Background()))
}

// TestRecovery_NonRecoverableContract: marked tasks stay out of checkpoints
// and get a definitive failure on shutdown; admission after shutdown fails
// immediately.
func TestRecovery_NonRecoverableContract(t *testing.T) {
	dir := t.TempDir()
	q := newRecoverableQueue(t, dir, "main", nil)

	nr := serviceTaskWithID("admission")
	nr.MarkNonRecoverable("admission response is synchronous")
	rec := serviceTaskWithID("regular")
	q.AddLast(nr, rec)
	require.Equal(t, 2, q.Length())

	// The checkpoint contains only the recoverable task.
	require.NoError(t, q.PersistShutdownBoundary(context.Background()))
	store, err := NewFileCheckpointStore(dir, "main")
	require.NoError(t, err)
	cp, err := store.Load(context.Background())
	require.NoError(t, err)
	require.Len(t, cp.Tasks, 1)
	assert.Equal(t, "regular", cp.Tasks[0].Id)

	// Reopen and repeat the tracked shutdown flow via BeginShutdown.
	q2 := reopenRecoverableQueue(t, dir, "main")
	nr2 := serviceTaskWithID("admission2")
	nr2.MarkNonRecoverable("admission response is synchronous")
	q2.AddLast(nr2)

	failed := make(chan error, 1)
	go func() {
		<-nr2.NonRecoverableDone()
		failed <- nr2.NonRecoverableErr()
	}()

	q2.BeginShutdown()
	select {
	case err := <-failed:
		assert.ErrorIs(t, err, task.ErrQueueNotAccepting)
	case <-time.After(time.Second):
		t.Fatal("non-recoverable task did not receive definitive failure")
	}

	// Enqueuing a non-recoverable task after shutdown fails it immediately.
	nr3 := serviceTaskWithID("admission3")
	nr3.MarkNonRecoverable("late")
	lengthBefore := q2.Length()
	q2.AddLast(nr3)
	assert.Equal(t, lengthBefore, q2.Length(), "task must not enter a shut-down queue")
	select {
	case <-nr3.NonRecoverableDone():
		assert.ErrorIs(t, nr3.NonRecoverableErr(), task.ErrQueueNotAccepting)
	case <-time.After(time.Second):
		t.Fatal("late non-recoverable task was not failed immediately")
	}

	// The in-queue non-recoverable task was never persisted.
	store2, err := NewFileCheckpointStore(dir, "main")
	require.NoError(t, err)
	cp2, err := store2.Load(context.Background())
	require.NoError(t, err)
	for _, dto := range cp2.Tasks {
		assert.NotContains(t, dto.Id, "admission")
	}
	require.NoError(t, q2.PersistShutdownBoundary(context.Background()))
}

// failingCheckpointStore simulates a broken checkpoint backend.
type failingCheckpointStore struct {
	saveErr error
	loadErr error
}

func (s *failingCheckpointStore) Save(_ context.Context, _ *QueueCheckpoint) error {
	return s.saveErr
}

func (s *failingCheckpointStore) Load(_ context.Context) (*QueueCheckpoint, error) {
	return nil, s.loadErr
}

func (s *failingCheckpointStore) Close() error { return nil }

// TestRecovery_CheckpointWriteFailureGatesAcks: on a save error the queue
// keeps the in-memory state but flips into a degraded mode and stops accepting
// new tasks; the previously complete generation remains the recovery baseline.
func TestRecovery_CheckpointWriteFailureGatesAcks(t *testing.T) {
	store := &failingCheckpointStore{
		saveErr: fmt.Errorf("disk on fire"),
		loadErr: nil, // no file yet -> clean baseline
	}
	q := NewTasksQueue("main",
		metricsstorage.NewMetricStorage(metricsstorage.WithNewRegistry()),
		WithContext(context.Background()),
		WithCheckpointStore(store),
	)
	require.NoError(t, q.InitPersistence(context.Background()))

	q.AddLast(serviceTaskWithID("t1"))

	waitFor(t, time.Second, func() bool { return !q.IsAccepting() })
	assert.Error(t, q.CheckpointError())

	// In-memory processing continues on top of the last complete generation.
	assert.Equal(t, 1, q.Length())

	// New tasks are not acknowledged.
	q.AddLast(serviceTaskWithID("t2"))
	assert.Equal(t, 1, q.Length())
}

// TestRecovery_NoStoreQueueIsPlainInMemory: queues without a checkpoint store
// keep their historical behavior: no epoch stamping from persistence, no
// writer, no errors.
func TestRecovery_NoStoreQueueIsPlainInMemory(t *testing.T) {
	q := NewTasksQueue("plain",
		metricsstorage.NewMetricStorage(metricsstorage.WithNewRegistry()),
		WithContext(context.Background()),
	)
	require.NoError(t, q.InitPersistence(context.Background()))

	t1 := serviceTaskWithID("t1")
	q.AddLast(t1)
	assert.Equal(t, 1, q.Length())
	assert.True(t, q.IsAccepting())
	assert.NoError(t, q.CheckpointError())

	// Shutdown must not panic and must be a no-op without persistence.
	q.BeginShutdown()
	require.NoError(t, q.PersistShutdownBoundary(context.Background()))
}

// TestRecovery_CorruptFileRefusesStart: corrupt checkpoint makes
// InitPersistence fail, quarantine the file and prevent the worker start.
func TestRecovery_CorruptFileRefusesStart(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileCheckpointStore(dir, "main")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(store.Path(), []byte("{broken"), 0o600))

	q := NewTasksQueue("main",
		metricsstorage.NewMetricStorage(metricsstorage.WithNewRegistry()),
		WithContext(context.Background()),
		WithCheckpointStore(store),
	)
	err = q.InitPersistence(context.Background())
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrCorruptCheckpoint))
	assert.False(t, q.IsAccepting())
	assert.NotNil(t, q.RestoreError())

	// The corrupt file is preserved for diagnostics.
	assert.FileExists(t, store.Path())
}
