package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"time"

	pkg "github.com/flant/shell-operator/pkg"
	"github.com/flant/shell-operator/pkg/task"
)

// This file wires recoverable checkpoints into the TaskQueue lifecycle:
//
//   - InitPersistence restores the last complete checkpoint before the worker
//     starts (or refuses to start with a quarantined corrupt file).
//   - buildCheckpoint snapshots the queue under the queue lock.
//   - checkpointRecord/checkpointFlush are the durability hooks called at
//     enqueue, compaction, claim and result points.
//   - BeginShutdown stops admission and PersistShutdownBoundary durably stores
//     the recoverable boundary.
//   - Non-recoverable tasks are tracked while held and get a definitive
//     failure whenever the queue stops accepting before they complete.

// InitPersistence loads the last complete checkpoint and starts the
// background checkpoint writer. It is a no-op for queues without a checkpoint
// store. A corrupt checkpoint is preserved on disk and returned as an error;
// the queue is then not started.
func (q *TaskQueue) InitPersistence(ctx context.Context) error {
	if q.cpStore == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	cp, err := q.cpStore.Load(ctx)
	switch {
	case err == nil && cp != nil:
		if err := q.applyCheckpoint(cp); err != nil {
			q.restoreErr = err
			q.accepting.Store(false)
			return err
		}
	case errors.Is(err, os.ErrNotExist), err == nil && cp == nil:
		// First start: empty queue is the valid baseline.
		q.epoch = 1
	case err != nil:
		q.restoreErr = err
		q.accepting.Store(false)
		q.SetStatus(QueueStatusStop)
		return err
	}

	if q.epoch <= 0 {
		q.epoch = 1
	}
	q.storage.setEpoch(q.epoch)

	q.cpWriter = newCheckpointWriter(q.cpStore, q.buildCheckpoint, q.cpFlushInterval)
	q.cpWriter.start()
	return nil
}

// applyCheckpoint rebuilds the in-memory queue from a loaded snapshot.
// Restored tasks all keep the snapshot's epoch: tasks born after this start
// get epoch+1, so compaction cannot merge across the restart boundary.
func (q *TaskQueue) applyCheckpoint(cp *QueueCheckpoint) error {
	tasks := make([]task.Task, 0, len(cp.Tasks))
	for _, dto := range cp.Tasks {
		t, err := dto.toTask()
		if err != nil {
			return fmt.Errorf("recover queue %q from checkpoint generation %d: %w", q.Name, cp.Generation, err)
		}
		tasks = append(tasks, t)
	}

	q.storage.ReplaceAll(tasks)
	for _, t := range tasks {
		q.queueTasksCounter.Add(t)
	}

	q.epoch = cp.Epoch + 1
	q.cpGeneration = cp.Generation
	q.bootstrapped = cp.Bootstrapped
	if len(cp.SeededKeys) > 0 {
		q.seededKeys = make(map[string]struct{}, len(cp.SeededKeys))
		for _, key := range cp.SeededKeys {
			q.seededKeys[key] = struct{}{}
		}
	}
	q.restored = true
	return nil
}

// buildCheckpoint creates the next complete generation from the live queue.
// Claimed tasks are persisted at their current position; non-recoverable
// tasks are excluded.
func (q *TaskQueue) buildCheckpoint() (*QueueCheckpoint, error) {
	q.m.Lock()
	defer q.m.Unlock()

	snapshot := q.storage.GetSnapshot()
	dtos := make([]TaskCheckpoint, 0, len(snapshot))
	for _, t := range snapshot {
		if !t.IsRecoverable() {
			continue
		}
		dto, err := taskToCheckpoint(t)
		if err != nil {
			return nil, fmt.Errorf("serialize task %s: %w", t.GetId(), err)
		}
		dtos = append(dtos, dto)
	}

	q.cpGeneration++
	seeded := make([]string, 0, len(q.seededKeys))
	for key := range q.seededKeys {
		seeded = append(seeded, key)
	}
	sort.Strings(seeded)
	return &QueueCheckpoint{
		QueueName:    q.Name,
		Generation:   q.cpGeneration,
		Epoch:        q.epoch,
		Bootstrapped: q.bootstrapped,
		SeededKeys:   seeded,
		UpdatedAt:    time.Now().UTC().Format(time.RFC3339Nano),
		Tasks:        dtos,
	}, nil
}

// MarkSeeded records initial-seeding keys already issued. The set is part of
// the checkpoint and lets restart seed only hooks introduced later.
func (q *TaskQueue) MarkSeeded(keys ...string) {
	q.m.Lock()
	defer q.m.Unlock()
	for _, key := range keys {
		q.seededKeys[key] = struct{}{}
	}
}

// IsSeeded reports whether an initial-seeding key was already issued.
func (q *TaskQueue) IsSeeded(key string) bool {
	q.m.RLock()
	defer q.m.RUnlock()
	_, ok := q.seededKeys[key]
	return ok
}

// SeededKeys returns a sorted snapshot of issued seeding keys.
func (q *TaskQueue) SeededKeys() []string {
	q.m.RLock()
	defer q.m.RUnlock()
	keys := make([]string, 0, len(q.seededKeys))
	for key := range q.seededKeys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// checkpointRecord schedules (and, in synchronous mode, waits for) a durable
// save after an enqueue or compaction mutation.
func (q *TaskQueue) checkpointRecord() {
	if q.cpWriter == nil {
		return
	}
	if err := q.cpWriter.Submit(); err != nil {
		q.markCheckpointFailure(err)
		return
	}
	if q.cpFlushInterval <= 0 {
		var ctx context.Context
		if q.ctx != nil {
			ctx = q.ctx
		}
		q.checkpointFlushContext(ctx)
	}
}

// checkpointSubmit marks the state dirty without waiting.
func (q *TaskQueue) checkpointSubmit() {
	if q.cpWriter == nil {
		return
	}
	if err := q.cpWriter.Submit(); err != nil {
		q.markCheckpointFailure(err)
	}
}

// checkpointFlush durably stores everything submitted so far. Caller
// cancellation (e.g. worker shutdown) is not treated as a storage failure.
func (q *TaskQueue) checkpointFlushContext(ctx context.Context) {
	if q.cpWriter == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := q.cpWriter.Flush(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		q.markCheckpointFailure(err)
	}
}

// markCheckpointFailure is terminal: the queue keeps processing the in-memory
// state based on the last complete generation but stops acknowledging any new
// task, and pending non-recoverable tasks receive their definitive failure.
func (q *TaskQueue) markCheckpointFailure(err error) {
	if err == nil {
		return
	}
	q.cpFailOnce.Do(func() {
		q.cpErr = err
		q.accepting.Store(false)
		q.failAllNonRecoverable(err)
		q.logger.Error("checkpoint failure: continuing on the last complete generation, stopping acknowledgement of new tasks",
			slog.String(pkg.LogKeyQueue, q.Name),
			slog.Any(pkg.LogKeyError, err))
		q.SetStatusText(fmt.Sprintf("checkpoint failure: %s", err.Error()))
	})
}

// admitTask decides whether t may enter the queue. Non-recoverable tasks are
// registered for definitive failure delivery; recoverable tasks with
// unregistered metadata immediately trigger the checkpoint-failure gate.
func (q *TaskQueue) admitTask(t task.Task) bool {
	if !q.accepting.Load() {
		if !t.IsRecoverable() {
			t.FailNonRecoverable(task.ErrQueueNotAccepting)
		}
		q.logger.Warn("queue is not accepting new tasks, task is not acknowledged",
			slog.String(pkg.LogKeyQueue, q.Name),
			slog.String(pkg.LogKeyTaskID, t.GetId()),
			slog.String(pkg.LogKeyTaskType, string(t.GetType())))
		return false
	}

	if !t.IsRecoverable() {
		q.trackNonRecoverable(t)
		return true
	}

	if q.cpStore != nil {
		if meta := t.GetMetadata(); meta != nil && !isNil(meta) {
			if _, ok := task.LookupMetadataTypeName(meta); !ok {
				q.markCheckpointFailure(fmt.Errorf("task %s of type %s carries non-serializable metadata %T but is not marked non-recoverable",
					t.GetId(), t.GetType(), meta))
			}
		}
	}

	return true
}

func (q *TaskQueue) trackNonRecoverable(t task.Task) {
	q.nonRecoverableMu.Lock()
	q.nonRecoverable[t.GetId()] = t
	q.nonRecoverableMu.Unlock()
}

// trackResultTasks registers any non-recoverable tasks produced by a handler
// result (they bypass admitTask because ProcessResult inserts them directly).
func (q *TaskQueue) trackResultTasks(res TaskResult) {
	track := func(tasks []task.Task) {
		for _, t := range tasks {
			if !t.IsRecoverable() {
				q.trackNonRecoverable(t)
			}
		}
	}
	track(res.GetAfterTasks())
	track(res.GetHeadTasks())
	track(res.GetTailTasks())
}

// untrackNonRecoverable forgets a completed non-recoverable task.
func (q *TaskQueue) untrackNonRecoverable(t task.Task) {
	if t == nil || t.IsRecoverable() {
		return
	}
	q.nonRecoverableMu.Lock()
	delete(q.nonRecoverable, t.GetId())
	q.nonRecoverableMu.Unlock()
}

// failAllNonRecoverable delivers definitive failures to every still-held
// non-recoverable task and clears the tracking set.
func (q *TaskQueue) failAllNonRecoverable(cause error) {
	q.nonRecoverableMu.Lock()
	pending := make([]task.Task, 0, len(q.nonRecoverable))
	for _, t := range q.nonRecoverable {
		pending = append(pending, t)
	}
	q.nonRecoverable = make(map[string]task.Task)
	q.nonRecoverableMu.Unlock()

	for _, t := range pending {
		t.FailNonRecoverable(cause)
	}
}

// BeginShutdown stops admission of new tasks, fails pending non-recoverable
// tasks with a definitive error and cancels the worker context.
func (q *TaskQueue) BeginShutdown() {
	if !q.accepting.CompareAndSwap(true, false) {
		// Already shutting down or degraded: make sure the worker is signaled.
		if q.cancel != nil {
			q.cancel()
		}
		return
	}
	q.failAllNonRecoverable(task.ErrQueueNotAccepting)
	if q.cancel != nil {
		q.cancel()
	}
}

// PersistShutdownBoundary stores the final complete generation after the
// worker stopped and releases the checkpoint writer. Claimed-but-unfinished
// tasks are normalized to unclaimed at their original position, so they re-run
// after recovery.
func (q *TaskQueue) PersistShutdownBoundary(ctx context.Context) error {
	if q.cpWriter == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	q.withLock(func() {
		for _, t := range q.storage.GetSnapshot() {
			if t.IsProcessing() {
				t.SetProcessing(false)
			}
		}
	})

	if err := q.cpWriter.Submit(); err != nil {
		q.markCheckpointFailure(err)
		return err
	}
	if err := q.cpWriter.Flush(ctx); err != nil {
		q.markCheckpointFailure(err)
		return err
	}
	return q.cpWriter.Close()
}

// IsAccepting reports whether the queue acknowledges new tasks.
func (q *TaskQueue) IsAccepting() bool {
	return q.accepting.Load()
}

// WasRestored reports whether the queue content came from a checkpoint.
func (q *TaskQueue) WasRestored() bool {
	return q.restored
}

// IsBootstrapped reports whether the initial seeding was already checkpointed.
func (q *TaskQueue) IsBootstrapped() bool {
	return q.bootstrapped
}

// MarkBootstrapped records that initial tasks were seeded, so a restart must
// not add them again.
func (q *TaskQueue) MarkBootstrapped() {
	q.bootstrapped = true
	q.checkpointRecord()
}

// RestoreError returns the error that prevented recovery (e.g. a corrupt
// checkpoint), or nil.
func (q *TaskQueue) RestoreError() error {
	return q.restoreErr
}

// CheckpointError returns the terminal persistence failure, if any.
func (q *TaskQueue) CheckpointError() error {
	if q.cpWriter != nil {
		if err := q.cpWriter.Failed(); err != nil {
			return err
		}
	}
	return q.cpErr
}
