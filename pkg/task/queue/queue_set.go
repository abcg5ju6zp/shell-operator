package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/deckhouse/deckhouse/pkg/log"
	metricsstorage "github.com/deckhouse/deckhouse/pkg/metrics-storage"

	pkg "github.com/flant/shell-operator/pkg"
	"github.com/flant/shell-operator/pkg/task"
)

const MainQueueName = "main"

// TaskQueueSet is a manager for a set of named queues
type TaskQueueSet struct {
	MainName string

	metricStorage metricsstorage.Storage

	// checkpointDir, when non-empty, enables recoverable checkpoints for every
	// named queue created afterwards (one JSON file per queue).
	checkpointDir      string
	checkpointInterval time.Duration

	ctx    context.Context
	cancel context.CancelFunc

	Queues *queueStorage

	logger *log.Logger
}

func NewTaskQueueSet() *TaskQueueSet {
	return &TaskQueueSet{
		Queues:   newQueueStorage(),
		MainName: MainQueueName,
		logger:   log.NewLogger().Named("task_queue_set"),
	}
}

func (tqs *TaskQueueSet) WithMainName(name string) {
	tqs.MainName = name
	tqs.logger = tqs.logger.Named(name)
}

func (tqs *TaskQueueSet) WithContext(ctx context.Context) {
	tqs.ctx, tqs.cancel = context.WithCancel(ctx)
}

func (tqs *TaskQueueSet) WithMetricStorage(mstor metricsstorage.Storage) *TaskQueueSet {
	tqs.metricStorage = mstor

	return tqs
}

// WithCheckpointDir enables durable, recoverable checkpoints: each named
// queue gets a JSON file inside dir and is restored from its last complete
// generation when created. Call before NewNamedQueue.
func (tqs *TaskQueueSet) WithCheckpointDir(dir string) *TaskQueueSet {
	tqs.checkpointDir = dir
	return tqs
}

// WithCheckpointFlushInterval overrides the coalescing window for asynchronous
// checkpoint writes in every durable queue created afterwards.
func (tqs *TaskQueueSet) WithCheckpointFlushInterval(interval time.Duration) *TaskQueueSet {
	tqs.checkpointInterval = interval
	return tqs
}

// CheckpointDir returns the configured checkpoint directory (may be empty).
func (tqs *TaskQueueSet) CheckpointDir() string {
	return tqs.checkpointDir
}

func (tqs *TaskQueueSet) WithLogger(logger *log.Logger) *TaskQueueSet {
	tqs.logger = logger

	return tqs
}

func (tqs *TaskQueueSet) Stop() {
	if tqs.cancel != nil {
		tqs.cancel()
	}
}

func (tqs *TaskQueueSet) StartMain(ctx context.Context) {
	tqs.GetByName(tqs.MainName).Start(ctx)
}

func (tqs *TaskQueueSet) Start(ctx context.Context) {
	tqs.IterateSnapshot(ctx, func(ctx context.Context, queue *TaskQueue) {
		defer func() {
			if r := recover(); r != nil {
				tqs.logger.Warn("panic recovered in TaskQueueSet Start",
					slog.Any(pkg.LogKeyError, r),
					slog.String(pkg.LogKeyStack, string(debug.Stack())),
				)
			}
		}()

		queue.Start(ctx)
	})
}

// Add register a new queue for TaskQueueSet.
func (tqs *TaskQueueSet) Add(queue *TaskQueue) {
	tqs.Queues.Set(queue.Name, queue)
}

// NewNamedQueue creates a queue, restores its last checkpoint when durable
// persistence is configured and registers it. A corrupt or unreadable
// checkpoint is preserved on disk and returned as an error; the queue is kept
// registered but will not start, so callers must not ignore the error.
func (tqs *TaskQueueSet) NewNamedQueue(name string, handler func(ctx context.Context, t task.Task) TaskResult, opts ...TaskQueueOption) error {
	allOpts := make([]TaskQueueOption, 0, len(opts)+2)
	allOpts = append(allOpts, WithHandler(handler), WithContext(tqs.ctx))
	if tqs.checkpointDir != "" {
		allOpts = append(allOpts, WithCheckpointDir(tqs.checkpointDir))
	}
	if tqs.checkpointInterval > 0 {
		allOpts = append(allOpts, WithCheckpointFlushInterval(tqs.checkpointInterval))
	}
	allOpts = append(allOpts, opts...)

	q := NewTasksQueue(
		name,
		tqs.metricStorage,
		allOpts...,
	)

	if q.logger == nil {
		q.logger = log.NewLogger().Named("task_queue")
	}

	if err := q.InitPersistence(tqs.ctx); err != nil {
		tqs.logger.Error("failed to restore queue from checkpoint; queue is stopped",
			slog.String(pkg.LogKeyQueue, name),
			slog.Any(pkg.LogKeyError, err))
		tqs.Queues.Set(q.Name, q)
		return err
	}

	tqs.Queues.Set(q.Name, q)
	return nil
}

func (tqs *TaskQueueSet) GetByName(name string) *TaskQueue {
	q, ok := tqs.Queues.Get(name)
	if !ok {
		return nil
	}

	return q
}

func (tqs *TaskQueueSet) GetMain() *TaskQueue {
	return tqs.GetByName(tqs.MainName)
}

func (tqs *TaskQueueSet) AddTailTasks(tasks ...task.Task) {
	tqs.logger.Debug("AddTailTasks: adding tasks to queues", slog.Int(pkg.LogKeyTasksCount, len(tasks)))

	for _, resTask := range tasks {
		q, ok := tqs.Queues.Get(resTask.GetQueueName())
		if ok {
			tqs.logger.Debug("AddTailTasks: adding task to queue",
				slog.String(pkg.LogKeyQueueName, resTask.GetQueueName()),
				slog.String(pkg.LogKeyDescription, resTask.GetDescription()))
			q.AddLast(resTask)

			continue
		}

		log.Error("Possible bug!!! Got task for queue but queue is not created yet.",
			slog.String(pkg.LogKeyQueueName, resTask.GetQueueName()),
			slog.String(pkg.LogKeyDescription, resTask.GetDescription()))
	}

	tqs.logger.Debug("AddTailTasks: adding tasks to queues done")
}

// GetSnapshot returns a snapshot of all queues at the time of the call.
// This is useful for iteration when you need to call methods on the queues
// that might acquire locks, preventing deadlocks.
//
// The returned slice is a snapshot and will not reflect subsequent changes.
// The main queue (tqs.MainName) is always placed first in the list.
func (tqs *TaskQueueSet) GetSnapshot() []*TaskQueue {
	tqs.logger.Debug("GetSnapshot: creating snapshot of queues")

	allQueues := tqs.Queues.List()
	queues := make([]*TaskQueue, 0, len(allQueues))

	// First, add the main queue if it exists
	if mainQueue := tqs.GetMain(); mainQueue != nil {
		queues = append(queues, mainQueue)
	}

	// Then add all other queues
	for _, queue := range allQueues {
		if queue.Name != tqs.MainName {
			queues = append(queues, queue)
		}
	}

	tqs.logger.Debug("GetSnapshot: creating snapshot of queues done")

	return queues
}

// IterateSnapshot creates a snapshot of all queues and iterates over the copy.
// This is safer than Iterate() when you need to call queue methods inside the callback,
// as no locks are held during callback execution.
//
// Note: The snapshot may become stale during iteration if queues are added/removed
// by other goroutines or by the callback itself.
//
// Use this method when:
//   - You need to call queue methods inside the callback (Start, Stop, etc.)
//   - You need to process queues asynchronously
//   - Safety is more important than performance
//
// Memory overhead: O(n) where n is the number of queues.
func (tqs *TaskQueueSet) IterateSnapshot(ctx context.Context, doFn func(ctx context.Context, queue *TaskQueue)) {
	if doFn == nil {
		return
	}

	// Create snapshot under lock (main queue is already first)
	snapshot := tqs.GetSnapshot()

	// Execute callbacks without holding any locks
	defer func() {
		if r := recover(); r != nil {
			tqs.logger.Warn("panic recovered in IterateSnapshot", slog.Any(pkg.LogKeyError, r))
		}
	}()

	for _, q := range snapshot {
		doFn(ctx, q)
	}
}

func (tqs *TaskQueueSet) Remove(name string) {
	ts, exists := tqs.Queues.Get(name)
	if exists {
		ts.Stop()
	}

	tqs.Queues.Delete(name)
}

func (tqs *TaskQueueSet) WaitStopWithTimeout(timeout time.Duration) {
	checkTick := time.NewTicker(time.Millisecond * 100)
	defer checkTick.Stop()

	timeoutTick := time.NewTicker(timeout)
	defer timeoutTick.Stop()

	for {
		select {
		case <-checkTick.C:
			stopped := func() bool {
				for _, q := range tqs.Queues.List() {
					if q.GetStatusType() != QueueStatusStop {
						return false
					}
				}

				return true
			}()

			if stopped {
				return
			}

		case <-timeoutTick.C:
			return
		}
	}
}

// Shutdown gracefully stops every queue and persists the recoverable
// boundary of each durable queue.
//
// Sequence:
//  1. Every queue stops accepting new tasks; pending non-recoverable tasks
//     get a definitive failure; worker contexts are canceled.
//  2. Wait (bounded by ctx) until workers reach QueueStatusStop.
//  3. Each durable queue writes its final complete checkpoint generation,
//     with claimed-but-unfinished tasks left at their original position.
//
// Returns ctx.Err() on drain timeout (checkpoint persistence is still
// attempted with a background-bound context) joined with persistence errors.
func (tqs *TaskQueueSet) Shutdown(ctx context.Context) error {
	if tqs.cancel != nil {
		tqs.cancel()
	}

	for _, q := range tqs.Queues.List() {
		q.BeginShutdown()
	}

	drainErr := tqs.waitStopped(ctx)

	// Persist even if draining hit the deadline: the process is going down
	// either way, and a flushed boundary is better than nothing.
	persistCtx := ctx
	if drainErr != nil {
		var cancel context.CancelFunc
		persistCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
	}

	var errs []error
	if drainErr != nil {
		errs = append(errs, drainErr)
	}
	for _, q := range tqs.Queues.List() {
		if err := q.PersistShutdownBoundary(persistCtx); err != nil {
			errs = append(errs, fmt.Errorf("persist queue %q checkpoint: %w", q.Name, err))
		}
	}
	return errors.Join(errs...)
}

// waitStopped polls until every queue reached QueueStatusStop or ctx expires.
// Queues whose worker was never started are treated as stopped.
func (tqs *TaskQueueSet) waitStopped(ctx context.Context) error {
	checkTick := time.NewTicker(50 * time.Millisecond)
	defer checkTick.Stop()

	for {
		stopped := true
		for _, q := range tqs.Queues.List() {
			if q.GetStatusType() == QueueStatusStop || !q.started.Load() {
				continue
			}
			stopped = false
			break
		}
		if stopped {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-checkTick.C:
		}
	}
}
