package queue

import (
	"context"
	"sync"
	"time"
)

// checkpointWriter serializes durable snapshot writes and coalesces bursts of
// queue mutations into one save per flush interval.
//
// Semantics:
//   - Submit marks the current queue state dirty; it never blocks on disk I/O.
//   - Flush blocks until every state submitted so far is durably stored (or
//     until the context expires / the writer entered the failed state).
//   - A failed save is terminal: Failed()/Flush return the error, the queue
//     stops acknowledging new tasks, and the previously saved complete
//     generation remains the recovery baseline.
type checkpointWriter struct {
	store      CheckpointStore
	snapshotFn func() (*QueueCheckpoint, error)
	interval   time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	signal chan struct{}

	mu           sync.Mutex
	dirty        bool
	submitSeq    int64 // number of Submit calls since start
	persistedSeq int64 // sequence included in the last successful save
	failed       error
	stopped      bool
	waiters      map[chan struct{}]struct{}
}

func newCheckpointWriter(store CheckpointStore, snapshotFn func() (*QueueCheckpoint, error), interval time.Duration) *checkpointWriter {
	w := &checkpointWriter{
		store:      store,
		snapshotFn: snapshotFn,
		interval:   interval,
		signal:     make(chan struct{}, 1),
		waiters:    make(map[chan struct{}]struct{}),
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	return w
}

func (w *checkpointWriter) start() {
	w.wg.Add(1)
	go w.run()
}

// Submit schedules a save of the latest queue state. It returns the current
// terminal failure without blocking when the writer is dead.
func (w *checkpointWriter) Submit() error {
	w.mu.Lock()
	if w.stopped {
		err := w.failed
		w.mu.Unlock()
		if err != nil {
			return err
		}
		return errWriterStopped
	}
	w.dirty = true
	w.submitSeq++
	w.mu.Unlock()

	select {
	case w.signal <- struct{}{}:
	default:
	}
	return nil
}

// Flush waits until all submitted states are durable.
func (w *checkpointWriter) Flush(ctx context.Context) error {
	for {
		w.mu.Lock()
		if w.failed != nil {
			err := w.failed
			w.mu.Unlock()
			return err
		}
		if w.persistedSeq >= w.submitSeq {
			w.mu.Unlock()
			return nil
		}
		ch := make(chan struct{})
		w.waiters[ch] = struct{}{}
		w.mu.Unlock()

		select {
		case <-ch:
			// re-check: the closed save may predate the state we wait for
		case <-ctx.Done():
			w.mu.Lock()
			delete(w.waiters, ch)
			w.mu.Unlock()
			return ctx.Err()
		case <-w.ctx.Done():
			w.mu.Lock()
			err := w.failed
			w.mu.Unlock()
			if err != nil {
				return err
			}
			return errWriterStopped
		}
	}
}

// Failed reports the terminal save failure, if any.
func (w *checkpointWriter) Failed() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failed
}

// Close stops the writer. It does not perform a final save: the caller must
// Submit+Flush the shutdown boundary first.
func (w *checkpointWriter) Close() error {
	w.mu.Lock()
	w.stopped = true
	w.cancel()
	w.mu.Unlock()
	w.wg.Wait()
	return w.Failed()
}

func (w *checkpointWriter) run() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.signal:
		}

		if w.interval > 0 {
			timer := time.NewTimer(w.interval)
			select {
			case <-w.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}

		w.saveUntilEmpty()
	}
}

// saveUntilEmpty saves coalesced dirty states, looping to absorb submissions
// that arrive while a save is in flight.
func (w *checkpointWriter) saveUntilEmpty() {
	for {
		w.mu.Lock()
		if !w.dirty {
			w.notifyWaiters()
			w.mu.Unlock()
			return
		}
		target := w.submitSeq
		w.dirty = false
		w.mu.Unlock()

		cp, buildErr := w.snapshotFn()
		if buildErr != nil || cp == nil {
			w.mu.Lock()
			if buildErr != nil {
				w.failed = buildErr
				w.stopped = true
			} else {
				w.persistedSeq = target
			}
			w.notifyWaiters()
			w.mu.Unlock()
			if buildErr != nil {
				w.cancel()
				return
			}
			continue
		}

		err := w.store.Save(w.ctx, cp)

		w.mu.Lock()
		if err != nil {
			w.failed = err
			w.stopped = true
			w.notifyWaiters()
			w.mu.Unlock()
			// Drain the signal channel and terminate the run loop.
			w.cancel()
			return
		}
		w.persistedSeq = target
		// Submissions made during the save set dirty again: loop.
		again := w.dirty
		w.notifyWaiters()
		w.mu.Unlock()

		if !again {
			return
		}
	}
}

func (w *checkpointWriter) notifyWaiters() {
	for ch := range w.waiters {
		close(ch)
		delete(w.waiters, ch)
	}
}

type checkpointWriterStoppedError struct{}

func (checkpointWriterStoppedError) Error() string { return "checkpoint writer is stopped" }

var errWriterStopped = checkpointWriterStoppedError{}
