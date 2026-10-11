package executor

import (
	"context"
	"os/exec"
	"sync"
	"time"
)

// ProcessTracker is a read-only view into the process registry.
// It is intended for consumers (such as a PID-1 zombie reaper) that need
// to check whether a PID is managed by the executor but must not modify
// the registry.
type ProcessTracker interface {
	// IsActive reports whether pid is currently tracked as a running process.
	IsActive(pid int) bool
}

// processRegistry tracks PIDs of processes started by the executor so that
// a PID-1 zombie reaper can skip them (their parent already calls Wait).
// This prevents the reaper from stealing a child that cmd.Wait expects to reap.
//
// The struct is intentionally unexported — all external access goes through
// the ProcessTracker interface (read-only) or the package-level helpers
// registerPID / unregisterPID (write, executor-internal).
type processRegistry struct {
	mu         sync.RWMutex
	activePIDs map[int]struct{}
}

// register adds pid to the set of active PIDs. Registering a PID that is
// already tracked is a no-op.
func (r *processRegistry) register(pid int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.activePIDs[pid] = struct{}{}
}

// unregister removes pid from the set of active PIDs. It is idempotent:
// unregistering a PID that was never registered (or already unregistered,
// e.g. by a detached reaper) does nothing and never panics.
func (r *processRegistry) unregister(pid int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.activePIDs, pid)
}

// waitPollInterval is the resolution at which Wait observes the registry.
const waitPollInterval = 50 * time.Millisecond

// Wait blocks until the registry contains no active PIDs (every executor
// subprocess has been reaped) or ctx is canceled. It is used during process
// shutdown to wait for in-flight hook executions: canceled commands are
// terminated through their own context and unregister as soon as cmd.Wait
// returns, while processes that could not be killed stay registered until
// the detached reaper finishes.
func (r *processRegistry) Wait(ctx context.Context) error {
	ticker := time.NewTicker(waitPollInterval)
	defer ticker.Stop()

	for {
		r.mu.RLock()
		remaining := len(r.activePIDs)
		r.mu.RUnlock()

		if remaining == 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// startAndRegister calls cmd.Start() and, on success, registers the child
// PID under the same write-lock. This eliminates the window between process
// creation and registration during which a PID-1 zombie reaper could
// prematurely reap a fast-exiting child.
func (r *processRegistry) startAndRegister(cmd *exec.Cmd) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := cmd.Start(); err != nil {
		return err
	}

	r.activePIDs[cmd.Process.Pid] = struct{}{}

	return nil
}

// IsActive reports whether pid is currently tracked as an active process.
func (r *processRegistry) IsActive(pid int) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, ok := r.activePIDs[pid]

	return ok
}

// registry is the singleton process registry.
// It is not exported — external packages obtain a ProcessTracker via Tracker().
var registry = &processRegistry{
	activePIDs: make(map[int]struct{}),
}

// Tracker returns a read-only view of the global process registry.
// The zombie reaper should call this once and use the returned ProcessTracker
// to check whether a PID is managed by the executor.
func Tracker() ProcessTracker {
	return registry
}

// registerPID and unregisterPID are package-internal helpers used by Run,
// Output, and RunAndLogLines to track child PIDs.
func registerPID(pid int) {
	registry.register(pid)
}

func unregisterPID(pid int) {
	registry.unregister(pid)
}

// startAndRegister calls cmd.Start() and registers the resulting PID
// atomically under the registry's write-lock. Callers must still call
// unregisterPID(cmd.Process.Pid) (typically via defer) when cmd.Wait returns.
func startAndRegister(cmd *exec.Cmd) error {
	return registry.startAndRegister(cmd)
}

// Wait blocks until all executor-managed subprocesses are reaped or ctx
// expires. Call it during process shutdown after request and queue contexts
// have been canceled so the executor has signaled the in-flight process
// groups to terminate.
func Wait(ctx context.Context) error {
	return registry.Wait(ctx)
}
