package executor

import (
	"bytes"
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse/pkg/log"
)

// isGroupGone reports that no process exists in the given process group.
func isGroupGone(pgid int) bool {
	return syscall.Kill(-pgid, 0) == syscall.ESRCH
}

// activePID peeks at the single active PID in the registry or fails the test.
func activePID(t *testing.T, r *processRegistry) int {
	t.Helper()

	r.mu.RLock()
	defer r.mu.RUnlock()

	for pid := range r.activePIDs {
		return pid
	}
	t.Fatal("expected an active PID in the registry")
	return 0
}

// TestRunAndLogLines_ContextCancel_KillsProcessGroup verifies that canceling
// the request context stops both the hook shell and its child process.
func TestRunAndLogLines_ContextCancel_KillsProcessGroup(t *testing.T) {
	r := newTestRegistry(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ex := NewExecutor("", "sh", []string{"-c", "sleep 30 & wait"}, nil).
		WithLogger(log.NewNop())

	errCh := make(chan error, 1)
	go func() {
		_, err := ex.RunAndLogLines(ctx, nil)
		errCh <- err
	}()

	require.Eventually(t, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return len(r.activePIDs) > 0
	}, time.Second, 10*time.Millisecond, "expected the process to be registered after start")

	pid := activePID(t, r)

	cancel()

	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("RunAndLogLines did not return after context cancellation")
	}

	// The whole process group (sh and its sleep child) must be gone.
	assert.Eventually(t, func() bool {
		return isGroupGone(pid)
	}, 3*time.Second, 10*time.Millisecond, "process group %d must be killed", pid)

	assert.Eventually(t, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return len(r.activePIDs) == 0
	}, time.Second, 10*time.Millisecond, "PID registration must be released after the process is reaped")
}

// TestRunAndLogLines_ContextCancel_TermResistantGroupForceKilled starts a
// group that ignores SIGTERM and verifies the SIGKILL escalation still stops
// it and unwinds the call.
func TestRunAndLogLines_ContextCancel_TermResistantGroupForceKilled(t *testing.T) {
	r := newTestRegistry(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// sh ignores TERM; the subshell ignores TERM too and execs into sleep, so
	// the ignored disposition survives execve — the group survives SIGTERM and
	// must be SIGKILLed.
	script := "trap '' TERM; ( trap '' TERM; exec sleep 30 ) & wait"
	ex := NewExecutor("", "sh", []string{"-c", script}, nil).
		WithLogger(log.NewNop())

	errCh := make(chan error, 1)
	go func() {
		_, err := ex.RunAndLogLines(ctx, nil)
		errCh <- err
	}()

	require.Eventually(t, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return len(r.activePIDs) > 0
	}, time.Second, 10*time.Millisecond)

	pid := activePID(t, r)

	cancel()

	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(15 * time.Second):
		t.Fatal("RunAndLogLines did not return after SIGTERM/SIGKILL escalation")
	}

	assert.Eventually(t, func() bool {
		return isGroupGone(pid)
	}, 3*time.Second, 10*time.Millisecond, "SIGKILL must terminate the group")
}

// TestCancelProcess_EscalatesToKillAndReportsUnstoppable verifies the
// TERM → KILL escalation and the error that tells RunAndLogLines to detach the
// wait when the process cannot be reaped.
func TestCancelProcess_EscalatesToKillAndReportsUnstoppable(t *testing.T) {
	waitCh := make(chan runOutcome)
	var signals []syscall.Signal
	kill := func(sig syscall.Signal) {
		signals = append(signals, sig)
	}

	start := time.Now()
	_, err := cancelProcess(waitCh, kill)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Equal(t, []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL}, signals)
	assert.GreaterOrEqual(t, elapsed, terminateGracePeriod+killGracePeriod-100*time.Millisecond)
}

// TestCancelProcess_ReturnsAsSoonAsReaped checks that SIGKILL is not sent and
// no grace time is wasted once the process exits after SIGTERM.
func TestCancelProcess_ReturnsAsSoonAsReaped(t *testing.T) {
	waitCh := make(chan runOutcome, 1)
	waitCh <- runOutcome{err: context.Canceled}

	var signals []syscall.Signal
	kill := func(sig syscall.Signal) { signals = append(signals, sig) }

	out, err := cancelProcess(waitCh, kill)

	require.NoError(t, err)
	assert.ErrorIs(t, out.err, context.Canceled)
	assert.Equal(t, []syscall.Signal{syscall.SIGTERM}, signals)
}

// TestCancelProcess_DetachedReaperReleasesRegistration mirrors what
// RunAndLogLines does when a process cannot be stopped: the wait stays
// pending, the PID remains registered, and a detached reaper releases it once
// the wait finally resolves.
func TestCancelProcess_DetachedReaperReleasesRegistration(t *testing.T) {
	r := newTestRegistry(t)
	const pid = 777001
	r.register(pid)

	waitCh := make(chan runOutcome, 1)
	reaped := make(chan struct{})

	// cancelProcess exhausts both grace periods: caller detaches.
	go func() {
		_, err := cancelProcess(waitCh, func(syscall.Signal) {})
		require.Error(t, err)
		close(reaped)
	}()

	require.Eventually(t, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		_, ok := r.activePIDs[pid]
		return ok
	}, time.Second, 10*time.Millisecond)
	<-reaped

	// Still registered while the detached wait is pending.
	r.mu.RLock()
	_, registered := r.activePIDs[pid]
	r.mu.RUnlock()
	assert.True(t, registered)

	// The kernel finally reaps the process: the detached reaper runs.
	waitCh <- runOutcome{}
	r.unregister(pid)

	assert.Eventually(t, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return len(r.activePIDs) == 0
	}, time.Second, 10*time.Millisecond)
}

func TestIsolatingWriter_DiscardsAfterIsolate(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	w := &isolatingWriter{w: buf}

	n, err := w.Write([]byte("before"))
	require.NoError(t, err)
	assert.Equal(t, len("before"), n)

	w.Isolate()

	n, err = w.Write([]byte("after"))
	require.NoError(t, err, "isolated writes must be acknowledged as consumed")
	assert.Equal(t, len("after"), n)

	assert.Equal(t, "before", buf.String())
}

func TestProcessRegistry_Wait(t *testing.T) {
	r := &processRegistry{activePIDs: make(map[int]struct{})}

	// Empty registry: immediate return.
	assert.NoError(t, r.Wait(context.Background()))

	r.register(4242)

	waitCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, r.Wait(waitCtx), context.DeadlineExceeded)

	r.unregister(4242)
	assert.NoError(t, r.Wait(context.Background()))
}

// TestRunAndLogLines_ContextCancel_NormalExitWins is a race safety net: if
// the process exits at the same time the context is canceled, a normal
// outcome is still returned when the wait wins the select.
func TestRunAndLogLines_NormalRunUnchanged(t *testing.T) {
	newTestRegistry(t)

	ex := NewExecutor("", "sh", []string{"-c", "echo ok"}, nil).
		WithLogger(log.NewNop())

	usage, err := ex.RunAndLogLines(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, usage)
}
