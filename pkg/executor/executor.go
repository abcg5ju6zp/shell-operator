package executor

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/deckhouse/deckhouse/pkg/log"
	"go.opentelemetry.io/otel"

	pkg "github.com/flant/shell-operator/pkg"
	json "github.com/flant/shell-operator/pkg/utils/json"
	utils "github.com/flant/shell-operator/pkg/utils/labels"
)

const (
	serviceName = "executor"
)

// Run starts the command, waits for it to complete, and returns the error.
// The child PID is registered in the global process registry while the process
// is running so that a PID-1 zombie reaper does not steal it.
func Run(cmd *exec.Cmd) error {
	// TODO context: hook name, hook phase, hook binding
	// TODO observability
	log.Debug("Executing command", slog.String(pkg.LogKeyCommand, strings.Join(cmd.Args, " ")), slog.String(pkg.LogKeyDir, cmd.Dir))

	if err := startAndRegister(cmd); err != nil {
		return err
	}
	defer unregisterPID(cmd.Process.Pid)

	return cmd.Wait()
}

// StderrError is returned by RunAndLogLines when a command fails and produces
// output on stderr. Callers can use errors.As to access the raw stderr content.
type StderrError struct {
	Message string
}

func (e *StderrError) Error() string {
	return e.Message
}

type Executor struct {
	cmd              *exec.Cmd
	logProxyHookJSON bool
	proxyJsonKey     string
	logger           *log.Logger
}

func (e *Executor) WithLogProxyHookJSON(logProxyHookJSON bool) *Executor {
	e.logProxyHookJSON = logProxyHookJSON

	return e
}

func (e *Executor) WithLogProxyHookJSONKey(logProxyHookJSONKey string) *Executor {
	if logProxyHookJSONKey == "" {
		return e
	}

	e.proxyJsonKey = logProxyHookJSONKey

	return e
}

func (e *Executor) WithLogger(logger *log.Logger) *Executor {
	e.logger = logger

	return e
}

func (e *Executor) WithChroot(path string) *Executor {
	if len(path) > 0 {
		e.cmd.SysProcAttr = &syscall.SysProcAttr{
			Chroot: path,
		}
		e.cmd.Path = strings.TrimPrefix(e.cmd.Path, path)
		e.cmd.Dir = "/"
	}

	return e
}

func (e *Executor) WithCMDStdout(w io.Writer) *Executor {
	e.cmd.Stdout = w

	return e
}

func (e *Executor) WithCMDStderr(w io.Writer) *Executor {
	e.cmd.Stderr = w

	return e
}

func NewExecutor(dir string, entrypoint string, args []string, envs []string) *Executor {
	cmd := exec.Command(entrypoint, args...)
	cmd.Env = append(cmd.Env, envs...)
	cmd.Dir = dir

	ex := &Executor{
		cmd:          cmd,
		proxyJsonKey: "proxyJsonLog",
		logger:       log.NewLogger().Named("auto-executor"),
	}

	return ex
}

func (e *Executor) Output() ([]byte, error) {
	e.logger.Debug("Executing command",
		slog.String(pkg.LogKeyCommand, strings.Join(e.cmd.Args, " ")),
		slog.String(pkg.LogKeyDir, e.cmd.Dir))

	// Reproduce cmd.Output() but interleave PID registration so that the
	// PID-1 zombie reaper skips this process.
	if e.cmd.Stdout != nil {
		return nil, errors.New("exec: Stdout already set")
	}
	var stdout bytes.Buffer
	e.cmd.Stdout = &stdout

	captureErr := e.cmd.Stderr == nil
	var stderrBuf bytes.Buffer
	if captureErr {
		e.cmd.Stderr = &stderrBuf
	}

	if err := startAndRegister(e.cmd); err != nil {
		return nil, err
	}
	defer unregisterPID(e.cmd.Process.Pid)

	err := e.cmd.Wait()
	if err != nil && captureErr {
		if ee, ok := err.(*exec.ExitError); ok {
			ee.Stderr = stderrBuf.Bytes()
		}
	}

	return stdout.Bytes(), err
}

type CmdUsage struct {
	Sys    time.Duration
	User   time.Duration
	MaxRss int64
}

// Grace periods used when a hook command must be stopped because its context
// is canceled (client disconnect, same-UID retry superseding it, process
// shutdown). The whole process group is asked to terminate first and then
// force-killed. If it still cannot be reaped after SIGKILL, the wait is
// detached (see RunAndLogLines).
const (
	terminateGracePeriod = 3 * time.Second
	killGracePeriod      = 2 * time.Second
)

// runOutcome carries cmd.Wait result and resource usage back from the
// goroutine that waits for a command.
type runOutcome struct {
	usage *CmdUsage
	err   error
}

func (e *Executor) RunAndLogLines(ctx context.Context, logLabels map[string]string) (*CmdUsage, error) {
	ctx, span := otel.Tracer(serviceName).Start(ctx, "RunAndLogLines")
	defer span.End()

	stdErr := bytes.NewBuffer(nil)
	logEntry := utils.EnrichLoggerWithLabels(e.logger, logLabels)
	stdoutLogEntry := logEntry.With(pkg.LogKeyOutput, "stdout")
	stderrLogEntry := logEntry.With(pkg.LogKeyOutput, "stderr")

	log.Debug("Executing command",
		slog.String(pkg.LogKeyCommand, strings.Join(e.cmd.Args, " ")),
		slog.String(pkg.LogKeyDir, e.cmd.Dir))

	plo := &proxyLogger{
		ctx:              ctx,
		logProxyHookJSON: e.logProxyHookJSON,
		proxyJsonLogKey:  e.proxyJsonKey,
		logger:           stdoutLogEntry,
		buf:              make([]byte, 0),
	}

	ple := &proxyLogger{
		ctx:              ctx,
		logProxyHookJSON: e.logProxyHookJSON,
		proxyJsonLogKey:  e.proxyJsonKey,
		logger:           stderrLogEntry,
		buf:              make([]byte, 0),
	}

	// Wrap the output writers so late output of a canceled/superseded
	// execution can be isolated: a process that ignores SIGTERM keeps the
	// pipes open, but nothing it writes afterwards may reach logs or metrics.
	stdoutWriter := &isolatingWriter{w: plo}
	stderrWriter := &isolatingWriter{w: io.MultiWriter(ple, stdErr)}
	e.cmd.Stdout = stdoutWriter
	e.cmd.Stderr = stderrWriter

	// Hooks are shell scripts that may spawn their own children. Start the
	// command in its own process group so cancellation can signal the whole
	// group at once instead of killing only the shell and orphaning children.
	ensureProcessGroup(e.cmd)

	if err := startAndRegister(e.cmd); err != nil {
		return nil, fmt.Errorf("cmd start: %w", err)
	}
	pid := e.cmd.Process.Pid

	waitCh := make(chan runOutcome, 1)
	go func() {
		waitCh <- e.waitCommand(stdErr)
	}()

	select {
	case out := <-waitCh:
		unregisterPID(pid)
		return out.usage, out.err
	case <-ctx.Done():
		// Request is canceled (API server disconnected, a same-UID retry
		// superseded this execution or the process is shutting down).
		// Isolate late results first, then stop the subprocess.
		stdoutWriter.Isolate()
		stderrWriter.Isolate()

		// If the process finished at the same instant, honor its outcome.
		select {
		case out := <-waitCh:
			unregisterPID(pid)
			return out.usage, out.err
		default:
		}

		_, abortErr := e.cancelProcessGroup(pid, waitCh)
		if abortErr == nil {
			unregisterPID(pid)
			return nil, fmt.Errorf("cmd canceled: %w", ctx.Err())
		}

		// The process cannot be stopped even with SIGKILL (e.g. stuck in
		// uninterruptible disk sleep). Detach the reaper instead of blocking
		// the handler/shutdown: late output is already isolated, callers
		// proceed with temp-file cleanup, and the PID stays registered (the
		// zombie reaper must keep skipping it; Wait keeps accounting for it)
		// until the background wait finally releases it.
		logEntry.Error("hook process did not stop after SIGKILL, reaping detached",
			slog.Int("pid", pid),
			log.Err(abortErr))
		go func() {
			out := <-waitCh
			unregisterPID(pid)
			if out.err != nil {
				e.logger.Debug("detached hook process reaped", log.Err(out.err))
			}
		}()

		return nil, fmt.Errorf("cmd canceled: %w", errors.Join(ctx.Err(), abortErr))
	}
}

// waitCommand waits for the command and converts the result into a runOutcome
// preserving the historical error behavior (stderr content wins, otherwise
// the wait error is wrapped as "cmd run").
func (e *Executor) waitCommand(stdErr *bytes.Buffer) runOutcome {
	err := e.cmd.Wait()
	if err != nil {
		if len(stdErr.Bytes()) > 0 {
			return runOutcome{err: &StderrError{Message: stdErr.String()}}
		}

		return runOutcome{err: fmt.Errorf("cmd run: %w", err)}
	}

	return runOutcome{usage: collectUsage(e.cmd.ProcessState)}
}

// collectUsage builds CmdUsage from the finished process state.
func collectUsage(state *os.ProcessState) *CmdUsage {
	if state == nil {
		return nil
	}

	usage := &CmdUsage{
		Sys:  state.SystemTime(),
		User: state.UserTime(),
	}

	// FIXME Maxrss is Unix specific.
	sysUsage := state.SysUsage()
	if v, ok := sysUsage.(*syscall.Rusage); ok {
		// v.Maxrss is int32 on arm/v7
		usage.MaxRss = int64(v.Maxrss) //nolint:unconvert
	}

	return usage
}

// ensureProcessGroup makes cmd start as the leader of its own process group.
// Existing SysProcAttr (e.g. Chroot) is preserved and augmented.
func ensureProcessGroup(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		return
	}
	cmd.SysProcAttr.Setpgid = true
}

// cancelProcessGroup signals the command's process group (negative PID targets
// every process in the group) and waits for the detached wait to finish:
// SIGTERM with a grace period, then SIGKILL with a grace period. Signaling an
// already-dead group is expected and ignored. A non-nil error means the group
// still could not be reaped and the caller must detach the wait.
func (e *Executor) cancelProcessGroup(pid int, waitCh <-chan runOutcome) (runOutcome, error) {
	return cancelProcess(waitCh, func(sig syscall.Signal) {
		_ = syscall.Kill(-pid, sig)
	})
}

// cancelProcess drives the TERM → KILL escalation against an injectable kill
// function. It returns the reaped outcome as soon as waitCh delivers, or a
// non-nil error when neither signal made the process exit within its grace
// period — in that case the caller detaches the wait.
func cancelProcess(waitCh <-chan runOutcome, kill func(sig syscall.Signal)) (runOutcome, error) {
	kill(syscall.SIGTERM)
	if out, ok := receiveOutcome(waitCh, terminateGracePeriod); ok {
		return out, nil
	}

	kill(syscall.SIGKILL)
	if out, ok := receiveOutcome(waitCh, killGracePeriod); ok {
		return out, nil
	}

	return runOutcome{}, fmt.Errorf("process did not exit after SIGTERM/SIGKILL")
}

// receiveOutcome waits up to d for the detached command wait to complete.
func receiveOutcome(ch <-chan runOutcome, d time.Duration) (runOutcome, bool) {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case out := <-ch:
		return out, true
	case <-timer.C:
		return runOutcome{}, false
	}
}

// isolatingWriter forwards writes to w until Isolate is called; afterwards it
// silently consumes everything. It lets a canceled hook subprocess keep
// writing (no broken-pipe feedback) while guaranteeing its late output cannot
// contaminate logs, metrics or temporary results of the current execution.
type isolatingWriter struct {
	mu       sync.Mutex
	w        io.Writer
	isolated bool
}

func (i *isolatingWriter) Write(p []byte) (int, error) {
	i.mu.Lock()
	w := i.w
	isolated := i.isolated
	i.mu.Unlock()

	if isolated {
		return len(p), nil
	}

	return w.Write(p)
}

func (i *isolatingWriter) Isolate() {
	i.mu.Lock()
	i.isolated = true
	i.mu.Unlock()
}

type proxyLogger struct {
	ctx context.Context

	logProxyHookJSON bool
	proxyJsonLogKey  string
	logger           *log.Logger
	buf              []byte
}

func (pl *proxyLogger) Write(p []byte) (int, error) {
	if !pl.logProxyHookJSON {
		pl.writerScanner(p)

		return len(p), nil
	}

	// join all parts of json
	pl.buf = append(pl.buf, p...)

	var line interface{}
	err := json.Unmarshal(pl.buf, &line)
	if err != nil {
		if err.Error() == "unexpected end of JSON input" {
			return len(p), nil
		}

		pl.logger.Debug("output is not json", log.Err(err))
		pl.writerScanner(p)

		return len(p), nil
	}

	logMap, ok := line.(map[string]interface{})
	defer func() {
		pl.buf = pl.buf[:0]
	}()

	if !ok {
		pl.logger.Debug("json log line not map[string]interface{}", slog.Any(pkg.LogKeyLine, line))

		// fall back to using the logger
		pl.logger.Info(string(p))

		return len(p), nil
	}

	pl.mergeAndLogInputLog(pl.ctx, logMap, "hook")

	return len(p), nil
}

func (pl *proxyLogger) writerScanner(p []byte) {
	scanner := bufio.NewScanner(bytes.NewReader(p))
	scanner.Buffer(make([]byte, bufio.MaxScanTokenSize), bufio.MaxScanTokenSize)

	var jsonBuf []string
	// Split large entries into chunks
	chunkSize := bufio.MaxScanTokenSize // 64KB
	splitFunc := func(data []byte, atEOF bool) (int, []byte, error) {
		if len(data) >= chunkSize {
			return chunkSize, data[:chunkSize], nil
		}
		return bufio.ScanLines(data, atEOF)
	}
	scanner.Split(splitFunc)
	// Scan the input and write it to the logger using the specified print function
	for scanner.Scan() {
		// Prevent empty logging
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if len(jsonBuf) > 0 || strings.HasPrefix(line, "{") {
			jsonBuf = append(jsonBuf, line)
			joined := strings.Join(jsonBuf, "\n")
			if err := pl.tryLogJSON(joined); err == nil {
				jsonBuf = nil
			}
			continue
		}

		if len(line) > 10000 {
			line = fmt.Sprintf("%s:truncated", line[:10000])
		}

		if err := pl.tryLogJSON(line); err == nil {
			continue
		}
		pl.logger.Info(line)
	}
	// If there was an error while scanning the input, log an error
	if err := scanner.Err(); err != nil {
		pl.logger.Error("reading from scanner", log.Err(err))
	}
}

// tryLogJSON tries to parse the string as JSON and log it if it succeeds
func (pl *proxyLogger) tryLogJSON(s string) error {
	var m map[string]interface{}
	err := json.Unmarshal([]byte(s), &m)
	if err == nil {
		pl.mergeAndLogInputLog(pl.ctx, m, "hook")
		return nil
	}
	return fmt.Errorf("failed to parse json: %w", err)
}

// level = level
// msg = msg
// prefix for all fields hook_
// source = hook_source
// stacktrace = hook_stacktrace
func (pl *proxyLogger) mergeAndLogInputLog(ctx context.Context, inputLog map[string]interface{}, prefix string) {
	var lvl log.Level

	lvlRaw, ok := inputLog[slog.LevelKey].(string)
	if ok {
		lvl = log.LogLevelFromStr(lvlRaw)
		delete(inputLog, slog.LevelKey)
	}

	msg, ok := inputLog[slog.MessageKey].(string)
	if !ok {
		msg = "hook result"
	}

	delete(inputLog, slog.MessageKey)
	delete(inputLog, slog.TimeKey)

	logLineRaw, _ := json.Marshal(inputLog)
	logLine := string(logLineRaw)

	logger := pl.logger.With(pl.proxyJsonLogKey, true)

	if len(logLine) > 10000 {
		logLine = fmt.Sprintf("%s:truncated", logLine[:10000])

		logger.Log(ctx, lvl.Level(), msg, slog.Any(pkg.LogKeyHook, map[string]any{
			"truncated": logLine,
		}))

		return
	}

	for key, val := range inputLog {
		logger = logger.With(slog.Any(prefix+"_"+key, val))
	}

	logger.Log(ctx, lvl.Level(), msg)
}
