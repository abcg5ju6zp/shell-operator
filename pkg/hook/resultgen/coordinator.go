package resultgen

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/deckhouse/deckhouse/pkg/log"
)

// Coordinator drives the staged, idempotent commit of a frozen hook result.
//
// Stage order is fixed: object patches → values → metrics → queue conclusion.
// Each stage transition is durable via the registry/store, so a retry or a
// restart resumes only the unfinished parts.
type Coordinator struct {
	registry *Registry

	executor PatchExecutor
	verifier PatchVerifier
	metrics  HookMetricsApplier

	valuesMu sync.RWMutex
	values   ValueCommitter

	// appliedGuard closes the in-process window between a successful external
	// apply and its durable journal confirmation: if the confirmation write
	// fails and the task is retried in the same process, guarded operations
	// and stages are adopted without a second external call. The guard is
	// intentionally not persisted (cross-process dedup relies on RV/content
	// verification), see Assumptions in the spec.
	guardMu sync.Mutex
	guard   map[string]any

	logger *log.Logger
}

// guard key kinds.
func opGuardKey(id GenerationID, index int) string {
	return fmt.Sprintf("op:%s:%d", id, index)
}

func stageGuardKey(id GenerationID, stage StageName) string {
	return "stage:" + string(id) + ":" + string(stage)
}

func (c *Coordinator) rememberGuarded(key string, v any) {
	c.guardMu.Lock()
	if c.guard == nil {
		c.guard = map[string]any{}
	}
	c.guard[key] = v
	c.guardMu.Unlock()
}

func (c *Coordinator) guarded(key string) (any, bool) {
	c.guardMu.Lock()
	defer c.guardMu.Unlock()
	v, ok := c.guard[key]
	return v, ok
}

// Option customizes a Coordinator.
type Option func(*Coordinator)

// WithValueCommitter registers the configuration-values stage implementation.
// Without one the values stage is Skipped (the shell-operator default).
func WithValueCommitter(v ValueCommitter) Option {
	return func(c *Coordinator) { c.SetValueCommitter(v) }
}

// SetValueCommitter registers (or replaces) the values stage implementation
// after the coordinator was built, as extenders may wire their committer
// later. It is safe for concurrent use.
func (c *Coordinator) SetValueCommitter(v ValueCommitter) {
	c.valuesMu.Lock()
	c.values = v
	c.valuesMu.Unlock()
}

func (c *Coordinator) valueCommitter() ValueCommitter {
	c.valuesMu.RLock()
	defer c.valuesMu.RUnlock()
	return c.values
}

// NewCoordinator wires the commit pipeline.
func NewCoordinator(
	registry *Registry,
	executor PatchExecutor,
	verifier PatchVerifier,
	metrics HookMetricsApplier,
	logger *log.Logger,
	opts ...Option,
) *Coordinator {
	c := &Coordinator{
		registry: registry,
		executor: executor,
		verifier: verifier,
		metrics:  metrics,
		logger:   logger,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Registry exposes the generation registry (debug queries, recovery).
func (c *Coordinator) Registry() *Registry { return c.registry }

// Commit freezes (or resumes) the generation and advances its stages as far
// as possible. Callers map the returned CommitDecision.Kind onto their queue
// protocol. It is safe to call repeatedly for retries of the same task.
func (c *Coordinator) Commit(ctx context.Context, bindingHash, fingerprint string, in CommitInput) *CommitDecision {
	rec, resumed, err := c.registry.BeginOrResume(in.FreezeInput(bindingHash, fingerprint))
	if err != nil {
		return &CommitDecision{Kind: DecisionFail, Err: err}
	}

	if resumed {
		c.logger.Info("resume commit of result generation",
			"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq)
	} else {
		c.logger.Info("freeze result generation before commit",
			"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq,
			"patchOps", in.ParsedPatchCount, "metrics", len(in.Metrics))
	}

	// A non-active record is a terminal transition whose first attempt was
	// interrupted (typically an index write failure). Complete the durable
	// transition idempotently instead of freezing a new generation: the
	// external side effects of this generation must never be replayed.
	if rec.State != GenActive {
		return c.completeTerminal(rec)
	}

	// Deterministic patch-spec parse failure captured at freeze time.
	if ps := rec.Stage(StageObjectPatches); ps.State == StageFailed && ps.LastError != "" && len(rec.Ops) == 0 {
		failure := &FailureRecord{
			Reason:  "InvalidPatch",
			Message: ps.LastError,
		}
		return c.terminal(rec, failure)
	}

	var decision *CommitDecision
	for _, stage := range OrderedStages {
		if out := c.gate(ctx, rec); out.kind != stageDone {
			decision = c.resolve(rec, out)
			break
		}

		var out stageOutcome
		switch stage {
		case StageObjectPatches:
			out = c.runPatchStage(ctx, rec)
		case StageValues:
			out = c.runValuesStage(ctx, rec)
		case StageMetrics:
			out = c.runMetricsStage(ctx, rec, in.CommonMetricLabels)
		case StageQueue:
			out = c.finishQueueStage(ctx, rec, in.Queued)
		}

		if out.kind != stageDone {
			decision = c.resolve(rec, out)
			break
		}
	}

	if decision == nil {
		if err := c.registry.Succeed(rec); err != nil {
			decision = &CommitDecision{Kind: DecisionFail, Err: err, Record: rec}
		} else {
			rec.State = GenSucceeded
			c.forgetGeneration(rec.ID)
			c.logger.Info("result generation committed",
				"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq)
			decision = &CommitDecision{Kind: DecisionSuccess, Record: rec}
		}
	}

	// A synchronous (webhook) execution has no in-memory task to retry: leave
	// its unfinished (still Active) generation resumable by fingerprint for
	// the next HTTP retry instead of leaking a non-orphaned active ref.
	// Finalized records are excluded: their transition is being completed.
	if !in.Queued && decision.Record != nil && decision.Record.State == GenActive &&
		(decision.Kind == DecisionFail || decision.Kind == DecisionRepeat) {
		if err := c.registry.MarkOrphaned(decision.Record); err != nil {
			c.logger.Warn("failed to orphan synchronous result generation after incomplete commit",
				"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq, "error", err.Error())
		}
	}

	return decision
}

// completeTerminal finishes an already-finalized generation (record file
// carries the terminal verdict; index/file cleanup may still be pending)
// without invoking any external stage. All store operations are idempotent,
// so a failed completion is retried by the next task processing.
func (c *Coordinator) completeTerminal(rec *GenerationRecord) *CommitDecision {
	switch rec.State {
	case GenFailedTerminal:
		failure := rec.Failure
		if failure == nil {
			failure = &FailureRecord{Reason: "Unknown", Message: "terminal generation without failure details"}
		}
		// terminal() re-fills coordinates and replays the idempotent store
		// transition (dead file overwrite, ref drop, failure dedup).
		return c.terminal(rec, failure)
	case GenSuperseded:
		if err := c.registry.Supersede(rec, "completing superseded result generation"); err != nil {
			return &CommitDecision{Kind: DecisionFail, Err: err, Record: rec}
		}
		c.forgetGeneration(rec.ID)
		return &CommitDecision{Kind: DecisionSuperseded, Record: rec}
	default: // GenSucceeded
		if err := c.registry.Succeed(rec); err != nil {
			return &CommitDecision{Kind: DecisionFail, Err: err, Record: rec}
		}
		rec.State = GenSucceeded
		c.forgetGeneration(rec.ID)
		return &CommitDecision{Kind: DecisionSuccess, Record: rec}
	}
}

// forgetGeneration drops the in-process applied guard of a generation that has
// reached a terminal state, so completed generations do not accumulate
// (potentially large) pre-image snapshots in memory.
func (c *Coordinator) forgetGeneration(id GenerationID) {
	opPrefix := "op:" + string(id) + ":"
	stagePrefix := "stage:" + string(id) + ":"
	c.guardMu.Lock()
	for key := range c.guard {
		if strings.HasPrefix(key, opPrefix) || strings.HasPrefix(key, stagePrefix) {
			delete(c.guard, key)
		}
	}
	c.guardMu.Unlock()
}

// gate enforces cancellation and per-hook generation fencing at every stage
// boundary.
func (c *Coordinator) gate(ctx context.Context, rec *GenerationRecord) stageOutcome {
	if err := ctx.Err(); err != nil {
		c.logger.Info("commit paused: context canceled before stage",
			"hook", rec.HookName, "generation", string(rec.ID), "error", err.Error())
		return canceledOutcome()
	}
	if !c.registry.CheckFence(rec) {
		c.logger.Info("commit fenced off: newer generation exists",
			"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq)
		return staleOutcome()
	}
	return doneOutcome()
}

// resolve maps a non-done stage outcome to a queue decision with the
// appropriate durable side effect.
func (c *Coordinator) resolve(rec *GenerationRecord, out stageOutcome) *CommitDecision {
	switch out.kind {
	case stageCanceled:
		return &CommitDecision{Kind: DecisionRepeat, Record: rec, Err: context.Canceled}
	case stageStale:
		if err := c.registry.Supersede(rec, "newer generation exists"); err != nil {
			return &CommitDecision{Kind: DecisionFail, Err: err, Record: rec}
		}
		rec.State = GenSuperseded
		c.forgetGeneration(rec.ID)
		return &CommitDecision{Kind: DecisionSuperseded, Record: rec}
	case stageTerminalFL:
		return c.terminal(rec, out.failure)
	default:
		return &CommitDecision{Kind: DecisionFail, Err: out.err, Record: rec}
	}
}

func (c *Coordinator) terminal(rec *GenerationRecord, failure *FailureRecord) *CommitDecision {
	if failure == nil {
		failure = &FailureRecord{Reason: "Unknown", Message: "terminal failure without details"}
	}
	failure.HookName = rec.HookName
	failure.TaskID = rec.TaskID
	failure.QueueName = rec.QueueName
	failure.BindingType = rec.BindingType
	failure.GenerationID = rec.ID
	failure.Seq = rec.Seq

	replayed := rec.State == GenFailedTerminal
	if err := c.registry.FailTerminal(rec, *failure); err != nil {
		return &CommitDecision{Kind: DecisionFail, Err: err, Record: rec}
	}
	rec.State = GenFailedTerminal
	rec.Failure = failure
	c.forgetGeneration(rec.ID)
	attrs := []any{
		"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq,
		"opIndex", failure.OpIndex, "reason", failure.Reason,
		"object", failure.Object, "message", failure.Message,
	}
	if replayed {
		// Completing an earlier verdict whose final index write was lost: do
		// not re-raise it as a fresh error on every backoff-driven retry.
		c.logger.Warn("completing dead-letter after an interrupted finalization", attrs...)
	} else {
		c.logger.Error("result generation failed terminally; task moved to dead-letter", attrs...)
	}
	return &CommitDecision{Kind: DecisionDeadLetter, Failure: failure, Record: rec}
}

func (c *Coordinator) finishQueueStage(_ context.Context, rec *GenerationRecord, queued bool) stageOutcome {
	st := rec.Stage(StageQueue)
	if st.Done() {
		return doneOutcome()
	}
	if queued {
		st.State = StageCommitted
	} else {
		// Synchronous webhook execution: the HTTP response IS the conclusion.
		st.State = StageSkipped
	}
	st.LastError = ""
	st.UpdatedAt = rec.UpdatedAt
	if err := c.registry.Save(rec); err != nil {
		return retryableOutcome(err)
	}
	return doneOutcome()
}
