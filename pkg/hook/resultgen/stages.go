package resultgen

import (
	"context"
	"encoding/json"
	"fmt"

	metricsop "github.com/deckhouse/deckhouse/pkg/metrics-storage/operation"
	sdkpkg "github.com/deckhouse/module-sdk/pkg"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	objectpatch "github.com/flant/shell-operator/pkg/kube/object_patch"
)

// runPatchStage drives each frozen patch operation to a durable Confirmed
// state. It never re-sends confirmed operations: on retry it verifies
// uncertain ones and only (re)sends operations that are proven not applied.
func (c *Coordinator) runPatchStage(ctx context.Context, rec *GenerationRecord) stageOutcome {
	st := rec.Stage(StageObjectPatches)
	if st.Done() {
		return doneOutcome()
	}

	// Parsing happens from the frozen bytes; JQ/filter operations rebuild their
	// closures deterministically from the serialized spec.
	ops, err := objectpatch.ParseOperations(rec.PatchBytes)
	if err != nil {
		return c.terminalPatchOutcome(rec, -1, nil, "InvalidPatch", err.Error())
	}
	if len(ops) != len(rec.Ops) {
		return c.terminalPatchOutcome(rec, -1, nil, "InvalidPatch",
			fmt.Sprintf("frozen op count mismatch: journal=%d parsed=%d", len(rec.Ops), len(ops)))
	}

	if st.State != StageApplying {
		st.State = StageApplying
		st.LastError = ""
		if err := c.registry.Save(rec); err != nil {
			return retryableOutcome(err)
		}
	}

	for i, op := range ops {
		if out := c.gate(ctx, rec); out.kind != stageDone {
			return out
		}

		oc := rec.Ops[i]
		if oc.Description == "" {
			oc.Description = op.Description()
		}

		// In-process guard: the external call already succeeded earlier but its
		// journal confirmation was lost to a disk write failure. Adopt the
		// guarded outcome without sending anything again.
		if g, ok := c.guarded(opGuardKey(rec.ID, i)); ok && oc.State != OpConfirmed && oc.State != OpIgnored {
			if guarded, isOp := g.(OpCommit); isOp {
				*oc = guarded
				c.logger.Info("adopting in-process confirmed patch op without resending",
					"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq, "op", i)
				if err := c.registry.Save(rec); err != nil {
					return retryableOutcome(err)
				}
				continue
			}
		}

		switch oc.State {
		case OpConfirmed, OpIgnored:
			continue
		case OpUncertain, OpApplying:
			out := c.verifyOrResend(ctx, rec, op, oc)
			if out.kind != stageDone {
				return out
			}
		default: // Pending, Failed
			out := c.sendOnce(ctx, rec, op, oc, false)
			if out.kind != stageDone {
				return out
			}
		}
	}

	st.State = StageCommitted
	st.LastError = ""
	if err := c.registry.Save(rec); err != nil {
		return retryableOutcome(err)
	}
	c.logger.Info("object patches stage committed",
		"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq, "ops", len(ops))
	return doneOutcome()
}

// verifyOrResend handles an operation whose send outcome was unknown (ambiguous
// API answer) or which was mid-flight when state was last persisted.
func (c *Coordinator) verifyOrResend(ctx context.Context, rec *GenerationRecord, op sdkpkg.PatchCollectorOperation, oc *OpCommit) stageOutcome {
	// Cancellation arrived while the in-flight request was finishing: record
	// nothing new and do not issue the verification Get until the next tick.
	if err := ctx.Err(); err != nil {
		c.logger.Info("patch verification paused: context canceled",
			"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq, "op", oc.Index)
		return canceledOutcome()
	}

	vr := c.verifier.VerifyOperation(ctx, op, objectpatch.VerifyEvidence{
		PreImage:            unmarshalPreImage(oc.PreImage),
		PostResourceVersion: oc.PostResourceVersion,
	})
	c.logger.Info("verified uncertain patch operation",
		"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq, "op", oc.Index,
		"verdict", string(vr.State), "reason", vr.Reason)

	switch vr.State {
	case objectpatch.VerifyApplied:
		oc.State = OpConfirmed
		if oc.PostResourceVersion == "" {
			oc.PostResourceVersion = vr.ResourceVersion
		}
		oc.LastError = ""
		c.rememberGuarded(opGuardKey(rec.ID, oc.Index), *oc)
		if err := c.registry.Save(rec); err != nil {
			return retryableOutcome(err)
		}
		return doneOutcome()
	case objectpatch.VerifyNotApplied:
		// Proven absent: safe to send the operation once now.
		return c.sendOnce(ctx, rec, op, oc, true)
	case objectpatch.VerifyGetUncertain:
		oc.State = OpUncertain
		oc.LastError = vr.Reason
		_ = c.registry.Save(rec)
		// The verification Get was interrupted by shutdown/cancel: no further
		// API call this pass; the next resume verifies the pending op.
		if err := ctx.Err(); err != nil {
			c.logger.Info("patch verification interrupted by cancellation",
				"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq, "op", oc.Index)
			return canceledOutcome()
		}
		return retryableOutcome(fmt.Errorf("patch op %d verification pending: %s", oc.Index, vr.Reason))
	default: // VerifyIncompatible, VerifyUndecidable
		reason := "Conflict"
		if vr.State == objectpatch.VerifyUndecidable {
			reason = "Undecidable"
		}
		return c.terminalPatchOutcome(rec, oc.Index, op, reason, vr.Reason)
	}
}

// sendOnce executes one operation and records the durable outcome.
func (c *Coordinator) sendOnce(ctx context.Context, rec *GenerationRecord, op sdkpkg.PatchCollectorOperation, oc *OpCommit, verifiedMissing bool) stageOutcome {
	// Plain merge/json patches issue a single PATCH without reading the
	// object first: capture the pre-write content with one best-effort Get so
	// an ambiguous response can later be verified. A capture failure does not
	// block the send (verification simply becomes conservative).
	if len(oc.PreImage) == 0 && objectpatch.NeedsPreImage(op) {
		if pre, err := c.executor.GetOperationObject(ctx, op); err == nil && pre != nil {
			if raw, mErr := json.Marshal(pre.Object); mErr == nil {
				oc.PreImage = raw
			}
		}
		// The Get is an in-flight request allowed to finish after cancellation,
		// but the mutating PATCH itself must never start afterwards.
		if err := ctx.Err(); err != nil {
			c.logger.Info("patch send skipped: context canceled after pre-image capture",
				"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq, "op", oc.Index)
			return canceledOutcome()
		}
	}

	// No new mutating request may leave the process after cancellation.
	if err := ctx.Err(); err != nil {
		return canceledOutcome()
	}

	oc.Attempts++
	oc.State = OpApplying
	oc.LastError = ""
	if err := c.registry.Save(rec); err != nil {
		return retryableOutcome(err)
	}

	outcome := c.executor.ExecuteOperationCtx(ctx, op)
	canceled := ctx.Err() != nil

	// Filter / create-or-update paths read the object internally right before
	// the write; adopt that pre-image when we did not fetch one ourselves.
	if len(oc.PreImage) == 0 && outcome.PreImage != nil {
		if raw, mErr := json.Marshal(outcome.PreImage.Object); mErr == nil {
			oc.PreImage = raw
		}
	}

	switch outcome.State {
	case objectpatch.OutcomeApplied:
		oc.State = OpConfirmed
		oc.PreResourceVersion = outcome.PreResourceVersion
		oc.PostResourceVersion = outcome.PostResourceVersion
		oc.LastError = ""
		c.rememberGuarded(opGuardKey(rec.ID, oc.Index), *oc)
		_ = c.registry.Save(rec)
		return doneOutcome()

	case objectpatch.OutcomeIgnored:
		oc.State = OpIgnored
		oc.PreResourceVersion = outcome.PreResourceVersion
		oc.LastError = ""
		c.rememberGuarded(opGuardKey(rec.ID, oc.Index), *oc)
		_ = c.registry.Save(rec)
		return doneOutcome()

	case objectpatch.OutcomeUncertain:
		oc.State = OpUncertain
		oc.PreResourceVersion = outcome.PreResourceVersion
		if outcome.PostResourceVersion != "" {
			oc.PostResourceVersion = outcome.PostResourceVersion
		}
		if outcome.Err != nil {
			oc.LastError = outcome.Err.Error()
		}
		_ = c.registry.Save(rec)
		// The in-flight request was allowed to finish, but after cancellation
		// no new API call (verification Get or resend) may start: pause and
		// let the next tick/resume verify the uncertain operation.
		if canceled {
			c.logger.Info("patch outcome uncertain while canceling; pausing before verification",
				"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq, "op", oc.Index)
			return canceledOutcome()
		}
		if verifiedMissing {
			// Re-send after a verified "not applied" came back ambiguous
			// again: stop here and let the backoff-driven retry verify/send
			// once more — at most one resend per tick.
			return retryableOutcome(outcome.Err)
		}
		// Re-verify immediately while the decision is fresh.
		return c.verifyOrResend(ctx, rec, op, oc)

	case objectpatch.OutcomeConflict:
		if outcome.Err != nil {
			oc.LastError = outcome.Err.Error()
		}
		if canceled {
			// Do not terminal on an unexamined 409 while shutting down:
			// verify content on resume instead.
			oc.State = OpUncertain
			_ = c.registry.Save(rec)
			return canceledOutcome()
		}
		if verifiedMissing {
			// The op was already re-sent once this tick after a "not applied"
			// verdict and conflicted again: stop here, the backoff-driven
			// retry verifies/resends at most once per tick (no unbounded
			// verify->send chain within a single processing pass). The op is
			// left Uncertain (not Failed): a 409 means another writer touched
			// the object, so the next tick must verify live content BEFORE any
			// further send instead of blindly replaying it.
			oc.State = OpUncertain
			_ = c.registry.Save(rec)
			return retryableOutcome(outcome.Err)
		}
		// A 409 only becomes a dead letter after FR-5 verification proves the
		// live content cannot be reconciled with the patch intent: a concurrent
		// writer may have persisted exactly the intended content.
		c.logger.Info("patch conflict; verifying live content before terminal decision",
			"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq, "op", oc.Index)
		return c.verifyOrResend(ctx, rec, op, oc)

	case objectpatch.OutcomeRejected:
		msg := oc.LastError
		if outcome.Err != nil {
			msg = outcome.Err.Error()
		}
		// Persist the op-level terminal state before the generation is
		// dead-lettered, so a crash between the two journal writes leaves a
		// record that does not look mid-flight (Applying).
		oc.State = OpFailed
		oc.LastError = msg
		_ = c.registry.Save(rec)
		return c.terminalPatchOutcome(rec, oc.Index, op, string(outcome.State), msg)

	default: // OutcomeRetryable
		if outcome.Err != nil {
			oc.LastError = outcome.Err.Error()
		}
		if canceled {
			// Context canceled around an error response: the write's fate is
			// unknown; verify instead of blind-resending on the next tick.
			oc.State = OpUncertain
			_ = c.registry.Save(rec)
			return canceledOutcome()
		}
		oc.State = OpFailed
		_ = c.registry.Save(rec)
		if verifiedMissing {
			c.logger.Warn("verified-missing patch op failed again",
				"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq, "op", oc.Index)
		}
		return retryableOutcome(outcome.Err)
	}
}

func (c *Coordinator) terminalPatchOutcome(rec *GenerationRecord, index int, op sdkpkg.PatchCollectorOperation, reason, message string) stageOutcome {
	failure := &FailureRecord{
		Reason:  reason,
		Message: message,
		OpIndex: index,
	}
	if op != nil {
		failure.OperationDescription = op.Description()
		failure.Object = op.Description()
	}
	if rec.Stages[StageObjectPatches] != nil {
		rec.Stages[StageObjectPatches].State = StageFailed
		rec.Stages[StageObjectPatches].LastError = message
		_ = c.registry.Save(rec)
	}
	return terminalOutcome(failure)
}

func unmarshalPreImage(raw []byte) *unstructured.Unstructured {
	if len(raw) == 0 {
		return nil
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil
	}
	return &unstructured.Unstructured{Object: obj}
}

// runValuesStage commits configuration values when a ValueCommitter is
// registered; otherwise the stage is Skipped. A committed values stage is
// never invoked again (no rollback, no replay).
func (c *Coordinator) runValuesStage(ctx context.Context, rec *GenerationRecord) stageOutcome {
	st := rec.Stage(StageValues)
	if st.Done() {
		return doneOutcome()
	}
	values := c.valueCommitter()
	if values == nil {
		st.State = StageSkipped
		if err := c.registry.Save(rec); err != nil {
			return retryableOutcome(err)
		}
		c.logger.Debug("values stage skipped: no committer registered",
			"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq)
		return doneOutcome()
	}

	// Adopt an external apply that succeeded in this process but whose
	// Committed record was lost: never invoke the committer a second time.
	_, appliedInProcess := c.guarded(stageGuardKey(rec.ID, StageValues))
	if !appliedInProcess {
		st.State = StageApplying
		if err := c.registry.Save(rec); err != nil {
			return retryableOutcome(err)
		}

		if err := values.CommitValues(ctx, newView(rec)); err != nil {
			st.State = StageFailed
			st.LastError = err.Error()
			_ = c.registry.Save(rec)
			c.logger.Warn("values stage commit failed",
				"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq, "error", err.Error())
			return retryableOutcome(err)
		}
		c.rememberGuarded(stageGuardKey(rec.ID, StageValues), true)
	} else {
		c.logger.Info("adopting in-process committed values without reinvoking committer",
			"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq)
	}

	st.State = StageCommitted
	st.LastError = ""
	if err := c.registry.Save(rec); err != nil {
		return retryableOutcome(err)
	}
	c.logger.Info("values stage committed",
		"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq)
	return doneOutcome()
}

// runMetricsStage applies frozen metric operations exactly once per
// generation. Later retries find the stage Committed and skip it entirely, so
// counters are never double-added and patches/values are never replayed.
func (c *Coordinator) runMetricsStage(_ context.Context, rec *GenerationRecord, commonLabels map[string]string) stageOutcome {
	st := rec.Stage(StageMetrics)
	if st.Done() {
		return doneOutcome()
	}
	if c.metrics == nil || len(rec.Metrics) == 0 {
		st.State = StageSkipped
		if err := c.registry.Save(rec); err != nil {
			return retryableOutcome(err)
		}
		c.logger.Debug("metrics stage skipped: no metric operations",
			"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq)
		return doneOutcome()
	}

	// Same in-process guard as the values stage: an applied metrics batch
	// whose Committed record was lost must not be added a second time.
	_, appliedInProcess := c.guarded(stageGuardKey(rec.ID, StageMetrics))
	if !appliedInProcess {
		st.State = StageApplying
		if err := c.registry.Save(rec); err != nil {
			return retryableOutcome(err)
		}

		ops := make([]metricsop.MetricOperation, 0, len(rec.Metrics))
		for _, m := range rec.Metrics {
			ops = append(ops, m.ToExternal())
		}

		if err := c.metrics.ApplyBatchOperations(ops, commonLabels); err != nil {
			// Do not touch committed stages: patches and values stay effective.
			st.State = StageFailed
			st.LastError = err.Error()
			_ = c.registry.Save(rec)
			c.logger.Warn("metrics stage failed",
				"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq, "error", err.Error())
			return retryableOutcome(err)
		}
		c.rememberGuarded(stageGuardKey(rec.ID, StageMetrics), true)
	} else {
		c.logger.Info("adopting in-process applied metrics without re-adding",
			"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq)
	}

	st.State = StageCommitted
	st.LastError = ""
	if err := c.registry.Save(rec); err != nil {
		return retryableOutcome(err)
	}
	c.logger.Info("metrics stage committed",
		"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq, "metrics", len(rec.Metrics))
	return doneOutcome()
}
