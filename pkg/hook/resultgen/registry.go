package resultgen

import (
	"sync"

	"github.com/deckhouse/deckhouse/pkg/log"
)

// Registry is the in-process view over the durable Store. It correlates hook
// executions with existing generations (exact task match for in-process
// retries, frozen-result fingerprint match after a restart), allocates new
// generations and provides per-hook fencing.
type Registry struct {
	store  Store
	logger *log.Logger

	mu sync.Mutex
}

// NewRegistry builds a registry over a prepared Store. Call Recover once
// before serving commits.
func NewRegistry(store Store, logger *log.Logger) *Registry {
	return &Registry{store: store, logger: logger}
}

// Recover loads persisted active generations and dead-letter records from
// disk. It is safe to call once at operator start.
func (r *Registry) Recover() ([]*GenerationRecord, []*FailureRecord, error) {
	return r.store.Recover()
}

// BeginOrResume returns the generation for a hook execution:
//
//  1. an active generation bound to the same task ID (in-process retry);
//  2. a restart-orphaned generation with the identical frozen fingerprint,
//     rebound to the new task (post-restart resume);
//  3. otherwise a freshly frozen generation; remaining restart orphans of the
//     same hook are retired (superseded) at that point.
func (r *Registry) BeginOrResume(in FreezeInput) (*GenerationRecord, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing := r.store.ByTask(in.HookName, in.TaskID); existing != nil {
		return existing, true, nil
	}

	if orphan := r.store.ResumeableByFingerprint(in.HookName, in.Fingerprint); orphan != nil {
		if err := r.store.RebindTask(orphan.ID, in.TaskID); err != nil {
			return nil, false, err
		}
		orphan.TaskID = in.TaskID
		r.logger.Info("resumed result generation from journal after restart",
			"hook", in.HookName, "generation", string(orphan.ID), "seq", orphan.Seq)
		return orphan, true, nil
	}

	rec, err := r.store.Freeze(in)
	if err != nil {
		return nil, false, err
	}

	retired, err := r.store.RetireOrphans(in.HookName)
	if err != nil {
		r.logger.Warn("failed to retire orphaned generations", "hook", in.HookName, "error", err.Error())
	}
	for _, id := range retired {
		r.logger.Info("superseded orphaned result generation",
			"hook", in.HookName, "generation", string(id), "byGeneration", string(rec.ID), "bySeq", rec.Seq)
	}

	return rec, false, nil
}

// MarkOrphaned flags a generation whose owner task is gone (a failed/canceled
// synchronous webhook execution), so it remains resumable by fingerprint.
func (r *Registry) MarkOrphaned(rec *GenerationRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.store.MarkOrphaned(rec.ID)
}

// CheckFence reports whether rec is still the newest generation of its hook.
// A false result means a newer generation exists and rec must stop committing.
func (r *Registry) CheckFence(rec *GenerationRecord) bool {
	return r.store.LatestSeq(rec.HookName) == rec.Seq
}

// Save persists a record update.
func (r *Registry) Save(rec *GenerationRecord) error { return r.store.Save(rec) }

// Succeed archives a fully committed generation.
func (r *Registry) Succeed(rec *GenerationRecord) error { return r.store.Succeed(rec.ID) }

// FailTerminal records a queryable dead letter and removes the generation.
func (r *Registry) FailTerminal(rec *GenerationRecord, failure FailureRecord) error {
	failure.GenerationID = rec.ID
	failure.Seq = rec.Seq
	if failure.HookName == "" {
		failure.HookName = rec.HookName
	}
	if failure.TaskID == "" {
		failure.TaskID = rec.TaskID
	}
	if failure.QueueName == "" {
		failure.QueueName = rec.QueueName
	}
	if failure.BindingType == "" {
		failure.BindingType = rec.BindingType
	}
	return r.store.FailTerminal(rec, failure)
}

// Supersede retires a generation fenced off by a newer one.
func (r *Registry) Supersede(rec *GenerationRecord, reason string) error {
	r.logger.Info("superseded result generation",
		"hook", rec.HookName, "generation", string(rec.ID), "seq", rec.Seq, "reason", reason)
	return r.store.MarkSuperseded(rec, reason)
}

// ListFailures / GetFailure expose dead-letter queries.
func (r *Registry) ListFailures() ([]*FailureRecord, error) { return r.store.ListFailures() }

func (r *Registry) GetFailure(id GenerationID) (*FailureRecord, error) {
	return r.store.GetFailure(id)
}

// Store exposes the underlying journal store (used during wiring and tests).
func (r *Registry) Store() Store { return r.store }
