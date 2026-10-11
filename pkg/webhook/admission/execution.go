package admission

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/deckhouse/deckhouse/pkg/log"
	v1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ErrReviewTimeout is wrapped by admission handlers when an AdmissionReview is
// canceled (the API server closed the connection, a same-UID retry superseded
// the execution or the process is shutting down) before the hook produced a
// verdict. Callers may use errors.Is to render an identifiable timeout
// response instead of a generic failure.
var ErrReviewTimeout = errors.New("admission review canceled or timed out")

// verdictRetention limits how long a finalized verdict is kept for a same-UID
// AdmissionReview retry after the HTTP response could not be delivered. When
// no retry arrives in time the entry is evicted.
const verdictRetention = 5 * time.Minute

type reviewPhase int

const (
	phaseRunning reviewPhase = iota
	phaseFinalized
)

// reviewTracker binds every AdmissionReview UID to its request context, its
// unique execution owner and the final response phase.
//
//   - A UID that arrives again while its previous execution is still running
//     (an API server retry) supersedes the old owner: the old execution's
//     context is canceled (its hook subprocess is stopped) and its eventual
//     verdict is rejected by Commit.
//   - A UID whose verdict was finalized but not delivered (the response write
//     failed or the client had already disconnected) keeps the verdict so the
//     retry reuses the original judgment instead of running the hook twice.
type reviewTracker struct {
	mu      sync.Mutex
	reviews map[types.UID]*reviewExecution
	logger  *log.Logger
}

func newReviewTracker(logger *log.Logger) *reviewTracker {
	return &reviewTracker{
		reviews: make(map[types.UID]*reviewExecution),
		logger:  logger,
	}
}

// reviewExecution is a single owner attempt for one AdmissionReview UID.
type reviewExecution struct {
	tracker *reviewTracker
	uid     types.UID

	// ctx is canceled when the request context ends, when the execution is
	// superseded by a newer owner or when Release runs. It is the context that
	// propagates cancellation into the hook queue and subprocess.
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	current bool
	phase   reviewPhase
	verdict *v1.AdmissionResponse
	evict   *time.Timer
}

// Begin starts a new execution owner for uid and returns it. If the UID
// already has a finalized but undelivered verdict (a same-UID retry), the
// cached verdict is returned together with the existing execution and no new
// owner is created. A still-running execution for the same UID is superseded
// and its context is canceled.
//
// parent must be canceled when the HTTP request ends (client disconnect) or
// the operator shuts down.
func (t *reviewTracker) Begin(parent context.Context, uid types.UID) (*reviewExecution, *v1.AdmissionResponse) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if old, ok := t.reviews[uid]; ok {
		old.mu.Lock()
		if old.phase == phaseFinalized && old.verdict != nil {
			// Final verdict awaiting delivery: reuse it verbatim. Cancel any
			// pending retention eviction while the retry is in flight.
			cached := old.verdict
			if old.evict != nil {
				old.evict.Stop()
				old.evict = nil
			}
			old.mu.Unlock()

			t.logger.Debug("reuse finalized verdict for same-UID AdmissionReview retry")
			return old, cached
		}
		old.current = false
		cancel := old.cancel
		old.mu.Unlock()

		// A same-UID retry arrived while the previous hook was still running.
		// Only one owner may exist: cancel the old execution (this also stops
		// its subprocess) and make its late result fail at Commit.
		cancel()
		t.logger.Info("supersede stale in-flight admission review")
	}

	ctx, cancel := context.WithCancel(parent)
	e := &reviewExecution{
		tracker: t,
		uid:     uid,
		ctx:     ctx,
		cancel:  cancel,
		current: true,
		phase:   phaseRunning,
	}
	t.reviews[uid] = e

	return e, nil
}

// Context returns the execution-scoped request context.
func (e *reviewExecution) Context() context.Context {
	return e.ctx
}

// Commit enters the final response phase exactly once and only for the
// current owner. It returns false (and leaves the verdict isolated) for a
// superseded execution.
func (e *reviewExecution) Commit(resp *v1.AdmissionResponse) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.current || e.phase != phaseRunning {
		return false
	}

	e.phase = phaseFinalized
	e.verdict = resp

	return true
}

// Delivered reports that the verdict was written to the client successfully;
// the UID registration is released.
func (e *reviewExecution) Delivered() {
	e.stopEviction()

	t := e.tracker
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.reviews[e.uid] == e {
		delete(t.reviews, e.uid)
	}
}

// Preserve keeps a finalized verdict for a same-UID retry after the response
// write failed. It is a no-op for a non-finalized (superseded) execution. The
// verdict is evicted automatically after verdictRetention if no retry comes.
func (e *reviewExecution) Preserve() {
	e.mu.Lock()
	if e.phase != phaseFinalized || e.verdict == nil {
		e.mu.Unlock()
		return
	}
	if e.evict != nil {
		e.evict.Stop()
	}
	e.mu.Unlock()

	t := e.tracker
	t.mu.Lock()
	if t.reviews[e.uid] != e {
		t.reviews[e.uid] = e
	}
	t.mu.Unlock()

	timer := time.AfterFunc(verdictRetention, e.evictOnTimeout)

	e.mu.Lock()
	e.evict = timer
	e.mu.Unlock()
}

// Release ends request handling for an execution. A superseded execution does
// nothing (the newer owner owns the registration). A still-running current
// execution unregisters itself; finalized executions remain managed by
// Delivered/Preserve. The context is canceled defensively so the subprocess
// cannot outlive the handler.
func (e *reviewExecution) Release() {
	e.cancel()

	e.mu.Lock()
	current := e.current
	phase := e.phase
	e.mu.Unlock()

	if !current || phase != phaseRunning {
		return
	}

	t := e.tracker
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.reviews[e.uid] == e {
		delete(t.reviews, e.uid)
	}
}

func (e *reviewExecution) stopEviction() {
	e.mu.Lock()
	timer := e.evict
	e.evict = nil
	e.mu.Unlock()

	if timer != nil {
		timer.Stop()
	}
}

func (e *reviewExecution) evictOnTimeout() {
	t := e.tracker
	t.mu.Lock()
	defer t.mu.Unlock()

	e.mu.Lock()
	if t.reviews[e.uid] == e {
		delete(t.reviews, e.uid)
	}
	e.evict = nil
	e.mu.Unlock()
}

// timeoutResponse is the identifiable deny response used when the review is
// canceled or times out before the hook produces a verdict.
func timeoutResponse(uid types.UID) *v1.AdmissionResponse {
	return &v1.AdmissionResponse{
		UID:     uid,
		Allowed: false,
		Result: &metav1.Status{
			Code:    http.StatusGatewayTimeout,
			Reason:  metav1.StatusReasonTimeout,
			Message: "admission review was canceled or timed out before the hook produced a verdict",
		},
	}
}

// internalDeny is the deterministic deny response when an admission execution
// panics (e.g. in the task queue or hook executor). It always rejects so a
// broken execution path can never become an implicit allow.
func internalDeny(uid types.UID, message string) *v1.AdmissionResponse {
	return &v1.AdmissionResponse{
		UID:     uid,
		Allowed: false,
		Result: &metav1.Status{
			Code:    http.StatusInternalServerError,
			Reason:  metav1.StatusReasonInternalError,
			Message: message,
		},
	}
}
