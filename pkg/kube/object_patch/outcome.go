package object_patch

import (
	"context"
	stderrors "errors"
	"io"
	"net"
	"syscall"

	sdkpkg "github.com/deckhouse/module-sdk/pkg"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// OutcomeState is the disposition of a single patch operation after one
// ExecuteOperationCtx attempt. It lets the result-generation coordinator
// decide whether an operation may be retried blindly, must be re-verified
// against the live object, or has reached a terminal failure.
type OutcomeState string

const (
	// OutcomeApplied means the API call succeeded (or the operation was a
	// tolerated no-op, e.g. nil operation, unchanged filter result).
	OutcomeApplied OutcomeState = "Applied"
	// OutcomeIgnored means the desired end state is already satisfied without
	// a write: object missing with IgnoreMissingObject, AlreadyExists with
	// ignoreIfExists, Delete of an absent object.
	OutcomeIgnored OutcomeState = "Ignored"
	// OutcomeUncertain means the request may or may not have reached the
	// apiserver (timeout, connection reset, 5xx) or the response implies a
	// possible prior application (AlreadyExists). The operation must be
	// re-verified against the live object before it is sent again.
	OutcomeUncertain OutcomeState = "Uncertain"
	// OutcomeConflict is a deterministic, irreconcilable concurrency conflict
	// (409 after retries). It is terminal: the coordinator records a queryable
	// failure instead of replaying the operation.
	OutcomeConflict OutcomeState = "Conflict"
	// OutcomeRejected is a deterministic apiserver rejection (400/422). The
	// request was validly delivered and will not succeed on retry unchanged.
	OutcomeRejected OutcomeState = "Rejected"
	// OutcomeRetryable groups all other errors (404 without ignore, 429,
	// discovery failures, refused connections...). The request can safely be
	// re-sent as-is after backoff.
	OutcomeRetryable OutcomeState = "Retryable"
)

// Terminal reports whether the state must not be followed by another send
// attempt (the failure is recorded as terminal by the coordinator).
func (s OutcomeState) Terminal() bool {
	return s == OutcomeConflict || s == OutcomeRejected
}

// OperationOutcome carries the result of one operation attempt together with
// object-version evidence used for uncertain-response verification.
type OperationOutcome struct {
	State       OutcomeState
	Description string
	Err         error

	// Attempts counts API send attempts consumed to produce this outcome
	// (relevant for RetryOnConflict wrappers).
	Attempts int

	// PreResourceVersion is the object RV captured before sending when the
	// execution path performed a Get anyway (create-or-update, filter).
	PreResourceVersion string
	// PostResourceVersion is the RV of the object returned by the write call.
	PostResourceVersion string

	// Sent indicates that a mutating request definitely left the client.
	// A false value with an error means retrying blindly is safe.
	Sent bool

	// PreImage is the object content read internally immediately before the
	// mutating write (filter and create-or-update paths perform a Get anyway).
	// It is never serialized in the outcome; the coordinator snapshots it into
	// its journal for ambiguous-response verification.
	PreImage *unstructured.Unstructured
}

func newOutcome(state OutcomeState, op sdkpkg.PatchCollectorOperation, err error) *OperationOutcome {
	desc := ""
	if op != nil {
		desc = op.Description()
	}
	return &OperationOutcome{State: state, Description: desc, Err: err}
}

// classifyRequestError maps an API error to an OutcomeState.
func classifyRequestError(err error) OutcomeState {
	if err == nil {
		return OutcomeApplied
	}

	// Deterministic apiserver answers.
	if apierrors.IsConflict(err) {
		return OutcomeConflict
	}
	if apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) {
		return OutcomeRejected
	}

	// Ambiguous server-side states: the write may already have been applied.
	if apierrors.IsTimeout(err) ||
		apierrors.IsServerTimeout(err) ||
		apierrors.IsServiceUnavailable(err) ||
		apierrors.IsInternalError(err) {
		return OutcomeUncertain
	}

	// 429 means the request was rate limited and not applied: safe to resend.
	if apierrors.IsTooManyRequests(err) {
		return OutcomeRetryable
	}

	// Local context endings: the fate of an in-flight request is unknown.
	if stderrors.Is(err, context.Canceled) || stderrors.Is(err, context.DeadlineExceeded) {
		return OutcomeUncertain
	}

	var opErr *net.OpError
	if stderrors.As(err, &opErr) {
		switch {
		case opErr.Timeout():
			return OutcomeUncertain
		case stderrors.Is(opErr.Err, syscall.ECONNRESET),
			stderrors.Is(opErr.Err, syscall.EPIPE),
			stderrors.Is(opErr.Err, syscall.ETIMEDOUT):
			return OutcomeUncertain
		default:
			// Connection refused / no route: request never reached apiserver.
			return OutcomeRetryable
		}
	}

	if stderrors.Is(err, io.EOF) || stderrors.Is(err, io.ErrUnexpectedEOF) {
		return OutcomeUncertain
	}

	return OutcomeRetryable
}
