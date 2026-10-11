package admission

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/deckhouse/deckhouse/pkg/log"
	v1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func verdict(allowed bool, msg string) *v1.AdmissionResponse {
	return &v1.AdmissionResponse{
		UID:     "uid-1",
		Allowed: allowed,
		Result:  &metav1.Status{Message: msg},
	}
}

func TestReviewTracker_CommitIsSingleUseAndOwnerBound(t *testing.T) {
	tr := newReviewTracker(log.NewNop())

	e, cached := tr.Begin(context.Background(), "uid-1")
	require.Nil(t, cached)

	require.True(t, e.Commit(verdict(true, "allow")))

	// The final phase is entered exactly once.
	assert.False(t, e.Commit(verdict(false, "late deny")))
	assert.True(t, e.verdict.Allowed)
}

func TestReviewTracker_SupersedeCancelsOldOwner(t *testing.T) {
	tr := newReviewTracker(log.NewNop())

	old, cached := tr.Begin(context.Background(), "uid-1")
	require.Nil(t, cached)

	newExec, cached := tr.Begin(context.Background(), "uid-1")
	require.Nil(t, cached, "a retry while the hook is still running must not produce a cached verdict")
	require.NotSame(t, old, newExec)

	// The stale owner's context is canceled (queue and subprocess unwind) and
	// its late verdict can no longer be committed.
	assert.ErrorIs(t, old.ctx.Err(), context.Canceled)
	assert.False(t, old.Commit(verdict(true, "stale allow")))

	require.True(t, newExec.Commit(verdict(false, "fresh deny")))

	// Releasing the stale owner must not touch the current registration.
	old.Release()
	tr.mu.Lock()
	assert.Same(t, newExec, tr.reviews["uid-1"])
	tr.mu.Unlock()

	newExec.Delivered()
	tr.mu.Lock()
	assert.Empty(t, tr.reviews)
	tr.mu.Unlock()
}

func TestReviewTracker_PreserveVerdictForSameUIDRetry(t *testing.T) {
	tr := newReviewTracker(log.NewNop())

	e, _ := tr.Begin(context.Background(), "uid-1")
	want := verdict(true, "original")
	require.True(t, e.Commit(want))

	// Response write failed: keep the original judgment for a retry.
	e.Preserve()
	tr.mu.Lock()
	assert.Len(t, tr.reviews, 1)
	tr.mu.Unlock()

	// Same UID arrives again: hook is not re-run, the cached verdict is reused.
	reused, cached := tr.Begin(context.Background(), "uid-1")
	require.NotNil(t, cached)
	assert.Same(t, e, reused)
	assert.Equal(t, want, cached)

	reused.Delivered()
	tr.mu.Lock()
	assert.Empty(t, tr.reviews)
	tr.mu.Unlock()
}

func TestReviewTracker_PreserveThenFailedDeliveryAgainKeepsVerdict(t *testing.T) {
	tr := newReviewTracker(log.NewNop())

	e, _ := tr.Begin(context.Background(), "uid-1")
	want := verdict(false, "original deny")
	require.True(t, e.Commit(want))
	e.Preserve()

	reused, cached := tr.Begin(context.Background(), "uid-1")
	require.NotNil(t, cached)

	// Retry delivery also fails — verdict must remain available for the next
	// same-UID retry.
	reused.Preserve()

	again, cached := tr.Begin(context.Background(), "uid-1")
	require.NotNil(t, cached)
	assert.Equal(t, want, cached)
	again.Delivered()

	tr.mu.Lock()
	assert.Empty(t, tr.reviews)
	tr.mu.Unlock()
}

func TestReviewTracker_ReleaseRunningUnregisters(t *testing.T) {
	tr := newReviewTracker(log.NewNop())

	e, _ := tr.Begin(context.Background(), "uid-1")
	e.Release()

	tr.mu.Lock()
	assert.Empty(t, tr.reviews)
	tr.mu.Unlock()
	assert.ErrorIs(t, e.ctx.Err(), context.Canceled)
}

func TestReviewTracker_DifferentUIDsDoNotInterfere(t *testing.T) {
	tr := newReviewTracker(log.NewNop())

	a, _ := tr.Begin(context.Background(), "uid-a")
	b, _ := tr.Begin(context.Background(), "uid-b")

	assert.NotSame(t, a, b)
	assert.NoError(t, a.ctx.Err())
	assert.NoError(t, b.ctx.Err())

	require.True(t, a.Commit(verdict(true, "a")))
	require.True(t, b.Commit(verdict(false, "b")))
	a.Delivered()

	tr.mu.Lock()
	assert.NotContains(t, tr.reviews, types.UID("uid-a"))
	assert.Contains(t, tr.reviews, types.UID("uid-b"))
	tr.mu.Unlock()
}

func TestTimeoutResponse_Identifiable(t *testing.T) {
	resp := timeoutResponse("uid-1")

	assert.False(t, resp.Allowed)
	require.NotNil(t, resp.Result)
	assert.Equal(t, int32(504), resp.Result.Code)
	assert.Equal(t, metav1.StatusReasonTimeout, resp.Result.Reason)
}

func TestInternalDeny_Deterministic(t *testing.T) {
	resp := internalDeny("uid-1", "boom")

	assert.False(t, resp.Allowed)
	require.NotNil(t, resp.Result)
	assert.Equal(t, int32(500), resp.Result.Code)
	assert.Contains(t, resp.Result.Message, "boom")
}
