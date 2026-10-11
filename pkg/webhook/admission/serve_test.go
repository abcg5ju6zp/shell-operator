package admission

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/deckhouse/deckhouse/pkg/log"
	v1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func reviewRequest(t *testing.T, uid string) *http.Request {
	t.Helper()

	review := v1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{Kind: "AdmissionReview", APIVersion: "admission.k8s.io/v1"},
		Request: &v1.AdmissionRequest{
			UID:       types.UID(uid),
			Name:      "pod",
			Namespace: "default",
		},
	}
	raw, err := json.Marshal(review)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/hooks/my-hook", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func decodeReview(t *testing.T, body *bytes.Buffer) v1.AdmissionReview {
	t.Helper()
	var got v1.AdmissionReview
	require.NoError(t, json.Unmarshal(body.Bytes(), &got))
	return got
}

// The success path (allow) must keep its historical shape: v1 review, UID
// echoed, request stripped.
func TestServeReviewRequest_AllowUnchanged(t *testing.T) {
	h := NewWebhookHandler(log.NewNop())
	h.Handler = func(_ context.Context, _ Event) (*Response, error) {
		return &Response{Allowed: true, Warnings: []string{"careful"}}, nil
	}

	rec := httptest.NewRecorder()
	h.serveReviewRequest(rec, reviewRequest(t, "uid-allow"))

	require.Equal(t, http.StatusOK, rec.Code)
	got := decodeReview(t, rec.Body)
	require.NotNil(t, got.Response)
	assert.True(t, got.Response.Allowed)
	assert.Equal(t, types.UID("uid-allow"), got.Response.UID)
	assert.Equal(t, []string{"careful"}, got.Response.Warnings)
	assert.Nil(t, got.Request)
	assert.Equal(t, "admission.k8s.io/v1", got.APIVersion)
}

// Hook explicit deny stays a 403 status deny with the hook message.
func TestServeReviewRequest_HookDenyUnchanged(t *testing.T) {
	h := NewWebhookHandler(log.NewNop())
	h.Handler = func(_ context.Context, _ Event) (*Response, error) {
		return &Response{Allowed: false, Message: "nope"}, nil
	}

	rec := httptest.NewRecorder()
	h.serveReviewRequest(rec, reviewRequest(t, "uid-deny"))

	got := decodeReview(t, rec.Body)
	require.NotNil(t, got.Response)
	assert.False(t, got.Response.Allowed)
	require.NotNil(t, got.Response.Result)
	assert.Equal(t, int32(http.StatusForbidden), got.Response.Result.Code)
	assert.Equal(t, "nope", got.Response.Result.Message)
}

// A canceled review without a hook verdict becomes the identifiable timeout
// response (504 / StatusReasonTimeout).
func TestServeReviewRequest_CanceledContextProducesTimeoutVerdict(t *testing.T) {
	h := NewWebhookHandler(log.NewNop())
	h.Handler = func(ctx context.Context, _ Event) (*Response, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	req := reviewRequest(t, "uid-timeout")
	ctx, cancel := context.WithCancel(req.Context())
	req = req.WithContext(ctx)
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	rec := httptest.NewRecorder()
	h.serveReviewRequest(rec, req)

	got := decodeReview(t, rec.Body)
	require.NotNil(t, got.Response)
	assert.False(t, got.Response.Allowed)
	require.NotNil(t, got.Response.Result)
	assert.Equal(t, int32(http.StatusGatewayTimeout), got.Response.Result.Code)
	assert.Equal(t, metav1.StatusReasonTimeout, got.Response.Result.Reason)
}

// A panic in the queue or hook executor yields a deterministic deny and the
// UID registration is released for subsequent requests.
func TestServeReviewRequest_PanicProducesDeterministicDeny(t *testing.T) {
	h := NewWebhookHandler(log.NewNop())
	h.Handler = func(_ context.Context, event Event) (*Response, error) {
		if event.Request.UID == "uid-panic" {
			panic("boom")
		}
		return &Response{Allowed: true}, nil
	}

	rec := httptest.NewRecorder()
	h.serveReviewRequest(rec, reviewRequest(t, "uid-panic"))

	got := decodeReview(t, rec.Body)
	require.NotNil(t, got.Response)
	assert.False(t, got.Response.Allowed)
	require.NotNil(t, got.Response.Result)
	assert.Equal(t, int32(http.StatusInternalServerError), got.Response.Result.Code)
	assert.Equal(t, metav1.StatusReasonInternalError, got.Response.Result.Reason)
	assert.Contains(t, got.Response.Result.Message, "boom")

	// Registration released: the next UID is processed normally.
	rec2 := httptest.NewRecorder()
	h.serveReviewRequest(rec2, reviewRequest(t, "uid-after-panic"))
	got2 := decodeReview(t, rec2.Body)
	require.NotNil(t, got2.Response)
	assert.True(t, got2.Response.Allowed)
}

// failingWriter makes every response write fail, simulating a client that
// disconnected right before the verdict was serialized.
type failingWriter struct {
	header http.Header
	code   int
}

func (w *failingWriter) Header() http.Header {
	if w.header == nil {
		w.header = http.Header{}
	}
	return w.header
}

func (w *failingWriter) WriteHeader(code int) { w.code = code }

func (w *failingWriter) Write(_ []byte) (int, error) {
	return 0, errors.New("connection broken")
}

// When the hook produced a verdict but the response write failed, the same UID
// retry must reuse the original verdict instead of running the hook again.
func TestServeReviewRequest_WriteFailurePreservesVerdictForRetry(t *testing.T) {
	h := NewWebhookHandler(log.NewNop())
	var calls atomic.Int32
	h.Handler = func(_ context.Context, _ Event) (*Response, error) {
		calls.Add(1)
		return &Response{Allowed: true, Warnings: []string{"original-verdict"}}, nil
	}

	fw := &failingWriter{}
	h.serveReviewRequest(fw, reviewRequest(t, "uid-retry"))
	require.Equal(t, int32(1), calls.Load())

	// Verdict registration is preserved awaiting the retry.
	h.reviews.mu.Lock()
	assert.Len(t, h.reviews.reviews, 1)
	h.reviews.mu.Unlock()

	rec := httptest.NewRecorder()
	h.serveReviewRequest(rec, reviewRequest(t, "uid-retry"))

	require.Equal(t, int32(1), calls.Load(), "the hook must not be executed again for the same UID")
	got := decodeReview(t, rec.Body)
	require.NotNil(t, got.Response)
	assert.True(t, got.Response.Allowed)
	assert.Equal(t, []string{"original-verdict"}, got.Response.Warnings)

	h.reviews.mu.Lock()
	assert.Empty(t, h.reviews.reviews)
	h.reviews.mu.Unlock()
}

// A same-UID retry while the first execution is still running supersedes it:
// the stale execution must not write anything, only the current owner's
// verdict is delivered.
func TestServeReviewRequest_SameUIDRetrySupersedesStaleExecution(t *testing.T) {
	h := NewWebhookHandler(log.NewNop())
	var calls atomic.Int32
	firstStarted := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})

	h.Handler = func(ctx context.Context, event Event) (*Response, error) {
		n := calls.Add(1)
		if n == 1 {
			firstStarted <- struct{}{}
			<-ctx.Done()
			<-releaseFirst
			return nil, ctx.Err()
		}
		return &Response{Allowed: true, Warnings: []string{"fresh"}}, nil
	}

	rec1 := httptest.NewRecorder()
	done1 := make(chan struct{})
	go func() {
		h.serveReviewRequest(rec1, reviewRequest(t, "uid-race"))
		close(done1)
	}()

	<-firstStarted

	// Retry with the same UID: cancels the stale execution and owns the UID.
	rec2 := httptest.NewRecorder()
	h.serveReviewRequest(rec2, reviewRequest(t, "uid-race"))

	got2 := decodeReview(t, rec2.Body)
	require.NotNil(t, got2.Response)
	assert.True(t, got2.Response.Allowed)
	assert.Equal(t, []string{"fresh"}, got2.Response.Warnings)

	// Let the stale execution finish unwinding.
	close(releaseFirst)
	select {
	case <-done1:
	case <-time.After(5 * time.Second):
		t.Fatal("stale execution did not finish")
	}

	// The superseded execution wrote nothing: no contradictory verdict.
	assert.Empty(t, rec1.Body.Bytes(), "stale execution must not write a response")
}

// Bad JSON / missing request keep returning 400 and never register an
// execution.
func TestServeReviewRequest_MalformedBodiesUnchanged(t *testing.T) {
	h := NewWebhookHandler(log.NewNop())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/hooks/x", bytes.NewReader([]byte("{not-json")))
	req.Header.Set("Content-Type", "application/json")
	h.serveReviewRequest(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/hooks/x", bytes.NewReader([]byte(`{"request":null}`)))
	req2.Header.Set("Content-Type", "application/json")
	h.serveReviewRequest(rec2, req2)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)

	h.reviews.mu.Lock()
	assert.Empty(t, h.reviews.reviews)
	h.reviews.mu.Unlock()
}
