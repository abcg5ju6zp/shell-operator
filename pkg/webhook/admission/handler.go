package admission

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"

	"github.com/deckhouse/deckhouse/pkg/log"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	v1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pkg "github.com/flant/shell-operator/pkg"
	json "github.com/flant/shell-operator/pkg/utils/json"
	structuredLogger "github.com/flant/shell-operator/pkg/utils/structured-logger"
)

type EventHandlerFn func(ctx context.Context, event Event) (*Response, error)

type WebhookHandler struct {
	Router  chi.Router
	Handler EventHandlerFn

	// BaseContext is canceled when the operator shuts down. Request handling
	// derives a context from both BaseContext and the HTTP request context, so
	// cancellation from either side propagates into the queue and the hook
	// subprocess. Defaults to context.Background().
	BaseContext context.Context

	// reviews binds every AdmissionReview UID to its owner execution and its
	// final response phase.
	reviews *reviewTracker

	Logger *log.Logger
}

func NewWebhookHandler(logger *log.Logger) *WebhookHandler {
	rtr := chi.NewRouter()
	h := &WebhookHandler{
		Router:      rtr,
		Logger:      logger,
		BaseContext: context.Background(),
		reviews:     newReviewTracker(logger),
	}

	rtr.Group(func(r chi.Router) {
		r.Get("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
			writer.WriteHeader(http.StatusOK)
		})
	})

	rtr.Group(func(r chi.Router) {
		r.Use(structuredLogger.NewStructuredLogger(logger.Named("admissionWebhook"), "admissionWebhook"))
		r.Use(middleware.Recoverer)
		r.Use(middleware.AllowContentType("application/json"))
		r.Post("/*", h.serveReviewRequest)
	})

	return h
}

func (h *WebhookHandler) serveReviewRequest(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var admissionReview v1.AdmissionReview
	err := json.NewDecoder(r.Body).Decode(&admissionReview)
	if err != nil {
		h.Logger.Error("failed to decode AdmissionReview body to json", log.Err(err))
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("Invalid JSON payload"))
		return
	}

	if admissionReview.Request == nil {
		h.Logger.Error("AdmissionReview request is nil")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("Missing parameters: request"))
		return
	}

	uid := admissionReview.Request.UID
	logger := h.Logger.With(
		slog.String(pkg.LogKeyRequest, string(uid)),
		slog.String(pkg.LogKeyName, admissionReview.Request.Name),
		slog.String(pkg.LogKeyNamespace, admissionReview.Request.Namespace),
		slog.String(pkg.LogKeyKind, admissionReview.Request.Kind.Kind),
	)

	// Bind the UID to both the operator lifetime and the HTTP connection:
	// shutdown and client disconnect both cancel the execution context.
	baseCtx := h.BaseContext
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	ctx, cancel := chainContext(baseCtx, r.Context())
	defer cancel()

	execution, cachedVerdict := h.reviews.Begin(ctx, uid)
	defer execution.Release()

	var admissionResponse *v1.AdmissionResponse
	if cachedVerdict != nil {
		logger.Info("reuse finalized verdict for same-UID AdmissionReview retry")
		admissionResponse = cachedVerdict
	} else {
		// Run against the owner context: it is canceled both when the request
		// ends and when this execution gets superseded by a same-UID retry.
		admissionResponse = h.runReview(execution.Context(), r.URL.Path, admissionReview.Request, logger)

		// Only the current owner may enter the final response phase, exactly
		// once. A superseded execution leaves both the verdict and the
		// connection to the newer owner.
		if !execution.Commit(admissionResponse) {
			logger.Warn("admission review was superseded by a same-UID retry, late verdict is discarded")
			return
		}
	}

	admissionReview.Response = admissionResponse
	admissionReview.Response.UID = uid
	admissionReview.Request = nil

	w.Header().Set("Content-type", "application/json")
	if encodeErr := json.NewEncoder(w).Encode(admissionReview); encodeErr != nil {
		// The hook had a verdict but the response could not be delivered
		// (client gone, broken connection). Preserve the original judgment for
		// the same-UID retry instead of running the hook again.
		logger.Error("error encoding AdmissionReview, verdict is preserved for same-UID retry", log.Err(encodeErr))
		execution.Preserve()
		return
	}

	execution.Delivered()
}

// runReview executes the event handler and maps every outcome to a
// deterministic AdmissionResponse:
//   - hook verdict (allow or explicit deny) is returned unchanged;
//   - a canceled/timed-out review without a verdict becomes the identifiable
//     timeoutResponse;
//   - a panic in the queue or executor becomes a deterministic internal deny;
//   - any other error keeps the historical 500 deny behavior.
func (h *WebhookHandler) runReview(ctx context.Context, path string, request *v1.AdmissionRequest, logger *log.Logger) *v1.AdmissionResponse {
	var panicVal any
	admissionResponse, err := func() (*v1.AdmissionResponse, error) {
		defer func() {
			panicVal = recover()
		}()
		return h.handleReviewRequest(ctx, path, request)
	}()

	if panicVal != nil {
		logger.Error("panic during admission review, denying the request",
			slog.Any(pkg.LogKeyError, panicVal),
			slog.String(pkg.LogKeyStack, string(debug.Stack())))
		return internalDeny(request.UID, fmt.Sprintf("admission review panic: %v", panicVal))
	}

	if err == nil {
		return admissionResponse
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrReviewTimeout) {
		logger.Warn("admission review canceled or timed out before the hook produced a verdict", log.Err(err))
		return timeoutResponse(request.UID)
	}

	logger.Error("validation failed", log.Err(err))
	return errored(err)
}

func (h *WebhookHandler) handleReviewRequest(ctx context.Context, path string, request *v1.AdmissionRequest) (*v1.AdmissionResponse, error) {
	configurationID, webhookID := detectConfigurationAndWebhook(path)
	h.Logger.Info("Got AdmissionReview request",
		slog.String(pkg.LogKeyConfigurationID, configurationID),
		slog.String(pkg.LogKeyWebhookID, webhookID),
		slog.String(pkg.LogKeyKind, request.Kind.Kind),
		slog.String(pkg.LogKeyName, request.Name),
		slog.String(pkg.LogKeyNamespace, request.Namespace),
	)

	if h.Handler == nil {
		return nil, fmt.Errorf("AdmissionReview handler is not defined")
	}

	event := Event{
		WebhookId:       webhookID,
		ConfigurationId: configurationID,
		Request:         request,
	}

	admissionResponse, err := h.Handler(ctx, event)
	if err != nil {
		return nil, fmt.Errorf("handle AdmissionReview: %w", err)
	}

	response := &v1.AdmissionResponse{
		UID:      request.UID,
		Allowed:  admissionResponse.Allowed,
		Warnings: admissionResponse.Warnings,
		Patch:    admissionResponse.Patch,
	}

	if !admissionResponse.Allowed {
		response.Result = &metav1.Status{
			Code:    http.StatusForbidden,
			Message: admissionResponse.Message,
		}
	}

	// When allowing a request, a mutating admission webhook may optionally modify the
	// incoming object as well. This is done using the patch and patchType fields in the response.
	// The only currently supported patchType is JSONPatch. See JSON patch documentation for
	// more details. For patchType: JSONPatch, the patch field contains a base64-encoded
	// array of JSON patch operations.
	if len(admissionResponse.Patch) > 0 {
		patchType := v1.PatchTypeJSONPatch
		response.PatchType = &patchType
	}

	return response, nil
}

// chainContext returns a context that is canceled when either the base
// (operator) context or the HTTP request context is canceled. The returned
// cancel function must be called when request handling finishes; it also
// stops the forwarding goroutine.
func chainContext(base, req context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(base)

	go func() {
		select {
		case <-req.Done():
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, cancel
}

// detectConfigurationAndWebhook extracts configurationID and a webhookID from the url path.
func detectConfigurationAndWebhook(path string) (string /*configurationID*/, string /*webhookID*/) {
	var configurationID string
	parts := strings.Split(path, "/")
	webhookParts := make([]string, 0)
	for _, p := range parts {
		if p == "" {
			continue
		}
		if configurationID == "" {
			configurationID = p
			continue
		}
		webhookParts = append(webhookParts, p)
	}
	webhookID := strings.Join(webhookParts, "/")

	return configurationID, webhookID
}

func errored(err error) *v1.AdmissionResponse {
	return &v1.AdmissionResponse{
		Allowed: false,
		Result: &metav1.Status{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		},
	}
}
