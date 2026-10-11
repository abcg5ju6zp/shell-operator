package shell_operator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/deckhouse/deckhouse/pkg/log"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/flant/shell-operator/pkg/debug"
	"github.com/flant/shell-operator/pkg/hook/resultgen"
)

func operatorWithDeadLetter(t *testing.T, want int) (*ShellOperator, []*resultgen.FailureRecord) {
	t.Helper()
	op := newBareShellOperator(context.Background(), WithLogger(log.NewNop()))

	store, err := resultgen.NewFileStore(filepath.Join(t.TempDir(), "rg"), log.NewNop())
	require.NoError(t, err)
	registry := resultgen.NewRegistry(store, log.NewNop())

	records := make([]*resultgen.FailureRecord, 0, want)
	for i := 0; i < want; i++ {
		rec, err := store.Freeze(resultgen.FreezeInput{
			HookName:  "hooks/test.sh",
			TaskID:    "task",
			QueueName: "main",
			Queued:    true,
		})
		require.NoError(t, err)
		failure := resultgen.FailureRecord{
			Reason:               "Conflict",
			Message:              "object content differs from the patch intent",
			Object:               "v1/ConfigMap/default/cm",
			OperationDescription: "Filter object v1/ConfigMap/default/cm",
		}
		require.NoError(t, registry.FailTerminal(rec, failure))
		fr, err := registry.GetFailure(rec.ID)
		require.NoError(t, err)
		records = append(records, fr)
	}

	op.ResultCommitter = resultgen.NewCoordinator(registry, nil, nil, nil, log.NewNop())
	return op, records
}

func Test_DebugGenerationRoutes(t *testing.T) {
	op, records := operatorWithDeadLetter(t, 1)
	dbgSrv := debug.NewServer("/debug", "", "", log.NewNop())
	op.RegisterDebugGenerationRoutes(dbgSrv)

	get := func(path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rr := httptest.NewRecorder()
		dbgSrv.Router.ServeHTTP(rr, req)
		return rr
	}

	t.Run("json list", func(t *testing.T) {
		rr := get("/generation/failures.json")
		require.Equal(t, http.StatusOK, rr.Code)
		var got []*resultgen.FailureRecord
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &got))
		require.Len(t, got, 1)
		require.Equal(t, "Conflict", got[0].Reason)
		require.Equal(t, "v1/ConfigMap/default/cm", got[0].Object)
		require.Equal(t, "hooks/test.sh", got[0].HookName)
	})

	t.Run("yaml list", func(t *testing.T) {
		rr := get("/generation/failures.yaml")
		require.Equal(t, http.StatusOK, rr.Code)
		body := rr.Body.String()
		require.Contains(t, body, "reason: Conflict")
		require.Contains(t, body, "hookName: hooks/test.sh")
		// Must be a plain YAML sequence, not a re-encoded block scalar string.
		require.NotContains(t, body, "|-")
		require.True(t, strings.HasPrefix(body, "- "), "expected YAML sequence, got %q", body)

		var got []*resultgen.FailureRecord
		require.NoError(t, yaml.Unmarshal(rr.Body.Bytes(), &got))
		require.Len(t, got, 1)
		require.Equal(t, "hooks/test.sh", got[0].HookName)
		require.Equal(t, "Conflict", got[0].Reason)
	})

	t.Run("text list", func(t *testing.T) {
		rr := get("/generation/failures.text")
		require.Equal(t, http.StatusOK, rr.Code)
		require.Contains(t, rr.Body.String(), "reason=Conflict")
		require.Contains(t, rr.Body.String(), "hook=hooks/test.sh")
	})

	t.Run("json get by id", func(t *testing.T) {
		rr := get("/generation/get.json?id=" + string(records[0].GenerationID))
		require.Equal(t, http.StatusOK, rr.Code)
		var got resultgen.FailureRecord
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &got))
		require.Equal(t, records[0].GenerationID, got.GenerationID)
	})

	t.Run("path-param get by id in all formats", func(t *testing.T) {
		// Spec-shaped URL: /generation/get/<id>.{json|yaml|text}.
		id := string(records[0].GenerationID)
		rr := get("/generation/get/" + id + ".json")
		require.Equal(t, http.StatusOK, rr.Code)
		var got resultgen.FailureRecord
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &got))
		require.Equal(t, records[0].GenerationID, got.GenerationID)

		rr = get("/generation/get/" + id + ".yaml")
		require.Equal(t, http.StatusOK, rr.Code)
		require.Contains(t, rr.Body.String(), "generationId: "+id)

		rr = get("/generation/get/" + id + ".text")
		require.Equal(t, http.StatusOK, rr.Code)
		require.Contains(t, rr.Body.String(), "generation="+id)
	})

	t.Run("path-param unknown id is not found", func(t *testing.T) {
		rr := get("/generation/get/does-not-exist.json")
		require.Equal(t, http.StatusNotFound, rr.Code)
	})

	t.Run("get without id is bad request", func(t *testing.T) {
		rr := get("/generation/get.json")
		require.Equal(t, http.StatusBadRequest, rr.Code)
	})

	t.Run("get unknown id is not found", func(t *testing.T) {
		rr := get("/generation/get.json?id=does-not-exist")
		require.Equal(t, http.StatusNotFound, rr.Code)
	})
}

func Test_DebugGenerationRoutes_NoCommitter(t *testing.T) {
	op := newBareShellOperator(context.Background(), WithLogger(log.NewNop()))
	dbgSrv := debug.NewServer("/debug", "", "", log.NewNop())
	op.RegisterDebugGenerationRoutes(dbgSrv)

	req := httptest.NewRequest(http.MethodGet, "/generation/failures.json", nil)
	rr := httptest.NewRecorder()
	dbgSrv.Router.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Contains(t, rr.Body.String(), "[]")
}
