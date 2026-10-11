package queue

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metricsstorage "github.com/deckhouse/deckhouse/pkg/metrics-storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	bindingcontext "github.com/flant/shell-operator/pkg/hook/binding_context"
	"github.com/flant/shell-operator/pkg/hook/task_metadata"
	"github.com/flant/shell-operator/pkg/hook/types"
	kemtypes "github.com/flant/shell-operator/pkg/kube_events_manager/types"
	"github.com/flant/shell-operator/pkg/task"
)

func newTestCheckpointStore(t *testing.T, queueName string) (*FileCheckpointStore, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := NewFileCheckpointStore(dir, queueName)
	require.NoError(t, err)
	return store, dir
}

func TestCheckpointStore_FileDoesNotExist(t *testing.T) {
	store, _ := newTestCheckpointStore(t, "main")

	_, err := store.Load(context.Background())
	require.Error(t, err)
	assert.True(t, errors.Is(err, os.ErrNotExist), "expected os.ErrNotExist, got %v", err)
}

func TestCheckpointStore_RoundTrip(t *testing.T) {
	store, _ := newTestCheckpointStore(t, "main")

	cp := &QueueCheckpoint{
		QueueName:    "main",
		Generation:   7,
		Epoch:        3,
		Bootstrapped: true,
		UpdatedAt:    time.Now().UTC().Format(time.RFC3339Nano),
		Tasks: []TaskCheckpoint{
			{
				Id:           "t1",
				Type:         "HookRun",
				QueueName:    "main",
				CompactionID: "hook-x",
				BirthEpoch:   3,
				Processing:   true,
				MetadataType: "HookMetadata",
				Metadata:     []byte(`{"HookName":"hook-x"}`),
			},
		},
	}

	require.NoError(t, store.Save(context.Background(), cp))

	loaded, err := store.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(7), loaded.Generation)
	assert.Equal(t, int64(3), loaded.Epoch)
	assert.True(t, loaded.Bootstrapped)
	require.Len(t, loaded.Tasks, 1)
	assert.Equal(t, "t1", loaded.Tasks[0].Id)
	assert.True(t, loaded.Tasks[0].Processing)
	assert.NotEmpty(t, loaded.Checksum)
}

func TestCheckpointStore_CorruptJSONIsQuarantined(t *testing.T) {
	store, dir := newTestCheckpointStore(t, "main")

	require.NoError(t, os.WriteFile(store.Path(), []byte("{not-json"), 0o600))

	_, err := store.Load(context.Background())
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrCorruptCheckpoint), "expected ErrCorruptCheckpoint, got %v", err)

	// The damaged payload is preserved for diagnostics.
	matches, err := filepath.Glob(filepath.Join(dir, "*.corrupt-*"))
	require.NoError(t, err)
	require.Len(t, matches, 1)
	content, err := os.ReadFile(matches[0])
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(string(content), "// quarantine cause:"))
}

func TestCheckpointStore_ChecksumTamperIsRejected(t *testing.T) {
	store, _ := newTestCheckpointStore(t, "main")

	cp := &QueueCheckpoint{
		QueueName:  "main",
		Generation: 1,
		Epoch:      1,
		Tasks:      []TaskCheckpoint{{Id: "t1", Type: "HookRun"}},
	}
	require.NoError(t, store.Save(context.Background(), cp))

	// Tamper with the payload while keeping valid JSON.
	raw, err := os.ReadFile(store.Path())
	require.NoError(t, err)
	tampered := strings.Replace(string(raw), `"t1"`, `"t1-tampered"`, 1)
	require.NotEqual(t, string(raw), tampered)
	require.NoError(t, os.WriteFile(store.Path(), []byte(tampered), 0o600))

	_, err = store.Load(context.Background())
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrCorruptCheckpoint))
}

func TestCheckpointStore_UnsupportedVersionRejected(t *testing.T) {
	store, _ := newTestCheckpointStore(t, "main")

	cp := &QueueCheckpoint{QueueName: "main", Generation: 1, Epoch: 1}
	require.NoError(t, store.Save(context.Background(), cp))

	raw, err := os.ReadFile(store.Path())
	require.NoError(t, err)
	corrupt := strings.Replace(string(raw), `"formatVersion":1`, `"formatVersion":999`, 1)
	require.NoError(t, os.WriteFile(store.Path(), []byte(corrupt), 0o600))

	_, err = store.Load(context.Background())
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrCorruptCheckpoint))
}

func TestCheckpointFileName_SafeAndStable(t *testing.T) {
	assert.Equal(t, "main.checkpoint.json", checkpointFileName("main"))
	n1 := checkpointFileName("kube/events")
	n2 := checkpointFileName("kube/events")
	n3 := checkpointFileName("kube!events")
	assert.Equal(t, n1, n2, "name must be stable")
	assert.NotEqual(t, n1, n3, "distinct names must not collide")
	assert.True(t, strings.HasPrefix(n1, "kube_events-"))
}

func TestTaskCheckpoint_HookMetadataRoundTrip(t *testing.T) {
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion("v1")
	obj.SetKind("ConfigMap")
	obj.SetName("cm1")
	obj.SetNamespace("default")

	bc := bindingcontext.BindingContext{
		Binding:    "monitor",
		Type:       kemtypes.TypeEvent,
		WatchEvent: kemtypes.WatchEventAdded,
		Objects: []kemtypes.ObjectAndFilterResult{
			{
				Object: obj,
				FilterResult: map[string]any{
					"foo": "bar",
				},
			},
		},
	}
	bc.Metadata.BindingType = types.OnKubernetesEvent
	bc.Metadata.Group = "g1"
	bc.Metadata.JqFilter = ".metadata"

	original := task.NewTask(task_metadata.HookRun).
		WithMetadata(task_metadata.HookMetadata{
			HookName:       "hook-x",
			Binding:        "monitor",
			Group:          "g1",
			BindingType:    types.OnKubernetesEvent,
			BindingContext: []bindingcontext.BindingContext{bc},
			AllowFailure:   true,
			MonitorIDs:     []string{"m1"},
		}).
		WithQueueName("main").
		WithCompactionID("hook-x")
	original.SetBirthEpoch(5)
	original.IncrementFailureCount()

	dto, err := taskToCheckpoint(original)
	require.NoError(t, err)
	assert.Equal(t, "HookMetadata", dto.MetadataType)
	assert.Equal(t, int64(5), dto.BirthEpoch)
	assert.Contains(t, string(dto.Metadata), "ConfigMap")

	restored, err := dto.toTask()
	require.NoError(t, err)

	assert.Equal(t, original.GetId(), restored.GetId())
	assert.Equal(t, original.GetCompactionID(), restored.GetCompactionID())
	assert.Equal(t, int64(5), restored.GetBirthEpoch())
	assert.False(t, restored.IsProcessing(), "claimed flag must be reset on restore")
	assert.Equal(t, 1, restored.GetFailureCount())

	meta, ok := task_metadata.HookMetadataAccessor(restored)
	require.True(t, ok)
	assert.Equal(t, "hook-x", meta.HookName)
	assert.Equal(t, "g1", meta.Group)
	assert.True(t, meta.AllowFailure)
	require.Len(t, meta.MonitorIDs, 1)
	require.Len(t, meta.BindingContext, 1)

	rbc := meta.BindingContext[0]
	assert.Equal(t, kemtypes.WatchEventAdded, rbc.WatchEvent)
	require.Len(t, rbc.Objects, 1)
	require.NotNil(t, rbc.Objects[0].Object)
	assert.Equal(t, "ConfigMap", rbc.Objects[0].Object.GetKind())
	assert.Equal(t, "cm1", rbc.Objects[0].Object.GetName())
	assert.Equal(t, "bar", rbc.Objects[0].FilterResult.(map[string]any)["foo"])
}

func TestTaskCheckpoint_NonRecoverableTaskExcludedBySnapshot(t *testing.T) {
	q := NewTasksQueue("test",
		metricsstorage.NewMetricStorage(metricsstorage.WithNewRegistry()),
		WithContext(context.Background()),
		WithCompactableTypes(task_metadata.HookRun))
	require.NoError(t, q.InitPersistence(context.Background()))

	recoverable := task.NewTask("Service")
	nonRecoverable := task.NewTask("Service")
	nonRecoverable.MarkNonRecoverable("admission response")

	q.AddLast(recoverable, nonRecoverable)
	require.Equal(t, 2, q.Length())

	cp, err := q.buildCheckpoint()
	require.NoError(t, err)
	require.Len(t, cp.Tasks, 1)
	assert.Equal(t, recoverable.GetId(), cp.Tasks[0].Id)
}
