package task

import (
	"fmt"
	"reflect"
	"sync"

	json "github.com/flant/shell-operator/pkg/utils/json"
)

// This file adds recoverability primitives to tasks:
//
//   - A metadata type registry allows durable queues to serialize task metadata
//     (which is stored as interface{}) and to reconstruct a concrete value on
//     recovery.
//   - A task can be marked non-recoverable. Tasks whose result is delivered
//     synchronously and cannot be replayed (e.g. admission webhook responses)
//     must be marked at enqueue time. The queue then promises a definitive
//     failure instead of silently dropping them on shutdown or crash.
//   - A birth epoch stamps each task with the queue incarnation it was created
//     in, so compaction never merges contexts restored from a checkpoint with
//     events received after restart.

// CheckpointMarshaler is implemented by metadata values that need a dedicated
// durable representation. The returned value is JSON-encoded into the
// checkpoint. Metadata without this interface is JSON-encoded as-is.
type CheckpointMarshaler interface {
	MarshalCheckpoint() (any, error)
}

// CheckpointUnmarshaler restores metadata from its durable representation.
// It is implemented on the pointer value returned by MetadataFactory.
type CheckpointUnmarshaler interface {
	UnmarshalCheckpoint(data json.RawMessage) error
}

// CheckpointMetadataValue is implemented by pointer metadata whose canonical
// in-memory representation is the value (e.g. HookMetadata is stored by value
// inside tasks). Factories return pointers for unmarshaling; the value
// returned here is what gets attached to the restored task.
type CheckpointMetadataValue interface {
	CheckpointMetadataValue() any
}

// MetadataFactory creates an empty metadata value to unmarshal a checkpoint
// into. It must return a pointer (e.g. func() any { return &MyMetadata{} }).
type MetadataFactory func() any

var (
	registryMu       sync.RWMutex
	metadataFactory  = map[string]MetadataFactory{}
	metadataTypeName = map[reflect.Type]string{}
)

// RegisterMetadataType registers a metadata type under a stable name. The name
// is written into checkpoints and used to pick the factory on recovery.
// Registering the same name twice, or registering two names for one type,
// panics: registrations are expected to happen from init() functions.
func RegisterMetadataType(name string, factory MetadataFactory) {
	if name == "" {
		panic("task: metadata type name must not be empty")
	}
	if factory == nil {
		panic("task: metadata factory must not be nil for type " + name)
	}

	prototype := factory()
	if prototype == nil {
		panic("task: metadata factory returned nil for type " + name)
	}
	t := reflect.TypeOf(prototype)
	if t.Kind() != reflect.Ptr {
		panic(fmt.Sprintf("task: metadata factory for %q must return a pointer, got %s", name, t.Kind()))
	}

	registryMu.Lock()
	defer registryMu.Unlock()

	if _, exists := metadataFactory[name]; exists {
		panic("task: metadata type already registered: " + name)
	}
	if existing, exists := metadataTypeName[t]; exists {
		panic(fmt.Sprintf("task: metadata type %s already registered as %q", t.String(), existing))
	}

	metadataFactory[name] = factory
	// Register both the pointer and the element type, because metadata is
	// usually kept as a value (e.g. HookMetadata{}, not &HookMetadata{}).
	metadataTypeName[t] = name
	metadataTypeName[t.Elem()] = name
}

// LookupMetadataTypeName returns the registered checkpoint name for the
// concrete type of v.
func LookupMetadataTypeName(v any) (string, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	name, ok := metadataTypeName[reflect.TypeOf(v)]
	return name, ok
}

// MarshalMetadataForCheckpoint serializes metadata into a tagged JSON payload.
// It returns an error when the concrete metadata type was never registered:
// such a task cannot be made durable and must be marked non-recoverable.
func MarshalMetadataForCheckpoint(v any) (typeName string, data json.RawMessage, err error) {
	if v == nil || isNilValue(v) {
		return "", nil, nil
	}

	name, ok := LookupMetadataTypeName(v)
	if !ok {
		return "", nil, fmt.Errorf("metadata type %T is not registered for checkpointing", v)
	}

	payload := v
	if m, ok := v.(CheckpointMarshaler); ok {
		payload, err = m.MarshalCheckpoint()
		if err != nil {
			return name, nil, fmt.Errorf("marshal %s metadata: %w", name, err)
		}
	}

	data, err = json.Marshal(payload)
	if err != nil {
		return name, nil, fmt.Errorf("encode %s metadata: %w", name, err)
	}
	return name, data, nil
}

// UnmarshalMetadataFromCheckpoint reconstructs metadata from a tagged payload.
// A nil/empty payload means the task carried no metadata.
func UnmarshalMetadataFromCheckpoint(typeName string, data json.RawMessage) (any, error) {
	if typeName == "" || len(data) == 0 {
		return nil, nil
	}

	registryMu.RLock()
	factory, ok := metadataFactory[typeName]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("metadata type %q is not registered, cannot restore checkpoint", typeName)
	}

	v := factory()
	if u, ok := v.(CheckpointUnmarshaler); ok {
		if err := u.UnmarshalCheckpoint(data); err != nil {
			return nil, fmt.Errorf("unmarshal %s metadata: %w", typeName, err)
		}
		return v, nil
	}

	if err := json.Unmarshal(data, v); err != nil {
		return nil, fmt.Errorf("decode %s metadata: %w", typeName, err)
	}
	return v, nil
}

func isNilValue(v any) bool {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

// ErrTaskNonRecoverable marks the semantic failure delivered for tasks that
// cannot survive a restart. Callers may wrap it with details.
type NonRecoverableTaskError struct {
	Reason string
}

func (e *NonRecoverableTaskError) Error() string {
	if e.Reason == "" {
		return "task is marked non-recoverable and was not completed before shutdown"
	}
	return "non-recoverable task is lost: " + e.Reason
}

// ErrQueueNotAccepting is returned (or delivered to non-recoverable tasks)
// after a durable queue entered shutdown or a degraded checkpoint state.
var ErrQueueNotAccepting = queueNotAcceptingError{}

type queueNotAcceptingError struct{}

func (queueNotAcceptingError) Error() string {
	return "task queue is not accepting new tasks (shutdown or checkpoint failure)"
}
