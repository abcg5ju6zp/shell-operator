package queue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	pkg "github.com/flant/shell-operator/pkg"
	"github.com/flant/shell-operator/pkg/task"
	json "github.com/flant/shell-operator/pkg/utils/json"
)

// Recoverable checkpoints.
//
// A checkpoint is a complete, content-addressed snapshot of one queue: the
// ordered task list (claimed tasks keep their original position), the current
// incarnation epoch and a bootstrapped marker. Every successful Save creates a
// new "generation"; a failed Save never touches the previously saved file, so
// recovery always starts from the last complete generation.
//
// Writes are atomic (temp file + fsync + rename + fsync of the directory) and
// the payload carries a SHA-256 checksum. A malformed or tampered file is not
// used: it is quarantined next to itself and Load returns ErrCorruptCheckpoint
// so the operator refuses recovery while keeping diagnostics.

const checkpointFormatVersion = 1

// ErrCorruptCheckpoint is returned when a checkpoint file cannot be parsed or
// fails checksum verification. The damaged file is preserved (quarantined)
// next to its original location.
var ErrCorruptCheckpoint = errors.New("corrupt task queue checkpoint")

// ErrCheckpointingDisabled is returned by persistence entry points when the
// queue was constructed without a checkpoint store.
var ErrCheckpointingDisabled = errors.New("task queue checkpointing is disabled")

// QueueCheckpoint is the durable snapshot of one TaskQueue.
type QueueCheckpoint struct {
	FormatVersion int    `json:"formatVersion"`
	QueueName     string `json:"queueName"`
	Generation    int64  `json:"generation"`
	Epoch         int64  `json:"epoch"`
	Bootstrapped  bool   `json:"bootstrapped"`
	// SeededKeys lists initial-seeding keys (e.g. "onStartup:hook-x") already
	// issued, so a restart replays old tasks but still seeds hooks added after
	// the previous start instead of blindly skipping all seeding.
	SeededKeys []string         `json:"seededKeys,omitempty"`
	UpdatedAt  string           `json:"updatedAt"`
	Tasks      []TaskCheckpoint `json:"tasks"`
	Checksum   string           `json:"checksum"`
}

// TaskCheckpoint is the durable representation of one task. Props are never
// persisted: they hold live, non-serializable execution artifacts (such as
// admission responses). Non-recoverable tasks never reach this struct.
type TaskCheckpoint struct {
	Id             string            `json:"id"`
	Type           string            `json:"type"`
	QueueName      string            `json:"queueName"`
	QueuedAt       string            `json:"queuedAt"`
	FailureCount   int               `json:"failureCount"`
	FailureMessage string            `json:"failureMessage,omitempty"`
	CompactionID   string            `json:"compactionId"`
	BirthEpoch     int64             `json:"birthEpoch"`
	Processing     bool              `json:"processing"`
	LogLabels      map[string]string `json:"logLabels,omitempty"`
	MetadataType   string            `json:"metadataType,omitempty"`
	Metadata       json.RawMessage   `json:"metadata,omitempty"`
}

// CheckpointStore persists and loads complete queue snapshots.
type CheckpointStore interface {
	// Save atomically replaces the stored snapshot with a new generation.
	Save(ctx context.Context, cp *QueueCheckpoint) error
	// Load returns the last complete snapshot. A wrapped os.ErrNotExist is
	// returned when no snapshot has been written yet.
	Load(ctx context.Context) (*QueueCheckpoint, error)
	// Close releases underlying resources.
	Close() error
}

// taskToCheckpoint converts a live task into its durable form.
func taskToCheckpoint(t task.Task) (TaskCheckpoint, error) {
	dto := TaskCheckpoint{
		Id:             t.GetId(),
		Type:           string(t.GetType()),
		QueueName:      t.GetQueueName(),
		FailureCount:   t.GetFailureCount(),
		FailureMessage: t.GetFailureMessage(),
		CompactionID:   t.GetCompactionID(),
		BirthEpoch:     t.GetBirthEpoch(),
		Processing:     t.IsProcessing(),
		LogLabels:      t.GetLogLabels(),
	}
	if !t.GetQueuedAt().IsZero() {
		dto.QueuedAt = t.GetQueuedAt().UTC().Format(time.RFC3339Nano)
	}

	if meta := t.GetMetadata(); meta != nil {
		typeName, raw, err := task.MarshalMetadataForCheckpoint(meta)
		if err != nil {
			return dto, err
		}
		dto.MetadataType = typeName
		dto.Metadata = raw
	}

	return dto, nil
}

// toTask reconstructs a live task from a durable record. Claimed tasks are
// restored as unclaimed, so a claimed-but-unfinished task re-runs from its
// original queue position.
func (dto TaskCheckpoint) toTask() (task.Task, error) {
	bt := task.NewTask(task.TaskType(dto.Type))
	bt.Id = dto.Id
	bt.WithCompactionID(dto.CompactionID)
	bt.WithQueueName(dto.QueueName)
	bt.FailureCount = dto.FailureCount
	bt.FailureMessage = dto.FailureMessage
	bt.BirthEpoch = dto.BirthEpoch
	// processing is deliberately not restored: without a completion record the
	// task must be executed again from its position.
	bt.SetProcessing(false)

	if len(dto.LogLabels) > 0 {
		labels := make(map[string]string, len(dto.LogLabels)+1)
		for k, v := range dto.LogLabels {
			labels[k] = v
		}
		labels[pkg.LogKeyTaskDotID] = dto.Id
		bt.LogLabels = labels
	}

	if dto.QueuedAt != "" {
		if queuedAt, err := time.Parse(time.RFC3339Nano, dto.QueuedAt); err == nil {
			bt.QueuedAt = queuedAt
		}
	}

	if dto.MetadataType != "" {
		meta, err := task.UnmarshalMetadataFromCheckpoint(dto.MetadataType, dto.Metadata)
		if err != nil {
			return nil, fmt.Errorf("restore task %s: %w", dto.Id, err)
		}
		if meta != nil {
			// Factories return pointers; metadata types whose canonical
			// in-memory form is a value opt into dereferencing.
			if cv, ok := meta.(task.CheckpointMetadataValue); ok {
				meta = cv.CheckpointMetadataValue()
			}
			bt.WithMetadata(meta)
		}
	}

	return bt, nil
}

// FileCheckpointStore stores the snapshot as a single JSON file replaced
// atomically on every save.
type FileCheckpointStore struct {
	dir  string
	path string

	mu sync.Mutex
}

func NewFileCheckpointStore(dir, queueName string) (*FileCheckpointStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("checkpoint directory must not be empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create checkpoint directory %q: %w", dir, err)
	}
	return &FileCheckpointStore{
		dir:  dir,
		path: filepath.Join(dir, checkpointFileName(queueName)),
	}, nil
}

func (s *FileCheckpointStore) Path() string {
	return s.path
}

func (s *FileCheckpointStore) Save(ctx context.Context, cp *QueueCheckpoint) error {
	if cp == nil {
		return fmt.Errorf("nil checkpoint")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	cp.FormatVersion = checkpointFormatVersion
	checksum, err := checksumCheckpoint(cp)
	if err != nil {
		return fmt.Errorf("compute checkpoint checksum: %w", err)
	}
	cp.Checksum = checksum

	data, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("encode checkpoint: %w", err)
	}

	tmp, err := os.CreateTemp(s.dir, ".checkpoint-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary checkpoint: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("write checkpoint: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("fsync checkpoint: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close checkpoint: %w", err)
	}

	if err := os.Rename(tmpName, s.path); err != nil {
		cleanup()
		return fmt.Errorf("rename checkpoint: %w", err)
	}

	// fsync the directory so the rename survives a power loss.
	if dir, err := os.Open(s.dir); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}

	return nil
}

func (s *FileCheckpointStore) Load(_ context.Context) (*QueueCheckpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if err != nil {
		return nil, err // os.ErrNotExist is preserved and wrapped by os
	}

	cp := &QueueCheckpoint{}
	if err := json.Unmarshal(data, cp); err != nil {
		s.quarantine(data, fmt.Errorf("parse checkpoint: %w", err))
		return nil, fmt.Errorf("%w: %s: parse error", ErrCorruptCheckpoint, s.path)
	}

	if cp.FormatVersion != checkpointFormatVersion {
		err := fmt.Errorf("unsupported checkpoint format version %d (want %d)", cp.FormatVersion, checkpointFormatVersion)
		s.quarantine(data, err)
		return nil, fmt.Errorf("%w: %s: %v", ErrCorruptCheckpoint, s.path, err)
	}

	expected := cp.Checksum
	got, err := checksumCheckpoint(cp)
	if err != nil {
		s.quarantine(data, err)
		return nil, fmt.Errorf("%w: %s: %v", ErrCorruptCheckpoint, s.path, err)
	}
	if expected == "" || expected != got {
		err := fmt.Errorf("checksum mismatch: expected %s, got %s", expected, got)
		s.quarantine(data, err)
		return nil, fmt.Errorf("%w: %s: %v", ErrCorruptCheckpoint, s.path, err)
	}

	return cp, nil
}

func (s *FileCheckpointStore) Close() error { return nil }

// quarantine copies the damaged payload (and its checksum error) aside for
// diagnostics without destroying the original evidence.
func (s *FileCheckpointStore) quarantine(data []byte, cause error) {
	quarantinePath := fmt.Sprintf("%s.corrupt-%s", s.path, time.Now().UTC().Format("20060102T150405.000000000"))
	diag := append(append([]byte("// quarantine cause: "+cause.Error()+"\n"), data...), '\n')
	if err := os.WriteFile(quarantinePath, diag, 0o600); err != nil {
		// Best effort: leave the original file untouched for manual inspection.
		_ = err
	}
}

// checksumCheckpoint computes a stable digest over all fields except Checksum.
func checksumCheckpoint(cp *QueueCheckpoint) (string, error) {
	clone := *cp
	clone.Checksum = ""
	data, err := json.Marshal(&clone)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

var unsafeCheckpointNameChars = regexp.MustCompile(`[^A-Za-z0-9._-]`)

// checkpointFileName maps an arbitrary queue name to a safe, collision-resistant
// file name inside the checkpoint directory.
func checkpointFileName(queueName string) string {
	safe := unsafeCheckpointNameChars.ReplaceAllString(queueName, "_")
	if safe != queueName {
		sum := sha256.Sum256([]byte(queueName))
		safe = safe + "-" + hex.EncodeToString(sum[:])[:12]
	}
	return safe + ".checkpoint.json"
}
