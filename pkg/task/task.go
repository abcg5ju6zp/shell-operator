package task

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/uuid/v5"

	pkg "github.com/flant/shell-operator/pkg"
	utils "github.com/flant/shell-operator/pkg/utils/labels"
)

type TaskType string

type Task interface {
	GetId() string
	GetType() TaskType
	IncrementFailureCount()
	GetFailureMessage() string
	UpdateFailureMessage(msg string)
	GetFailureCount() int
	GetLogLabels() map[string]string
	GetQueueName() string
	GetQueuedAt() time.Time
	WithQueuedAt(time.Time) Task
	GetMetadata() interface{}
	UpdateMetadata(interface{})
	GetDescription() string
	GetProp(key string) interface{}
	SetProp(key string, value interface{})
	IsProcessing() bool
	SetProcessing(bool)
	DeepCopyWithNewUUID() Task
	GetCompactionID() string

	// BirthEpoch stamps the task with the incarnation of the queue it was
	// born in. Compaction never merges tasks from different epochs, so a
	// compacted task restored after a restart keeps representing only the
	// contexts it held before the restart.
	GetBirthEpoch() int64
	SetBirthEpoch(epoch int64)

	// Recoverability. A non-recoverable task (e.g. an admission webhook task
	// whose response can only be delivered to the live caller) must be marked
	// explicitly at enqueue time; the queue excludes it from checkpoints and
	// delivers a definitive failure via FailNonRecoverable when it cannot be
	// completed before shutdown instead of silently dropping it.
	IsRecoverable() bool
	MarkNonRecoverable(reason string)
	NonRecoverableReason() string
	FailNonRecoverable(err error) bool
	NonRecoverableDone() <-chan struct{}
	NonRecoverableErr() error
}

type BaseTask struct {
	Id             string
	Type           TaskType
	LogLabels      map[string]string
	FailureCount   int // Failed executions count
	FailureMessage string
	QueueName      string
	QueuedAt       time.Time

	Props map[string]interface{}

	lock     sync.RWMutex
	Metadata interface{}

	compactionID string

	processing atomic.Bool

	// BirthEpoch is the incarnation of the queue in which the task was first
	// enqueued. Zero means "not stamped yet"; the queue stamps it on enqueue.
	BirthEpoch int64

	// Recoverability state. nonRecoverable tasks are excluded from checkpoints.
	nrMu     sync.Mutex
	nrOnce   sync.Once
	nrDone   chan struct{}
	nrErr    error
	nrReason string
}

func NewTask(taskType TaskType) *BaseTask {
	taskId := uuid.Must(uuid.NewV4()).String()
	return &BaseTask{
		Id:           taskId,
		FailureCount: 0,
		Type:         taskType,
		LogLabels:    map[string]string{pkg.LogKeyTaskDotID: taskId},
		Props:        make(map[string]interface{}),
		compactionID: taskId,
	}
}

func (t *BaseTask) deepCopy() *BaseTask {
	t.lock.RLock()
	defer t.lock.RUnlock()

	newTask := &BaseTask{
		Id:             t.Id,
		Type:           t.Type,
		FailureCount:   t.FailureCount,
		FailureMessage: t.FailureMessage,
		QueueName:      t.QueueName,
		QueuedAt:       t.QueuedAt,
		Metadata:       t.Metadata,
		BirthEpoch:     t.BirthEpoch,
	}

	// Deep copy LogLabels
	newTask.LogLabels = make(map[string]string)
	for k, v := range t.LogLabels {
		newTask.LogLabels[k] = v
	}

	// Deep copy Props
	newTask.Props = make(map[string]interface{})
	for k, v := range t.Props {
		newTask.Props[k] = v
	}

	// Copy atomic bool value
	newTask.processing.Store(t.processing.Load())

	return newTask
}

func (t *BaseTask) DeepCopyWithNewUUID() Task {
	newTask := t.deepCopy()
	newTask.Id = uuid.Must(uuid.NewV4()).String()
	newTask.LogLabels[pkg.LogKeyTaskDotID] = newTask.Id

	// A copy of a non-recoverable task is its own, non-recoverable promise and
	// therefore gets its own failure notification channel.
	if reason := t.NonRecoverableReason(); !t.IsRecoverable() {
		newTask.MarkNonRecoverable(reason)
	}
	return newTask
}

func (t *BaseTask) GetCompactionID() string {
	return t.compactionID
}

func (t *BaseTask) WithLogLabels(labels map[string]string) *BaseTask {
	t.LogLabels = utils.MergeLabels(t.LogLabels, labels)
	return t
}

func (t *BaseTask) WithQueueName(name string) *BaseTask {
	t.QueueName = name
	return t
}

func (t *BaseTask) WithMetadata(metadata interface{}) *BaseTask {
	t.lock.Lock()
	t.Metadata = metadata
	t.lock.Unlock()
	return t
}

func (t *BaseTask) WithCompactionID(id string) *BaseTask {
	t.lock.Lock()
	t.compactionID = id
	t.lock.Unlock()
	return t
}

func (t *BaseTask) GetId() string {
	return t.Id
}

func (t *BaseTask) GetType() TaskType {
	return t.Type
}

func (t *BaseTask) GetLogLabels() map[string]string {
	return t.LogLabels
}

func (t *BaseTask) GetQueueName() string {
	return t.QueueName
}

func (t *BaseTask) GetQueuedAt() time.Time {
	return t.QueuedAt
}

func (t *BaseTask) WithQueuedAt(queuedAt time.Time) Task {
	t.QueuedAt = queuedAt
	return t
}

func (t *BaseTask) GetMetadata() interface{} {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.Metadata
}

func (t *BaseTask) UpdateMetadata(meta interface{}) {
	t.lock.Lock()
	t.Metadata = meta
	t.lock.Unlock()
}

func (t *BaseTask) GetProp(key string) interface{} {
	return t.Props[key]
}

func (t *BaseTask) SetProp(key string, value interface{}) {
	t.Props[key] = value
}

func (t *BaseTask) GetFailureCount() int {
	return t.FailureCount
}

func (t *BaseTask) IncrementFailureCount() {
	t.FailureCount++
}

func (t *BaseTask) UpdateFailureMessage(msg string) {
	t.FailureMessage = msg
}

func (t *BaseTask) GetFailureMessage() string {
	return t.FailureMessage
}

type MetadataDescriptionGetter interface {
	GetDescription() string
}

func (t *BaseTask) GetDescription() string {
	metaDescription := ""
	t.lock.RLock()
	if descriptor, ok := t.Metadata.(MetadataDescriptionGetter); ok {
		metaDescription = ":" + descriptor.GetDescription()
	}
	t.lock.RUnlock()
	failDescription := ""
	if t.FailureCount > 0 {
		failDescription = fmt.Sprintf(":failures %d", t.FailureCount)
		if t.FailureMessage != "" {
			failDescription += ":" + t.FailureMessage
		}
	}

	return fmt.Sprintf("%s:%s%s%s", t.GetType(), t.GetQueueName(), metaDescription, failDescription)
}

func (t *BaseTask) SetProcessing(val bool) {
	t.processing.Store(val)
}

func (t *BaseTask) IsProcessing() bool {
	return t.processing.Load()
}

func (t *BaseTask) GetBirthEpoch() int64 {
	return t.BirthEpoch
}

func (t *BaseTask) SetBirthEpoch(epoch int64) {
	t.BirthEpoch = epoch
}

// IsRecoverable reports whether the task is allowed to be persisted into a
// queue checkpoint. All tasks are recoverable until explicitly marked.
func (t *BaseTask) IsRecoverable() bool {
	t.nrMu.Lock()
	defer t.nrMu.Unlock()
	return t.nrDone == nil
}

// MarkNonRecoverable explicitly declares that the task cannot be persisted and
// replayed (e.g. it carries a synchronous admission response). It must be
// called at task construction/enqueue time. Repeated calls update the reason.
func (t *BaseTask) MarkNonRecoverable(reason string) {
	t.nrMu.Lock()
	defer t.nrMu.Unlock()
	if t.nrDone == nil {
		t.nrDone = make(chan struct{})
	}
	t.nrReason = reason
}

func (t *BaseTask) NonRecoverableReason() string {
	t.nrMu.Lock()
	defer t.nrMu.Unlock()
	return t.nrReason
}

// FailNonRecoverable delivers the definitive failure promised to the caller
// of a non-recoverable task. It is idempotent and returns true only for the
// first invocation. It is a no-op for recoverable tasks.
func (t *BaseTask) FailNonRecoverable(err error) bool {
	t.nrMu.Lock()
	done := t.nrDone
	alreadyFailed := t.nrErr != nil
	if done != nil && err == nil {
		err = &NonRecoverableTaskError{Reason: t.nrReason}
	}
	if done != nil && !alreadyFailed {
		t.nrErr = err
	}
	t.nrMu.Unlock()

	if done == nil || alreadyFailed {
		return false
	}
	t.nrOnce.Do(func() { close(done) })
	return true
}

// NonRecoverableDone returns a channel closed once the definitive failure for
// a non-recoverable task is known. It returns nil for recoverable tasks.
func (t *BaseTask) NonRecoverableDone() <-chan struct{} {
	t.nrMu.Lock()
	defer t.nrMu.Unlock()
	return t.nrDone
}

// NonRecoverableErr returns the definitive failure after NonRecoverableDone is
// closed.
func (t *BaseTask) NonRecoverableErr() error {
	t.nrMu.Lock()
	defer t.nrMu.Unlock()
	return t.nrErr
}
