// Package resultgen implements stable result generations for hook executions.
//
// Every successful hook run is "frozen" into an immutable generation record
// before any object patch, value or metric is committed. Each commit stage
// (object patches, values, metrics, queue conclusion) carries its own durable
// state, so retries — within the process or after a restart — resume only the
// unconfirmed parts instead of replaying the whole result.
package resultgen

import (
	"encoding/json"
	"time"
)

// GenerationID is the random, process-restart-stable identity of one hook
// execution result.
type GenerationID string

// Seq is a per-hook monotonically increasing generation number used to fence
// off stale generations.
type Seq int64

// GenState is the lifecycle state of a generation record.
type GenState string

const (
	// GenActive: at least one required stage is not finished yet.
	GenActive GenState = "Active"
	// GenSucceeded: every required stage committed; journal is archived.
	GenSucceeded GenState = "Succeeded"
	// GenFailedTerminal: an irreconcilable failure occurred; a dead-letter
	// FailureRecord is kept for inspection and the task is not acknowledged.
	GenFailedTerminal GenState = "FailedTerminal"
	// GenSuperseded: a newer generation for the same hook took over; uncommitted
	// parts of this generation must not be applied anymore.
	GenSuperseded GenState = "Superseded"
)

// StageName enumerates commit stages in their fixed execution order.
type StageName string

const (
	StageObjectPatches StageName = "objectPatches"
	StageValues        StageName = "values"
	StageMetrics       StageName = "metrics"
	StageQueue         StageName = "queue"
)

// OrderedStages lists stages in execution order.
var OrderedStages = []StageName{StageObjectPatches, StageValues, StageMetrics, StageQueue}

// StageState is the durable state of one commit stage.
type StageState string

const (
	StagePending    StageState = "Pending"
	StageApplying   StageState = "Applying"
	StageCommitted  StageState = "Committed"
	StageFailed     StageState = "Failed"
	StageSkipped    StageState = "Skipped"
	StageSuperseded StageState = "Superseded"
)

// Done reports whether the stage needs no further work.
func (s StageState) Done() bool {
	return s == StageCommitted || s == StageSkipped
}

// Done reports whether the stage needs no further work.
func (s *StageCommit) Done() bool {
	return s == nil || s.State.Done()
}

// OpState is the durable per-patch-operation commit state.
type OpState string

const (
	OpPending   OpState = "Pending"
	OpApplying  OpState = "Applying"
	OpConfirmed OpState = "Confirmed"
	OpIgnored   OpState = "Ignored"
	OpUncertain OpState = "Uncertain"
	OpFailed    OpState = "Failed"
)

// FrozenMetricOp is the serializable snapshot of one hook metric operation.
type FrozenMetricOp struct {
	Name    string            `json:"name"`
	Value   *float64          `json:"value,omitempty"`
	Buckets []float64         `json:"buckets,omitempty"`
	Labels  map[string]string `json:"labels,omitempty"`
	Group   string            `json:"group,omitempty"`
	Action  string            `json:"action"`
}

// OpCommit tracks the commit state of one patch operation.
type OpCommit struct {
	Index int `json:"index"`
	// Description is the human-readable object coordinate/operation summary.
	Description string `json:"description"`

	State OpState `json:"state"`

	PreResourceVersion  string `json:"preResourceVersion,omitempty"`
	PostResourceVersion string `json:"postResourceVersion,omitempty"`
	// PreImage is the JSON snapshot captured before the send; required to
	// verify non-idempotent JSONPatch operations after ambiguous responses.
	PreImage []byte `json:"preImage,omitempty"`

	Attempts  int       `json:"attempts"`
	LastError string    `json:"lastError,omitempty"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// StageCommit tracks the state of a non-patch stage.
type StageCommit struct {
	State     StageState `json:"state"`
	LastError string     `json:"lastError,omitempty"`
	UpdatedAt time.Time  `json:"updatedAt"`
}

// FailureRecord is the queryable dead-letter entry for terminally failed
// generations.
type FailureRecord struct {
	GenerationID GenerationID `json:"generationId" yaml:"generationId"`
	Seq          Seq          `json:"seq" yaml:"seq"`
	HookName     string       `json:"hookName" yaml:"hookName"`
	TaskID       string       `json:"taskId" yaml:"taskId"`
	QueueName    string       `json:"queueName,omitempty" yaml:"queueName,omitempty"`
	BindingType  string       `json:"bindingType,omitempty" yaml:"bindingType,omitempty"`

	Reason  string `json:"reason" yaml:"reason"`
	Message string `json:"message" yaml:"message"`

	OpIndex              int    `json:"opIndex,omitempty" yaml:"opIndex,omitempty"`
	Object               string `json:"object,omitempty" yaml:"object,omitempty"`
	OperationDescription string `json:"operationDescription,omitempty" yaml:"operationDescription,omitempty"`

	FailedAt time.Time `json:"failedAt" yaml:"failedAt"`
}

// GenerationRecord is the durable journal entry for one frozen hook result.
type GenerationRecord struct {
	ID    GenerationID `json:"id"`
	Seq   Seq          `json:"seq"`
	State GenState     `json:"state"`

	HookName    string `json:"hookName"`
	TaskID      string `json:"taskId"`
	QueueName   string `json:"queueName,omitempty"`
	BindingType string `json:"bindingType,omitempty"`

	BindingContextHash string `json:"bindingContextHash"`
	Fingerprint        string `json:"fingerprint"`

	FrozenAt   time.Time  `json:"frozenAt"`
	UpdatedAt  time.Time  `json:"updatedAt"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`

	// Raw hook outputs captured at freeze time. PatchBytes are stored in the
	// companion <id>.patch file; in-memory records always carry them.
	PatchBytes             []byte           `json:"-"`
	Metrics                []FrozenMetricOp `json:"metrics,omitempty"`
	AdmissionResponseJSON  json.RawMessage  `json:"admissionResponse,omitempty"`
	ConversionResponseJSON json.RawMessage  `json:"conversionResponse,omitempty"`

	Stages map[StageName]*StageCommit `json:"stages"`
	Ops    []*OpCommit                `json:"ops,omitempty"`

	Failure *FailureRecord `json:"failure,omitempty"`
}

// Stage returns the stage commit, initializing it when absent.
func (r *GenerationRecord) Stage(name StageName) *StageCommit {
	if r.Stages == nil {
		r.Stages = map[StageName]*StageCommit{}
	}
	if r.Stages[name] == nil {
		r.Stages[name] = &StageCommit{State: StagePending}
	}
	return r.Stages[name]
}

// Op returns the indexed operation commit.
func (r *GenerationRecord) Op(index int) *OpCommit {
	if index >= 0 && index < len(r.Ops) {
		return r.Ops[index]
	}
	return nil
}

// FreezeInput carries everything the store needs to freeze a result before any
// commit stage starts.
type FreezeInput struct {
	HookName    string
	TaskID      string
	QueueName   string
	BindingType string
	BindingHash string
	Fingerprint string
	Queued      bool

	PatchBytes             []byte
	Metrics                []FrozenMetricOp
	AdmissionResponseJSON  []byte
	ConversionResponseJSON []byte

	// PatchOpCount is the number of parsed patch operations; used to seed
	// per-operation commit state. ParseError holds a deterministic parse
	// failure (the coordinator turns such generations into dead letters).
	PatchOpCount int
	ParseError   string
}
