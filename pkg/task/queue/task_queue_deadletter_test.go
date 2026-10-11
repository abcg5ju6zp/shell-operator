package queue

import (
	"context"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	"github.com/flant/shell-operator/pkg/hook/task_metadata"
	"github.com/flant/shell-operator/pkg/metric"
	"github.com/flant/shell-operator/pkg/task"
)

func Test_TaskQueue_DeadLetter(t *testing.T) {
	g := NewWithT(t)

	metricStorage := metric.NewStorageMock(t)
	metricStorage.HistogramObserveMock.Optional().Set(func(string, float64, map[string]string, []float64) {})
	metricStorage.GaugeSetMock.Optional().Set(func(string, float64, map[string]string) {})
	metricStorage.CounterAddMock.Optional().Set(func(string, float64, map[string]string) {})

	var mu sync.Mutex
	executed := make([]string, 0)

	q := NewTasksQueue(
		"deadletter-test-queue",
		metricStorage,
		WithContext(context.Background()),
	)
	q.WaitLoopCheckInterval = 5 * time.Millisecond
	q.DelayOnQueueIsEmpty = 5 * time.Millisecond

	q.Handler = func(_ context.Context, tsk task.Task) TaskResult {
		mu.Lock()
		executed = append(executed, tsk.GetId())
		mu.Unlock()

		if tsk.GetId() == "BadTask" {
			// A terminal generation failure: even if a buggy hook attached
			// follow-up tasks to its result, none of them may be scheduled.
			res := TaskResult{Status: DeadLetter}
			res.AddAfterTasks(&task.BaseTask{Id: "AfterTask", Type: task_metadata.HookRun})
			res.AddHeadTasks(&task.BaseTask{Id: "HeadTask", Type: task_metadata.HookRun})
			res.AddTailTasks(&task.BaseTask{Id: "TailTask", Type: task_metadata.HookRun})
			return res
		}
		return TaskResult{Status: Success}
	}

	bad := task.BaseTask{Id: "BadTask", Type: task_metadata.HookRun}
	good := task.BaseTask{Id: "GoodTask", Type: task_metadata.HookRun}
	q.AddLast(&bad)
	q.AddLast(&good)

	go q.Start(context.Background())
	defer q.Stop()

	// The dead-lettered task leaves the queue and the task behind it proceeds
	// without waiting for a fail backoff.
	g.Eventually(func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := make([]string, len(executed))
		copy(out, executed)
		return out
	}, "2s", "5ms").Should(Equal([]string{"BadTask", "GoodTask"}))

	g.Eventually(func() int { return q.Length() }, "2s", "5ms").Should(Equal(0))

	// The dead-lettered task is gone from storage, not retried, and carries a
	// failure count; the successful task is simply done.
	g.Expect(q.Get("BadTask")).To(BeNil())
	g.Expect(bad.GetFailureCount()).To(Equal(1))
}
