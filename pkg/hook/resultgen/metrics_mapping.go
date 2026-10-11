package resultgen

import (
	metricsop "github.com/deckhouse/deckhouse/pkg/metrics-storage/operation"
)

// Metric actions are persisted by their stable library-defined name so a
// frozen generation does not depend on the integer values of the external
// enum remaining identical across library upgrades.
var metricActionByName = map[string]metricsop.MetricAction{
	"CounterAdd":       metricsop.ActionCounterAdd,
	"GaugeAdd":         metricsop.ActionGaugeAdd,
	"GaugeSet":         metricsop.ActionGaugeSet,
	"HistogramObserve": metricsop.ActionHistogramObserve,
	"ExpireMetrics":    metricsop.ActionExpireMetrics,
}

// FrozenMetricOpsFromExternal converts external metric operations to the
// serializable frozen form.
func FrozenMetricOpsFromExternal(ops []metricsop.MetricOperation) []FrozenMetricOp {
	if len(ops) == 0 {
		return nil
	}
	out := make([]FrozenMetricOp, 0, len(ops))
	for _, op := range ops {
		out = append(out, FrozenMetricOp{
			Name:    op.Name,
			Value:   op.Value,
			Buckets: op.Buckets,
			Labels:  op.Labels,
			Group:   op.Group,
			Action:  op.Action.String(),
		})
	}
	return out
}

// ToExternal converts the frozen form back to external metric operations.
// Unknown action names map to the zero value so the external validation
// rejects them deterministically instead of applying a wrong action.
func (m FrozenMetricOp) ToExternal() metricsop.MetricOperation {
	return metricsop.MetricOperation{
		Name:    m.Name,
		Value:   m.Value,
		Buckets: m.Buckets,
		Labels:  m.Labels,
		Group:   m.Group,
		Action:  metricActionByName[m.Action],
	}
}
