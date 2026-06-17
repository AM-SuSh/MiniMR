package mr

import "time"

// DecisionType categorizes scheduler events shown on the dashboard.
type DecisionType string

const (
	DecisionAssign          DecisionType = "assign"
	DecisionComplete        DecisionType = "complete"
	DecisionFail            DecisionType = "fail"
	DecisionSpeculative     DecisionType = "speculative"
	DecisionTimeout         DecisionType = "timeout"
	DecisionWorkerTimeout   DecisionType = "worker_timeout"
	DecisionBlacklist       DecisionType = "blacklist"
	DecisionReduceSlowStart DecisionType = "reduce_slow_start"
	DecisionReduceReady     DecisionType = "reduce_ready"
	DecisionJobFailed       DecisionType = "job_failed"
	DecisionJobCompleted    DecisionType = "job_completed"
)

// DecisionEvent is one schedulable action or inference surfaced to the UI.
type DecisionEvent struct {
	Time      time.Time    `json:"time"`
	Type      DecisionType `json:"type"`
	Message   string       `json:"message"`
	WorkerID  string       `json:"worker_id,omitempty"`
	TaskType  string       `json:"task_type,omitempty"`
	TaskID    int          `json:"task_id,omitempty"`
	AttemptID int          `json:"attempt_id,omitempty"`
}

const maxDecisionEvents = 200

func (tm *TaskManager) logDecision(ev DecisionEvent) {
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	tm.decisions = append(tm.decisions, ev)
	if len(tm.decisions) > maxDecisionEvents {
		tm.decisions = tm.decisions[len(tm.decisions)-maxDecisionEvents:]
	}
}

// RecentDecisions returns a copy of the latest decision events.
func (tm *TaskManager) RecentDecisions() []DecisionEvent {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	out := make([]DecisionEvent, len(tm.decisions))
	copy(out, tm.decisions)
	return out
}
