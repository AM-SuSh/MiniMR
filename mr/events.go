package mr

import (
	"fmt"
	"time"
)

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

const maxDecisionEvents = 2000

func trimDecisionEvents(events []DecisionEvent) []DecisionEvent {
	if len(events) <= maxDecisionEvents {
		return events
	}
	return events[len(events)-maxDecisionEvents:]
}

func appendJobDecision(job *Job, ev DecisionEvent) {
	if job == nil {
		return
	}
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	job.Decisions = trimDecisionEvents(append(job.Decisions, ev))
	if job.jobLog != nil {
		job.jobLog.writeDecision(ev)
	}
}

// LogJobCompletedDecision appends a terminal banner for a finished job.
func LogJobCompletedDecision(job *Job, at time.Time) {
	if job == nil || job.State != JobCompleted {
		return
	}
	for i := len(job.Decisions) - 1; i >= 0; i-- {
		if job.Decisions[i].Type == DecisionJobCompleted {
			return
		}
	}
	mapDone, mapTotal := countCompleted(job.MapTasks)
	reduceDone, reduceTotal := countCompleted(job.ReduceTasks)
	appendJobDecision(job, DecisionEvent{
		Time: at,
		Type: DecisionJobCompleted,
		Message: fmt.Sprintf(
			"════ 作业完成 ════ Map %d/%d · Reduce %d/%d · Job %s",
			mapDone, mapTotal, reduceDone, reduceTotal, job.ID,
		),
	})
}

func (tm *TaskManager) logDecision(ev DecisionEvent) {
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	tm.decisions = trimDecisionEvents(append(tm.decisions, ev))
	if tm.job != nil {
		appendJobDecision(tm.job, ev)
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
