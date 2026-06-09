package mr

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

// TaskManager handles task state transitions, timeouts, and worker health.
type TaskManager struct {
	mu                sync.Mutex
	job               *Job
	workers           map[string]*WorkerInfo
	taskTimeout       time.Duration
	reduceTaskTimeout time.Duration
	workerTimeout     time.Duration

	completedMapTimes    []time.Duration
	completedReduceTimes []time.Duration
}

func NewTaskManager(job *Job) *TaskManager {
	reduceTimeout := DefaultTaskTimeout
	if job.Config.ReduceSlowStart < 1.0 {
		reduceTimeout = DefaultReduceTaskTimeout
	}
	return &TaskManager{
		job:               job,
		workers:           make(map[string]*WorkerInfo),
		taskTimeout:       DefaultTaskTimeout,
		reduceTaskTimeout: reduceTimeout,
		workerTimeout:     DefaultWorkerTimeout,
	}
}

// RegisterWorker creates or refreshes a worker entry.
// An existing entry (including Blacklisted flag) is preserved.
func (tm *TaskManager) RegisterWorker(id string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if w, ok := tm.workers[id]; ok {
		w.LastHeartbeat = time.Now()
		return
	}
	tm.workers[id] = &WorkerInfo{
		ID:            id,
		LastHeartbeat: time.Now(),
		CurrentTask:   -1,
	}
}

func (tm *TaskManager) Heartbeat(workerID string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if w, ok := tm.workers[workerID]; ok {
		w.LastHeartbeat = time.Now()
	}
}

// AssignTask transitions a task to InProgress and bumps its AttemptID.
func (tm *TaskManager) AssignTask(task *Task, workerID string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	task.State = InProgress
	task.WorkerID = workerID
	task.StartTime = time.Now()
	task.AttemptID++
	if w, ok := tm.workers[workerID]; ok {
		w.CurrentTask = task.ID
		w.CurrentType = task.Type
	}
}

// CompleteTask marks a task as Completed or Failed, tracks worker reliability,
// and records completion time for speculative execution decisions.
func (tm *TaskManager) CompleteTask(task *Task, success bool, workerID string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if w, ok := tm.workers[workerID]; ok {
		w.CurrentTask = -1
		if success {
			w.FailureCount = 0
		} else {
			w.FailureCount++
			if w.FailureCount >= DefaultMaxWorkerFailures && !w.Blacklisted {
				w.Blacklisted = true
				log.Printf("worker %s blacklisted after %d consecutive failures", w.ID, w.FailureCount)
				tm.resetWorkerTasksLocked(w.ID)
			}
		}
	}

	if success {
		task.State = Completed
		elapsed := time.Since(task.StartTime)
		if task.Type == MapTask {
			tm.completedMapTimes = append(tm.completedMapTimes, elapsed)
		} else if task.Type == ReduceTask {
			tm.completedReduceTimes = append(tm.completedReduceTimes, elapsed)
		}
	} else {
		task.State = Failed
	}
}

func (tm *TaskManager) ResetTask(task *Task) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	task.State = Idle
	task.WorkerID = ""
	task.StartTime = time.Time{}
}

// IsWorkerBlacklisted returns true if the worker has been flagged as unreliable.
func (tm *TaskManager) IsWorkerBlacklisted(workerID string) bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if w, ok := tm.workers[workerID]; ok {
		return w.Blacklisted
	}
	return false
}

// ---------------------------------------------------------------------------
// Monitor loop — timeout / failure / speculative
// ---------------------------------------------------------------------------

// StartMonitor runs periodic timeout checks until the job completes.
func (tm *TaskManager) StartMonitor(done <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			tm.checkTimeouts()
		}
	}
}

func (tm *TaskManager) checkTimeouts() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.job.State == JobFailed {
		return
	}

	now := time.Now()

	var deadWorkers []string
	for _, w := range tm.workers {
		if !w.Blacklisted && now.Sub(w.LastHeartbeat) > tm.workerTimeout {
			log.Printf("worker %s heartbeat timed out, resetting its tasks", w.ID)
			tm.resetWorkerTasksLocked(w.ID)
			deadWorkers = append(deadWorkers, w.ID)
		}
	}
	for _, id := range deadWorkers {
		delete(tm.workers, id)
	}

	mapMedian := medianDuration(tm.completedMapTimes)
	reduceMedian := medianDuration(tm.completedReduceTimes)

	for _, task := range tm.job.MapTasks {
		tm.checkSingleTaskLocked(task, now, mapMedian, len(tm.completedMapTimes))
	}
	for _, task := range tm.job.ReduceTasks {
		tm.checkSingleTaskLocked(task, now, reduceMedian, len(tm.completedReduceTimes))
	}
}

func (tm *TaskManager) checkSingleTaskLocked(task *Task, now time.Time, median time.Duration, completedCount int) {
	switch task.State {
	case InProgress:
		if task.StartTime.IsZero() {
			return
		}
		timeout := tm.taskTimeout
		if task.Type == ReduceTask {
			timeout = tm.reduceTaskTimeout
		}
		elapsed := now.Sub(task.StartTime)

		if elapsed > timeout {
			task.RetryCount++
			log.Printf("task %s-%d hard timeout (attempt %d, retry %d/%d)",
				task.Type, task.ID, task.AttemptID, task.RetryCount, DefaultMaxRetries)
			if task.RetryCount > DefaultMaxRetries {
				tm.failJobLocked(fmt.Sprintf(
					"task %s-%d exceeded max retries (%d)", task.Type, task.ID, DefaultMaxRetries))
				return
			}
			task.State = Idle
			task.WorkerID = ""
			task.StartTime = time.Time{}
			return
		}

		if completedCount >= SpeculativeMinCompleted && median > 0 {
			threshold := time.Duration(float64(median) * SpeculativeMultiplier)
			if elapsed > threshold {
				log.Printf("speculative re-exec: task %s-%d running %v (median %v, threshold %v)",
					task.Type, task.ID,
					elapsed.Round(time.Millisecond),
					median.Round(time.Millisecond),
					threshold.Round(time.Millisecond))
				task.State = Idle
				task.WorkerID = ""
				task.StartTime = time.Time{}
			}
		}

	case Failed:
		task.RetryCount++
		log.Printf("task %s-%d failed (retry %d/%d)",
			task.Type, task.ID, task.RetryCount, DefaultMaxRetries)
		if task.RetryCount > DefaultMaxRetries {
			tm.failJobLocked(fmt.Sprintf(
				"task %s-%d exceeded max retries (%d)", task.Type, task.ID, DefaultMaxRetries))
			return
		}
		task.State = Idle
		task.WorkerID = ""
		task.StartTime = time.Time{}
	}
}

func (tm *TaskManager) failJobLocked(reason string) {
	if tm.job.State == JobFailed {
		return
	}
	tm.job.State = JobFailed
	tm.job.Error = reason
	tm.job.CompletedAt = time.Now()
	log.Printf("JOB FAILED: %s", reason)
}

// resetWorkerTasksLocked resets all InProgress tasks belonging to the given
// worker.  It does NOT remove the worker entry so that blacklist state survives.
func (tm *TaskManager) resetWorkerTasksLocked(workerID string) {
	for _, task := range tm.job.MapTasks {
		if task.WorkerID == workerID && task.State == InProgress {
			task.RetryCount++
			task.State = Idle
			task.WorkerID = ""
			task.StartTime = time.Time{}
		}
	}
	for _, task := range tm.job.ReduceTasks {
		if task.WorkerID == workerID && task.State == InProgress {
			task.RetryCount++
			task.State = Idle
			task.WorkerID = ""
			task.StartTime = time.Time{}
		}
	}
}

// ---------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------

func (tm *TaskManager) IsJobComplete() bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for _, t := range tm.job.MapTasks {
		if t.State != Completed {
			return false
		}
	}
	for _, t := range tm.job.ReduceTasks {
		if t.State != Completed {
			return false
		}
	}
	return true
}

func (tm *TaskManager) MarkMapDoneForReduce(mapID, reduceID int) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if mapID >= 0 && mapID < len(tm.job.MapDoneForReduce[reduceID]) {
		tm.job.MapDoneForReduce[reduceID][mapID] = true
	}
}

func (tm *TaskManager) IsReduceReady(reduceID int) bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if reduceID < 0 || reduceID >= len(tm.job.MapDoneForReduce) {
		return false
	}
	for _, done := range tm.job.MapDoneForReduce[reduceID] {
		if !done {
			return false
		}
	}
	return true
}

// CanScheduleReduce returns true when the global map completion ratio meets the
// ReduceSlowStart threshold, allowing reduce tasks to begin their shuffle phase
// before all maps finish.
func (tm *TaskManager) CanScheduleReduce() bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	totalMaps := len(tm.job.MapTasks)
	if totalMaps == 0 {
		return false
	}

	completedMaps := 0
	for _, t := range tm.job.MapTasks {
		if t.State == Completed {
			completedMaps++
		}
	}

	threshold := tm.job.Config.ReduceSlowStart
	if threshold <= 0 || threshold > 1.0 {
		threshold = 1.0
	}

	return float64(completedMaps)/float64(totalMaps) >= threshold
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

func medianDuration(ds []time.Duration) time.Duration {
	n := len(ds)
	if n == 0 {
		return 0
	}
	sorted := make([]time.Duration, n)
	copy(sorted, ds)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	if n%2 == 0 {
		return (sorted[n/2-1] + sorted[n/2]) / 2
	}
	return sorted[n/2]
}
