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

	completedMapTimes     []time.Duration
	completedReduceTimes  []time.Duration
	decisions             []DecisionEvent
	reduceSlowStartLogged bool
}

func NewTaskManager(job *Job) *TaskManager {
	mapTimeout := DefaultTaskTimeout
	if maxMapSplitLength(job) >= LargeTaskTimeoutThreshold {
		mapTimeout = DefaultLargeTaskTimeout
	}
	reduceTimeout := DefaultTaskTimeout
	if job.Config.ReduceSlowStart < 1.0 {
		reduceTimeout = DefaultReduceTaskTimeout
	}
	return &TaskManager{
		job:               job,
		workers:           make(map[string]*WorkerInfo),
		taskTimeout:       mapTimeout,
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
	tm.job.snapshotWorkersLocked(tm.workers, time.Now(), tm.workerTimeout)
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
	tm.job.snapshotWorkersLocked(tm.workers, time.Now(), tm.workerTimeout)
	tm.logDecision(DecisionEvent{
		Type:      DecisionAssign,
		Message:   fmt.Sprintf("分配 %s-%d → %s (attempt %d)", task.Type, task.ID, workerID, task.AttemptID),
		WorkerID:  workerID,
		TaskType:  task.Type.String(),
		TaskID:    task.ID,
		AttemptID: task.AttemptID,
	})
	if task.Type == ReduceTask && !allMapTasksCompleted(tm.job) {
		tm.job.Metrics.EarlyReduceStarts++
		tm.logDecision(DecisionEvent{
			Type:      DecisionReduceSlowStart,
			Message:   fmt.Sprintf("Reduce-%d 在 Map 尚未全部完成时提前启动 Shuffle", task.ID),
			WorkerID:  workerID,
			TaskType:  task.Type.String(),
			TaskID:    task.ID,
			AttemptID: task.AttemptID,
		})
	}
}

// CompleteTask marks a task as Completed or Failed, tracks worker reliability,
// and records completion time for speculative execution decisions.
func (tm *TaskManager) CompleteTask(task *Task, success bool, workerID string, metrics TaskMetrics) {
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
				tm.job.Metrics.BlacklistedWorkers++
				log.Printf("worker %s blacklisted after %d consecutive failures", w.ID, w.FailureCount)
				tm.logDecision(DecisionEvent{
					Type:     DecisionBlacklist,
					Message:  fmt.Sprintf("Worker %s 连续失败 %d 次，已拉黑", w.ID, w.FailureCount),
					WorkerID: w.ID,
				})
				tm.resetWorkerTasksLocked(w.ID)
			}
		}
	}

	if success {
		task.State = Completed
		tm.job.Metrics.AddTask(metrics)
		elapsed := time.Since(task.StartTime)
		if task.Type == MapTask {
			tm.completedMapTimes = append(tm.completedMapTimes, elapsed)
		} else if task.Type == ReduceTask {
			tm.completedReduceTimes = append(tm.completedReduceTimes, elapsed)
		}
		tm.logDecision(DecisionEvent{
			Type:      DecisionComplete,
			Message:   fmt.Sprintf("%s-%d 完成 (耗时 %v, worker %s)", task.Type, task.ID, elapsed.Round(time.Millisecond), workerID),
			WorkerID:  workerID,
			TaskType:  task.Type.String(),
			TaskID:    task.ID,
			AttemptID: task.AttemptID,
		})
	} else {
		task.State = Failed
		tm.job.Metrics.TaskFailures++
		tm.logDecision(DecisionEvent{
			Type:      DecisionFail,
			Message:   fmt.Sprintf("%s-%d 失败 (worker %s, attempt %d)", task.Type, task.ID, workerID, task.AttemptID),
			WorkerID:  workerID,
			TaskType:  task.Type.String(),
			TaskID:    task.ID,
			AttemptID: task.AttemptID,
		})
	}
	tm.job.snapshotWorkersLocked(tm.workers, time.Now(), tm.workerTimeout)
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
			tm.mu.Lock()
			finished := tm.job.State == JobFailed || tm.job.State == JobCompleted
			tm.mu.Unlock()
			if finished {
				return
			}
		}
	}
}

func (tm *TaskManager) checkTimeouts() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.job.State == JobFailed || tm.job.State == JobCompleted {
		return
	}

	now := time.Now()

	var deadWorkers []string
	for _, w := range tm.workers {
		if !w.Blacklisted && now.Sub(w.LastHeartbeat) > tm.workerTimeout {
			log.Printf("worker %s heartbeat timed out, resetting its tasks", w.ID)
			tm.resetWorkerTasksLocked(w.ID)
			tm.job.Metrics.WorkerTimeouts++
			deadWorkers = append(deadWorkers, w.ID)
		}
	}
	for _, id := range deadWorkers {
		tm.logDecision(DecisionEvent{
			Type:     DecisionWorkerTimeout,
			Message:  fmt.Sprintf("Worker %s 心跳超时，重置其进行中任务", id),
			WorkerID: id,
		})
		delete(tm.workers, id)
	}

	tm.maybeLogReduceSlowStartLocked()

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
			tm.job.Metrics.TaskTimeouts++
			tm.job.Metrics.Retries++
			log.Printf("task %s-%d hard timeout (attempt %d, retry %d/%d)",
				task.Type, task.ID, task.AttemptID, task.RetryCount, DefaultMaxRetries)
			tm.logDecision(DecisionEvent{
				Type:      DecisionTimeout,
				Message:   fmt.Sprintf("%s-%d 硬超时 (%v > %v)，重试 %d/%d", task.Type, task.ID, elapsed.Round(time.Millisecond), timeout, task.RetryCount, DefaultMaxRetries),
				WorkerID:  task.WorkerID,
				TaskType:  task.Type.String(),
				TaskID:    task.ID,
				AttemptID: task.AttemptID,
			})
			if task.RetryCount > DefaultMaxRetries {
				tm.failJobLocked(fmt.Sprintf(
					"task %s-%d exceeded max retries (%d)", task.Type, task.ID, DefaultMaxRetries))
				return
			}
			tm.clearWorkerTaskLocked(task.WorkerID, task)
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
				tm.logDecision(DecisionEvent{
					Type:      DecisionSpeculative,
					Message:   fmt.Sprintf("推测执行：%s-%d 运行 %v 超过阈值 %v (中位数 %v × %.1f)", task.Type, task.ID, elapsed.Round(time.Millisecond), threshold.Round(time.Millisecond), median.Round(time.Millisecond), SpeculativeMultiplier),
					WorkerID:  task.WorkerID,
					TaskType:  task.Type.String(),
					TaskID:    task.ID,
					AttemptID: task.AttemptID,
				})
				tm.job.Metrics.SpeculativeRequeues++
				tm.clearWorkerTaskLocked(task.WorkerID, task)
				task.State = Idle
				task.WorkerID = ""
				task.StartTime = time.Time{}
			}
		}

	case Failed:
		task.RetryCount++
		tm.job.Metrics.Retries++
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
	tm.logDecision(DecisionEvent{
		Type:    DecisionJobFailed,
		Message: reason,
	})
	tm.job.snapshotWorkersLocked(tm.workers, tm.job.CompletedAt, tm.workerTimeout)
	tm.job.closeJobLog()
}

func (tm *TaskManager) maybeLogReduceSlowStartLocked() {
	if tm.reduceSlowStartLogged {
		return
	}
	totalMaps := len(tm.job.MapTasks)
	if totalMaps == 0 {
		return
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
	ratio := float64(completedMaps) / float64(totalMaps)
	if ratio >= threshold {
		tm.reduceSlowStartLogged = true
		tm.logDecision(DecisionEvent{
			Type: DecisionReduceSlowStart,
			Message: fmt.Sprintf(
				"Reduce Slow Start 解锁：Map 完成率 %.0f%% ≥ 阈值 %.0f%%，可调度 Reduce Shuffle",
				ratio*100, threshold*100,
			),
		})
	}
}

// resetWorkerTasksLocked resets all InProgress tasks belonging to the given
// worker.  It does NOT remove the worker entry so that blacklist state survives.
func (tm *TaskManager) resetWorkerTasksLocked(workerID string) {
	for _, task := range tm.job.MapTasks {
		if task.WorkerID == workerID && task.State == InProgress {
			task.RetryCount++
			tm.job.Metrics.Retries++
			tm.clearWorkerTaskLocked(workerID, task)
			task.State = Idle
			task.WorkerID = ""
			task.StartTime = time.Time{}
		}
	}
	for _, task := range tm.job.ReduceTasks {
		if task.WorkerID == workerID && task.State == InProgress {
			task.RetryCount++
			tm.job.Metrics.Retries++
			tm.clearWorkerTaskLocked(workerID, task)
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
	return tm.canScheduleReduceLocked()
}

// CanLaunchEarlyReduce limits slow-start reducers so they do not occupy every
// worker while there are still idle map tasks waiting.
func (tm *TaskManager) CanLaunchEarlyReduce() bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if allMapTasksCompleted(tm.job) {
		return true
	}
	liveWorkers := tm.liveWorkerCountLocked(time.Now())
	if liveWorkers < 2 {
		return false
	}
	inProgressReduce := 0
	for _, task := range tm.job.ReduceTasks {
		if task.State == InProgress {
			inProgressReduce++
		}
	}
	limit := liveWorkers / 2
	if limit < 1 {
		limit = 1
	}
	return inProgressReduce < limit
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

func allMapTasksCompleted(job *Job) bool {
	for _, task := range job.MapTasks {
		if task.State != Completed {
			return false
		}
	}
	return true
}

func (tm *TaskManager) liveWorkerCountLocked(now time.Time) int {
	count := 0
	for _, w := range tm.workers {
		if !w.Blacklisted && now.Sub(w.LastHeartbeat) <= tm.workerTimeout {
			count++
		}
	}
	return count
}

func maxMapSplitLength(job *Job) int64 {
	var maxLen int64
	for _, task := range job.MapTasks {
		if task.MapInfo != nil && task.MapInfo.Split.Length > maxLen {
			maxLen = task.MapInfo.Split.Length
		}
	}
	return maxLen
}

func (tm *TaskManager) clearWorkerTaskLocked(workerID string, task *Task) {
	if w, ok := tm.workers[workerID]; ok && w.CurrentTask == task.ID && w.CurrentType == task.Type {
		w.CurrentTask = -1
		w.CurrentType = WaitTask
	}
}
