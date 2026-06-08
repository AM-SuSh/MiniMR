package mr

import (
	"sync"
	"time"
)

// TaskManager handles task state transitions, timeouts, and worker health.
type TaskManager struct {
	mu            sync.Mutex
	job           *Job
	workers       map[string]*WorkerInfo
	taskTimeout   time.Duration
	workerTimeout time.Duration
}

func NewTaskManager(job *Job) *TaskManager {
	return &TaskManager{
		job:           job,
		workers:       make(map[string]*WorkerInfo),
		taskTimeout:   DefaultTaskTimeout,
		workerTimeout: DefaultWorkerTimeout,
	}
}

func (tm *TaskManager) RegisterWorker(id string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
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

func (tm *TaskManager) AssignTask(task *Task, workerID string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	task.State = InProgress
	task.WorkerID = workerID
	task.StartTime = time.Now()
	if w, ok := tm.workers[workerID]; ok {
		w.CurrentTask = task.ID
		w.CurrentType = task.Type
	}
}

func (tm *TaskManager) CompleteTask(task *Task, success bool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if success {
		task.State = Completed
	} else {
		task.State = Failed
	}
	if w, ok := tm.workers[task.WorkerID]; ok {
		w.CurrentTask = -1
	}
}

func (tm *TaskManager) ResetTask(task *Task) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	task.State = Idle
	task.WorkerID = ""
	task.StartTime = time.Time{}
}

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
	now := time.Now()

	for _, w := range tm.workers {
		if now.Sub(w.LastHeartbeat) > tm.workerTimeout {
			tm.resetWorkerTasksLocked(w.ID)
		}
	}

	allTasks := append(tm.job.MapTasks, tm.job.ReduceTasks...)
	for _, task := range allTasks {
		if task.State == InProgress && !task.StartTime.IsZero() {
			if now.Sub(task.StartTime) > tm.taskTimeout {
				task.State = Idle
				task.WorkerID = ""
				task.StartTime = time.Time{}
			}
		}
		if task.State == Failed {
			task.State = Idle
			task.WorkerID = ""
			task.StartTime = time.Time{}
		}
	}
}

func (tm *TaskManager) resetWorkerTasksLocked(workerID string) {
	allTasks := append(tm.job.MapTasks, tm.job.ReduceTasks...)
	for _, task := range allTasks {
		if task.WorkerID == workerID && task.State == InProgress {
			task.State = Idle
			task.WorkerID = ""
			task.StartTime = time.Time{}
		}
	}
	delete(tm.workers, workerID)
}

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
