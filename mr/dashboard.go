package mr

import "time"

// DashboardTask is a task row for the web UI.
type DashboardTask struct {
	ID              int    `json:"id"`
	Type            string `json:"type"`
	State           string `json:"state"`
	WorkerID        string `json:"worker_id,omitempty"`
	AttemptID       int    `json:"attempt_id"`
	RetryCount      int    `json:"retry_count"`
	ElapsedMs       int64  `json:"elapsed_ms,omitempty"`
	SpeculativeRisk bool   `json:"speculative_risk"`
	InputFile       string `json:"input_file,omitempty"`
	InputOffset     int64  `json:"input_offset,omitempty"`
	InputLength     int64  `json:"input_length,omitempty"`
	ReduceID        int    `json:"reduce_id,omitempty"`
}

// DashboardWorker is a worker row for the web UI.
type DashboardWorker struct {
	ID            string `json:"id"`
	LastHeartbeat string `json:"last_heartbeat"`
	CurrentTask   int    `json:"current_task"`
	CurrentType   string `json:"current_type,omitempty"`
	FailureCount  int    `json:"failure_count"`
	Blacklisted   bool   `json:"blacklisted"`
	Alive         bool   `json:"alive"`
}

// ReducePartitionStatus tracks shuffle readiness per reduce partition.
type ReducePartitionStatus struct {
	ReduceID  int  `json:"reduce_id"`
	MapsReady int  `json:"maps_ready"`
	MapsTotal int  `json:"maps_total"`
	Ready     bool `json:"ready"`
}

// SchedulingInsight exposes parameters used for speculative execution.
type SchedulingInsight struct {
	MapMedianMs           int64   `json:"map_median_ms"`
	ReduceMedianMs        int64   `json:"reduce_median_ms"`
	SpeculativeMultiplier float64 `json:"speculative_multiplier"`
	SpeculativeMinCompleted int   `json:"speculative_min_completed"`
	MapCompletedSamples   int     `json:"map_completed_samples"`
	ReduceCompletedSamples int    `json:"reduce_completed_samples"`
}

// DashboardSnapshot is the full payload for GET /api/dashboard.
type DashboardSnapshot struct {
	Job               DashboardJob            `json:"job"`
	Progress          DashboardProgress       `json:"progress"`
	MapTasks          []DashboardTask         `json:"map_tasks"`
	ReduceTasks       []DashboardTask         `json:"reduce_tasks"`
	Workers           []DashboardWorker       `json:"workers"`
	ReducePartitions  []ReducePartitionStatus `json:"reduce_partitions"`
	Scheduling        SchedulingInsight       `json:"scheduling"`
	Decisions         []DecisionEvent         `json:"decisions"`
	ServerTime        time.Time               `json:"server_time"`
}

type DashboardJob struct {
	ID          string        `json:"id"`
	State       string        `json:"state"`
	CreatedAt   time.Time     `json:"created_at"`
	CompletedAt *time.Time    `json:"completed_at,omitempty"`
	Error       string        `json:"error,omitempty"`
	Config      DashboardConf `json:"config"`
}

type DashboardConf struct {
	InputFiles      []string `json:"input_files"`
	NMap            int      `json:"n_map"`
	NReduce         int      `json:"n_reduce"`
	MapFunc         string   `json:"map_func"`
	ReduceFunc      string   `json:"reduce_func"`
	CombineFunc     string   `json:"combine_func"`
	SplitSize       int64    `json:"split_size"`
	WorkDir         string   `json:"work_dir"`
	ReduceSlowStart float64  `json:"reduce_slow_start"`
}

type DashboardProgress struct {
	MapCompleted              int     `json:"map_completed"`
	MapTotal                  int     `json:"map_total"`
	ReduceCompleted           int     `json:"reduce_completed"`
	ReduceTotal               int     `json:"reduce_total"`
	MapRatio                  float64 `json:"map_ratio"`
	ReduceSlowStartThreshold  float64 `json:"reduce_slow_start_threshold"`
	ReduceSchedulingUnlocked  bool    `json:"reduce_scheduling_unlocked"`
}

// BuildDashboardSnapshot assembles UI state for the given job.
func (m *Master) BuildDashboardSnapshot(job *Job) DashboardSnapshot {
	now := time.Now()
	snap := DashboardSnapshot{ServerTime: now}

	if job == nil {
		return snap
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.tm != nil && job == m.current {
		snap = m.snapshotWithTM(job, m.tm, now)
	} else {
		snap = m.snapshotWithoutTM(job, now)
	}
	if m.allTasksCompleteLocked(job) && job.State != JobFailed {
		job.State = JobCompleted
	}

	return snap
}

func (m *Master) snapshotWithoutTM(job *Job, now time.Time) DashboardSnapshot {
	snap := baseJobSnapshot(job, now)
	mapDone, mapTotal := countCompleted(job.MapTasks)
	reduceDone, reduceTotal := countCompleted(job.ReduceTasks)
	snap.Progress = progressFromCounts(job, mapDone, mapTotal, reduceDone, reduceTotal, false)
	snap.MapTasks = tasksToDashboard(job.MapTasks, nil, now)
	snap.ReduceTasks = tasksToDashboard(job.ReduceTasks, nil, now)
	snap.ReducePartitions = partitionStatus(job)
	return snap
}

func (m *Master) snapshotWithTM(job *Job, tm *TaskManager, now time.Time) DashboardSnapshot {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	snap := baseJobSnapshot(job, now)
	mapDone, mapTotal := countCompleted(job.MapTasks)
	reduceDone, reduceTotal := countCompleted(job.ReduceTasks)
	unlocked := tm.canScheduleReduceLocked()
	snap.Progress = progressFromCounts(job, mapDone, mapTotal, reduceDone, reduceTotal, unlocked)

	mapMedian := medianDuration(tm.completedMapTimes)
	reduceMedian := medianDuration(tm.completedReduceTimes)
	snap.Scheduling = SchedulingInsight{
		MapMedianMs:             mapMedian.Milliseconds(),
		ReduceMedianMs:          reduceMedian.Milliseconds(),
		SpeculativeMultiplier:   SpeculativeMultiplier,
		SpeculativeMinCompleted: SpeculativeMinCompleted,
		MapCompletedSamples:     len(tm.completedMapTimes),
		ReduceCompletedSamples:  len(tm.completedReduceTimes),
	}

	snap.MapTasks = tasksToDashboard(job.MapTasks, &taskInsight{mapMedian, len(tm.completedMapTimes)}, now)
	snap.ReduceTasks = tasksToDashboard(job.ReduceTasks, &taskInsight{reduceMedian, len(tm.completedReduceTimes)}, now)
	snap.Workers = workersToDashboard(tm.workers, now, tm.workerTimeout)
	snap.ReducePartitions = partitionStatus(job)
	snap.Decisions = make([]DecisionEvent, len(tm.decisions))
	copy(snap.Decisions, tm.decisions)

	return snap
}

func baseJobSnapshot(job *Job, now time.Time) DashboardSnapshot {
	snap := DashboardSnapshot{
		ServerTime: now,
		Job: DashboardJob{
			ID:        job.ID,
			State:     job.State.String(),
			CreatedAt: job.CreatedAt,
			Error:     job.Error,
			Config: DashboardConf{
				InputFiles:      job.Config.InputFiles,
				NMap:            job.Config.NMap,
				NReduce:         job.Config.NReduce,
				MapFunc:         job.Config.MapFunc,
				ReduceFunc:      job.Config.ReduceFunc,
				CombineFunc:     job.Config.CombineFunc,
				SplitSize:       job.Config.SplitSize,
				WorkDir:         job.Config.WorkDir,
				ReduceSlowStart: job.Config.ReduceSlowStart,
			},
		},
	}
	if !job.CompletedAt.IsZero() {
		t := job.CompletedAt
		snap.Job.CompletedAt = &t
	}
	return snap
}

func progressFromCounts(job *Job, mapDone, mapTotal, reduceDone, reduceTotal int, unlocked bool) DashboardProgress {
	threshold := job.Config.ReduceSlowStart
	if threshold <= 0 || threshold > 1.0 {
		threshold = 1.0
	}
	ratio := 0.0
	if mapTotal > 0 {
		ratio = float64(mapDone) / float64(mapTotal)
	}
	return DashboardProgress{
		MapCompleted:             mapDone,
		MapTotal:                 mapTotal,
		ReduceCompleted:          reduceDone,
		ReduceTotal:              reduceTotal,
		MapRatio:                 ratio,
		ReduceSlowStartThreshold: threshold,
		ReduceSchedulingUnlocked: unlocked,
	}
}

type taskInsight struct {
	median          time.Duration
	completedCount  int
}

func tasksToDashboard(tasks []*Task, insight *taskInsight, now time.Time) []DashboardTask {
	out := make([]DashboardTask, 0, len(tasks))
	for _, t := range tasks {
		row := DashboardTask{
			ID:         t.ID,
			Type:       t.Type.String(),
			State:      t.State.String(),
			WorkerID:   t.WorkerID,
			AttemptID:  t.AttemptID,
			RetryCount: t.RetryCount,
			ReduceID:   t.ReduceID,
		}
		if t.MapInfo != nil {
			row.InputFile = t.MapInfo.Split.File
			row.InputOffset = t.MapInfo.Split.Offset
			row.InputLength = t.MapInfo.Split.Length
		}
		if t.State == InProgress && !t.StartTime.IsZero() {
			elapsed := now.Sub(t.StartTime)
			row.ElapsedMs = elapsed.Milliseconds()
			if insight != nil &&
				insight.completedCount >= SpeculativeMinCompleted &&
				insight.median > 0 {
				threshold := time.Duration(float64(insight.median) * SpeculativeMultiplier)
				row.SpeculativeRisk = elapsed > threshold
			}
		}
		out = append(out, row)
	}
	return out
}

func workersToDashboard(workers map[string]*WorkerInfo, now time.Time, timeout time.Duration) []DashboardWorker {
	out := make([]DashboardWorker, 0, len(workers))
	for _, w := range workers {
		alive := !w.Blacklisted && now.Sub(w.LastHeartbeat) <= timeout
		ct := ""
		if w.CurrentTask >= 0 {
			ct = w.CurrentType.String()
		}
		out = append(out, DashboardWorker{
			ID:            w.ID,
			LastHeartbeat: w.LastHeartbeat.Format(time.RFC3339),
			CurrentTask:   w.CurrentTask,
			CurrentType:   ct,
			FailureCount:  w.FailureCount,
			Blacklisted:   w.Blacklisted,
			Alive:         alive,
		})
	}
	return out
}

func partitionStatus(job *Job) []ReducePartitionStatus {
	out := make([]ReducePartitionStatus, 0, job.Config.NReduce)
	for r := 0; r < job.Config.NReduce; r++ {
		ready := 0
		total := len(job.MapTasks)
		if r < len(job.MapDoneForReduce) {
			for _, done := range job.MapDoneForReduce[r] {
				if done {
					ready++
				}
			}
		}
		out = append(out, ReducePartitionStatus{
			ReduceID:  r,
			MapsReady: ready,
			MapsTotal: total,
			Ready:     total > 0 && ready == total,
		})
	}
	return out
}

func (tm *TaskManager) canScheduleReduceLocked() bool {
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
