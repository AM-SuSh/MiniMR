package mr

import (
	"sort"
	"time"
)

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
	MapMedianMs             int64   `json:"map_median_ms"`
	ReduceMedianMs          int64   `json:"reduce_median_ms"`
	SpeculativeMultiplier   float64 `json:"speculative_multiplier"`
	SpeculativeMinCompleted int     `json:"speculative_min_completed"`
	MapCompletedSamples     int     `json:"map_completed_samples"`
	ReduceCompletedSamples  int     `json:"reduce_completed_samples"`
}

// DashboardSnapshot is the full payload for GET /api/dashboard.
type DashboardSnapshot struct {
	Job              DashboardJob            `json:"job"`
	JobHistory       []DashboardJobSummary   `json:"job_history"`
	Progress         DashboardProgress       `json:"progress"`
	MapTasks         []DashboardTask         `json:"map_tasks"`
	ReduceTasks      []DashboardTask         `json:"reduce_tasks"`
	Workers          []DashboardWorker       `json:"workers"`
	ReducePartitions []ReducePartitionStatus `json:"reduce_partitions"`
	Scheduling       SchedulingInsight       `json:"scheduling"`
	Optimizations    OptimizationSnapshot    `json:"optimizations"`
	Decisions        []DecisionEvent         `json:"decisions"`
	ServerTime       time.Time               `json:"server_time"`
}

type DashboardJobSummary struct {
	ID              string     `json:"id"`
	State           string     `json:"state"`
	CreatedAt       time.Time  `json:"created_at"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
	InputFiles      []string   `json:"input_files"`
	NMap            int        `json:"n_map"`
	NReduce         int        `json:"n_reduce"`
	WorkDir         string     `json:"work_dir"`
	MapCompleted    int        `json:"map_completed"`
	MapTotal        int        `json:"map_total"`
	ReduceCompleted int        `json:"reduce_completed"`
	ReduceTotal     int        `json:"reduce_total"`
	IsCurrent       bool       `json:"is_current"`
	IsSelected      bool       `json:"is_selected"`
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
	MapCompleted             int     `json:"map_completed"`
	MapTotal                 int     `json:"map_total"`
	ReduceCompleted          int     `json:"reduce_completed"`
	ReduceTotal              int     `json:"reduce_total"`
	MapRatio                 float64 `json:"map_ratio"`
	ReduceSlowStartThreshold float64 `json:"reduce_slow_start_threshold"`
	ReduceSchedulingUnlocked bool    `json:"reduce_scheduling_unlocked"`
}

type OptimizationSnapshot struct {
	SlowStart      SlowStartEffect      `json:"slow_start"`
	Shuffle        ShuffleEffect        `json:"shuffle"`
	Streaming      StreamingEffect      `json:"streaming"`
	FaultTolerance FaultToleranceEffect `json:"fault_tolerance"`
}

type SlowStartEffect struct {
	Enabled           bool    `json:"enabled"`
	Threshold         float64 `json:"threshold"`
	MapRatio          float64 `json:"map_ratio"`
	Unlocked          bool    `json:"unlocked"`
	EarlyReduceStarts int64   `json:"early_reduce_starts"`
	ActiveReduces     int     `json:"active_reduces"`
}

type ShuffleEffect struct {
	Records             int64   `json:"records"`
	Files               int64   `json:"files"`
	JSONBytes           int64   `json:"json_bytes"`
	BinaryBytes         int64   `json:"binary_bytes"`
	CompressedBytes     int64   `json:"compressed_bytes"`
	BinarySavedPercent  float64 `json:"binary_saved_percent"`
	CompressedSavedPct  float64 `json:"compressed_saved_percent"`
	CompressedToJSONPct float64 `json:"compressed_to_json_percent"`
	WriteMs             int64   `json:"write_ms"`
}

type StreamingEffect struct {
	StreamedRecords   int64 `json:"streamed_records"`
	OutputKeys        int64 `json:"output_keys"`
	OpenedStreams     int64 `json:"opened_streams"`
	MaxBufferedValues int64 `json:"max_buffered_values"`
	ShuffleWaitMs     int64 `json:"shuffle_wait_ms"`
	StreamMs          int64 `json:"stream_ms"`
	WriteMs           int64 `json:"write_ms"`
}

type FaultToleranceEffect struct {
	WorkersTotal        int   `json:"workers_total"`
	WorkersAlive        int   `json:"workers_alive"`
	BlacklistedWorkers  int64 `json:"blacklisted_workers"`
	TaskFailures        int64 `json:"task_failures"`
	TaskTimeouts        int64 `json:"task_timeouts"`
	WorkerTimeouts      int64 `json:"worker_timeouts"`
	SpeculativeRequeues int64 `json:"speculative_requeues"`
	StaleReports        int64 `json:"stale_reports"`
	Retries             int64 `json:"retries"`
}

// BuildDashboardSnapshot assembles UI state for the given job.
func (m *Master) BuildDashboardSnapshot(job *Job) DashboardSnapshot {
	now := time.Now()
	snap := DashboardSnapshot{ServerTime: now}

	m.mu.Lock()
	defer m.mu.Unlock()

	if job == nil {
		snap.JobHistory = m.jobHistoryLocked(nil, now)
		return snap
	}

	m.completeJobIfDoneLocked(job, now)

	if m.tm != nil && job == m.current {
		snap = m.snapshotWithTM(job, m.tm, now)
	} else {
		snap = m.snapshotWithoutTM(job, now)
	}
	snap.JobHistory = m.jobHistoryLocked(job, now)

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
	snap.Optimizations = optimizationSnapshot(job, snap.Progress, job.WorkerSnapshot)
	snap.Workers = copyDashboardWorkers(job.WorkerSnapshot)
	snap.Decisions = copyDecisions(job.Decisions)
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
	snap.Optimizations = optimizationSnapshot(job, snap.Progress, snap.Workers)
	snap.Decisions = copyDecisions(tm.decisions)

	return snap
}

func (m *Master) jobHistoryLocked(selected *Job, now time.Time) []DashboardJobSummary {
	out := make([]DashboardJobSummary, 0, len(m.jobs))
	for _, job := range m.jobs {
		mapDone, mapTotal, reduceDone, reduceTotal := m.jobProgressCountsLocked(job, now)
		row := DashboardJobSummary{
			ID:              job.ID,
			State:           job.State.String(),
			CreatedAt:       job.CreatedAt,
			InputFiles:      job.Config.InputFiles,
			NMap:            job.Config.NMap,
			NReduce:         job.Config.NReduce,
			WorkDir:         job.Config.WorkDir,
			MapCompleted:    mapDone,
			MapTotal:        mapTotal,
			ReduceCompleted: reduceDone,
			ReduceTotal:     reduceTotal,
			IsCurrent:       job == m.current,
			IsSelected:      selected != nil && job.ID == selected.ID,
		}
		if !job.CompletedAt.IsZero() {
			t := job.CompletedAt
			row.CompletedAt = &t
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID > out[j].ID
		}
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	return out
}

func (m *Master) jobProgressCountsLocked(job *Job, now time.Time) (mapDone, mapTotal, reduceDone, reduceTotal int) {
	if job == m.current && m.tm != nil {
		m.tm.mu.Lock()
		defer m.tm.mu.Unlock()
		if m.allTasksCompleteLocked(job) && job.State != JobFailed {
			m.completeJobLocked(job, now)
		}
		mapDone, mapTotal = countCompleted(job.MapTasks)
		reduceDone, reduceTotal = countCompleted(job.ReduceTasks)
		return
	}
	if m.allTasksCompleteLocked(job) && job.State != JobFailed {
		m.completeJobLocked(job, now)
	}
	mapDone, mapTotal = countCompleted(job.MapTasks)
	reduceDone, reduceTotal = countCompleted(job.ReduceTasks)
	return
}

func (m *Master) completeJobIfDoneLocked(job *Job, now time.Time) {
	if job == m.current && m.tm != nil {
		m.tm.mu.Lock()
		defer m.tm.mu.Unlock()
		if m.allTasksCompleteLocked(job) && job.State != JobFailed {
			m.completeJobLocked(job, now)
		}
		return
	}
	if m.allTasksCompleteLocked(job) && job.State != JobFailed {
		m.completeJobLocked(job, now)
	}
}

func copyDecisions(in []DecisionEvent) []DecisionEvent {
	out := make([]DecisionEvent, len(in))
	copy(out, in)
	return out
}

func copyDashboardWorkers(in []DashboardWorker) []DashboardWorker {
	if len(in) == 0 {
		return nil
	}
	out := make([]DashboardWorker, len(in))
	copy(out, in)
	return out
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
	median         time.Duration
	completedCount int
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

func optimizationSnapshot(job *Job, progress DashboardProgress, workers []DashboardWorker) OptimizationSnapshot {
	m := job.Metrics
	activeReduces := 0
	for _, task := range job.ReduceTasks {
		if task.State == InProgress {
			activeReduces++
		}
	}
	aliveWorkers := 0
	for _, worker := range workers {
		if worker.Alive {
			aliveWorkers++
		}
	}
	return OptimizationSnapshot{
		SlowStart: SlowStartEffect{
			Enabled:           job.Config.ReduceSlowStart < 1.0,
			Threshold:         progress.ReduceSlowStartThreshold,
			MapRatio:          progress.MapRatio,
			Unlocked:          progress.ReduceSchedulingUnlocked,
			EarlyReduceStarts: m.EarlyReduceStarts,
			ActiveReduces:     activeReduces,
		},
		Shuffle: ShuffleEffect{
			Records:             m.CombineOutputRecords,
			Files:               m.ShuffleFiles,
			JSONBytes:           m.ShuffleJSONBytes,
			BinaryBytes:         m.ShuffleBinaryBytes,
			CompressedBytes:     m.ShuffleCompressedBytes,
			BinarySavedPercent:  savedPercent(m.ShuffleJSONBytes, m.ShuffleBinaryBytes),
			CompressedSavedPct:  savedPercent(m.ShuffleJSONBytes, m.ShuffleCompressedBytes),
			CompressedToJSONPct: ratioPercent(m.ShuffleCompressedBytes, m.ShuffleJSONBytes),
			WriteMs:             m.ShuffleWriteMs,
		},
		Streaming: StreamingEffect{
			StreamedRecords:   m.ReduceStreamedRecords,
			OutputKeys:        m.ReduceOutputKeys,
			OpenedStreams:     m.ReduceOpenedStreams,
			MaxBufferedValues: m.ReduceMaxBufferedValues,
			ShuffleWaitMs:     m.ReduceShuffleWaitMs,
			StreamMs:          m.ReduceReadMs,
			WriteMs:           m.ReduceWriteMs,
		},
		FaultTolerance: FaultToleranceEffect{
			WorkersTotal:        len(workers),
			WorkersAlive:        aliveWorkers,
			BlacklistedWorkers:  m.BlacklistedWorkers,
			TaskFailures:        m.TaskFailures,
			TaskTimeouts:        m.TaskTimeouts,
			WorkerTimeouts:      m.WorkerTimeouts,
			SpeculativeRequeues: m.SpeculativeRequeues,
			StaleReports:        m.StaleReports,
			Retries:             m.Retries,
		},
	}
}

func savedPercent(before, after int64) float64 {
	if before <= 0 {
		return 0
	}
	return (1 - float64(after)/float64(before)) * 100
}

func ratioPercent(part, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return float64(part) / float64(total) * 100
}
