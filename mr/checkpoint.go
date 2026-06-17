package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const checkpointVersion = 1

// CheckpointDir stores per-job JSON checkpoints for master recovery.
var CheckpointDir = "checkpoints"

var ErrJobNotRecoverable = errors.New("job is not recoverable")

// CheckpointJobSummary is a lightweight checkpoint row for history APIs.
type CheckpointJobSummary struct {
	ID          string    `json:"id"`
	State       string    `json:"state"`
	SavedAt     time.Time `json:"saved_at"`
	CreatedAt   time.Time `json:"created_at"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
	WorkDir     string    `json:"work_dir"`
	InputFiles  []string  `json:"input_files,omitempty"`
	NMap        int       `json:"n_map"`
	NReduce     int       `json:"n_reduce"`
	MapDone     int       `json:"map_completed"`
	MapTotal    int       `json:"map_total"`
	ReduceDone  int       `json:"reduce_completed"`
	ReduceTotal int       `json:"reduce_total"`
}

// RecoverableJobSummary is kept as an alias for backward-compatible APIs.
type RecoverableJobSummary = CheckpointJobSummary

func checkpointToSummary(cp *jobCheckpoint) CheckpointJobSummary {
	mapDone, mapTotal := countCheckpointCompleted(cp.MapTasks)
	reduceDone, reduceTotal := countCheckpointCompleted(cp.ReduceTasks)
	state := cp.State
	if isRecoverableCheckpoint(cp) {
		state = "recoverable"
	}
	return CheckpointJobSummary{
		ID:          cp.ID,
		State:       state,
		SavedAt:     cp.SavedAt,
		CreatedAt:   cp.CreatedAt,
		CompletedAt: cp.CompletedAt,
		WorkDir:     cp.Config.WorkDir,
		InputFiles:  append([]string(nil), cp.Config.InputFiles...),
		NMap:        cp.Config.NMap,
		NReduce:     cp.Config.NReduce,
		MapDone:     mapDone,
		MapTotal:    mapTotal,
		ReduceDone:  reduceDone,
		ReduceTotal: reduceTotal,
	}
}

// ListAllCheckpointJobs returns every job snapshot stored on disk.
func ListAllCheckpointJobs() ([]CheckpointJobSummary, error) {
	entries, err := os.ReadDir(CheckpointDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []CheckpointJobSummary
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".json") || strings.HasSuffix(ent.Name(), ".tmp") {
			continue
		}
		jobID := strings.TrimSuffix(ent.Name(), ".json")
		cp, err := loadJobCheckpoint(jobID)
		if err != nil {
			continue
		}
		out = append(out, checkpointToSummary(cp))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID > out[j].ID
		}
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	return out, nil
}

// LoadArchivedJobsFromCheckpoints restores completed/failed jobs into master memory.
func (m *Master) LoadArchivedJobsFromCheckpoints() error {
	summaries, err := ListAllCheckpointJobs()
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	loaded := 0
	for _, summary := range summaries {
		if summary.State == "recoverable" {
			continue
		}
		if _, ok := m.jobs[summary.ID]; ok {
			continue
		}
		cp, err := loadJobCheckpoint(summary.ID)
		if err != nil {
			continue
		}
		m.jobs[summary.ID] = checkpointToJob(cp)
		loaded++
	}
	if loaded > 0 {
		log.Printf("loaded %d archived job(s) from checkpoints", loaded)
	}
	return nil
}

type taskCheckpoint struct {
	ID                int          `json:"id"`
	Type              string       `json:"type"`
	State             string       `json:"state"`
	WorkerID          string       `json:"worker_id,omitempty"`
	StartTime         time.Time    `json:"start_time,omitempty"`
	MapInfo           *MapTaskInfo `json:"map_info,omitempty"`
	ReduceID          int          `json:"reduce_id,omitempty"`
	AttemptID         int          `json:"attempt_id"`
	RetryCount        int          `json:"retry_count"`
	LastFailureReason string       `json:"last_failure_reason,omitempty"`
}

type jobCheckpoint struct {
	Version          int              `json:"version"`
	SavedAt          time.Time        `json:"saved_at"`
	ID               string           `json:"id"`
	Config           JobConfig        `json:"config"`
	State            string           `json:"state"`
	Error            string           `json:"error,omitempty"`
	CreatedAt        time.Time        `json:"created_at"`
	CompletedAt      time.Time        `json:"completed_at,omitempty"`
	MapTasks         []taskCheckpoint `json:"map_tasks"`
	ReduceTasks      []taskCheckpoint `json:"reduce_tasks"`
	MapDoneForReduce [][]bool         `json:"map_done_for_reduce"`
	Metrics          JobMetrics       `json:"metrics"`
	Decisions        []DecisionEvent  `json:"decisions,omitempty"`
}

func checkpointPath(jobID string) string {
	return filepath.Join(CheckpointDir, jobID+".json")
}

// SaveJobCheckpoint writes the current job state to disk.
func SaveJobCheckpoint(job *Job) error {
	if job == nil || job.ID == "" {
		return nil
	}
	if err := os.MkdirAll(CheckpointDir, 0755); err != nil {
		return err
	}
	cp := jobToCheckpoint(job, time.Now())
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return err
	}
	tmp := checkpointPath(job.ID) + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, checkpointPath(job.ID))
}

func jobToCheckpoint(job *Job, savedAt time.Time) jobCheckpoint {
	cp := jobCheckpoint{
		Version:          checkpointVersion,
		SavedAt:          savedAt,
		ID:               job.ID,
		Config:           job.Config,
		State:            job.State.String(),
		Error:            job.Error,
		CreatedAt:        job.CreatedAt,
		CompletedAt:      job.CompletedAt,
		MapDoneForReduce: job.MapDoneForReduce,
		Metrics:          job.Metrics,
		Decisions:        copyDecisions(job.Decisions),
	}
	for _, t := range job.MapTasks {
		cp.MapTasks = append(cp.MapTasks, taskToCheckpoint(t))
	}
	for _, t := range job.ReduceTasks {
		cp.ReduceTasks = append(cp.ReduceTasks, taskToCheckpoint(t))
	}
	return cp
}

func taskToCheckpoint(t *Task) taskCheckpoint {
	return taskCheckpoint{
		ID:                t.ID,
		Type:              t.Type.String(),
		State:             t.State.String(),
		WorkerID:          t.WorkerID,
		StartTime:         t.StartTime,
		MapInfo:           t.MapInfo,
		ReduceID:          t.ReduceID,
		AttemptID:         t.AttemptID,
		RetryCount:        t.RetryCount,
		LastFailureReason: t.LastFailureReason,
	}
}

func loadJobCheckpoint(jobID string) (*jobCheckpoint, error) {
	data, err := os.ReadFile(checkpointPath(jobID))
	if err != nil {
		return nil, err
	}
	var cp jobCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, err
	}
	if cp.Version != checkpointVersion {
		return nil, fmt.Errorf("unsupported checkpoint version %d", cp.Version)
	}
	return &cp, nil
}

// ListRecoverableJobs scans checkpoint files for interrupted running jobs.
func ListRecoverableJobs() ([]RecoverableJobSummary, error) {
	all, err := ListAllCheckpointJobs()
	if err != nil {
		return nil, err
	}
	var out []RecoverableJobSummary
	for _, row := range all {
		if row.State == "recoverable" {
			out = append(out, row)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].SavedAt.After(out[j].SavedAt)
	})
	return out, nil
}

func isRecoverableCheckpoint(cp *jobCheckpoint) bool {
	if cp == nil || cp.State != JobRunning.String() {
		return false
	}
	mapDone, mapTotal := countCheckpointCompleted(cp.MapTasks)
	reduceDone, reduceTotal := countCheckpointCompleted(cp.ReduceTasks)
	return mapDone < mapTotal || reduceDone < reduceTotal
}

func countCheckpointCompleted(tasks []taskCheckpoint) (done, total int) {
	total = len(tasks)
	for _, t := range tasks {
		if t.State == Completed.String() {
			done++
		}
	}
	return
}

// RebuildJobFromCheckpoint loads a checkpoint and reconciles task progress from WorkDir.
func RebuildJobFromCheckpoint(jobID string) (*Job, error) {
	cp, err := loadJobCheckpoint(jobID)
	if err != nil {
		return nil, err
	}
	if !isRecoverableCheckpoint(cp) {
		return nil, fmt.Errorf("%w: job %s state=%s", ErrJobNotRecoverable, jobID, cp.State)
	}
	job := checkpointToJob(cp)
	prepareInterruptedTasks(job)
	reconcileJobFromWorkDir(job)
	appendJobDecision(job, DecisionEvent{
		Type: DecisionJobRecovered,
		Message: fmt.Sprintf(
			"Master 从 checkpoint 恢复作业 %s（保存于 %s）",
			job.ID, cp.SavedAt.Format(time.RFC3339),
		),
	})
	if allTasksComplete(job) {
		job.State = JobCompleted
		job.CompletedAt = time.Now()
	} else {
		job.State = JobRunning
		job.Error = ""
	}
	return job, nil
}

func checkpointToJob(cp *jobCheckpoint) *Job {
	job := &Job{
		ID:               cp.ID,
		Config:           cp.Config,
		State:            parseJobState(cp.State),
		Error:            cp.Error,
		CreatedAt:        cp.CreatedAt,
		CompletedAt:      cp.CompletedAt,
		MapDoneForReduce: cp.MapDoneForReduce,
		Metrics:          cp.Metrics,
		Decisions:        copyDecisions(cp.Decisions),
	}
	for _, tc := range cp.MapTasks {
		job.MapTasks = append(job.MapTasks, checkpointToTask(tc, MapTask))
	}
	for _, tc := range cp.ReduceTasks {
		job.ReduceTasks = append(job.ReduceTasks, checkpointToTask(tc, ReduceTask))
	}
	if len(job.MapDoneForReduce) != job.Config.NReduce {
		job.MapDoneForReduce = make([][]bool, job.Config.NReduce)
	}
	for r := 0; r < job.Config.NReduce; r++ {
		if r >= len(job.MapDoneForReduce) || len(job.MapDoneForReduce[r]) != len(job.MapTasks) {
			job.MapDoneForReduce[r] = make([]bool, len(job.MapTasks))
		}
	}
	return job
}

func checkpointToTask(tc taskCheckpoint, defaultType TaskType) *Task {
	typ := defaultType
	if parsed, ok := parseTaskType(tc.Type); ok {
		typ = parsed
	}
	return &Task{
		ID:                tc.ID,
		Type:              typ,
		State:             parseTaskState(tc.State),
		WorkerID:          tc.WorkerID,
		StartTime:         tc.StartTime,
		MapInfo:           tc.MapInfo,
		ReduceID:          tc.ReduceID,
		AttemptID:         tc.AttemptID,
		RetryCount:        tc.RetryCount,
		LastFailureReason: tc.LastFailureReason,
	}
}

func prepareInterruptedTasks(job *Job) {
	reset := func(task *Task) {
		switch task.State {
		case InProgress, Failed:
			task.State = Idle
			task.WorkerID = ""
			task.StartTime = time.Time{}
			task.AttemptID++
			task.LastFailureReason = ""
		}
	}
	for _, task := range job.MapTasks {
		reset(task)
	}
	for _, task := range job.ReduceTasks {
		reset(task)
	}
}

func reconcileJobFromWorkDir(job *Job) {
	workDir := job.Config.WorkDir
	nReduce := job.Config.NReduce
	for _, task := range job.MapTasks {
		if task.State == Completed {
			continue
		}
		if mapOutputsReady(workDir, job.ID, task.ID, nReduce) {
			task.State = Completed
			task.WorkerID = ""
			task.StartTime = time.Time{}
			task.LastFailureReason = ""
			for r := 0; r < nReduce; r++ {
				job.MapDoneForReduce[r][task.ID] = true
			}
		}
	}
	for _, task := range job.ReduceTasks {
		if task.State == Completed {
			continue
		}
		if reduceOutputReady(workDir, task.ReduceID) {
			task.State = Completed
			task.WorkerID = ""
			task.StartTime = time.Time{}
			task.LastFailureReason = ""
		}
	}
}

func mapOutputsReady(workDir, jobID string, mapID, nReduce int) bool {
	for r := 0; r < nReduce; r++ {
		path := intermediatePath(workDir, mapID, r)
		if !intermediateReady(path, jobID) {
			return false
		}
	}
	return true
}

func reduceOutputReady(workDir string, reduceID int) bool {
	path := filepath.Join(workDir, fmt.Sprintf("mr-out-%d", reduceID))
	info, err := os.Stat(path)
	return err == nil && info.Size() > 0
}

func allTasksComplete(job *Job) bool {
	for _, t := range job.MapTasks {
		if t.State != Completed {
			return false
		}
	}
	for _, t := range job.ReduceTasks {
		if t.State != Completed {
			return false
		}
	}
	return true
}

func parseJobState(s string) JobState {
	switch s {
	case JobPending.String():
		return JobPending
	case JobRunning.String():
		return JobRunning
	case JobCompleted.String():
		return JobCompleted
	case JobFailed.String():
		return JobFailed
	default:
		return JobRunning
	}
}

func parseTaskState(s string) TaskState {
	switch s {
	case Idle.String():
		return Idle
	case InProgress.String():
		return InProgress
	case Completed.String():
		return Completed
	case Failed.String():
		return Failed
	default:
		return Idle
	}
}

// StatusFromCheckpoint builds a status payload for jobs only present on disk.
func StatusFromCheckpoint(jobID string) (map[string]interface{}, error) {
	cp, err := loadJobCheckpoint(jobID)
	if err != nil {
		return nil, err
	}
	mapDone, mapTotal := countCheckpointCompleted(cp.MapTasks)
	reduceDone, reduceTotal := countCheckpointCompleted(cp.ReduceTasks)
	state := cp.State
	recoverable := isRecoverableCheckpoint(cp)
	if recoverable {
		state = "recoverable"
	}
	payload := map[string]interface{}{
		"job_id":           jobID,
		"state":            state,
		"map_completed":    mapDone,
		"map_total":        mapTotal,
		"reduce_completed": reduceDone,
		"reduce_total":     reduceTotal,
		"recoverable":      recoverable,
		"checkpoint_saved": cp.SavedAt,
	}
	if cp.Error != "" {
		payload["error"] = cp.Error
	}
	if recoverable {
		payload["message"] = "作业在 checkpoint 中，查询状态时将自动恢复"
	}
	return payload, nil
}

func parseTaskType(s string) (TaskType, bool) {
	switch s {
	case MapTask.String():
		return MapTask, true
	case ReduceTask.String():
		return ReduceTask, true
	default:
		return MapTask, false
	}
}
