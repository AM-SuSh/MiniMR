package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// JobLogDir is the directory for per-job JSONL scheduler logs.
var JobLogDir = "logs"

type jobLogWriter struct {
	mu    sync.Mutex
	jobID string
	f     *os.File
}

type jobLogRecord struct {
	Time    time.Time `json:"time"`
	Kind    string    `json:"kind"`
	JobID   string    `json:"job_id,omitempty"`
	State   string    `json:"state,omitempty"`
	Error   string    `json:"error,omitempty"`
	Message string    `json:"message,omitempty"`

	Type      DecisionType `json:"type,omitempty"`
	WorkerID  string       `json:"worker_id,omitempty"`
	TaskType  string       `json:"task_type,omitempty"`
	TaskID    int          `json:"task_id,omitempty"`
	AttemptID int          `json:"attempt_id,omitempty"`

	InputFiles  []string `json:"input_files,omitempty"`
	NMap        int      `json:"n_map,omitempty"`
	NReduce     int      `json:"n_reduce,omitempty"`
	MapFunc     string   `json:"map_func,omitempty"`
	ReduceFunc  string   `json:"reduce_func,omitempty"`
	CombineFunc string   `json:"combine_func,omitempty"`
	WorkDir     string   `json:"work_dir,omitempty"`
}

func openJobLog(job *Job) (*jobLogWriter, error) {
	if err := os.MkdirAll(JobLogDir, 0755); err != nil {
		return nil, err
	}
	path := filepath.Join(JobLogDir, job.ID+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	w := &jobLogWriter{jobID: job.ID, f: f}
	if err := w.write(jobLogRecord{
		Time:        job.CreatedAt,
		Kind:        "start",
		JobID:       job.ID,
		InputFiles:  append([]string(nil), job.Config.InputFiles...),
		NMap:        job.Config.NMap,
		NReduce:     job.Config.NReduce,
		MapFunc:     job.Config.MapFunc,
		ReduceFunc:  job.Config.ReduceFunc,
		CombineFunc: job.Config.CombineFunc,
		WorkDir:     job.Config.WorkDir,
		Message:     fmt.Sprintf("job started: %d map tasks, %d reduce tasks", job.Config.NMap, job.Config.NReduce),
	}); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

func (w *jobLogWriter) writeDecision(ev DecisionEvent) {
	if w == nil {
		return
	}
	t := ev.Time
	if t.IsZero() {
		t = time.Now()
	}
	_ = w.write(jobLogRecord{
		Time:      t,
		Kind:      "decision",
		JobID:     w.jobID,
		Type:      ev.Type,
		Message:   ev.Message,
		WorkerID:  ev.WorkerID,
		TaskType:  ev.TaskType,
		TaskID:    ev.TaskID,
		AttemptID: ev.AttemptID,
	})
}

func (w *jobLogWriter) writeFinish(state JobState, errMsg string) {
	if w == nil {
		return
	}
	rec := jobLogRecord{
		Time:    time.Now(),
		Kind:    "finish",
		JobID:   w.jobID,
		State:   state.String(),
		Message: fmt.Sprintf("job %s", state.String()),
	}
	if errMsg != "" {
		rec.Error = errMsg
	}
	_ = w.write(rec)
}

func (w *jobLogWriter) write(rec jobLogRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	if rec.Time.IsZero() {
		rec.Time = time.Now()
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = w.f.Write(append(data, '\n'))
	return err
}

func (w *jobLogWriter) close() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f != nil {
		_ = w.f.Sync()
		_ = w.f.Close()
		w.f = nil
	}
}

// JobLogPath returns the on-disk log file path for a job ID.
func JobLogPath(jobID string) string {
	return filepath.Join(JobLogDir, jobID+".log")
}

func (job *Job) snapshotWorkersLocked(workers map[string]*WorkerInfo, now time.Time, timeout time.Duration) {
	job.WorkerSnapshot = workersToDashboard(workers, job, now, timeout)
}

func (job *Job) closeJobLog() {
	if job.jobLog == nil {
		return
	}
	job.jobLog.writeFinish(job.State, job.Error)
	job.jobLog.close()
	job.jobLog = nil
	_ = SaveJobCheckpoint(job)
}

func reopenJobLog(job *Job) error {
	if err := os.MkdirAll(JobLogDir, 0755); err != nil {
		return err
	}
	path := filepath.Join(JobLogDir, job.ID+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	w := &jobLogWriter{jobID: job.ID, f: f}
	if err := w.write(jobLogRecord{
		Time:    time.Now(),
		Kind:    "recover",
		JobID:   job.ID,
		State:   job.State.String(),
		Message: fmt.Sprintf("master recovered job %s from checkpoint", job.ID),
	}); err != nil {
		f.Close()
		return err
	}
	job.jobLog = w
	return nil
}
