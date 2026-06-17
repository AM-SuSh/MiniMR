package mr

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Master coordinates job scheduling, RPC, and HTTP API.
type Master struct {
	mu       sync.Mutex
	jobs     map[string]*Job //JobID-Job
	current  *Job            //当前正在执行的作业（简化版只执行单作业，即同一时间Master只处理一个作业）
	tm       *TaskManager    //任务管理器：跟踪任务分配、超时、重试
	starting bool            // true while StartJob is preparing splits for the next active job
	rpcAddr  string
	httpAddr string
	done     chan struct{} //关闭信号，用于停止服务。
}

var ErrJobAlreadyRunning = errors.New("job already running")

// NewMaster creates a Master instance.
func NewMaster(rpcAddr, httpAddr string) *Master {
	return &Master{
		jobs:     make(map[string]*Job),
		rpcAddr:  rpcAddr,
		httpAddr: httpAddr,
		done:     make(chan struct{}),
	}
}

func generateJobID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// StartJob creates and starts a new MapReduce job.
func (m *Master) StartJob(config JobConfig) (*Job, error) {
	if config.WorkDir == "" {
		config.WorkDir = "mr-work"
	}
	if config.NReduce <= 0 {
		config.NReduce = 3
	}
	if config.SplitSize <= 0 {
		config.SplitSize = DefaultSplitSize
	}
	if config.ReduceSlowStart <= 0 || config.ReduceSlowStart > 1.0 {
		config.ReduceSlowStart = DefaultReduceSlowStart
	}

	m.mu.Lock()
	if err := m.canStartJobLocked(time.Now()); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	m.starting = true
	m.mu.Unlock()

	reserved := true
	releaseReservation := func() {
		if !reserved {
			return
		}
		m.mu.Lock()
		m.starting = false
		m.mu.Unlock()
		reserved = false
	}

	if err := os.MkdirAll(config.WorkDir, 0755); err != nil {
		releaseReservation()
		return nil, err
	}

	splits, err := SplitInput(config.InputFiles, config.SplitSize)
	if err != nil {
		releaseReservation()
		return nil, err
	}
	if len(splits) == 0 {
		releaseReservation()
		return nil, fmt.Errorf("no input splits from files: %v", config.InputFiles)
	}

	config.NMap = len(splits)
	jobID := generateJobID()
	job := &Job{
		ID:               jobID,
		Config:           config,
		State:            JobRunning,
		CreatedAt:        time.Now(),
		MapDoneForReduce: make([][]bool, config.NReduce),
	}

	for r := 0; r < config.NReduce; r++ {
		job.MapDoneForReduce[r] = make([]bool, config.NMap)
	}

	for i, split := range splits {
		job.MapTasks = append(job.MapTasks, &Task{
			ID:      i,
			Type:    MapTask,
			State:   Idle,
			MapInfo: &MapTaskInfo{Split: split},
		})
	}

	for r := 0; r < config.NReduce; r++ {
		job.ReduceTasks = append(job.ReduceTasks, &Task{
			ID:       r,
			Type:     ReduceTask,
			State:    Idle,
			ReduceID: r,
		})
	}

	tm := NewTaskManager(job)
	jobLog, err := openJobLog(job)
	if err != nil {
		releaseReservation()
		return nil, fmt.Errorf("open job log: %w", err)
	}
	job.jobLog = jobLog

	m.mu.Lock()
	m.jobs[jobID] = job
	m.current = job
	m.tm = tm
	m.starting = false
	m.mu.Unlock()
	reserved = false

	go tm.StartMonitor(m.done)
	_ = SaveJobCheckpoint(job)
	return job, nil
}

// RecoverJob reloads an interrupted job from checkpoint and resumes scheduling.
func (m *Master) RecoverJob(jobID string) (*Job, error) {
	if jobID == "" {
		return nil, fmt.Errorf("job id is required")
	}

	m.mu.Lock()
	if err := m.canStartJobLocked(time.Now()); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	m.starting = true
	m.mu.Unlock()

	release := func() {
		m.mu.Lock()
		m.starting = false
		m.mu.Unlock()
	}

	job, err := RebuildJobFromCheckpoint(jobID)
	if err != nil {
		release()
		return nil, err
	}

	if err := reopenJobLog(job); err != nil {
		release()
		return nil, fmt.Errorf("reopen job log: %w", err)
	}

	tm := NewTaskManager(job)
	tm.mu.Lock()
	tm.decisions = copyDecisions(job.Decisions)
	tm.mu.Unlock()

	m.mu.Lock()
	m.jobs[job.ID] = job
	m.current = job
	m.tm = tm
	m.starting = false
	if job.State == JobCompleted {
		m.finalizeJobLocked(job, job.CompletedAt)
		m.mu.Unlock()
		_ = SaveJobCheckpoint(job)
		log.Printf("recovered job %s already complete", job.ID)
		return job, nil
	}
	m.mu.Unlock()

	go tm.StartMonitor(m.done)
	_ = SaveJobCheckpoint(job)
	log.Printf("recovered job %s (map %d/%d, reduce %d/%d)",
		job.ID,
		countTasksCompleted(job.MapTasks), len(job.MapTasks),
		countTasksCompleted(job.ReduceTasks), len(job.ReduceTasks))
	return job, nil
}

func countTasksCompleted(tasks []*Task) int {
	n := 0
	for _, t := range tasks {
		if t.State == Completed {
			n++
		}
	}
	return n
}

func (m *Master) canStartJobLocked(now time.Time) error {
	if m.starting {
		return fmt.Errorf("%w: another job is being prepared", ErrJobAlreadyRunning)
	}
	if m.current == nil || m.current.State == JobCompleted || m.current.State == JobFailed {
		return nil
	}
	if m.tm != nil {
		m.tm.mu.Lock()
		defer m.tm.mu.Unlock()
	}
	if m.allTasksCompleteLocked(m.current) {
		m.completeJobLocked(m.current, now)
		return nil
	}
	return fmt.Errorf("%w: current job %s is still running", ErrJobAlreadyRunning, m.current.ID)
}

func (m *Master) completeJobLocked(job *Job, completedAt time.Time) {
	if job == nil || job.State == JobFailed {
		return
	}
	if job.State == JobCompleted && job.jobLog == nil {
		return
	}
	if job.State != JobCompleted {
		job.State = JobCompleted
		if job.CompletedAt.IsZero() {
			job.CompletedAt = completedAt
		}
	}
	m.finalizeJobLocked(job, completedAt)
}

func (m *Master) finalizeJobLocked(job *Job, at time.Time) {
	if job == m.current && m.tm != nil {
		m.tm.mu.Lock()
		job.snapshotWorkersLocked(m.tm.workers, at, m.tm.workerTimeout)
		m.tm.mu.Unlock()
	}
	if job.State == JobCompleted {
		LogJobCompletedDecision(job, at)
	}
	job.closeJobLog()
}

// RequestTask assigns the next available task to a Worker.
func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.current == nil {
		m.replyWaitTask(nil, reply)
		return nil
	}

	job := m.current
	if m.tm != nil {
		m.tm.RegisterWorker(args.WorkerID)
	}

	if m.tm.IsWorkerBlacklisted(args.WorkerID) {
		log.Printf("worker %s is blacklisted, sending exit", args.WorkerID)
		reply.TaskType = ExitTask
		return nil
	}

	if job.State == JobFailed || job.State == JobCompleted {
		m.replyWaitTask(job, reply)
		return nil
	}

	reduceUnlocked := m.tm.CanScheduleReduce()
	if reduceUnlocked && m.tm.CanLaunchEarlyReduce() {
		for _, task := range job.ReduceTasks {
			if task.State == Idle {
				m.assignTaskLocked(job, task, args.WorkerID, reply)
				return nil
			}
		}
	}

	// Prefer map tasks while slow-start reducers are already occupying their
	// worker budget.
	for _, task := range job.MapTasks {
		if task.State == Idle {
			m.assignTaskLocked(job, task, args.WorkerID, reply)
			return nil
		}
	}

	// Reduce slow start: schedule reduce when map completion ratio >= threshold,
	// allowing the reduce worker to begin its shuffle phase (polling for
	// intermediate files) while remaining maps are still running.
	if reduceUnlocked {
		for _, task := range job.ReduceTasks {
			if task.State == Idle {
				m.assignTaskLocked(job, task, args.WorkerID, reply)
				return nil
			}
		}
	}

	if m.allTasksCompleteLocked(job) {
		m.replyWaitTask(job, reply)
		m.completeJobLocked(job, time.Now())
		return nil
	}

	m.replyWaitTask(job, reply)
	return nil
}

func (m *Master) replyWaitTask(job *Job, reply *RequestTaskReply) {
	reply.TaskType = WaitTask
	if job == nil {
		return
	}
	reply.JobID = job.ID
	reply.JobState = job.State.String()
}

func (m *Master) assignTaskLocked(job *Job, task *Task, workerID string, reply *RequestTaskReply) {
	if job.State != JobRunning {
		m.replyWaitTask(job, reply)
		return
	}
	m.tm.AssignTask(task, workerID)
	reply.TaskType = task.Type
	reply.TaskID = task.ID
	reply.NReduce = job.Config.NReduce
	reply.NMap = job.Config.NMap
	reply.MapFunc = job.Config.MapFunc
	reply.ReduceFunc = job.Config.ReduceFunc
	reply.CombineFunc = job.Config.CombineFunc
	reply.WorkDir = job.Config.WorkDir
	reply.JobID = job.ID
	reply.JobState = job.State.String()
	reply.ReduceID = task.ReduceID
	reply.AttemptID = task.AttemptID

	if task.Type == MapTask && task.MapInfo != nil {
		reply.InputFile = task.MapInfo.Split.File
		reply.InputOffset = task.MapInfo.Split.Offset
		reply.InputLength = task.MapInfo.Split.Length
	}
}

func (m *Master) allTasksCompleteLocked(job *Job) bool {
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

// ReportTask handles task completion reports from Workers.
func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	reply.OK = true
	if m.current == nil || m.current.ID != args.JobID {
		return nil
	}

	job := m.current
	if job.State == JobFailed {
		job.Metrics.StaleReports++
		return nil
	}

	var task *Task
	if args.TaskType == MapTask {
		if args.TaskID >= 0 && args.TaskID < len(job.MapTasks) {
			task = job.MapTasks[args.TaskID]
		}
	} else if args.TaskType == ReduceTask {
		if args.TaskID >= 0 && args.TaskID < len(job.ReduceTasks) {
			task = job.ReduceTasks[args.TaskID]
		}
	}
	if task == nil {
		return nil
	}

	if task.AttemptID != args.AttemptID {
		log.Printf("stale report ignored: task %s-%d attempt %d (current %d) from worker %s",
			args.TaskType, args.TaskID, args.AttemptID, task.AttemptID, args.WorkerID)
		job.Metrics.StaleReports++
		return nil
	}

	if task.State == Completed {
		return nil
	}

	if task.State != InProgress || task.WorkerID != args.WorkerID || task.StartTime.IsZero() {
		log.Printf("stale report ignored: task %s-%d attempt %d state=%s worker=%s report_worker=%s",
			args.TaskType, args.TaskID, args.AttemptID, task.State, task.WorkerID, args.WorkerID)
		job.Metrics.StaleReports++
		return nil
	}

	if args.Success {
		m.tm.CompleteTask(task, true, args.WorkerID, args.Metrics, "")
		if args.TaskType == MapTask {
			for r := 0; r < job.Config.NReduce; r++ {
				m.tm.MarkMapDoneForReduce(args.TaskID, r)
			}
		}
	} else {
		m.tm.CompleteTask(task, false, args.WorkerID, args.Metrics, args.FailureReason)
	}
	if job.State == JobFailed {
		return nil
	}
	if m.allTasksCompleteLocked(job) {
		m.completeJobLocked(job, time.Now())
	}

	return nil
}

// Heartbeat updates worker liveness.
func (m *Master) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	m.mu.Lock()
	tm := m.tm
	m.mu.Unlock()
	if tm != nil {
		tm.Heartbeat(args.WorkerID)
	}
	reply.Acknowledged = true
	return nil
}

// Serve starts RPC and HTTP servers.
func (m *Master) Serve() error {
	rpc.Register(m)
	rpc.HandleHTTP()

	l, err := net.Listen("tcp", m.rpcAddr)
	if err != nil {
		return fmt.Errorf("rpc listen: %w", err)
	}
	log.Printf("Master RPC listening on %s", m.rpcAddr)

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				select {
				case <-m.done:
					return
				default:
					log.Printf("rpc accept: %v", err)
					continue
				}
			}
			go rpc.ServeConn(conn)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/job", m.withCORS(m.handleSubmitJob))
	mux.HandleFunc("/api/recover", m.withCORS(m.handleRecoverJob))
	mux.HandleFunc("/api/recoverable", m.withCORS(m.handleListRecoverable))
	mux.HandleFunc("/api/status", m.withCORS(m.handleStatus))
	mux.HandleFunc("/api/result", m.withCORS(m.handleResult))
	mux.HandleFunc("/api/dashboard", m.withCORS(m.handleDashboard))
	mux.Handle("/", http.FileServer(http.Dir("web")))

	log.Printf("Master HTTP listening on %s (dashboard: http://localhost%s/)", m.httpAddr, m.httpAddr)
	if err := m.LoadArchivedJobsFromCheckpoints(); err != nil {
		log.Printf("checkpoint archive load: %v", err)
	}
	if jobs, err := ListRecoverableJobs(); err != nil {
		log.Printf("checkpoint scan: %v", err)
	} else if len(jobs) > 0 {
		log.Printf("recoverable jobs: %d (POST /api/recover {\"job_id\":\"...\"})", len(jobs))
		for _, j := range jobs {
			log.Printf("  - %s map %d/%d reduce %d/%d saved %s",
				j.ID, j.MapDone, j.MapTotal, j.ReduceDone, j.ReduceTotal, j.SavedAt.Format(time.RFC3339))
		}
	}
	return http.ListenAndServe(m.httpAddr, mux)
}

func (m *Master) Shutdown() {
	close(m.done)
}

type submitJobRequest struct {
	InputFiles      []string `json:"input_files"`
	NReduce         int      `json:"n_reduce"`
	MapFunc         string   `json:"map_func"`
	ReduceFunc      string   `json:"reduce_func"`
	CombineFunc     string   `json:"combine_func"`
	SplitSize       int64    `json:"split_size"`
	WorkDir         string   `json:"work_dir"`
	ReduceSlowStart float64  `json:"reduce_slow_start"`
}

type submitJobResponse struct {
	JobID string `json:"job_id"`
	State string `json:"state"`
}

func (m *Master) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req submitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	job, err := m.StartJob(JobConfig{
		InputFiles:      req.InputFiles,
		NReduce:         req.NReduce,
		MapFunc:         req.MapFunc,
		ReduceFunc:      req.ReduceFunc,
		CombineFunc:     req.CombineFunc,
		SplitSize:       req.SplitSize,
		WorkDir:         req.WorkDir,
		ReduceSlowStart: req.ReduceSlowStart,
	})
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, ErrJobAlreadyRunning) {
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(submitJobResponse{
		JobID: job.ID,
		State: job.State.String(),
	})
}

type recoverJobRequest struct {
	JobID string `json:"job_id"`
}

func (m *Master) handleRecoverJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req recoverJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	job, err := m.RecoverJob(req.JobID)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, ErrJobAlreadyRunning) {
			status = http.StatusConflict
		}
		if errors.Is(err, ErrJobNotRecoverable) {
			status = http.StatusBadRequest
		}
		http.Error(w, err.Error(), status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(submitJobResponse{
		JobID: job.ID,
		State: job.State.String(),
	})
}

func (m *Master) handleListRecoverable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	jobs, err := ListRecoverableJobs()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if jobs == nil {
		jobs = []RecoverableJobSummary{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"jobs": jobs,
	})
}

func (m *Master) withCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

func (m *Master) tryAutoRecover(jobID string) (*Job, error) {
	if jobID == "" {
		return nil, fmt.Errorf("empty job id")
	}
	m.mu.Lock()
	if job, ok := m.jobs[jobID]; ok {
		m.mu.Unlock()
		return job, nil
	}
	m.mu.Unlock()

	cp, err := loadJobCheckpoint(jobID)
	if err != nil {
		return nil, err
	}
	if !isRecoverableCheckpoint(cp) {
		return nil, ErrJobNotRecoverable
	}
	return m.RecoverJob(jobID)
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func (m *Master) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	jobID := r.URL.Query().Get("job")
	m.mu.Lock()
	job := m.lookupJobLocked(jobID)
	hasCurrent := m.current != nil
	m.mu.Unlock()

	if job == nil {
		target := jobID
		if target == "" && !hasCurrent {
			if list, err := ListRecoverableJobs(); err == nil && len(list) > 0 {
				target = list[0].ID
				log.Printf("dashboard: auto-recovering latest interrupted job %s", target)
			}
		}
		if target != "" {
			if recovered, err := m.tryAutoRecover(target); err == nil {
				job = recovered
			} else if errors.Is(err, ErrJobAlreadyRunning) {
				log.Printf("dashboard: skip auto-recover %s: %v", target, err)
			}
		}
	}

	if job == nil {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(m.BuildDashboardSnapshot(nil))
		return
	}

	snap := m.BuildDashboardSnapshot(job)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(snap)
}

func (m *Master) handleStatus(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("job")
	if jobID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "job id is required",
		})
		return
	}

	m.mu.Lock()
	job := m.lookupJobLocked(jobID)
	m.mu.Unlock()

	if job == nil {
		recovered, err := m.tryAutoRecover(jobID)
		if err == nil {
			job = recovered
			log.Printf("status poll: auto-recovered job %s", jobID)
		} else if errors.Is(err, ErrJobAlreadyRunning) {
			if payload, cpErr := StatusFromCheckpoint(jobID); cpErr == nil {
				writeJSON(w, http.StatusOK, payload)
				return
			}
		}
	}
	if job == nil {
		if payload, err := StatusFromCheckpoint(jobID); err == nil {
			writeJSON(w, http.StatusOK, payload)
			return
		}
		writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"error":  "job not found",
			"job_id": jobID,
		})
		return
	}

	m.mu.Lock()
	mapDone, mapTotal := countCompleted(job.MapTasks)
	reduceDone, reduceTotal := countCompleted(job.ReduceTasks)
	state := job.State.String()
	switch job.State {
	case JobFailed, JobCompleted:
		// Terminal states must not be overwritten by progress heuristics.
	default:
		if m.allTasksCompleteLocked(job) {
			m.completeJobLocked(job, time.Now())
			state = JobCompleted.String()
		} else if mapDone < mapTotal || reduceDone < reduceTotal {
			job.State = JobRunning
			state = JobRunning.String()
		}
	}
	errMsg := job.Error
	m.mu.Unlock()

	payload := map[string]interface{}{
		"job_id":           job.ID,
		"state":            state,
		"map_completed":    mapDone,
		"map_total":        mapTotal,
		"reduce_completed": reduceDone,
		"reduce_total":     reduceTotal,
	}
	if errMsg != "" {
		payload["error"] = errMsg
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

func countCompleted(tasks []*Task) (done, total int) {
	total = len(tasks)
	for _, t := range tasks {
		if t.State == Completed {
			done++
		}
	}
	return
}

func (m *Master) handleResult(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("job")
	m.mu.Lock()
	job := m.lookupJobLocked(jobID)
	m.mu.Unlock()

	if job == nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	var outputFiles []string
	for rID := 0; rID < job.Config.NReduce; rID++ {
		outputFiles = append(outputFiles, filepath.Join(job.Config.WorkDir, fmt.Sprintf("mr-out-%d", rID)))
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"job_id":       job.ID,
		"state":        job.State.String(),
		"output_files": outputFiles,
		"work_dir":     job.Config.WorkDir,
	})
}

func (m *Master) lookupJobLocked(jobID string) *Job {
	if jobID == "" {
		return m.current
	}
	return m.jobs[jobID]
}

// GetJob returns a job by ID (for testing).
func (m *Master) GetJob(id string) *Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.jobs[id]
}

// CompleteJobForTest marks all tasks completed and finalizes worker snapshots/logs.
func (m *Master) CompleteJobForTest(job *Job) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range job.MapTasks {
		t.State = Completed
	}
	for _, t := range job.ReduceTasks {
		t.State = Completed
	}
	job.State = JobCompleted
	job.CompletedAt = time.Now()
	m.finalizeJobLocked(job, job.CompletedAt)
}

// GetCurrentJob returns the active job (for testing).
func (m *Master) GetCurrentJob() *Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current
}
