package mr

// RequestTaskArgs is sent by a Worker when asking for work.
type RequestTaskArgs struct {
	WorkerID string
}

// RequestTaskReply carries the next task assignment.
type RequestTaskReply struct {
	TaskType    TaskType
	TaskID      int
	InputFile   string
	InputOffset int64
	InputLength int64
	NReduce     int
	NMap        int
	ReduceID    int
	MapFunc     string
	ReduceFunc  string
	CombineFunc string
	WorkDir     string
	JobID       string
	JobState    string // running / failed / completed — worker uses this to stop work on a dead job
	AttemptID   int    // worker must echo this back in ReportTask
}

// ReportTaskArgs reports task completion or failure.
type ReportTaskArgs struct {
	WorkerID  string
	JobID     string
	TaskType  TaskType
	TaskID    int
	AttemptID int // must match the current attempt; stale reports are ignored
	Success   bool
	Metrics   TaskMetrics
	// FailureReason is set when Success=false; used to distinguish data faults from worker faults.
	FailureReason string
}

// ReportTaskReply acknowledges a task report.
type ReportTaskReply struct {
	OK bool
}

// HeartbeatArgs keeps the Worker alive on the Master.
type HeartbeatArgs struct {
	WorkerID string
}

// HeartbeatReply acknowledges a heartbeat.
type HeartbeatReply struct {
	Acknowledged bool
}

// RegisterWorkerArgs registers a new Worker.
type RegisterWorkerArgs struct {
	WorkerID string
	Address  string
}

// RegisterWorkerReply confirms registration.
type RegisterWorkerReply struct {
	OK bool
}
