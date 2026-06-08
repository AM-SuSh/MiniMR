package mr

import "time"

// KeyValue is the basic data unit flowing through MapReduce.
type KeyValue struct {
	Key   string
	Value string
}

// TaskType identifies the kind of work assigned to a Worker.
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

func (t TaskType) String() string {
	switch t {
	case MapTask:
		return "map"
	case ReduceTask:
		return "reduce"
	case WaitTask:
		return "wait"
	case ExitTask:
		return "exit"
	default:
		return "unknown"
	}
}

// TaskState tracks lifecycle of an individual task.
type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
	Failed
)

func (s TaskState) String() string {
	switch s {
	case Idle:
		return "idle"
	case InProgress:
		return "in_progress"
	case Completed:
		return "completed"
	case Failed:
		return "failed"
	default:
		return "unknown"
	}
}

// JobState tracks overall job progress.
type JobState int

const (
	JobPending JobState = iota
	JobRunning
	JobCompleted
	JobFailed
)

func (s JobState) String() string {
	switch s {
	case JobPending:
		return "pending"
	case JobRunning:
		return "running"
	case JobCompleted:
		return "completed"
	case JobFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// JobConfig holds parameters for a MapReduce job.
type JobConfig struct {
	InputFiles  []string
	NReduce     int
	NMap        int
	MapFunc     string
	ReduceFunc  string
	CombineFunc string
	SplitSize   int64
	WorkDir     string
}

// Split describes one map input slice.
type Split struct {
	File   string
	Offset int64
	Length int64
}

// MapTaskInfo holds map-specific metadata.
type MapTaskInfo struct {
	Split Split
}

// Task represents a schedulable unit of work.
type Task struct {
	ID        int
	Type      TaskType
	State     TaskState
	WorkerID  string
	StartTime time.Time
	MapInfo   *MapTaskInfo
	ReduceID  int
}

// WorkerInfo tracks a registered worker.
type WorkerInfo struct {
	ID            string
	LastHeartbeat time.Time
	CurrentTask   int
	CurrentType   TaskType
}

// Job represents one submitted MapReduce job.
type Job struct {
	ID          string
	Config      JobConfig
	State       JobState
	MapTasks    []*Task
	ReduceTasks []*Task
	// mapDoneForReduce[reduceID][mapID] = true when map task wrote partition file
	MapDoneForReduce [][]bool
	CreatedAt        time.Time
	CompletedAt      time.Time
	Error            string
}

const (
	DefaultTaskTimeout   = 10 * time.Second
	DefaultWorkerTimeout = 30 * time.Second
	DefaultSplitSize     = 64 * 1024
	DefaultHeartbeat     = 5 * time.Second
)
