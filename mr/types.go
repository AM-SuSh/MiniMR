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
	InputFiles      []string
	NReduce         int
	NMap            int
	MapFunc         string
	ReduceFunc      string
	CombineFunc     string
	SplitSize       int64
	WorkDir         string
	ReduceSlowStart float64 // fraction of maps that must complete before reduce scheduling (0.0-1.0)
}

// TaskMetrics carries runtime counters from workers back to the master.
type TaskMetrics struct {
	InputBytes              int64
	MapOutputRecords        int64
	CombineOutputRecords    int64
	ShuffleFiles            int64
	ShuffleJSONBytes        int64
	ShuffleBinaryBytes      int64
	ShuffleCompressedBytes  int64
	ShuffleWriteMs          int64
	ShuffleWaitMs           int64
	ReduceReadMs            int64
	ReduceWriteMs           int64
	ReduceStreamedRecords   int64
	ReduceOutputKeys        int64
	ReduceMaxBufferedValues int64
	ReduceOpenedStreams     int64
}

// Add folds another metrics sample into the receiver.
func (m *TaskMetrics) Add(other TaskMetrics) {
	m.InputBytes += other.InputBytes
	m.MapOutputRecords += other.MapOutputRecords
	m.CombineOutputRecords += other.CombineOutputRecords
	m.ShuffleFiles += other.ShuffleFiles
	m.ShuffleJSONBytes += other.ShuffleJSONBytes
	m.ShuffleBinaryBytes += other.ShuffleBinaryBytes
	m.ShuffleCompressedBytes += other.ShuffleCompressedBytes
	m.ShuffleWriteMs += other.ShuffleWriteMs
	m.ShuffleWaitMs += other.ShuffleWaitMs
	m.ReduceReadMs += other.ReduceReadMs
	m.ReduceWriteMs += other.ReduceWriteMs
	m.ReduceStreamedRecords += other.ReduceStreamedRecords
	m.ReduceOutputKeys += other.ReduceOutputKeys
	if other.ReduceMaxBufferedValues > m.ReduceMaxBufferedValues {
		m.ReduceMaxBufferedValues = other.ReduceMaxBufferedValues
	}
	m.ReduceOpenedStreams += other.ReduceOpenedStreams
}

// JobMetrics aggregates optimization and reliability counters for one job.
type JobMetrics struct {
	InputBytes              int64
	MapOutputRecords        int64
	CombineOutputRecords    int64
	ShuffleFiles            int64
	ShuffleJSONBytes        int64
	ShuffleBinaryBytes      int64
	ShuffleCompressedBytes  int64
	ShuffleWriteMs          int64
	ReduceShuffleWaitMs     int64
	ReduceReadMs            int64
	ReduceWriteMs           int64
	ReduceStreamedRecords   int64
	ReduceOutputKeys        int64
	ReduceMaxBufferedValues int64
	ReduceOpenedStreams     int64
	EarlyReduceStarts       int64
	TaskFailures            int64
	TaskTimeouts            int64
	WorkerTimeouts          int64
	BlacklistedWorkers      int64
	SpeculativeRequeues     int64
	StaleReports            int64
	Retries                 int64
}

// AddTask folds a completed task sample into job-level metrics.
func (m *JobMetrics) AddTask(sample TaskMetrics) {
	m.InputBytes += sample.InputBytes
	m.MapOutputRecords += sample.MapOutputRecords
	m.CombineOutputRecords += sample.CombineOutputRecords
	m.ShuffleFiles += sample.ShuffleFiles
	m.ShuffleJSONBytes += sample.ShuffleJSONBytes
	m.ShuffleBinaryBytes += sample.ShuffleBinaryBytes
	m.ShuffleCompressedBytes += sample.ShuffleCompressedBytes
	m.ShuffleWriteMs += sample.ShuffleWriteMs
	m.ReduceShuffleWaitMs += sample.ShuffleWaitMs
	m.ReduceReadMs += sample.ReduceReadMs
	m.ReduceWriteMs += sample.ReduceWriteMs
	m.ReduceStreamedRecords += sample.ReduceStreamedRecords
	m.ReduceOutputKeys += sample.ReduceOutputKeys
	if sample.ReduceMaxBufferedValues > m.ReduceMaxBufferedValues {
		m.ReduceMaxBufferedValues = sample.ReduceMaxBufferedValues
	}
	m.ReduceOpenedStreams += sample.ReduceOpenedStreams
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
	ID                int
	Type              TaskType
	State             TaskState
	WorkerID          string
	StartTime         time.Time
	MapInfo           *MapTaskInfo
	ReduceID          int
	AttemptID         int // monotonically increasing; each (re)assignment bumps this
	RetryCount        int // incremented on hard timeout or explicit failure
	LastFailureReason string
}

// WorkerInfo tracks a registered worker.
type WorkerInfo struct {
	ID            string
	LastHeartbeat time.Time
	CurrentTask   int
	CurrentType   TaskType
	FailureCount  int  // consecutive task failures reported by this worker
	Blacklisted   bool // true → worker receives ExitTask, no new assignments
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
	Metrics          JobMetrics
	Decisions        []DecisionEvent
	WorkerSnapshot   []DashboardWorker // frozen at job finish for historical dashboard
	jobLog           *jobLogWriter
}

const (
	DefaultTaskTimeout        = 10 * time.Second
	DefaultLargeTaskTimeout   = 120 * time.Second
	LargeTaskTimeoutThreshold = 8 * 1024 * 1024
	DefaultWorkerTimeout      = 30 * time.Second
	DefaultSplitSize          = 32 * 1024 * 1024
	DefaultMaxSplitScan       = 4 * 1024 * 1024
	DefaultHeartbeat          = 5 * time.Second
	DefaultReduceSlowStart    = 0.8
	DefaultReduceTaskTimeout  = 120 * time.Second
	ReduceShuffleTimeout      = 5 * time.Minute
	ReduceShufflePollInterval = 500 * time.Millisecond

	DefaultMaxRetries        = 3   // per-task retry limit before job failure
	DefaultMaxWorkerFailures = 3   // consecutive failures before worker blacklist
	SpeculativeMinCompleted  = 3   // need N completed tasks before speculative checks
	SpeculativeMultiplier    = 1.5 // task running > multiplier × median → speculative re-exec
)
