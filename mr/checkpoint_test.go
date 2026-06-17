package mr

import (
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSaveAndLoadCheckpoint(t *testing.T) {
	dir := t.TempDir()
	old := CheckpointDir
	CheckpointDir = dir
	t.Cleanup(func() { CheckpointDir = old })

	job := &Job{
		ID:        "cp-test",
		State:     JobRunning,
		CreatedAt: time.Now(),
		Config:    JobConfig{NMap: 2, NReduce: 1, WorkDir: t.TempDir()},
		MapTasks: []*Task{
			{ID: 0, Type: MapTask, State: Completed, MapInfo: &MapTaskInfo{Split: Split{File: "a.txt"}}},
			{ID: 1, Type: MapTask, State: Idle, MapInfo: &MapTaskInfo{Split: Split{File: "b.txt"}}},
		},
		ReduceTasks:      []*Task{{ID: 0, Type: ReduceTask, State: Idle}},
		MapDoneForReduce: [][]bool{{true, false}},
	}
	if err := SaveJobCheckpoint(job); err != nil {
		t.Fatal(err)
	}

	cp, err := loadJobCheckpoint("cp-test")
	if err != nil {
		t.Fatal(err)
	}
	if cp.State != JobRunning.String() || len(cp.MapTasks) != 2 {
		t.Fatalf("unexpected checkpoint: state=%s maps=%d", cp.State, len(cp.MapTasks))
	}
}

func TestReconcileMapFromWorkDir(t *testing.T) {
	workDir := t.TempDir()
	jobID := "reconcile-job"
	dataDir := JobWorkDir(workDir, jobID)
	nReduce := 2

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatal(err)
	}
	// map-0 fully written
	for r := 0; r < nReduce; r++ {
		path := intermediatePath(dataDir, 0, r)
		if err := atomicWriteBinary(path, []KeyValue{{Key: "a", Value: "1"}}); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(readyPath(path, jobID), []byte("ready\n"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	job := &Job{
		ID:    jobID,
		State: JobRunning,
		Config: JobConfig{
			NMap:    2,
			NReduce: nReduce,
			WorkDir: workDir,
		},
		MapTasks: []*Task{
			{ID: 0, Type: MapTask, State: Idle},
			{ID: 1, Type: MapTask, State: InProgress, WorkerID: "w1", AttemptID: 1},
		},
		ReduceTasks:      []*Task{{ID: 0, Type: ReduceTask, State: Idle}, {ID: 1, Type: ReduceTask, State: Idle}},
		MapDoneForReduce: [][]bool{{false, false}, {false, false}},
	}
	prepareInterruptedTasks(job)
	reconcileJobFromWorkDir(job)

	if job.MapTasks[0].State != Completed {
		t.Fatalf("map-0 should be completed from disk, got %s", job.MapTasks[0].State)
	}
	if !job.MapDoneForReduce[0][0] || !job.MapDoneForReduce[1][0] {
		t.Fatal("MapDoneForReduce should reflect map-0 completion")
	}
	if job.MapTasks[1].State != Idle {
		t.Fatalf("in-progress map should reset to idle, got %s", job.MapTasks[1].State)
	}
}

func TestPublishMapOutputCommitChoosesAcceptedAttempt(t *testing.T) {
	workDir := t.TempDir()
	jobID := "commit-attempt"
	dataDir := JobWorkDir(workDir, jobID)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatal(err)
	}
	path := intermediatePath(dataDir, 0, 0)

	if _, err := atomicWriteBinaryWithStats(path, []KeyValue{{Key: "old", Value: "1"}}, jobID, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := atomicWriteBinaryWithStats(path, []KeyValue{{Key: "new", Value: "1"}}, jobID, 2); err != nil {
		t.Fatal(err)
	}
	if err := publishMapOutputCommit(dataDir, jobID, 0, 1, 2); err != nil {
		t.Fatal(err)
	}

	committed, ok := committedIntermediatePath(path, jobID)
	if !ok {
		t.Fatal("expected committed intermediate path")
	}
	r, err := openKVStream(committed)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	kv, ok, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !ok || kv.Key != "new" {
		t.Fatalf("expected committed attempt 2 data, got ok=%v kv=%+v", ok, kv)
	}
}

func TestKVStreamReaderReportsTruncatedStream(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broken")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gw := gzip.NewWriter(f)
	if _, err := gw.Write([]byte{0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := gw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := openKVStream(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, _, err := r.Next(); err == nil {
		t.Fatal("expected truncated stream error")
	}
}

func TestReduceOutputReadyAcceptsCommittedEmptyOutput(t *testing.T) {
	workDir := t.TempDir()
	jobID := "empty-reduce"
	dataDir := JobWorkDir(workDir, jobID)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatal(err)
	}
	attemptPath := reduceAttemptPath(dataDir, 0, 1)
	if err := os.WriteFile(attemptPath, nil, 0644); err != nil {
		t.Fatal(err)
	}
	if err := commitReduceOutput(dataDir, jobID, 0, 1); err != nil {
		t.Fatal(err)
	}
	if !reduceOutputReady(dataDir, jobID, 0) {
		t.Fatal("empty committed reduce output should be ready")
	}
}

func TestRecoverJobResumesScheduling(t *testing.T) {
	root := t.TempDir()
	cpDir := filepath.Join(root, "checkpoints")
	workDir := filepath.Join(root, "work")
	oldCP := CheckpointDir
	CheckpointDir = cpDir
	t.Cleanup(func() { CheckpointDir = oldCP })

	if err := os.MkdirAll(workDir, 0755); err != nil {
		t.Fatal(err)
	}
	dataDir := JobWorkDir(workDir, "recover-me")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatal(err)
	}
	for r := 0; r < 1; r++ {
		path := intermediatePath(dataDir, 0, r)
		if err := atomicWriteBinary(path, []KeyValue{{Key: "x", Value: "1"}}); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(readyPath(path, "recover-me"), []byte("ready\n"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	job := &Job{
		ID:        "recover-me",
		State:     JobRunning,
		CreatedAt: time.Now(),
		Config: JobConfig{
			NMap:    2,
			NReduce: 1,
			WorkDir: workDir,
			MapFunc: "wordcount_map",
		},
		MapTasks: []*Task{
			{ID: 0, Type: MapTask, State: Completed, MapInfo: &MapTaskInfo{Split: Split{File: filepath.Join("testdata", "input.txt")}}},
			{ID: 1, Type: MapTask, State: InProgress, WorkerID: "w1", AttemptID: 2, MapInfo: &MapTaskInfo{Split: Split{File: filepath.Join("testdata", "input.txt")}}},
		},
		ReduceTasks:      []*Task{{ID: 0, Type: ReduceTask, State: Idle}},
		MapDoneForReduce: [][]bool{{true, false}},
	}
	if err := SaveJobCheckpoint(job); err != nil {
		t.Fatal(err)
	}

	master := NewMaster(":0", ":0")
	recovered, err := master.RecoverJob("recover-me")
	if err != nil {
		t.Fatal(err)
	}
	if recovered.MapTasks[0].State != Completed {
		t.Fatal("map-0 should stay completed after reconcile")
	}
	if recovered.MapTasks[1].State != Idle {
		t.Fatalf("map-1 should be idle for reschedule, got %s", recovered.MapTasks[1].State)
	}

	var reply RequestTaskReply
	master.RequestTask(&RequestTaskArgs{WorkerID: "w2"}, &reply)
	if reply.TaskType != MapTask || reply.TaskID != 1 {
		t.Fatalf("expected map-1 assignment, got %v id=%d", reply.TaskType, reply.TaskID)
	}
}

func TestListRecoverableJobs(t *testing.T) {
	dir := t.TempDir()
	old := CheckpointDir
	CheckpointDir = dir
	t.Cleanup(func() { CheckpointDir = old })

	running := &Job{
		ID: "run-1", State: JobRunning, CreatedAt: time.Now(),
		Config:           JobConfig{NMap: 1, NReduce: 1, WorkDir: t.TempDir()},
		MapTasks:         []*Task{{ID: 0, Type: MapTask, State: Idle}},
		ReduceTasks:      []*Task{{ID: 0, Type: ReduceTask, State: Idle}},
		MapDoneForReduce: [][]bool{{false}},
	}
	done := &Job{
		ID: "done-1", State: JobCompleted, CreatedAt: time.Now(),
		Config:           JobConfig{NMap: 1, NReduce: 1, WorkDir: t.TempDir()},
		MapTasks:         []*Task{{ID: 0, Type: MapTask, State: Completed}},
		ReduceTasks:      []*Task{{ID: 0, Type: ReduceTask, State: Completed}},
		MapDoneForReduce: [][]bool{{true}},
	}
	_ = SaveJobCheckpoint(running)
	_ = SaveJobCheckpoint(done)

	list, err := ListRecoverableJobs()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ID != "run-1" {
		t.Fatalf("expected only running incomplete job, got %+v", list)
	}
}
