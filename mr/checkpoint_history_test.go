package mr

import (
	"testing"
	"time"
)

func TestLoadArchivedJobsRestoresHistory(t *testing.T) {
	dir := t.TempDir()
	old := CheckpointDir
	CheckpointDir = dir
	t.Cleanup(func() { CheckpointDir = old })

	completed := &Job{
		ID:          "done-history",
		State:       JobCompleted,
		CreatedAt:   time.Now().Add(-time.Hour),
		CompletedAt: time.Now().Add(-30 * time.Minute),
		Config: JobConfig{
			NMap:       2,
			NReduce:    1,
			WorkDir:    t.TempDir(),
			InputFiles: []string{"a.txt"},
		},
		MapTasks:         []*Task{{ID: 0, Type: MapTask, State: Completed}, {ID: 1, Type: MapTask, State: Completed}},
		ReduceTasks:      []*Task{{ID: 0, Type: ReduceTask, State: Completed}},
		MapDoneForReduce: [][]bool{{true, true}},
	}
	if err := SaveJobCheckpoint(completed); err != nil {
		t.Fatal(err)
	}

	master := NewMaster(":0", ":0")
	if err := master.LoadArchivedJobsFromCheckpoints(); err != nil {
		t.Fatal(err)
	}
	if master.GetJob("done-history") == nil {
		t.Fatal("completed job should be loaded from checkpoint")
	}
	all, err := ListAllCheckpointJobs()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || all[0].State != JobCompleted.String() {
		t.Fatalf("expected completed checkpoint summary, got %+v", all)
	}
	snap := master.BuildDashboardSnapshot(master.GetJob("done-history"))
	if len(snap.JobHistory) == 0 {
		t.Fatal("expected job history to include archived job")
	}
}

func TestListAllCheckpointJobsIncludesTerminalStates(t *testing.T) {
	dir := t.TempDir()
	old := CheckpointDir
	CheckpointDir = dir
	t.Cleanup(func() { CheckpointDir = old })

	for id, state := range map[string]JobState{
		"job-done": JobCompleted,
		"job-fail": JobFailed,
		"job-run":  JobRunning,
	} {
		job := &Job{
			ID: id, State: state, CreatedAt: time.Now(),
			Config:           JobConfig{NMap: 1, NReduce: 1, WorkDir: t.TempDir()},
			MapTasks:         []*Task{{ID: 0, Type: MapTask, State: Idle}},
			ReduceTasks:      []*Task{{ID: 0, Type: ReduceTask, State: Idle}},
			MapDoneForReduce: [][]bool{{false}},
		}
		if state == JobCompleted {
			job.MapTasks[0].State = Completed
			job.ReduceTasks[0].State = Completed
		}
		if err := SaveJobCheckpoint(job); err != nil {
			t.Fatal(err)
		}
	}

	all, err := ListAllCheckpointJobs()
	if err != nil || len(all) != 3 {
		t.Fatalf("expected 3 checkpoints, got %d err=%v", len(all), err)
	}
	states := map[string]int{}
	for _, row := range all {
		states[row.State]++
	}
	if states["completed"] != 1 || states["failed"] != 1 || states["recoverable"] != 1 {
		t.Fatalf("unexpected states: %v", states)
	}
}
