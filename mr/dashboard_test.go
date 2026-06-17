package mr

import (
	"strings"
	"testing"
)

func TestDashboardFailedJobKeepsFailureBanner(t *testing.T) {
	job := &Job{
		ID:     "failed-job",
		Config: JobConfig{NMap: 2, NReduce: 1},
		State:  JobFailed,
		Error:  "输入数据不可用：gone.txt (no such file)",
		MapTasks: []*Task{
			{ID: 0, Type: MapTask, State: Failed},
			{ID: 1, Type: MapTask, State: Idle},
		},
		ReduceTasks: []*Task{{ID: 0, Type: ReduceTask, State: Idle}},
		Decisions: []DecisionEvent{
			{Type: DecisionFail, Message: "map-0 失败"},
			{Type: DecisionJobFailed, Message: "════ 作业失败 ════ Map 0/2 · Reduce 0/1 · 输入数据不可用：gone.txt"},
		},
	}

	tm := NewTaskManager(job)
	tm.decisions = []DecisionEvent{{Type: DecisionFail, Message: "map-0 失败"}}

	m := NewMaster(":0", ":0")
	m.mu.Lock()
	m.current = job
	m.tm = tm
	m.jobs[job.ID] = job
	m.mu.Unlock()

	snap := m.BuildDashboardSnapshot(job)
	if len(snap.Decisions) == 0 {
		t.Fatal("expected decisions on failed job snapshot")
	}
	last := snap.Decisions[len(snap.Decisions)-1]
	if last.Type != DecisionJobFailed {
		t.Fatalf("expected terminal job_failed decision, got %s", last.Type)
	}
	if !strings.Contains(last.Message, "════ 作业失败") {
		t.Fatalf("expected failure banner, got %q", last.Message)
	}
}
