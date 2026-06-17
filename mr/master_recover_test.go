package mr

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleStatusAutoRecoversCheckpointJob(t *testing.T) {
	dir := t.TempDir()
	old := CheckpointDir
	CheckpointDir = dir
	t.Cleanup(func() { CheckpointDir = old })

	workDir := t.TempDir()
	job := &Job{
		ID:               "auto-recover-status",
		State:            JobRunning,
		Config:           JobConfig{NMap: 2, NReduce: 1, WorkDir: workDir},
		MapTasks:         []*Task{{ID: 0, Type: MapTask, State: Completed}, {ID: 1, Type: MapTask, State: Idle}},
		ReduceTasks:      []*Task{{ID: 0, Type: ReduceTask, State: Idle}},
		MapDoneForReduce: [][]bool{{true, false}},
	}
	if err := SaveJobCheckpoint(job); err != nil {
		t.Fatal(err)
	}

	master := NewMaster(":0", ":0")
	req := httptest.NewRequest(http.MethodGet, "/api/status?job=auto-recover-status", nil)
	rec := httptest.NewRecorder()
	master.handleStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var payload map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if payload["state"] != "running" {
		t.Fatalf("expected running after auto-recover, got %v", payload["state"])
	}
	if master.GetJob("auto-recover-status") == nil {
		t.Fatal("job should be loaded into master memory")
	}
}
