package mr

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleStatusPreservesFailedState(t *testing.T) {
	master := NewMaster(":0", ":0")
	job := &Job{
		ID:     "status-failed",
		State:  JobFailed,
		Error:  "输入数据不可用：gone.txt",
		Config: JobConfig{NMap: 2, NReduce: 1},
		MapTasks: []*Task{
			{ID: 0, Type: MapTask, State: Failed},
			{ID: 1, Type: MapTask, State: Failed},
		},
		ReduceTasks: []*Task{{ID: 0, Type: ReduceTask, State: Failed}},
	}
	master.mu.Lock()
	master.jobs[job.ID] = job
	master.current = job
	master.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/api/status?job="+job.ID, nil)
	rec := httptest.NewRecorder()
	master.handleStatus(rec, req)

	var payload map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if payload["state"] != "failed" {
		t.Fatalf("expected failed state in status, got %v", payload["state"])
	}
	if payload["error"] != job.Error {
		t.Fatalf("expected error in status, got %v", payload["error"])
	}
	if master.GetJob(job.ID).State != JobFailed {
		t.Fatalf("handleStatus must not revert JobFailed, got %s", master.GetJob(job.ID).State)
	}
}
