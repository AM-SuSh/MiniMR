package main_test

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"mapreduce/mr"
	"mapreduce/udf"

	_ "mapreduce/udf"
)

func TestSplitInput(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	content := strings.Repeat("hello world\n", 100)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	splits, err := mr.SplitInput([]string{path}, 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(splits) < 2 {
		t.Fatalf("expected multiple splits, got %d", len(splits))
	}

	var total int64
	for _, s := range splits {
		total += s.Length
	}
	info, _ := os.Stat(path)
	if total != info.Size() {
		t.Fatalf("split total %d != file size %d", total, info.Size())
	}
}

func TestSplitInputRejectsOverlongLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	content := strings.Repeat("a", mr.DefaultMaxSplitScan+2)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	if _, err := mr.SplitInput([]string{path}, 1); err == nil {
		t.Fatal("expected overlong line to be rejected")
	}
}

func TestWordCountUDF(t *testing.T) {
	input := "hello world hello Go 分布式"
	kvs := udf.WordCountMap("test", input)
	if len(kvs) != 5 {
		t.Fatalf("expected 5 kvs, got %d", len(kvs))
	}

	combined := udf.WordCountCombine("hello", []string{"1", "1", "1"})
	if combined != "3" {
		t.Fatalf("expected 3, got %s", combined)
	}
}

func TestCrawlCleanUDF(t *testing.T) {
	line := `{"url":"https://news.example.cn/a","html":"<p>测试 <b>HTML</b></p>","timestamp":"2026-01-01"}`
	kvs := udf.CrawlCleanMap("test", line+"\n")
	if len(kvs) != 1 {
		t.Fatalf("expected 1 kv, got %d", len(kvs))
	}
	if kvs[0].Key != "news.example.cn" {
		t.Fatalf("expected news.example.cn, got %s", kvs[0].Key)
	}

	out := udf.CrawlCleanReduce(kvs[0].Key, []string{kvs[0].Value, kvs[0].Value})
	if !strings.Contains(out, `"unique_count":1`) {
		t.Fatalf("dedup failed: %s", out)
	}
}

func TestTaskManagerTimeout(t *testing.T) {
	job := &mr.Job{
		MapTasks: []*mr.Task{{ID: 0, Type: mr.MapTask, State: mr.InProgress, StartTime: time.Now().Add(-20 * time.Second)}},
	}
	tm := mr.NewTaskManager(job)
	done := make(chan struct{})
	go tm.StartMonitor(done)
	time.Sleep(1500 * time.Millisecond)
	close(done)

	if job.MapTasks[0].State != mr.Idle {
		t.Fatalf("expected idle after timeout, got %s", job.MapTasks[0].State)
	}
}

func TestDistributedWordCount(t *testing.T) {
	rpcAddr := freePort(t)
	httpAddr := freePort(t)

	master := mr.NewMaster(rpcAddr, httpAddr)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- master.Serve()
	}()
	t.Cleanup(func() {
		master.Shutdown()
		select {
		case err := <-serveDone:
			if err != nil {
				t.Errorf("master serve: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("master Serve did not stop after Shutdown")
		}
	})
	time.Sleep(500 * time.Millisecond)

	workDir := t.TempDir()
	inputPath := filepath.Join("testdata", "input.txt")

	job, err := master.StartJob(mr.JobConfig{
		InputFiles:  []string{inputPath},
		NReduce:     3,
		MapFunc:     "wordcount_map",
		ReduceFunc:  "wordcount_reduce",
		CombineFunc: "wordcount_combine",
		WorkDir:     workDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 2; i++ {
		w := mr.NewWorker(fmt.Sprintf("test-worker-%d", i), rpcAddr)
		go w.Run()
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		j := master.GetJob(job.ID)
		if j != nil && allComplete(j) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	j := master.GetJob(job.ID)
	if j == nil || !allComplete(j) {
		t.Fatal("job did not complete in time")
	}

	counts := readOutputCounts(t, workDir, j.ID, j.Config.NReduce)
	if counts["hello"] < 2 {
		t.Fatalf("expected hello count >= 2, got %d (full: %v)", counts["hello"], counts)
	}
}

func TestWorkerWaitsAfterJobComplete(t *testing.T) {
	master := mr.NewMaster(":0", ":0")
	workDir := t.TempDir()
	job, err := master.StartJob(mr.JobConfig{
		InputFiles:  []string{filepath.Join("testdata", "input.txt")},
		NReduce:     1,
		MapFunc:     "wordcount_map",
		ReduceFunc:  "wordcount_reduce",
		CombineFunc: "wordcount_combine",
		WorkDir:     workDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, task := range job.MapTasks {
		task.State = mr.Completed
	}
	for _, task := range job.ReduceTasks {
		task.State = mr.Completed
	}

	var reply mr.RequestTaskReply
	if err := master.RequestTask(&mr.RequestTaskArgs{WorkerID: "long-lived-worker"}, &reply); err != nil {
		t.Fatal(err)
	}
	if reply.TaskType != mr.WaitTask {
		t.Fatalf("expected WaitTask after job completion, got %s", reply.TaskType)
	}
	if job.State != mr.JobCompleted {
		t.Fatalf("expected job to be marked completed, got %s", job.State)
	}
}

func TestStartJobRejectsWhileCurrentRunning(t *testing.T) {
	master := mr.NewMaster(":0", ":0")
	_, err := master.StartJob(mr.JobConfig{
		InputFiles:  []string{filepath.Join("testdata", "input.txt")},
		NReduce:     1,
		MapFunc:     "wordcount_map",
		ReduceFunc:  "wordcount_reduce",
		CombineFunc: "wordcount_combine",
		WorkDir:     t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = master.StartJob(mr.JobConfig{
		InputFiles:  []string{filepath.Join("testdata", "input.txt")},
		NReduce:     1,
		MapFunc:     "wordcount_map",
		ReduceFunc:  "wordcount_reduce",
		CombineFunc: "wordcount_combine",
		WorkDir:     t.TempDir(),
	})
	if !errors.Is(err, mr.ErrJobAlreadyRunning) {
		t.Fatalf("expected ErrJobAlreadyRunning, got %v", err)
	}
}

func TestStartJobAllowedAfterJobFailed(t *testing.T) {
	master := mr.NewMaster(":0", ":0")
	workDir := t.TempDir()
	job, err := master.StartJob(mr.JobConfig{
		InputFiles:  []string{filepath.Join("testdata", "input.txt")},
		NReduce:     1,
		SplitSize:   10,
		MapFunc:     "wordcount_map",
		ReduceFunc:  "wordcount_reduce",
		CombineFunc: "wordcount_combine",
		WorkDir:     workDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	var reply mr.RequestTaskReply
	master.RequestTask(&mr.RequestTaskArgs{WorkerID: "w1"}, &reply)
	var rr mr.ReportTaskReply
	master.ReportTask(&mr.ReportTaskArgs{
		WorkerID:      "w1",
		JobID:         job.ID,
		TaskType:      mr.MapTask,
		TaskID:        reply.TaskID,
		AttemptID:     reply.AttemptID,
		Success:       false,
		FailureReason: "input_read: open missing.txt: no such file",
	}, &rr)

	if master.GetJob(job.ID).State != mr.JobFailed {
		t.Fatalf("expected JobFailed, got %s", master.GetJob(job.ID).State)
	}

	_, err = master.StartJob(mr.JobConfig{
		InputFiles:  []string{filepath.Join("testdata", "input.txt")},
		NReduce:     1,
		MapFunc:     "wordcount_map",
		ReduceFunc:  "wordcount_reduce",
		CombineFunc: "wordcount_combine",
		WorkDir:     t.TempDir(),
	})
	if err != nil {
		t.Fatalf("expected new job after failure, got %v", err)
	}
}

func TestJobHistoryKeepsCompletedJobs(t *testing.T) {
	logDir := t.TempDir()
	oldLogDir := mr.JobLogDir
	mr.JobLogDir = logDir
	t.Cleanup(func() { mr.JobLogDir = oldLogDir })

	master := mr.NewMaster(":0", ":0")
	first, err := master.StartJob(mr.JobConfig{
		InputFiles:  []string{filepath.Join("testdata", "input.txt")},
		NReduce:     1,
		MapFunc:     "wordcount_map",
		ReduceFunc:  "wordcount_reduce",
		CombineFunc: "wordcount_combine",
		WorkDir:     t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	var reply mr.RequestTaskReply
	if err := master.RequestTask(&mr.RequestTaskArgs{WorkerID: "worker-1"}, &reply); err != nil {
		t.Fatal(err)
	}
	master.CompleteJobForTest(first)

	second, err := master.StartJob(mr.JobConfig{
		InputFiles:  []string{filepath.Join("testdata", "input.txt")},
		NReduce:     1,
		MapFunc:     "wordcount_map",
		ReduceFunc:  "wordcount_reduce",
		CombineFunc: "wordcount_combine",
		WorkDir:     t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if master.GetJob(first.ID) == nil {
		t.Fatal("completed first job should remain addressable by Job ID")
	}

	oldSnap := master.BuildDashboardSnapshot(first)
	if oldSnap.Job.ID != first.ID {
		t.Fatalf("expected old snapshot for %s, got %s", first.ID, oldSnap.Job.ID)
	}

	currentSnap := master.BuildDashboardSnapshot(second)
	seen := map[string]bool{}
	for _, item := range currentSnap.JobHistory {
		seen[item.ID] = true
	}
	if !seen[first.ID] || !seen[second.ID] {
		t.Fatalf("history should contain both jobs, got %#v", currentSnap.JobHistory)
	}

	if len(oldSnap.Workers) == 0 {
		t.Fatal("historical job should retain worker snapshot")
	}
	if _, err := os.Stat(mr.JobLogPath(first.ID)); err != nil {
		t.Fatalf("expected persisted job log: %v", err)
	}

	master.CompleteJobForTest(second)
}

func TestJobLogRecordsDecisions(t *testing.T) {
	logDir := t.TempDir()
	oldLogDir := mr.JobLogDir
	mr.JobLogDir = logDir
	t.Cleanup(func() { mr.JobLogDir = oldLogDir })

	master := mr.NewMaster(":0", ":0")
	job, err := master.StartJob(mr.JobConfig{
		InputFiles:  []string{filepath.Join("testdata", "input.txt")},
		NReduce:     1,
		MapFunc:     "wordcount_map",
		ReduceFunc:  "wordcount_reduce",
		CombineFunc: "wordcount_combine",
		WorkDir:     t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	var reply mr.RequestTaskReply
	if err := master.RequestTask(&mr.RequestTaskArgs{WorkerID: "worker-a"}, &reply); err != nil {
		t.Fatal(err)
	}
	master.CompleteJobForTest(job)

	data, err := os.ReadFile(mr.JobLogPath(job.ID))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, want := range []string{`"kind":"start"`, `"kind":"decision"`, `"kind":"finish"`, `"worker-a"`} {
		if !strings.Contains(text, want) {
			t.Fatalf("log missing %q:\n%s", want, text)
		}
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func allComplete(job *mr.Job) bool {
	for _, task := range job.MapTasks {
		if task.State != mr.Completed {
			return false
		}
	}
	for _, task := range job.ReduceTasks {
		if task.State != mr.Completed {
			return false
		}
	}
	return true
}

func completeTasks(job *mr.Job) {
	for _, task := range job.MapTasks {
		task.State = mr.Completed
	}
	for _, task := range job.ReduceTasks {
		task.State = mr.Completed
	}
}

func readOutputCounts(t *testing.T, workDir, jobID string, nReduce int) map[string]int {
	t.Helper()
	dataDir := mr.JobWorkDir(workDir, jobID)
	counts := make(map[string]int)
	for r := 0; r < nReduce; r++ {
		path := filepath.Join(dataDir, fmt.Sprintf("mr-out-%d", r))
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			parts := strings.SplitN(line, "\t", 2)
			if len(parts) == 2 {
				n, err := strconv.Atoi(parts[1])
				if err == nil {
					counts[parts[0]] += n
				}
			}
		}
	}
	return counts
}

func TestReduceSlowStartScheduling(t *testing.T) {
	job := &mr.Job{
		Config: mr.JobConfig{NMap: 5, NReduce: 2, ReduceSlowStart: 0.6},
		MapTasks: []*mr.Task{
			{ID: 0, Type: mr.MapTask, State: mr.Idle},
			{ID: 1, Type: mr.MapTask, State: mr.Idle},
			{ID: 2, Type: mr.MapTask, State: mr.Idle},
			{ID: 3, Type: mr.MapTask, State: mr.Idle},
			{ID: 4, Type: mr.MapTask, State: mr.Idle},
		},
		MapDoneForReduce: [][]bool{
			{false, false, false, false, false},
			{false, false, false, false, false},
		},
	}
	tm := mr.NewTaskManager(job)

	if tm.CanScheduleReduce() {
		t.Fatal("reduce should not be schedulable with 0/5 maps done")
	}

	job.MapTasks[0].State = mr.Completed
	job.MapTasks[1].State = mr.Completed
	if tm.CanScheduleReduce() {
		t.Fatal("reduce should not be schedulable with 2/5 (40%) maps done, threshold 60%")
	}

	job.MapTasks[2].State = mr.Completed
	if !tm.CanScheduleReduce() {
		t.Fatal("reduce should be schedulable with 3/5 (60%) maps done, threshold 60%")
	}
}

func TestStaleAttemptRejection(t *testing.T) {
	master := mr.NewMaster(":0", ":0")
	workDir := t.TempDir()
	job, err := master.StartJob(mr.JobConfig{
		InputFiles:  []string{filepath.Join("testdata", "input.txt")},
		NReduce:     1,
		MapFunc:     "wordcount_map",
		ReduceFunc:  "wordcount_reduce",
		CombineFunc: "wordcount_combine",
		WorkDir:     workDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	var reply mr.RequestTaskReply
	master.RequestTask(&mr.RequestTaskArgs{WorkerID: "w1"}, &reply)
	if reply.TaskType != mr.MapTask {
		t.Fatalf("expected MapTask, got %v", reply.TaskType)
	}
	savedAttempt := reply.AttemptID

	j := master.GetJob(job.ID)
	task := j.MapTasks[reply.TaskID]
	if task.AttemptID != savedAttempt {
		t.Fatalf("attempt mismatch: task=%d reply=%d", task.AttemptID, savedAttempt)
	}

	task.State = mr.Idle
	var reply2 mr.RequestTaskReply
	master.RequestTask(&mr.RequestTaskArgs{WorkerID: "w2"}, &reply2)
	if reply2.AttemptID == savedAttempt {
		t.Fatal("expected bumped AttemptID after reassignment")
	}

	var rr mr.ReportTaskReply
	master.ReportTask(&mr.ReportTaskArgs{
		WorkerID:  "w1",
		JobID:     job.ID,
		TaskType:  mr.MapTask,
		TaskID:    reply.TaskID,
		AttemptID: savedAttempt,
		Success:   true,
	}, &rr)

	if task.State == mr.Completed {
		t.Fatal("stale report should not have completed the task")
	}
}

func TestTimedOutReportRejection(t *testing.T) {
	master := mr.NewMaster(":0", ":0")
	workDir := t.TempDir()
	job, err := master.StartJob(mr.JobConfig{
		InputFiles:  []string{filepath.Join("testdata", "input.txt")},
		NReduce:     1,
		MapFunc:     "wordcount_map",
		ReduceFunc:  "wordcount_reduce",
		CombineFunc: "wordcount_combine",
		WorkDir:     workDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	var reply mr.RequestTaskReply
	master.RequestTask(&mr.RequestTaskArgs{WorkerID: "slow-worker"}, &reply)
	if reply.TaskType != mr.MapTask {
		t.Fatalf("expected MapTask, got %v", reply.TaskType)
	}

	task := master.GetJob(job.ID).MapTasks[reply.TaskID]
	task.State = mr.Idle
	task.WorkerID = ""
	task.StartTime = time.Time{}

	var rr mr.ReportTaskReply
	master.ReportTask(&mr.ReportTaskArgs{
		WorkerID:  "slow-worker",
		JobID:     job.ID,
		TaskType:  mr.MapTask,
		TaskID:    reply.TaskID,
		AttemptID: reply.AttemptID,
		Success:   true,
	}, &rr)

	if task.State == mr.Completed {
		t.Fatal("timed-out report should not complete an idle task")
	}
	if master.GetJob(job.ID).Metrics.StaleReports == 0 {
		t.Fatal("expected stale report metric to increment")
	}
}

func TestRetryLimitJobFailure(t *testing.T) {
	job := &mr.Job{
		Config: mr.JobConfig{NMap: 1, NReduce: 1},
		MapTasks: []*mr.Task{
			{ID: 0, Type: mr.MapTask, State: mr.InProgress,
				StartTime: time.Now().Add(-20 * time.Second)},
		},
		ReduceTasks:      []*mr.Task{{ID: 0, Type: mr.ReduceTask, State: mr.Idle}},
		MapDoneForReduce: [][]bool{{false}},
	}

	job.MapTasks[0].RetryCount = mr.DefaultMaxRetries

	tm := mr.NewTaskManager(job)
	done := make(chan struct{})
	go tm.StartMonitor(done)
	time.Sleep(1500 * time.Millisecond)
	close(done)

	if job.State != mr.JobFailed {
		t.Fatalf("expected JobFailed, got %s", job.State)
	}
	if job.Error == "" {
		t.Fatal("expected non-empty error reason")
	}
}

func TestWorkerBlacklist(t *testing.T) {
	master := mr.NewMaster(":0", ":0")
	workDir := t.TempDir()
	job, err := master.StartJob(mr.JobConfig{
		InputFiles:  []string{filepath.Join("testdata", "input.txt")},
		NReduce:     1,
		SplitSize:   10, // small splits → many map tasks so each failure uses a fresh task
		MapFunc:     "wordcount_map",
		ReduceFunc:  "wordcount_reduce",
		CombineFunc: "wordcount_combine",
		WorkDir:     workDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	badWorker := "bad-worker"
	for i := 0; i < mr.DefaultMaxWorkerFailures; i++ {
		var reply mr.RequestTaskReply
		master.RequestTask(&mr.RequestTaskArgs{WorkerID: badWorker}, &reply)
		if reply.TaskType == mr.ExitTask {
			t.Logf("got ExitTask at iteration %d", i)
			break
		}
		if reply.TaskType == mr.MapTask {
			var rr mr.ReportTaskReply
			master.ReportTask(&mr.ReportTaskArgs{
				WorkerID:  badWorker,
				JobID:     job.ID,
				TaskType:  mr.MapTask,
				TaskID:    reply.TaskID,
				AttemptID: reply.AttemptID,
				Success:   false,
			}, &rr)
		}
	}

	var reply mr.RequestTaskReply
	master.RequestTask(&mr.RequestTaskArgs{WorkerID: badWorker}, &reply)
	if reply.TaskType != mr.ExitTask {
		t.Fatalf("expected ExitTask for blacklisted worker, got %v", reply.TaskType)
	}
	_ = job
}

func TestInputReadFailureFailsJobFast(t *testing.T) {
	logDir := t.TempDir()
	mr.JobLogDir = logDir

	master := mr.NewMaster(":0", ":0")
	workDir := t.TempDir()
	job, err := master.StartJob(mr.JobConfig{
		InputFiles:  []string{filepath.Join("testdata", "input.txt")},
		NReduce:     1,
		SplitSize:   10,
		MapFunc:     "wordcount_map",
		ReduceFunc:  "wordcount_reduce",
		CombineFunc: "wordcount_combine",
		WorkDir:     workDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	var reply mr.RequestTaskReply
	master.RequestTask(&mr.RequestTaskArgs{WorkerID: "data-worker"}, &reply)
	if reply.TaskType != mr.MapTask {
		t.Fatalf("expected MapTask, got %v", reply.TaskType)
	}

	var rr mr.ReportTaskReply
	master.ReportTask(&mr.ReportTaskArgs{
		WorkerID:      "data-worker",
		JobID:         job.ID,
		TaskType:      mr.MapTask,
		TaskID:        reply.TaskID,
		AttemptID:     reply.AttemptID,
		Success:       false,
		FailureReason: "input_read: open " + reply.InputFile + ": no such file or directory",
	}, &rr)

	job = master.GetJob(job.ID)
	if job.State != mr.JobFailed {
		t.Fatalf("expected JobFailed, got %s", job.State)
	}
	if !strings.Contains(job.Error, "输入数据不可用") {
		t.Fatalf("expected input data error, got %q", job.Error)
	}
	if !strings.Contains(job.Error, reply.InputFile) {
		t.Fatalf("expected input file in error, got %q", job.Error)
	}

	var waitReply mr.RequestTaskReply
	master.RequestTask(&mr.RequestTaskArgs{WorkerID: "data-worker"}, &waitReply)
	if waitReply.TaskType != mr.WaitTask {
		t.Fatalf("expected WaitTask after job failure (worker stays alive), got %v", waitReply.TaskType)
	}

	bannerFound := false
	for _, d := range job.Decisions {
		if d.Type == mr.DecisionJobFailed && strings.Contains(d.Message, "════ 作业失败") {
			bannerFound = true
			break
		}
	}
	if !bannerFound {
		t.Fatal("expected job_failed banner in decisions")
	}

	logData, err := os.ReadFile(mr.JobLogPath(job.ID))
	if err != nil {
		t.Fatal(err)
	}
	text := string(logData)
	for _, want := range []string{`"kind":"finish"`, `"state":"failed"`, "输入数据不可用"} {
		if !strings.Contains(text, want) {
			t.Fatalf("job log missing %q:\n%s", want, text)
		}
	}
}

func TestJobFailureAbortsRemainingTasks(t *testing.T) {
	master := mr.NewMaster(":0", ":0")
	workDir := t.TempDir()
	job, err := master.StartJob(mr.JobConfig{
		InputFiles:  []string{filepath.Join("testdata", "input.txt")},
		NReduce:     1,
		SplitSize:   10,
		MapFunc:     "wordcount_map",
		ReduceFunc:  "wordcount_reduce",
		CombineFunc: "wordcount_combine",
		WorkDir:     workDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(job.MapTasks) < 2 {
		t.Fatalf("need multiple map tasks, got %d", len(job.MapTasks))
	}

	var replyA mr.RequestTaskReply
	master.RequestTask(&mr.RequestTaskArgs{WorkerID: "w-a"}, &replyA)
	if replyA.TaskType != mr.MapTask {
		t.Fatalf("expected MapTask for w-a, got %v", replyA.TaskType)
	}

	var replyB mr.RequestTaskReply
	master.RequestTask(&mr.RequestTaskArgs{WorkerID: "w-b"}, &replyB)
	if replyB.TaskType != mr.MapTask {
		t.Fatalf("expected MapTask for w-b, got %v", replyB.TaskType)
	}
	if replyB.TaskID == replyA.TaskID {
		t.Fatal("expected two different map tasks")
	}

	var rr mr.ReportTaskReply
	master.ReportTask(&mr.ReportTaskArgs{
		WorkerID:      "w-a",
		JobID:         job.ID,
		TaskType:      mr.MapTask,
		TaskID:        replyA.TaskID,
		AttemptID:     replyA.AttemptID,
		Success:       false,
		FailureReason: "input_read: open missing.txt: no such file",
	}, &rr)

	job = master.GetJob(job.ID)
	if job.State != mr.JobFailed {
		t.Fatalf("expected JobFailed, got %s", job.State)
	}

	abortedInProgress := job.MapTasks[replyB.TaskID]
	if abortedInProgress.State != mr.Failed {
		t.Fatalf("in-progress map should be aborted, got %s", abortedInProgress.State)
	}
	if abortedInProgress.AttemptID != replyB.AttemptID+1 {
		t.Fatalf("expected attempt bump on abort, got %d want %d", abortedInProgress.AttemptID, replyB.AttemptID+1)
	}

	idleAborted := 0
	for _, task := range job.MapTasks {
		if task.State == mr.Failed && task.LastFailureReason == "job_aborted" {
			idleAborted++
		}
	}
	if idleAborted == 0 {
		t.Fatal("expected idle map tasks to be marked job_aborted")
	}

	var waitReply mr.RequestTaskReply
	master.RequestTask(&mr.RequestTaskArgs{WorkerID: "w-c"}, &waitReply)
	if waitReply.TaskType != mr.WaitTask {
		t.Fatalf("expected WaitTask after job failure, got %v", waitReply.TaskType)
	}
	if waitReply.JobState != mr.JobFailed.String() {
		t.Fatalf("expected failed job state in reply, got %q", waitReply.JobState)
	}

	master.ReportTask(&mr.ReportTaskArgs{
		WorkerID:  "w-b",
		JobID:     job.ID,
		TaskType:  mr.MapTask,
		TaskID:    replyB.TaskID,
		AttemptID: replyB.AttemptID,
		Success:   true,
	}, &rr)
	if job.Metrics.StaleReports == 0 {
		t.Fatal("expected stale report from aborted in-progress task")
	}
}

func TestInputFailureDoesNotBlacklistWorker(t *testing.T) {
	job := &mr.Job{
		Config:           mr.JobConfig{NMap: 1, NReduce: 1},
		State:            mr.JobRunning,
		ReduceTasks:      []*mr.Task{{ID: 0, Type: mr.ReduceTask, State: mr.Idle}},
		MapDoneForReduce: [][]bool{{false}},
		MapTasks: []*mr.Task{{
			ID:        0,
			Type:      mr.MapTask,
			State:     mr.InProgress,
			WorkerID:  "w1",
			StartTime: time.Now(),
			AttemptID: 1,
			MapInfo:   &mr.MapTaskInfo{Split: mr.Split{File: "testdata/input.txt"}},
		}},
	}
	tm := mr.NewTaskManager(job)
	tm.RegisterWorker("w1")
	tm.CompleteTask(job.MapTasks[0], false, "w1", mr.TaskMetrics{}, "input_read: open testdata/input.txt: no such file")

	if tm.IsWorkerBlacklisted("w1") {
		t.Fatal("input failure should not blacklist worker")
	}
	if job.State != mr.JobFailed {
		t.Fatalf("expected JobFailed, got %s", job.State)
	}
}

// Ensure rpc types compile with net/rpc
var _ = rpc.DefaultServer
