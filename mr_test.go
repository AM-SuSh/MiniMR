package main_test

import (
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
	go func() {
		if err := master.Serve(); err != nil {
			t.Logf("master serve: %v", err)
		}
	}()
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

	counts := readOutputCounts(t, workDir, j.Config.NReduce)
	if counts["hello"] < 2 {
		t.Fatalf("expected hello count >= 2, got %d (full: %v)", counts["hello"], counts)
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

func readOutputCounts(t *testing.T, workDir string, nReduce int) map[string]int {
	t.Helper()
	counts := make(map[string]int)
	for r := 0; r < nReduce; r++ {
		path := filepath.Join(workDir, fmt.Sprintf("mr-out-%d", r))
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

func TestReduceReadyScheduling(t *testing.T) {
	job := &mr.Job{
		Config: mr.JobConfig{NMap: 2, NReduce: 2},
		MapDoneForReduce: [][]bool{
			{false, false},
			{false, false},
		},
	}
	tm := mr.NewTaskManager(job)
	tm.MarkMapDoneForReduce(0, 0)
	tm.MarkMapDoneForReduce(0, 1)

	if tm.IsReduceReady(0) {
		t.Fatal("reduce 0 should not be ready yet")
	}
	tm.MarkMapDoneForReduce(1, 0)
	if !tm.IsReduceReady(0) {
		t.Fatal("reduce 0 should be ready")
	}
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

// Ensure rpc types compile with net/rpc
var _ = rpc.DefaultServer
