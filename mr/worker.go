package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Worker executes MapReduce tasks assigned by the Master.
type Worker struct {
	ID         string
	MasterAddr string
}

// NewWorker creates a Worker with the given ID and master address.
func NewWorker(id, masterAddr string) *Worker {
	return &Worker{ID: id, MasterAddr: masterAddr}
}

// Run starts the worker loop until ExitTask is received.
func (w *Worker) Run() {
	go w.heartbeatLoop()

	for {
		var reply RequestTaskReply
		err := w.call("Master.RequestTask", &RequestTaskArgs{WorkerID: w.ID}, &reply)
		if err != nil {
			log.Printf("RequestTask failed: %v, retrying...", err)
			time.Sleep(time.Second)
			continue
		}

		switch reply.TaskType {
		case MapTask:
			success := w.doMap(reply)
			w.report(reply.JobID, MapTask, reply.TaskID, success)
		case ReduceTask:
			success := w.doReduce(reply)
			w.report(reply.JobID, ReduceTask, reply.TaskID, success)
		case WaitTask:
			time.Sleep(time.Second)
		case ExitTask:
			log.Printf("Worker %s received exit signal", w.ID)
			return
		}
	}
}

func (w *Worker) heartbeatLoop() {
	for {
		var reply HeartbeatReply
		err := w.call("Master.Heartbeat", &HeartbeatArgs{WorkerID: w.ID}, &reply)
		if err != nil {
			log.Printf("Heartbeat failed: %v", err)
		}
		time.Sleep(DefaultHeartbeat)
	}
}

func (w *Worker) report(jobID string, taskType TaskType, taskID int, success bool) {
	var reply ReportTaskReply
	_ = w.call("Master.ReportTask", &ReportTaskArgs{
		WorkerID: w.ID,
		JobID:    jobID,
		TaskType: taskType,
		TaskID:   taskID,
		Success:  success,
	}, &reply)
}

func (w *Worker) doMap(reply RequestTaskReply) bool {
	mapFn, ok := GetMapFunc(reply.MapFunc)
	if !ok {
		log.Printf("unknown map func: %s", reply.MapFunc)
		return false
	}

	content, err := readSplit(reply.InputFile, reply.InputOffset, reply.InputLength)
	if err != nil {
		log.Printf("read split: %v", err)
		return false
	}

	kvs := mapFn(reply.InputFile, content)

	if reply.CombineFunc != "" {
		if combineFn, ok := GetCombineFunc(reply.CombineFunc); ok {
			kvs = combineLocal(kvs, combineFn)
		}
	}

	partitions := make([][]KeyValue, reply.NReduce)
	for _, kv := range kvs {
		r := ihash(kv.Key) % reply.NReduce
		partitions[r] = append(partitions[r], kv)
	}

	for r := 0; r < reply.NReduce; r++ {
		sort.Slice(partitions[r], func(i, j int) bool {
			return partitions[r][i].Key < partitions[r][j].Key
		})
		outPath := intermediatePath(reply.WorkDir, reply.TaskID, r)
		if err := atomicWriteJSONL(outPath, partitions[r]); err != nil {
			log.Printf("write intermediate %s: %v", outPath, err)
			return false
		}
	}
	return true
}

func (w *Worker) doReduce(reply RequestTaskReply) bool {
	reduceFn, ok := GetReduceFunc(reply.ReduceFunc)
	if !ok {
		log.Printf("unknown reduce func: %s", reply.ReduceFunc)
		return false
	}

	var all []KeyValue
	for m := 0; m < reply.NMap; m++ {
		path := intermediatePath(reply.WorkDir, m, reply.ReduceID)
		kvs, err := readJSONL(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			log.Printf("read intermediate %s: %v", path, err)
			return false
		}
		all = append(all, kvs...)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Key < all[j].Key
	})

	outPath := filepath.Join(reply.WorkDir, fmt.Sprintf("mr-out-%d", reply.ReduceID))
	if err := atomicWriteReduceOutput(outPath, all, reduceFn); err != nil {
		log.Printf("write output %s: %v", outPath, err)
		return false
	}
	return true
}

func combineLocal(kvs []KeyValue, combineFn CombineFunc) []KeyValue {
	if len(kvs) == 0 {
		return kvs
	}
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})

	var result []KeyValue
	i := 0
	for i < len(kvs) {
		key := kvs[i].Key
		j := i + 1
		for j < len(kvs) && kvs[j].Key == key {
			j++
		}
		values := make([]string, j-i)
		for k := i; k < j; k++ {
			values[k-i] = kvs[k].Value
		}
		result = append(result, KeyValue{Key: key, Value: combineFn(key, values)})
		i = j
	}
	return result
}

func readSplit(file string, offset, length int64) (string, error) {
	f, err := os.Open(file)
	if err != nil {
		return "", err
	}
	defer f.Close()

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return "", err
	}
	buf := make([]byte, length)
	n, err := io.ReadFull(f, buf)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return "", err
	}
	return string(buf[:n]), nil
}

func intermediatePath(workDir string, mapID, reduceID int) string {
	return filepath.Join(workDir, fmt.Sprintf("mr-%d-%d", mapID, reduceID))
}

func atomicWriteJSONL(path string, kvs []KeyValue) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, kv := range kvs {
		if err := enc.Encode(kv); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func readJSONL(path string) ([]KeyValue, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var kvs []KeyValue
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var kv KeyValue
		if err := json.Unmarshal([]byte(line), &kv); err != nil {
			return nil, err
		}
		kvs = append(kvs, kv)
	}
	return kvs, sc.Err()
}

func atomicWriteReduceOutput(path string, sorted []KeyValue, reduceFn ReduceFunc) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)

	i := 0
	for i < len(sorted) {
		key := sorted[i].Key
		j := i + 1
		for j < len(sorted) && sorted[j].Key == key {
			j++
		}
		values := make([]string, j-i)
		for k := i; k < j; k++ {
			values[k-i] = sorted[k].Value
		}
		out := reduceFn(key, values)
		fmt.Fprintf(w, "%s\t%s\n", key, out)
		i = j
	}

	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32())
}

func (w *Worker) call(method string, args interface{}, reply interface{}) error {
	client, err := rpc.Dial("tcp", w.MasterAddr)
	if err != nil {
		return err
	}
	defer client.Close()
	return client.Call(method, args, reply)
}
