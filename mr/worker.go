package mr

import (
	"bufio"
	"compress/gzip"
	"container/heap"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
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

// ---------------------------------------------------------------------------
// Map task: read split → MapFunc → Combine → partition → sort → write binary
// ---------------------------------------------------------------------------

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
		if err := atomicWriteBinary(outPath, partitions[r]); err != nil {
			log.Printf("write intermediate %s: %v", outPath, err)
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Reduce task: shuffle (poll files) → K-way heap merge → streaming reduce
// ---------------------------------------------------------------------------

func (w *Worker) doReduce(reply RequestTaskReply) bool {
	reduceFn, ok := GetReduceFunc(reply.ReduceFunc)
	if !ok {
		log.Printf("unknown reduce func: %s", reply.ReduceFunc)
		return false
	}

	// Phase 1  Shuffle — poll until every intermediate file is present.
	collected := make([]bool, reply.NMap)
	collectCount := 0
	shuffleDeadline := time.Now().Add(ReduceShuffleTimeout)

	for collectCount < reply.NMap {
		for m := 0; m < reply.NMap; m++ {
			if collected[m] {
				continue
			}
			path := intermediatePath(reply.WorkDir, m, reply.ReduceID)
			if _, err := os.Stat(path); err == nil {
				collected[m] = true
				collectCount++
			}
		}
		if collectCount >= reply.NMap {
			break
		}
		if time.Now().After(shuffleDeadline) {
			log.Printf("reduce %d: shuffle timed out (%d/%d files collected)",
				reply.ReduceID, collectCount, reply.NMap)
			return false
		}
		time.Sleep(ReduceShufflePollInterval)
	}

	// Phase 2  Open K sorted streams and seed the min-heap.
	readers := make([]*kvStreamReader, reply.NMap)
	for m := 0; m < reply.NMap; m++ {
		path := intermediatePath(reply.WorkDir, m, reply.ReduceID)
		r, err := openKVStream(path)
		if err != nil {
			log.Printf("open intermediate %s: %v", path, err)
			for _, rr := range readers {
				if rr != nil {
					rr.Close()
				}
			}
			return false
		}
		readers[m] = r
	}
	defer func() {
		for _, r := range readers {
			if r != nil {
				r.Close()
			}
		}
	}()

	mh := &mergeHeap{}
	heap.Init(mh)
	for i, r := range readers {
		if kv, ok := r.Next(); ok {
			heap.Push(mh, mergeItem{kv: kv, streamID: i})
		}
	}

	// Phase 3  Streaming merge-reduce: pop from heap, group by key, reduce.
	outPath := filepath.Join(reply.WorkDir, fmt.Sprintf("mr-out-%d", reply.ReduceID))
	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		log.Printf("mkdir: %v", err)
		return false
	}
	tmp := outPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		log.Printf("create %s: %v", tmp, err)
		return false
	}
	bw := bufio.NewWriterSize(f, 64*1024)

	var curKey string
	var curVals []string
	first := true

	for mh.Len() > 0 {
		item := heap.Pop(mh).(mergeItem)

		if first || item.kv.Key != curKey {
			if !first {
				fmt.Fprintf(bw, "%s\t%s\n", curKey, reduceFn(curKey, curVals))
			}
			curKey = item.kv.Key
			curVals = curVals[:0]
			first = false
		}
		curVals = append(curVals, item.kv.Value)

		if kv, ok := readers[item.streamID].Next(); ok {
			heap.Push(mh, mergeItem{kv: kv, streamID: item.streamID})
		}
	}
	if !first && len(curVals) > 0 {
		fmt.Fprintf(bw, "%s\t%s\n", curKey, reduceFn(curKey, curVals))
	}

	if err := bw.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		return false
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return false
	}
	return os.Rename(tmp, outPath) == nil
}

// ===========================================================================
// K-way merge heap
// ===========================================================================

type mergeItem struct {
	kv       KeyValue
	streamID int
}

type mergeHeap []mergeItem

func (h mergeHeap) Len() int            { return len(h) }
func (h mergeHeap) Less(i, j int) bool  { return h[i].kv.Key < h[j].kv.Key }
func (h mergeHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x interface{}) { *h = append(*h, x.(mergeItem)) }
func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// ===========================================================================
// Binary + gzip intermediate file I/O
//
// Wire format (gzip-compressed):
//   [4 B keyLen big-endian][keyLen B key][4 B valLen big-endian][valLen B val]
//   ... repeated per record ...
//
// Compared to the previous JSON Lines format, this eliminates JSON
// marshalling overhead and leverages gzip to reduce file size / disk IO.
// ===========================================================================

// kvStreamReader streams KV pairs from a compressed binary intermediate file.
type kvStreamReader struct {
	f   *os.File
	gz  *gzip.Reader
	br  *bufio.Reader
	hdr [4]byte
}

func openKVStream(path string) (*kvStreamReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if info.Size() == 0 {
		f.Close()
		return &kvStreamReader{}, nil
	}
	gz, err := gzip.NewReader(bufio.NewReaderSize(f, 32*1024))
	if err != nil {
		f.Close()
		return nil, err
	}
	return &kvStreamReader{
		f:  f,
		gz: gz,
		br: bufio.NewReaderSize(gz, 32*1024),
	}, nil
}

// Next returns the next KV pair. ok is false when the stream is exhausted.
func (r *kvStreamReader) Next() (KeyValue, bool) {
	if r.br == nil {
		return KeyValue{}, false
	}
	if _, err := io.ReadFull(r.br, r.hdr[:]); err != nil {
		return KeyValue{}, false
	}
	keyLen := binary.BigEndian.Uint32(r.hdr[:])
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r.br, key); err != nil {
		return KeyValue{}, false
	}
	if _, err := io.ReadFull(r.br, r.hdr[:]); err != nil {
		return KeyValue{}, false
	}
	valLen := binary.BigEndian.Uint32(r.hdr[:])
	val := make([]byte, valLen)
	if _, err := io.ReadFull(r.br, val); err != nil {
		return KeyValue{}, false
	}
	return KeyValue{Key: string(key), Value: string(val)}, true
}

func (r *kvStreamReader) Close() {
	if r.gz != nil {
		r.gz.Close()
	}
	if r.f != nil {
		r.f.Close()
	}
}

// atomicWriteBinary writes sorted KV pairs as a gzip-compressed binary file.
func atomicWriteBinary(path string, kvs []KeyValue) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	bw := bufio.NewWriterSize(f, 64*1024)
	gw := gzip.NewWriter(bw)

	writeErr := writeBinaryKVs(gw, kvs)
	gzErr := gw.Close()
	flushErr := bw.Flush()
	fErr := f.Close()

	for _, e := range []error{writeErr, gzErr, flushErr, fErr} {
		if e != nil {
			os.Remove(tmp)
			return e
		}
	}
	return os.Rename(tmp, path)
}

func writeBinaryKVs(w io.Writer, kvs []KeyValue) error {
	hdr := make([]byte, 4)
	for _, kv := range kvs {
		binary.BigEndian.PutUint32(hdr, uint32(len(kv.Key)))
		if _, err := w.Write(hdr); err != nil {
			return err
		}
		if _, err := w.Write([]byte(kv.Key)); err != nil {
			return err
		}
		binary.BigEndian.PutUint32(hdr, uint32(len(kv.Value)))
		if _, err := w.Write(hdr); err != nil {
			return err
		}
		if _, err := w.Write([]byte(kv.Value)); err != nil {
			return err
		}
	}
	return nil
}

// ===========================================================================
// Shared helpers
// ===========================================================================

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
