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
	"strconv"
	"strings"
	"sync"
	"time"
)

// Worker executes MapReduce tasks assigned by the Master.
type Worker struct {
	ID         string
	MasterAddr string
	done       chan struct{}
	stopOnce   sync.Once
}

// NewWorker creates a Worker with the given ID and master address.
func NewWorker(id, masterAddr string) *Worker {
	return &Worker{ID: id, MasterAddr: masterAddr, done: make(chan struct{})}
}

// Run starts the worker loop until ExitTask is received.
func (w *Worker) Run() {
	if w.done == nil {
		w.done = make(chan struct{})
	}
	go w.heartbeatLoop()
	defer w.stop()

	for {
		var reply RequestTaskReply
		err := w.call("Master.RequestTask", &RequestTaskArgs{WorkerID: w.ID}, &reply)
		if err != nil {
			log.Printf("RequestTask failed: %v, retrying...", err)
			if !w.sleepOrDone(time.Second) {
				return
			}
			continue
		}

		switch reply.TaskType {
		case MapTask:
			if reply.JobState == JobFailed.String() {
				log.Printf("Worker %s: job %s already failed, skipping map-%d", w.ID, reply.JobID, reply.TaskID)
				break
			}
			success, metrics, reason := w.doMap(reply)
			w.report(reply.JobID, MapTask, reply.TaskID, reply.AttemptID, success, metrics, reason)
		case ReduceTask:
			if reply.JobState == JobFailed.String() {
				log.Printf("Worker %s: job %s already failed, skipping reduce-%d", w.ID, reply.JobID, reply.TaskID)
				break
			}
			success, metrics, reason := w.doReduce(reply)
			w.report(reply.JobID, ReduceTask, reply.TaskID, reply.AttemptID, success, metrics, reason)
		case WaitTask:
			if reply.JobState == JobFailed.String() {
				log.Printf("Worker %s: job %s failed, idle until next job", w.ID, reply.JobID)
			}
			if !w.sleepOrDone(time.Second) {
				return
			}
		case ExitTask:
			log.Printf("Worker %s received exit signal", w.ID)
			return
		}
	}
}

func (w *Worker) heartbeatLoop() {
	for {
		select {
		case <-w.done:
			return
		default:
		}
		var reply HeartbeatReply
		err := w.call("Master.Heartbeat", &HeartbeatArgs{WorkerID: w.ID}, &reply)
		if err != nil {
			log.Printf("Heartbeat failed: %v", err)
		}
		if !w.sleepOrDone(DefaultHeartbeat) {
			return
		}
	}
}

func (w *Worker) stop() {
	w.stopOnce.Do(func() {
		if w.done == nil {
			return
		}
		close(w.done)
	})
}

func (w *Worker) sleepOrDone(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-w.done:
		return false
	case <-timer.C:
		return true
	}
}

func (w *Worker) report(jobID string, taskType TaskType, taskID int, attemptID int, success bool, metrics TaskMetrics, failureReason string) {
	var reply ReportTaskReply
	_ = w.call("Master.ReportTask", &ReportTaskArgs{
		WorkerID:      w.ID,
		JobID:         jobID,
		TaskType:      taskType,
		TaskID:        taskID,
		AttemptID:     attemptID,
		Success:       success,
		Metrics:       metrics,
		FailureReason: failureReason,
	}, &reply)
}

// ---------------------------------------------------------------------------
// Map task: read split → MapFunc → Combine → partition → sort → write binary
// ---------------------------------------------------------------------------

func (w *Worker) doMap(reply RequestTaskReply) (bool, TaskMetrics, string) {
	metrics := TaskMetrics{InputBytes: reply.InputLength}
	mapFn, ok := GetMapFunc(reply.MapFunc)
	if !ok {
		reason := fmt.Sprintf("config: unknown map func %s", reply.MapFunc)
		log.Printf("unknown map func: %s", reply.MapFunc)
		return false, metrics, reason
	}

	content, err := readSplit(reply.InputFile, reply.InputOffset, reply.InputLength)
	if err != nil {
		reason := fmt.Sprintf("input_read: %v", err)
		log.Printf("read split %s [%d+%d]: %v", reply.InputFile, reply.InputOffset, reply.InputLength, err)
		return false, metrics, reason
	}

	kvs := mapFn(reply.InputFile, content)
	metrics.MapOutputRecords = int64(len(kvs))

	if reply.CombineFunc != "" {
		if combineFn, ok := GetCombineFunc(reply.CombineFunc); ok {
			kvs = combineLocal(kvs, combineFn)
		}
	}
	metrics.CombineOutputRecords = int64(len(kvs))

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
		fileMetrics, err := atomicWriteBinaryWithStats(outPath, partitions[r], reply.JobID, reply.AttemptID)
		if err != nil {
			reason := fmt.Sprintf("intermediate_write: %v", err)
			log.Printf("write intermediate %s: %v", outPath, err)
			return false, metrics, reason
		}
		metrics.Add(fileMetrics)
	}
	return true, metrics, ""
}

// ---------------------------------------------------------------------------
// Reduce task: shuffle (poll files) → K-way heap merge → streaming reduce
// ---------------------------------------------------------------------------

func (w *Worker) doReduce(reply RequestTaskReply) (bool, TaskMetrics, string) {
	metrics := TaskMetrics{}
	reduceFn, ok := GetReduceFunc(reply.ReduceFunc)
	if !ok {
		reason := fmt.Sprintf("config: unknown reduce func %s", reply.ReduceFunc)
		log.Printf("unknown reduce func: %s", reply.ReduceFunc)
		return false, metrics, reason
	}

	// Phase 1  Shuffle — poll until every intermediate file is present.
	shuffleStart := time.Now()
	collected := make([]bool, reply.NMap)
	collectCount := 0
	shuffleDeadline := time.Now().Add(ReduceShuffleTimeout)

	for collectCount < reply.NMap {
		for m := 0; m < reply.NMap; m++ {
			if collected[m] {
				continue
			}
			path := intermediatePath(reply.WorkDir, m, reply.ReduceID)
			if intermediateReady(path, reply.JobID) {
				collected[m] = true
				collectCount++
			}
		}
		if collectCount >= reply.NMap {
			break
		}
		if time.Now().After(shuffleDeadline) {
			reason := fmt.Sprintf("shuffle_timeout: collected %d/%d intermediate files", collectCount, reply.NMap)
			log.Printf("reduce %d: shuffle timed out (%d/%d files collected)",
				reply.ReduceID, collectCount, reply.NMap)
			metrics.ShuffleWaitMs = time.Since(shuffleStart).Milliseconds()
			return false, metrics, reason
		}
		time.Sleep(ReduceShufflePollInterval)
	}
	metrics.ShuffleWaitMs = time.Since(shuffleStart).Milliseconds()

	// Phase 2  Open K sorted streams and seed the min-heap.
	readers := make([]*kvStreamReader, reply.NMap)
	for m := 0; m < reply.NMap; m++ {
		basePath := intermediatePath(reply.WorkDir, m, reply.ReduceID)
		path, ok := committedIntermediatePath(basePath, reply.JobID)
		if !ok {
			reason := fmt.Sprintf("intermediate_read: committed intermediate missing for map %d reduce %d", m, reply.ReduceID)
			log.Printf("%s", reason)
			for _, rr := range readers {
				if rr != nil {
					rr.Close()
				}
			}
			return false, metrics, reason
		}
		r, err := openKVStream(path)
		if err != nil {
			reason := fmt.Sprintf("intermediate_read: %v", err)
			log.Printf("open intermediate %s: %v", path, err)
			for _, rr := range readers {
				if rr != nil {
					rr.Close()
				}
			}
			return false, metrics, reason
		}
		readers[m] = r
		metrics.ReduceOpenedStreams++
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
		kv, ok, err := r.Next()
		if err != nil {
			return false, metrics, fmt.Sprintf("intermediate_read: %v", err)
		}
		if ok {
			heap.Push(mh, mergeItem{kv: kv, streamID: i})
		}
	}

	// Phase 3  Streaming merge-reduce: pop from heap, group by key, reduce.
	outPath := reduceAttemptPath(reply.WorkDir, reply.ReduceID, reply.AttemptID)
	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		reason := fmt.Sprintf("output_write: mkdir %v", err)
		log.Printf("mkdir: %v", err)
		return false, metrics, reason
	}
	tmp, err := tempPathFor(outPath)
	if err != nil {
		reason := fmt.Sprintf("output_write: temp %v", err)
		log.Printf("temp path %s: %v", outPath, err)
		return false, metrics, reason
	}
	f, err := os.Create(tmp)
	if err != nil {
		reason := fmt.Sprintf("output_write: create %v", err)
		log.Printf("create %s: %v", tmp, err)
		return false, metrics, reason
	}
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = os.Remove(tmp)
		}
	}()
	bw := bufio.NewWriterSize(f, 64*1024)

	var curKey string
	var curVals []string
	first := true
	streamStart := time.Now()

	for mh.Len() > 0 {
		item := heap.Pop(mh).(mergeItem)
		metrics.ReduceStreamedRecords++

		if first || item.kv.Key != curKey {
			if !first {
				fmt.Fprintf(bw, "%s\t%s\n", curKey, reduceFn(curKey, curVals))
				metrics.ReduceOutputKeys++
			}
			curKey = item.kv.Key
			curVals = curVals[:0]
			first = false
		}
		curVals = append(curVals, item.kv.Value)
		if int64(len(curVals)) > metrics.ReduceMaxBufferedValues {
			metrics.ReduceMaxBufferedValues = int64(len(curVals))
		}
		kv, ok, err := readers[item.streamID].Next()
		if err != nil {
			f.Close()
			return false, metrics, fmt.Sprintf("intermediate_read: %v", err)
		}
		if ok {
			heap.Push(mh, mergeItem{kv: kv, streamID: item.streamID})
		}
	}
	if !first && len(curVals) > 0 {
		fmt.Fprintf(bw, "%s\t%s\n", curKey, reduceFn(curKey, curVals))
		metrics.ReduceOutputKeys++
	}
	metrics.ReduceReadMs = time.Since(streamStart).Milliseconds()

	writeStart := time.Now()
	if err := bw.Flush(); err != nil {
		f.Close()
		return false, metrics, fmt.Sprintf("output_write: flush %v", err)
	}
	if err := f.Close(); err != nil {
		return false, metrics, fmt.Sprintf("output_write: close %v", err)
	}
	if err := atomicReplaceFile(tmp, outPath); err != nil {
		return false, metrics, fmt.Sprintf("output_write: publish attempt %v", err)
	}
	cleanupTmp = false
	metrics.ReduceWriteMs = time.Since(writeStart).Milliseconds()
	return true, metrics, ""
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

// Next returns the next KV pair. ok is false only when the stream is cleanly exhausted.
func (r *kvStreamReader) Next() (KeyValue, bool, error) {
	if r.br == nil {
		return KeyValue{}, false, nil
	}
	if _, err := io.ReadFull(r.br, r.hdr[:]); err != nil {
		if err == io.EOF {
			return KeyValue{}, false, nil
		}
		return KeyValue{}, false, fmt.Errorf("read key length: %w", err)
	}
	keyLen := binary.BigEndian.Uint32(r.hdr[:])
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r.br, key); err != nil {
		return KeyValue{}, false, fmt.Errorf("read key: %w", err)
	}
	if _, err := io.ReadFull(r.br, r.hdr[:]); err != nil {
		return KeyValue{}, false, fmt.Errorf("read value length: %w", err)
	}
	valLen := binary.BigEndian.Uint32(r.hdr[:])
	val := make([]byte, valLen)
	if _, err := io.ReadFull(r.br, val); err != nil {
		return KeyValue{}, false, fmt.Errorf("read value: %w", err)
	}
	return KeyValue{Key: string(key), Value: string(val)}, true, nil
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
	_, err := atomicWriteBinaryWithStats(path, kvs, "", 0)
	return err
}

func atomicWriteBinaryWithStats(path string, kvs []KeyValue, jobID string, attemptID int) (TaskMetrics, error) {
	metrics := TaskMetrics{
		ShuffleFiles:           1,
		ShuffleJSONBytes:       estimateJSONLBytes(kvs),
		ShuffleBinaryBytes:     estimateBinaryBytes(kvs),
		ShuffleCompressedBytes: 0,
	}
	start := time.Now()
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return metrics, err
	}
	target := path
	if attemptID > 0 {
		target = intermediateAttemptPath(path, attemptID)
	}
	tmp, err := tempPathFor(target)
	if err != nil {
		return metrics, err
	}
	f, err := os.Create(tmp)
	if err != nil {
		return metrics, err
	}
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = os.Remove(tmp)
		}
	}()

	bw := bufio.NewWriterSize(f, 64*1024)
	gw := gzip.NewWriter(bw)

	writeErr := writeBinaryKVs(gw, kvs)
	gzErr := gw.Close()
	flushErr := bw.Flush()
	fErr := f.Close()

	for _, e := range []error{writeErr, gzErr, flushErr, fErr} {
		if e != nil {
			return metrics, e
		}
	}
	if err := atomicReplaceFile(tmp, target); err != nil {
		return metrics, err
	}
	cleanupTmp = false
	if attemptID <= 0 {
		if err := writeReadyMarker(readyPath(path, jobID), 0); err != nil {
			return metrics, err
		}
	}
	if info, err := os.Stat(target); err == nil {
		metrics.ShuffleCompressedBytes = info.Size()
	}
	metrics.ShuffleWriteMs = time.Since(start).Milliseconds()
	return metrics, nil
}

func tempPathFor(path string) (string, error) {
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return "", err
	}
	name := f.Name()
	if err := f.Close(); err != nil {
		_ = os.Remove(name)
		return "", err
	}
	if err := os.Remove(name); err != nil {
		return "", err
	}
	return name, nil
}

func atomicReplaceFile(src, dst string) error {
	_ = os.Remove(dst)
	return os.Rename(src, dst)
}

func readyPath(path, jobID string) string {
	if jobID == "" {
		return path + ".ready"
	}
	return fmt.Sprintf("%s.%s.ready", path, jobID)
}

func writeReadyMarker(path string, attemptID int) error {
	content := "ready\n"
	if attemptID > 0 {
		content = fmt.Sprintf("attempt=%d\n", attemptID)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tmp, err := tempPathFor(path)
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, []byte(content), 0644); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := atomicReplaceFile(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func intermediateAttemptPath(path string, attemptID int) string {
	if attemptID <= 0 {
		return path
	}
	return fmt.Sprintf("%s.attempt-%d", path, attemptID)
}

func committedIntermediateAttempt(path, jobID string) (int, bool) {
	marker, err := os.ReadFile(readyPath(path, jobID))
	if err != nil {
		return 0, false
	}
	text := strings.TrimSpace(string(marker))
	if strings.HasPrefix(text, "attempt=") {
		attemptID, err := strconv.Atoi(strings.TrimPrefix(text, "attempt="))
		if err != nil || attemptID <= 0 {
			return 0, false
		}
		return attemptID, true
	}
	return 0, true
}

func committedIntermediatePath(path, jobID string) (string, bool) {
	attemptID, ok := committedIntermediateAttempt(path, jobID)
	if !ok {
		return "", false
	}
	if attemptID > 0 {
		candidate := intermediateAttemptPath(path, attemptID)
		if _, err := os.Stat(candidate); err == nil {
			return candidate, true
		}
		return "", false
	}
	if _, err := os.Stat(path); err == nil {
		return path, true
	}
	return "", false
}

func intermediateReady(path, jobID string) bool {
	_, ok := committedIntermediatePath(path, jobID)
	return ok
}

func publishMapOutputCommit(workDir, jobID string, mapID, nReduce, attemptID int) error {
	if attemptID <= 0 {
		return fmt.Errorf("invalid attempt id %d", attemptID)
	}
	for r := 0; r < nReduce; r++ {
		path := intermediatePath(workDir, mapID, r)
		if _, err := os.Stat(intermediateAttemptPath(path, attemptID)); err != nil {
			return fmt.Errorf("map %d reduce %d attempt %d: %w", mapID, r, attemptID, err)
		}
	}
	writtenMarkers := make([]struct {
		path   string
		marker string
	}, 0, nReduce)
	for r := 0; r < nReduce; r++ {
		path := intermediatePath(workDir, mapID, r)
		marker := readyPath(path, jobID)
		if err := writeReadyMarker(marker, attemptID); err != nil {
			for _, written := range writtenMarkers {
				if committedAttempt, ok := committedIntermediateAttempt(written.path, jobID); ok && committedAttempt == attemptID {
					_ = os.Remove(written.marker)
				}
			}
			return fmt.Errorf("map %d reduce %d marker: %w", mapID, r, err)
		}
		writtenMarkers = append(writtenMarkers, struct {
			path   string
			marker string
		}{path: path, marker: marker})
	}
	return nil
}

func reduceOutputPath(workDir string, reduceID int) string {
	return filepath.Join(workDir, fmt.Sprintf("mr-out-%d", reduceID))
}

func reduceAttemptPath(workDir string, reduceID, attemptID int) string {
	if attemptID <= 0 {
		return reduceOutputPath(workDir, reduceID)
	}
	return fmt.Sprintf("%s.attempt-%d", reduceOutputPath(workDir, reduceID), attemptID)
}

func commitReduceOutput(workDir, jobID string, reduceID, attemptID int) error {
	if attemptID <= 0 {
		return fmt.Errorf("invalid attempt id %d", attemptID)
	}
	src := reduceAttemptPath(workDir, reduceID, attemptID)
	dst := reduceOutputPath(workDir, reduceID)
	if _, err := os.Stat(src); err != nil {
		return err
	}
	if err := atomicReplaceFile(src, dst); err != nil {
		return err
	}
	return writeReadyMarker(readyPath(dst, jobID), attemptID)
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

func estimateBinaryBytes(kvs []KeyValue) int64 {
	var n int64
	for _, kv := range kvs {
		n += int64(8 + len(kv.Key) + len(kv.Value))
	}
	return n
}

func estimateJSONLBytes(kvs []KeyValue) int64 {
	var n int64
	for _, kv := range kvs {
		// encoding/json would produce {"Key":"...","Value":"..."}\n for
		// wordcount keys. Account for common escaping so the dashboard remains
		// honest for paths and crawled text too.
		n += int64(22 + escapedJSONStringLen(kv.Key) + escapedJSONStringLen(kv.Value))
	}
	return n
}

func escapedJSONStringLen(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\', '"':
			n += 2
		case '\n', '\r', '\t':
			n += 2
		default:
			if s[i] < 0x20 {
				n += 6
			} else {
				n++
			}
		}
	}
	return n
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
