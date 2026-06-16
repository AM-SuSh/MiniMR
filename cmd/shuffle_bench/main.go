// shuffle_bench compares intermediate shuffle file sizes:
// JSON Lines vs Length-Prefixed Binary vs Binary + gzip.
package main

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"

	"mapreduce/mr"

	_ "mapreduce/udf"
)

func main() {
	input := flag.String("input", "testdata/pd.train", "输入文件路径")
	sampleBytes := flag.Int64("bytes", 50*1024*1024, "读取字节数（模拟一个 Map 分片）")
	nReduce := flag.Int("nreduce", 3, "Reduce 分区数量")
	workDir := flag.String("workdir", "mr-bench-shuffle", "基准输出目录")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "用法: go run ./cmd/shuffle_bench [选项]\n\n")
		fmt.Fprintf(os.Stderr, "对比 Shuffle 中间文件三种格式的大小: JSON Lines / Binary / Binary+gzip\n\n")
		fmt.Fprintf(os.Stderr, "选项:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\n示例:\n")
		fmt.Fprintf(os.Stderr, "  go run ./cmd/shuffle_bench\n")
		fmt.Fprintf(os.Stderr, "  go run ./cmd/shuffle_bench -input testdata/pd.train -bytes 52428800 -nreduce 3\n")
		fmt.Fprintf(os.Stderr, "  go run ./cmd/shuffle_bench -nreduce 5 -workdir mr-bench-n5\n")
	}
	flag.Parse()

	if *nReduce < 1 {
		fmt.Fprintln(os.Stderr, "错误: -nreduce 必须 >= 1")
		flag.Usage()
		os.Exit(2)
	}

	content, nRead, err := readSample(*input, *sampleBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read input: %v\n", err)
		os.Exit(1)
	}

	mapFn, ok := mr.GetMapFunc("wordcount_map")
	if !ok {
		fmt.Fprintln(os.Stderr, "wordcount_map not registered")
		os.Exit(1)
	}

	kvs := mapFn(*input, content)
	if combineFn, ok := mr.GetCombineFunc("wordcount_combine"); ok {
		kvs = combineLocal(kvs, combineFn)
	}

	partitions := make([][]mr.KeyValue, *nReduce)
	for _, kv := range kvs {
		r := ihash(kv.Key) % *nReduce
		partitions[r] = append(partitions[r], kv)
	}
	for r := 0; r < *nReduce; r++ {
		sort.Slice(partitions[r], func(i, j int) bool {
			return partitions[r][i].Key < partitions[r][j].Key
		})
	}

	if err := os.RemoveAll(*workDir); err != nil {
		fmt.Fprintf(os.Stderr, "clean workdir: %v\n", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(*workDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir: %v\n", err)
		os.Exit(1)
	}

	var jsonTotal, binTotal, gzipTotal int64
	fmt.Println("=== Shuffle 中间文件大小对比 (pd.train) ===")
	fmt.Printf("输入: %s\n", *input)
	fmt.Printf("模拟 Map 分片: %s (%d bytes)\n", humanBytes(nRead), nRead)
	fmt.Printf("Reduce 分区数: %d\n", *nReduce)
	fmt.Printf("Map 产出 KV 数 (combine 后): %d\n\n", len(kvs))

	fmt.Printf("%-12s %12s %12s %12s %12s\n", "分区", "JSON Lines", "Binary", "Binary+gzip", "gzip/JSON")
	fmt.Println(stringRepeat("-", 64))

	for r := 0; r < *nReduce; r++ {
		jsonPath := filepath.Join(*workDir, fmt.Sprintf("mr-0-%d.jsonl", r))
		binPath := filepath.Join(*workDir, fmt.Sprintf("mr-0-%d.bin", r))
		gzPath := filepath.Join(*workDir, fmt.Sprintf("mr-0-%d", r))

		if err := writeJSONL(jsonPath, partitions[r]); err != nil {
			fmt.Fprintf(os.Stderr, "write jsonl: %v\n", err)
			os.Exit(1)
		}
		if err := writeBinary(binPath, partitions[r]); err != nil {
			fmt.Fprintf(os.Stderr, "write binary: %v\n", err)
			os.Exit(1)
		}
		if err := writeBinaryGzip(gzPath, partitions[r]); err != nil {
			fmt.Fprintf(os.Stderr, "write gzip: %v\n", err)
			os.Exit(1)
		}

		js := fileSize(jsonPath)
		bs := fileSize(binPath)
		gs := fileSize(gzPath)
		jsonTotal += js
		binTotal += bs
		gzipTotal += gs

		ratio := float64(gs) / float64(js) * 100
		fmt.Printf("mr-0-%d      %12s %12s %12s %11.1f%%\n",
			r, humanBytes(js), humanBytes(bs), humanBytes(gs), ratio)
	}

	fmt.Println(stringRepeat("-", 64))
	totalRatio := float64(gzipTotal) / float64(jsonTotal) * 100
	fmt.Printf("%-12s %12s %12s %12s %11.1f%%\n",
		"合计", humanBytes(jsonTotal), humanBytes(binTotal), humanBytes(gzipTotal), totalRatio)
	fmt.Println()
	fmt.Printf("Binary vs JSON:     缩减 %.1f%% (%.2fx 更小)\n",
		(1-float64(binTotal)/float64(jsonTotal))*100, float64(jsonTotal)/float64(binTotal))
	fmt.Printf("Binary+gzip vs JSON: 缩减 %.1f%% (%.2fx 更小)\n",
		(1-float64(gzipTotal)/float64(jsonTotal))*100, float64(jsonTotal)/float64(gzipTotal))
	fmt.Printf("Binary+gzip vs Binary: 再缩减 %.1f%%\n",
		(1-float64(gzipTotal)/float64(binTotal))*100)
	fmt.Printf("\n基准文件目录: %s\n", *workDir)
	fmt.Printf("复现命令: go run ./cmd/shuffle_bench -input %s -bytes %d -nreduce %d -workdir %s\n",
		*input, *sampleBytes, *nReduce, *workDir)
}

func readSample(path string, maxBytes int64) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	if maxBytes <= 0 {
		data, err := io.ReadAll(f)
		return string(data), int64(len(data)), err
	}

	buf := make([]byte, maxBytes)
	n, err := io.ReadFull(f, buf)
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		return string(buf[:n]), int64(n), nil
	}
	if err != nil {
		return "", 0, err
	}
	return string(buf), int64(n), nil
}

func writeJSONL(path string, kvs []mr.KeyValue) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, kv := range kvs {
		if err := enc.Encode(kv); err != nil {
			f.Close()
			return err
		}
	}
	return f.Close()
}

func writeBinary(path string, kvs []mr.KeyValue) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return writeBinaryKVs(f, kvs)
}

func writeBinaryGzip(path string, kvs []mr.KeyValue) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(f, 64*1024)
	gw := gzip.NewWriter(bw)
	if err := writeBinaryKVs(gw, kvs); err != nil {
		f.Close()
		return err
	}
	if err := gw.Close(); err != nil {
		f.Close()
		return err
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func writeBinaryKVs(w io.Writer, kvs []mr.KeyValue) error {
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

func combineLocal(kvs []mr.KeyValue, combineFn mr.CombineFunc) []mr.KeyValue {
	if len(kvs) == 0 {
		return kvs
	}
	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })
	var result []mr.KeyValue
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
		result = append(result, mr.KeyValue{Key: key, Value: combineFn(key, values)})
		i = j
	}
	return result
}

func ihash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32())
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}

func stringRepeat(s string, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = s[0]
	}
	return string(out)
}
