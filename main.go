package main

import (
	"flag"
	"fmt"
	"log"

	"mapreduce/mr"

	_ "mapreduce/udf"
)

func main() {
	input := flag.String("input", "testdata/input.txt", "Input file")
	output := flag.String("output", "mr-out-standalone", "Output file prefix")
	nReduce := flag.Int("nreduce", 3, "Number of reduce partitions")
	udfName := flag.String("udf", "wordcount", "UDF set: wordcount or crawl_clean")
	splitSize := flag.Int64("split", 0, "Split size in bytes (0 = default 32 MiB)")
	flag.Parse()

	mapFn, reduceFn, combineFn := resolveUDF(*udfName)
	job, err := mr.RunLocal(mr.JobConfig{
		InputFiles:  []string{*input},
		NReduce:     *nReduce,
		MapFunc:     mapFn,
		ReduceFunc:  reduceFn,
		CombineFunc: combineFn,
		SplitSize:   *splitSize,
	}, *output)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Completed local job: %d map tasks, %d reduce tasks\n", job.Config.NMap, job.Config.NReduce)
	for r := 0; r < job.Config.NReduce; r++ {
		fmt.Printf("Wrote %s-%d\n", *output, r)
	}
	printOptimizationSummary(job.Metrics)
}

func resolveUDF(name string) (mapFn, reduceFn, combineFn string) {
	switch name {
	case "crawl_clean":
		return "crawl_clean_map", "crawl_clean_reduce", ""
	default:
		return "wordcount_map", "wordcount_reduce", "wordcount_combine"
	}
}

func printOptimizationSummary(m mr.JobMetrics) {
	fmt.Println("Optimization summary:")
	if m.ShuffleJSONBytes > 0 {
		saved := 100 * (1 - float64(m.ShuffleCompressedBytes)/float64(m.ShuffleJSONBytes))
		fmt.Printf("  shuffle: JSONL %s -> binary+gzip %s (saved %.1f%%)\n",
			humanBytes(m.ShuffleJSONBytes), humanBytes(m.ShuffleCompressedBytes), saved)
	}
	fmt.Printf("  streaming reduce: %d records -> %d keys, max buffered values/key %d\n",
		m.ReduceStreamedRecords, m.ReduceOutputKeys, m.ReduceMaxBufferedValues)
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
