// 单机模式入口 — 向后兼容，无需 Master/Worker 即可运行 WordCount。
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sort"

	"mapreduce/mr"

	_ "mapreduce/udf"
)

func main() {
	input := flag.String("input", "testdata/input.txt", "Input file")               //输入文件路径
	output := flag.String("output", "mr-out-standalone", "Output file prefix")      // 输出前缀
	nReduce := flag.Int("nreduce", 3, "Number of reduce partitions")                // reduce任务数
	udfName := flag.String("udf", "wordcount", "UDF set: wordcount or crawl_clean") // 要使用的函数集（通过udfName决定加载哪种算法，通过mr包提供的注册机制获取具体的业务逻辑函数）
	flag.Parse()

	var mapFn, reduceFn, combineFn string
	switch *udfName {
	case "crawl_clean":
		mapFn, reduceFn, combineFn = "crawl_clean_map", "crawl_clean_reduce", ""
	default:
		mapFn, reduceFn, combineFn = "wordcount_map", "wordcount_reduce", "wordcount_combine"
	}

	content, err := os.ReadFile(*input)
	if err != nil {
		log.Fatal(err)
	}

	// Map phase
	mapFunc, _ := mr.GetMapFunc(mapFn)
	kvs := mapFunc(*input, string(content))

	if combineFn != "" {
		if cf, ok := mr.GetCombineFunc(combineFn); ok {
			kvs = combineInProcess(kvs, cf)
		}
	}

	partitions := make([][]mr.KeyValue, *nReduce)
	for _, kv := range kvs {
		r := ihash(kv.Key) % *nReduce
		partitions[r] = append(partitions[r], kv)
	}

	reduceFunc, _ := mr.GetReduceFunc(reduceFn)
	for r := 0; r < *nReduce; r++ {
		sort.Slice(partitions[r], func(i, j int) bool {
			return partitions[r][i].Key < partitions[r][j].Key
		})

		outPath := fmt.Sprintf("%s-%d", *output, r)
		f, err := os.Create(outPath)
		if err != nil {
			log.Fatal(err)
		}
		i := 0
		vals := partitions[r]
		for i < len(vals) {
			key := vals[i].Key
			j := i + 1
			for j < len(vals) && vals[j].Key == key {
				j++
			}
			values := make([]string, j-i)
			for k := i; k < j; k++ {
				values[k-i] = vals[k].Value
			}
			fmt.Fprintf(f, "%s\t%s\n", key, reduceFunc(key, values))
			i = j
		}
		f.Close()
		fmt.Printf("Wrote %s\n", outPath)
	}

}

func combineInProcess(kvs []mr.KeyValue, combineFn mr.CombineFunc) []mr.KeyValue {
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
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h = h*16777619 ^ uint32(key[i])
	}
	return int(h)
}
