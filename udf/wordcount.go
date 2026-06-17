package udf

import (
	"mapreduce/mr"
	"regexp"
	"strconv"
	"strings"
)

var wordRe = regexp.MustCompile(`[a-zA-Z\p{Han}]+`)

func registerWordCount() {
	mr.RegisterMap("wordcount_map", WordCountMap)
	mr.RegisterReduce("wordcount_reduce", WordCountReduce)
	mr.RegisterCombine("wordcount_combine", WordCountCombine)
}

// WordCountMap splits text into words and emits (word, "1").
func WordCountMap(filename string, contents string) []mr.KeyValue {
	var kvs []mr.KeyValue
	words := wordRe.FindAllString(contents, -1)
	for _, w := range words {
		w = strings.ToLower(w)
		if w != "" {
			kvs = append(kvs, mr.KeyValue{Key: w, Value: "1"})
		}
	}
	return kvs
}

// WordCountCombine sums counts locally on the map side.
func WordCountCombine(key string, values []string) string {
	return WordCountReduce(key, values)
}

// WordCountReduce sums all values for a key.
func WordCountReduce(key string, values []string) string {
	sum := 0
	for _, v := range values {
		n, err := strconv.Atoi(v)
		if err == nil {
			sum += n
		}
	}
	return strconv.Itoa(sum)
}
