//go:build plugin

package main

import (
	"mapreduce/mr"
	"mapreduce/udf"
)

func Map(filename string, contents string) []mr.KeyValue {
	return udf.WordCountMap(filename, contents)
}
