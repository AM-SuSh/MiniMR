//go:build plugin

package main

import "mapreduce/udf"

func Reduce(key string, values []string) string {
	return udf.WordCountReduce(key, values)
}
