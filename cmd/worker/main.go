package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"mapreduce/mr"

	_ "mapreduce/udf" //匿名导入，触发用户定义函数
)

func main() {
	masterAddr := flag.String("master", "localhost:8080", "Master RPC address")
	workerID := flag.String("id", "", "Worker ID (auto-generated if empty)")
	flag.Parse()

	id := *workerID
	if id == "" {
		hostname, _ := os.Hostname()
		id = fmt.Sprintf("%s-%d", hostname, os.Getpid())
	}

	w := mr.NewWorker(id, *masterAddr)
	log.Printf("Worker %s connecting to master %s", id, *masterAddr)
	w.Run()
}
