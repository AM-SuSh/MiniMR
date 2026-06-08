package main

import (
	"flag"
	"log"

	"mapreduce/mr"
)

func main() {
	rpcAddr := flag.String("port", ":8080", "RPC listen address")
	httpAddr := flag.String("http", ":8081", "HTTP API listen address")
	flag.Parse()

	m := mr.NewMaster(*rpcAddr, *httpAddr)
	log.Fatal(m.Serve())
}
