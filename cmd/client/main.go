package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

func main() {
	masterHTTP := flag.String("master-http", "http://localhost:8081", "Master HTTP address")
	inputFiles := flag.String("input", "", "Comma-separated input files")
	nMap := flag.Int("nmap", 0, "Number of map tasks (0 = auto from split size)")
	nReduce := flag.Int("nreduce", 3, "Number of reduce tasks")
	mapFunc := flag.String("map", "wordcount_map", "Map function name")
	reduceFunc := flag.String("reduce", "wordcount_reduce", "Reduce function name")
	combineFunc := flag.String("combine", "wordcount_combine", "Combine function name")
	splitSize := flag.Int64("split", 0, "Map split size in bytes (0 = default 32 MiB, ignored when -nmap > 0)")
	workDir := flag.String("workdir", "mr-work", "Working directory")
	slowStart := flag.Float64("slowstart", 0, "Reduce slow start threshold (0 = default 0.8)")
	flag.Parse()

	if *inputFiles == "" {
		log.Fatal("请指定 -input 参数")
	}
	//  将逗号分割的字符串转换为切片，方便底层循环处理
	files := splitCSV(*inputFiles)
	jobID, err := submitJob(*masterHTTP, map[string]interface{}{
		"input_files":       files,
		"n_map":             *nMap,
		"n_reduce":          *nReduce,
		"map_func":          *mapFunc,
		"reduce_func":       *reduceFunc,
		"combine_func":      *combineFunc,
		"split_size":        *splitSize,
		"work_dir":          *workDir,
		"reduce_slow_start": *slowStart,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Job submitted: %s\n", jobID)

	for {
		status, err := getStatus(*masterHTTP, jobID)
		if err != nil {
			log.Printf("status poll: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		fmt.Printf("Status: %s (map %v/%v, reduce %v/%v)\n",
			status["state"],
			status["map_completed"], status["map_total"],
			status["reduce_completed"], status["reduce_total"])

		if state, _ := status["state"].(string); state == "completed" {
			break
		}
		time.Sleep(2 * time.Second)
	}

	result, err := getResult(*masterHTTP, jobID)
	if err != nil {
		log.Fatal(err)
	}
	data, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(data))
}

// 逗号分割输入文件
func splitCSV(s string) []string {
	var files []string
	for _, f := range bytes.Split([]byte(s), []byte(",")) {
		f := string(bytes.TrimSpace(f))
		if f != "" {
			files = append(files, f)
		}
	}
	return files
}

func submitJob(baseURL string, payload map[string]interface{}) (string, error) {
	body, _ := json.Marshal(payload)
	resp, err := http.Post(baseURL+"/api/job", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("submit failed: %s", string(b))
	}
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	jobID, _ := result["job_id"].(string)
	return jobID, nil
}

func getStatus(baseURL, jobID string) (map[string]interface{}, error) {
	resp, err := http.Get(fmt.Sprintf("%s/api/status?job=%s", baseURL, jobID))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var status map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}
	return status, nil
}

func getResult(baseURL, jobID string) (map[string]interface{}, error) {
	resp, err := http.Get(fmt.Sprintf("%s/api/result?job=%s", baseURL, jobID))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}
