package mr

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// RunLocal executes a MapReduce job in-process while still using the same
// intermediate file format and streaming reduce path as distributed workers.
func RunLocal(config JobConfig, outputPrefix string) (*Job, error) {
	if outputPrefix == "" {
		outputPrefix = "mr-out-standalone"
	}
	if config.NReduce <= 0 {
		config.NReduce = 3
	}
	if config.NMap <= 0 && config.SplitSize <= 0 {
		config.SplitSize = DefaultSplitSize
	}
	if config.ReduceSlowStart <= 0 || config.ReduceSlowStart > 1.0 {
		config.ReduceSlowStart = DefaultReduceSlowStart
	}

	splits, err := PrepareSplits(config.InputFiles, config.SplitSize, config.NMap)
	if err != nil {
		return nil, err
	}
	if len(splits) == 0 {
		return nil, fmt.Errorf("no input splits from files: %v", config.InputFiles)
	}

	outputDir := filepath.Dir(outputPrefix)
	if outputDir == "" {
		outputDir = "."
	}
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, err
	}
	workDir, err := os.MkdirTemp("", "minimr-local-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(workDir)

	config.NMap = len(splits)
	config.WorkDir = workDir
	job := &Job{
		ID:               "local",
		Config:           config,
		State:            JobRunning,
		CreatedAt:        time.Now(),
		MapDoneForReduce: make([][]bool, config.NReduce),
	}
	for r := 0; r < config.NReduce; r++ {
		job.MapDoneForReduce[r] = make([]bool, config.NMap)
	}

	localWorker := &Worker{ID: "local"}
	for i, split := range splits {
		task := &Task{
			ID:      i,
			Type:    MapTask,
			State:   InProgress,
			MapInfo: &MapTaskInfo{Split: split},
		}
		job.MapTasks = append(job.MapTasks, task)

		ok, metrics := localWorker.doMap(RequestTaskReply{
			TaskType:    MapTask,
			TaskID:      i,
			InputFile:   split.File,
			InputOffset: split.Offset,
			InputLength: split.Length,
			NReduce:     config.NReduce,
			NMap:        config.NMap,
			MapFunc:     config.MapFunc,
			ReduceFunc:  config.ReduceFunc,
			CombineFunc: config.CombineFunc,
			WorkDir:     workDir,
			JobID:       job.ID,
			AttemptID:   1,
		})
		if !ok {
			job.State = JobFailed
			job.Error = fmt.Sprintf("map-%d failed", i)
			job.CompletedAt = time.Now()
			return job, errors.New(job.Error)
		}
		task.State = Completed
		job.Metrics.AddTask(metrics)
		for r := 0; r < config.NReduce; r++ {
			job.MapDoneForReduce[r][i] = true
		}
	}

	for r := 0; r < config.NReduce; r++ {
		task := &Task{ID: r, Type: ReduceTask, State: InProgress, ReduceID: r}
		job.ReduceTasks = append(job.ReduceTasks, task)
		ok, metrics := localWorker.doReduce(RequestTaskReply{
			TaskType:   ReduceTask,
			TaskID:     r,
			NReduce:    config.NReduce,
			NMap:       config.NMap,
			ReduceID:   r,
			ReduceFunc: config.ReduceFunc,
			WorkDir:    workDir,
			JobID:      job.ID,
			AttemptID:  1,
		})
		if !ok {
			job.State = JobFailed
			job.Error = fmt.Sprintf("reduce-%d failed", r)
			job.CompletedAt = time.Now()
			return job, errors.New(job.Error)
		}
		job.Metrics.AddTask(metrics)
		task.State = Completed

		src := filepath.Join(workDir, fmt.Sprintf("mr-out-%d", r))
		dst := fmt.Sprintf("%s-%d", outputPrefix, r)
		if err := copyFile(src, dst); err != nil {
			return job, err
		}
	}

	job.State = JobCompleted
	job.CompletedAt = time.Now()
	sort.Slice(job.MapTasks, func(i, j int) bool { return job.MapTasks[i].ID < job.MapTasks[j].ID })
	sort.Slice(job.ReduceTasks, func(i, j int) bool { return job.ReduceTasks[i].ID < job.ReduceTasks[j].ID })
	return job, nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
