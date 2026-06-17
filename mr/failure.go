package mr

import (
	"fmt"
	"strings"
)

// FailureCategory classifies task failure causes for scheduling decisions.
type FailureCategory int

const (
	FailureUnknown FailureCategory = iota
	FailureInput
	FailureIntermediate
	FailureConfig
	FailureWorker
)

// ClassifyFailure maps a worker-reported reason string to a category.
func ClassifyFailure(reason string) FailureCategory {
	reason = strings.TrimSpace(reason)
	switch {
	case strings.HasPrefix(reason, "input_read:"):
		return FailureInput
	case strings.HasPrefix(reason, "config:"):
		return FailureConfig
	case strings.HasPrefix(reason, "intermediate_"), strings.HasPrefix(reason, "shuffle_"):
		return FailureIntermediate
	case reason == "":
		return FailureWorker
	default:
		return FailureWorker
	}
}

// IsWorkerFault reports whether a failure should count against worker reliability.
func IsWorkerFault(cat FailureCategory) bool {
	return cat == FailureWorker || cat == FailureUnknown
}

func formatInputJobFailure(task *Task, reason, inputFile string) string {
	if inputFile == "" && task != nil && task.MapInfo != nil {
		inputFile = task.MapInfo.Split.File
	}
	detail := strings.TrimSpace(strings.TrimPrefix(reason, "input_read:"))
	if inputFile != "" {
		return fmt.Sprintf("输入数据不可用：%s (%s)", inputFile, detail)
	}
	return fmt.Sprintf("输入数据不可用：%s", detail)
}

func formatJobFailureReason(task *Task, base string) string {
	if task == nil || strings.TrimSpace(task.LastFailureReason) == "" {
		return base
	}
	cat := ClassifyFailure(task.LastFailureReason)
	switch cat {
	case FailureInput:
		return formatInputJobFailure(task, task.LastFailureReason, "")
	case FailureConfig:
		return fmt.Sprintf("作业配置错误：%s", strings.TrimSpace(strings.TrimPrefix(task.LastFailureReason, "config:")))
	case FailureIntermediate:
		return fmt.Sprintf("%s（%s）", base, strings.TrimSpace(task.LastFailureReason))
	default:
		detail := strings.TrimSpace(task.LastFailureReason)
		if detail == "" {
			return base
		}
		return fmt.Sprintf("%s（最近失败：%s）", base, detail)
	}
}
