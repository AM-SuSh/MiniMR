package mr

import (
	"os"
	"path/filepath"
)

// JobWorkDir returns the per-job subdirectory under the configured work root.
// Intermediate and output files live under WorkDir/jobID/ so multiple jobs can
// share the same work root without overwriting each other's artifacts.
func JobWorkDir(workDir, jobID string) string {
	if workDir == "" {
		workDir = "mr-work"
	}
	if jobID == "" {
		return workDir
	}
	return filepath.Join(workDir, jobID)
}

// resolveJobDataDir locates on-disk artifacts for a job. New jobs use the scoped
// layout; legacy checkpoints that wrote directly into WorkDir are still found.
func resolveJobDataDir(workDir, jobID string) string {
	scoped := JobWorkDir(workDir, jobID)
	if _, err := os.Stat(scoped); err == nil {
		return scoped
	}
	matches, _ := filepath.Glob(filepath.Join(workDir, "*."+jobID+".ready"))
	if len(matches) > 0 {
		return workDir
	}
	return scoped
}
