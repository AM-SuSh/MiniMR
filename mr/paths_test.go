package mr

import (
	"os"
	"path/filepath"
	"testing"
)

func TestJobWorkDirIsolation(t *testing.T) {
	root := t.TempDir()
	jobA := "aaaaaaaaaaaaaaaa"
	jobB := "bbbbbbbbbbbbbbbb"
	dirA := JobWorkDir(root, jobA)
	dirB := JobWorkDir(root, jobB)
	if err := os.MkdirAll(dirA, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dirB, 0755); err != nil {
		t.Fatal(err)
	}

	pathA := intermediatePath(dirA, 0, 0)
	pathB := intermediatePath(dirB, 0, 0)
	if err := os.WriteFile(pathA, []byte("a"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pathB, []byte("b"), 0644); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(pathA)
	if err != nil || string(data) != "a" {
		t.Fatalf("job A file corrupted: %q err=%v", data, err)
	}
	if pathA == pathB {
		t.Fatal("jobs should not share intermediate paths")
	}
	if filepath.Dir(pathA) == filepath.Dir(pathB) {
		t.Fatal("jobs should use separate subdirectories")
	}
}

func TestResolveJobDataDirLegacyFallback(t *testing.T) {
	root := t.TempDir()
	jobID := "legacy-job"
	path := intermediatePath(root, 0, 0)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(readyPath(path, jobID), []byte("ready\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if got := resolveJobDataDir(root, jobID); got != root {
		t.Fatalf("expected legacy root %q, got %q", root, got)
	}
}
