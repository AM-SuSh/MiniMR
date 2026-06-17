package mr

import "testing"

func TestClassifyFailure(t *testing.T) {
	cases := []struct {
		reason string
		want   FailureCategory
		fault  bool
	}{
		{"input_read: open x: no such file", FailureInput, false},
		{"config: unknown map func foo", FailureConfig, false},
		{"intermediate_write: disk full", FailureIntermediate, false},
		{"shuffle_timeout: collected 1/3", FailureIntermediate, false},
		{"", FailureWorker, true},
		{"panic in user code", FailureWorker, true},
	}
	for _, tc := range cases {
		got := ClassifyFailure(tc.reason)
		if got != tc.want {
			t.Fatalf("ClassifyFailure(%q) = %v, want %v", tc.reason, got, tc.want)
		}
		if IsWorkerFault(got) != tc.fault {
			t.Fatalf("IsWorkerFault(%v) = %v, want %v", got, IsWorkerFault(got), tc.fault)
		}
	}
}
