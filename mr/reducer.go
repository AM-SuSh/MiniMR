package mr

// ReduceFunc aggregates all values for a key and produces the final value.
type ReduceFunc func(key string, values []string) string
