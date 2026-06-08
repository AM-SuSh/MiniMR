package mr

// CombineFunc performs local pre-aggregation on the map side.
// It shares the same signature as ReduceFunc.
type CombineFunc func(key string, values []string) string
