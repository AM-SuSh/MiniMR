package mr

// MapFunc processes one input split and emits intermediate key-value pairs.
type MapFunc func(filename string, contents string) []KeyValue
