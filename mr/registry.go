package mr

var (
	mapFuncs     = map[string]MapFunc{}
	reduceFuncs  = map[string]ReduceFunc{}
	combineFuncs = map[string]CombineFunc{}
)

// RegisterMap registers a named map function.
func RegisterMap(name string, f MapFunc) {
	mapFuncs[name] = f
}

// RegisterReduce registers a named reduce function.
func RegisterReduce(name string, f ReduceFunc) {
	reduceFuncs[name] = f
}

// RegisterCombine registers a named combine function.
func RegisterCombine(name string, f CombineFunc) {
	combineFuncs[name] = f
}

// GetMapFunc returns a registered map function.
func GetMapFunc(name string) (MapFunc, bool) {
	f, ok := mapFuncs[name]
	return f, ok
}

// GetReduceFunc returns a registered reduce function.
func GetReduceFunc(name string) (ReduceFunc, bool) {
	f, ok := reduceFuncs[name]
	return f, ok
}

// GetCombineFunc returns a registered combine function.
func GetCombineFunc(name string) (CombineFunc, bool) {
	f, ok := combineFuncs[name]
	return f, ok
}
