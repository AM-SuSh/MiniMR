//go:build linux

package mr

import (
	"fmt"
	"plugin"
)

// LoadPlugin dynamically loads a .so plugin and registers any exported
// Map / Reduce / Combine functions under the given name prefix.
// E.g. LoadPlugin("wordcount", "wc.so") registers "wordcount_map", etc.
func LoadPlugin(namePrefix, path string) error {
	p, err := plugin.Open(path)
	if err != nil {
		return fmt.Errorf("plugin.Open(%s): %w", path, err)
	}

	if sym, err := p.Lookup("Map"); err == nil {
		fn, ok := sym.(func(string, string) []KeyValue)
		if !ok {
			return fmt.Errorf("plugin %s: Map has wrong signature", path)
		}
		RegisterMap(namePrefix+"_map", fn)
	}

	if sym, err := p.Lookup("Reduce"); err == nil {
		fn, ok := sym.(func(string, []string) string)
		if !ok {
			return fmt.Errorf("plugin %s: Reduce has wrong signature", path)
		}
		RegisterReduce(namePrefix+"_reduce", fn)
	}

	if sym, err := p.Lookup("Combine"); err == nil {
		fn, ok := sym.(func(string, []string) string)
		if !ok {
			return fmt.Errorf("plugin %s: Combine has wrong signature", path)
		}
		RegisterCombine(namePrefix+"_combine", fn)
	}

	return nil
}
