//go:build !linux

package mr

import "fmt"

// LoadPlugin is a no-op on platforms that do not support Go plugins.
func LoadPlugin(namePrefix, path string) error {
	return fmt.Errorf("plugin loading is not supported on this platform; use static registration instead")
}
