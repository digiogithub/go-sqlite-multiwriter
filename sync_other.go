//go:build unix && !linux

package multiwriter

import "os"

// datasync: on macOS os.File.Sync is F_FULLFSYNC, which is what a commit needs to be durable.
func datasync(f *os.File) error { return f.Sync() }
