//go:build !unix

package multiwriter

import "os"

// The multi-process mode needs shared mappings and fcntl locks: unix only for now.
const mpSupported = false

const (
	lockPublish = 1
	lockCompact = 2
	lockPrefill = 3
)

type shmMap struct{ mem []byte }

func mapShared(*os.File, int) (*shmMap, error) { return nil, os.ErrInvalid }
func (m *shmMap) unmap()                       {}
func byteLock(*os.File, int64, bool) bool      { return false }
func byteUnlock(*os.File, int64)               {}
func flockTry(*os.File, bool) bool             { return false }
func flockWait(*os.File, bool) error           { return os.ErrInvalid }
func flockShared(*os.File) bool                { return false }
func pidAlive(int32) bool                      { return false }
