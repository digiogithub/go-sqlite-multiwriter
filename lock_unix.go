//go:build unix

package multiwriter

import (
	"os"
	"syscall"
)

// lockFile takes an exclusive advisory lock on the log: one process owns the database.
func lockFile(f *os.File) error   { return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) }
func unlockFile(f *os.File) error { return syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }

func fsyncFile(f *os.File) error { return f.Sync() }

// syncDir flushes the directory of path: a file that was created is in its directory only once it is.
func syncDir(path string) {
	dir := path
	for i := len(dir) - 1; i >= 0; i-- {
		if dir[i] == '/' {
			dir = dir[:i]
			if dir == "" {
				dir = "/"
			}
			break
		}
		if i == 0 {
			dir = "."
		}
	}
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}

// datasyncFile flushes the data of a file whose size did not change (the log is written ahead of its end).
func datasyncFile(f *os.File) error { return datasync(f) }
