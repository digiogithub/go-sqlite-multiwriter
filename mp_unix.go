//go:build unix

package multiwriter

// The operating-system side of the multi-process mode: the shared header mapped by every process, the byte locks
// (fcntl: the kernel releases them when a process dies, whatever it was doing), membership (flock) and liveness.

import (
	"os"
	"syscall"
)

const mpSupported = true

// Byte ranges of the lock file used as cross-process locks.
const (
	lockPublish = 1 // the publication lock: one publisher at a time over all processes
	lockCompact = 2 // one compactor at a time
	lockPrefill = 3 // one preparer of the next log segment at a time
)

type shmMap struct {
	f    *os.File
	mem  []byte
	size int
}

func mapShared(f *os.File, size int) (*shmMap, error) {
	if fi, err := f.Stat(); err != nil {
		return nil, err
	} else if fi.Size() < int64(size) {
		// written whole (not a sparse file: a first touch of a hole on a full file system is a SIGBUS)
		if _, err := f.WriteAt(make([]byte, int64(size)-fi.Size()), fi.Size()); err != nil {
			return nil, err
		}
	}
	mem, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &shmMap{f: f, mem: mem, size: size}, nil
}

func (m *shmMap) unmap() {
	if m.mem != nil {
		syscall.Munmap(m.mem)
		m.mem = nil
	}
}

// byteLock takes (wait) or tries (!wait) an exclusive fcntl lock on one byte of f.
func byteLock(f *os.File, b int64, wait bool) bool {
	lk := syscall.Flock_t{Type: syscall.F_WRLCK, Whence: 0, Start: b, Len: 1}
	cmd := syscall.F_SETLK
	if wait {
		cmd = syscall.F_SETLKW
	}
	for {
		err := syscall.FcntlFlock(f.Fd(), cmd, &lk)
		if err == syscall.EINTR {
			continue
		}
		return err == nil
	}
}

func byteUnlock(f *os.File, b int64) {
	lk := syscall.Flock_t{Type: syscall.F_UNLCK, Whence: 0, Start: b, Len: 1}
	syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &lk)
}

// Membership of the database: every process holds a shared flock; the first one (and the last one) gets it exclusive.
func flockTry(f *os.File, exclusive bool) bool {
	how := syscall.LOCK_SH
	if exclusive {
		how = syscall.LOCK_EX
	}
	return syscall.Flock(int(f.Fd()), how|syscall.LOCK_NB) == nil
}

func flockWait(f *os.File, exclusive bool) error {
	how := syscall.LOCK_SH
	if exclusive {
		how = syscall.LOCK_EX
	}
	for {
		err := syscall.Flock(int(f.Fd()), how)
		if err != syscall.EINTR {
			return err
		}
	}
}

func flockShared(f *os.File) bool {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB) == nil
}

func pidAlive(pid int32) bool {
	if pid <= 0 {
		return false
	}
	if int(pid) == os.Getpid() {
		return true
	}
	err := syscall.Kill(int(pid), 0)
	return err == nil || err == syscall.EPERM
}
