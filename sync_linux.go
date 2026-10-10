package multiwriter

import (
	"os"
	"syscall"
)

func datasync(f *os.File) error { return syscall.Fdatasync(int(f.Fd())) }
