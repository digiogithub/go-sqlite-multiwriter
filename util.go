package multiwriter

import (
	"fmt"
	"math/bits"
	"os"
	"time"
)

// MW_DEBUG=1 prints why the engine refuses what it refuses.
var debugOn = os.Getenv("MW_DEBUG") != ""

func debugf(format string, args ...any) {
	if debugOn {
		fmt.Fprintf(os.Stderr, "multiwriter: "+format+"\n", args...)
	}
}

// SQLite result codes used by the engine.
const (
	rcOK            = 0
	rcError         = 1
	rcInternal      = 2
	rcBusy          = 5
	rcNoMem         = 7
	rcIOErr         = 10
	rcCorrupt       = 11
	rcNotFound      = 12
	rcFull          = 13
	rcCantOpen      = 14
	rcMisuse        = 21
	rcNotADB        = 26
	rcBusySnapshot  = rcBusy | 2<<8
	rcIOErrRead     = rcIOErr | 1<<8
	rcIOErrShort    = rcIOErr | 2<<8
	rcIOErrWrite    = rcIOErr | 3<<8
	rcIOErrFsync    = rcIOErr | 4<<8
	rcIOErrTruncate = rcIOErr | 6<<8
	rcIOErrShmSize  = rcIOErr | 19<<8
)

// Internal results of a publication (never seen by SQLite).
const (
	errConflict       = 0x4d57 // a page the transaction wrote changed since its snapshot
	errConflictSchema = 0x4d58 // the schema changed since the snapshot
	errConflictRead   = 0x4d59 // a page the transaction only read changed
	errRelocNA        = 0x4d5a // the conflict is not a growth-only one
)

func be32(p []byte) uint32 {
	return uint32(p[0])<<24 | uint32(p[1])<<16 | uint32(p[2])<<8 | uint32(p[3])
}
func be16(p []byte) int { return int(p[0])<<8 | int(p[1]) }
func put32(p []byte, v uint32) {
	p[0], p[1], p[2], p[3] = byte(v>>24), byte(v>>16), byte(v>>8), byte(v)
}

func trailingZeros64(x uint64) int { return bits.TrailingZeros64(x) }

// varint decodes a SQLite varint; n is 0 if it runs past the end of p.
func varint(p []byte) (v uint64, n int) {
	for i := 0; i < 8; i++ {
		if i >= len(p) {
			return 0, 0
		}
		v = v<<7 | uint64(p[i]&0x7f)
		if p[i]&0x80 == 0 {
			return v, i + 1
		}
	}
	if len(p) < 9 {
		return 0, 0
	}
	return v<<8 | uint64(p[8]), 9
}

func nowNs() int64 { return time.Now().UnixNano() }
