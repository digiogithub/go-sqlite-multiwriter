//go:build unix

package multiwriter

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// flockBlocksOwnFcntl reports whether, on this system, a process's own shared flock on a file prevents it from
// taking an fcntl write lock on a byte of the same file (true on macOS and the BSDs, false on Linux).
func flockBlocksOwnFcntl(t *testing.T) bool {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(t.TempDir(), "probe"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if !flockShared(f) {
		t.Fatal("probe: shared flock refused")
	}
	return !byteLock(f, lockPublish, false)
}

// The layout of the locks must suit the system: where flock and fcntl locks interact, the byte locks cannot share
// the file of the membership flock (every commit would wait forever for the publication lock).
func TestByteLockLayoutSuitsPlatform(t *testing.T) {
	if flockBlocksOwnFcntl(t) && !separateByteLocks {
		t.Fatal("flock and fcntl locks interact on this system but the byte locks share the membership file")
	}
}

// A process that opened the database in multi-process mode commits (the publication lock is taken while the
// membership flock is held), with the lock layout of the platform and with the separate one.
func TestMPCommitWithMembershipHeld(t *testing.T) {
	for _, separate := range []bool{separateByteLocks, true} {
		prev := separateByteLocks
		separateByteLocks = separate
		path := filepath.Join(t.TempDir(), "db")
		db := openT(t, path, mpOpts())
		done := make(chan error, 1)
		go func() {
			if _, err := db.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
				done <- err
				return
			}
			_, err := db.Exec("INSERT INTO t(v) VALUES('x')")
			done <- err
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("separate=%v: %v", separate, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("separate=%v: the commit did not finish (publication lock never granted)", separate)
		}
		_, statErr := os.Stat(path + "-mwfcntl")
		if separate != (statErr == nil) {
			t.Fatalf("separate=%v: -mwfcntl present=%v", separate, statErr == nil)
		}
		db.Close()
		for _, suffix := range []string{"-mwlock", "-mwfcntl"} {
			if _, err := os.Stat(path + suffix); err == nil {
				t.Fatalf("separate=%v: %s left after the last close", separate, suffix)
			}
		}
		separateByteLocks = prev
	}
}

