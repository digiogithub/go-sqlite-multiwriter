package multiwriter

// The durable commit log. Committed state = the database file + the committed page versions; the versions live in
// memory, so every commit is first appended here as one checksummed record.
//
// Files: "<db>-mw" (the meta header: format, page size, salt, compacted base epoch; it is also the lock that makes
// one process the owner of the database) and the segments "<db>-mw.000001", "<db>-mw.000002"... A segment is
// written up to segBytes and then the log rolls to the next one; compaction deletes the segments whose records are
// all in the database file. No log is ever rewritten in place.
//
//	meta     (64 B): magic "MWGOLOG1", version, page size, base epoch, salt, checksum
//	segment  (32 B header): magic "MWGOSEG1", id, page size, salt, checksum
//	record   (40 B header + npages * (4 + pgsz)): magic, npages, epoch, dbsize, pgsz, 0, 0, checksum
//
// Durability ordering:
//  1. the commit is validated and installed in the store invisibly; its epoch and its place in the log are assigned
//     together under the store's seqMu (epoch order == log order);
//  2. outside any lock the record is written; with synchronous=FULL the committer waits for an fsync that started
//     after its write (group commit: one fsync covers every record written before it began);
//  3. only then does the epoch become visible, and only after the previous one did.
//
// Recovery replays the longest valid, epoch-contiguous prefix of records newer than the base and cuts the rest.

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	logMetaMagic   = "MWGOLOG1"
	segMagic       = "MWGOSEG1"
	logFormat      = 1
	logMetaSize    = 64
	segHdrSize     = 32
	recHdrSize     = 40
	recMagic       = 0x4731574d // "MW1G"
	defaultSegSize = 64 << 20
	minRollBytes   = 1 << 20
)

var errLogBusy = errors.New("multiwriter: the database is open in another process")

type segment struct {
	id       uint32
	f        *os.File
	dirty    atomic.Bool // written since the last fsync
	maxEpoch uint64      // newest epoch recorded in it (seqMu)
	end      int64       // where the next record goes (seqMu)

	// The file is written ahead of its end with zeros: a record then overwrites blocks that exist, and its fsync
	// does not have to commit a new size of the file (measured: about half the cost of an fsync after an append).
	// Records are only placed below ready; the prefiller only writes above it.
	ready   atomic.Int64
	fillMu  sync.Mutex
	filling atomic.Bool
}

const prefillChunk = 2 << 20

var zeroChunk = make([]byte, 256<<10)

// prefill writes zeros in [ready, ready+n) and moves ready (fillMu held).
func (s *segment) prefill(n int64) error {
	from := s.ready.Load()
	for off := int64(0); off < n; off += int64(len(zeroChunk)) {
		k := min(int64(len(zeroChunk)), n-off)
		if _, err := s.f.WriteAt(zeroChunk[:k], from+off); err != nil {
			return err
		}
	}
	if err := fsyncFile(s.f); err != nil {
		return err
	}
	s.ready.Store(from + n)
	return nil
}

// prefillBG keeps the file written ahead of its end, outside every lock the commits need.
func (s *segment) prefillBG() {
	if !s.filling.CompareAndSwap(false, true) {
		return
	}
	go func() {
		s.fillMu.Lock()
		s.prefill(prefillChunk)
		s.fillMu.Unlock()
		s.filling.Store(false)
	}()
}

type wlog struct {
	path string
	pgsz int
	salt uint64
	meta *os.File
	base atomic.Uint64

	segMu    sync.Mutex // the list of segments (assignment also holds the store's seqMu)
	segs     []*segment
	segBytes int64
	roll     bool // the next record starts a new segment (seqMu)
	total    atomic.Int64
	// bytes of records ever placed (monotonic) and the value of it the last compaction covered: the difference is
	// what waits for a compaction (the size of the files is not: a segment is only deleted whole)
	appended      atomic.Int64
	compactedMark atomic.Int64

	syncMu      sync.Mutex
	syncCond    *sync.Cond
	syncStarted uint64
	syncDone    uint64
	syncRunning bool
	syncBroken  bool // an fsync failed: the data it should have covered may never reach the disk (syncMu)
	nSyncs      atomic.Int64
}

// MARK: - checksums

// A 64-bit word-at-a-time multiply/xor mixer: only has to detect torn or stale records.
func mix64(h uint64, p []byte) uint64 {
	for len(p) >= 8 {
		h ^= binary.LittleEndian.Uint64(p)
		h *= 0x9E3779B97F4A7C15
		h ^= h >> 32
		p = p[8:]
	}
	for _, b := range p {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return h
}

const mixSeed = 1469598103934665603

// MARK: - open / recovery

func segPath(base string, id uint32) string { return fmt.Sprintf("%s.%06d", base, id) }

func listSegments(base string) ([]uint32, error) {
	m, err := filepath.Glob(base + ".*")
	if err != nil {
		return nil, err
	}
	var ids []uint32
	for _, p := range m {
		suf := p[len(base)+1:]
		if len(suf) != 6 || strings.Trim(suf, "0123456789") != "" {
			continue
		}
		v, err := strconv.ParseUint(suf, 10, 32)
		if err == nil && v > 0 {
			ids = append(ids, uint32(v))
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

func encodeMeta(pgsz int, base, salt uint64) []byte {
	b := make([]byte, logMetaSize)
	copy(b, logMetaMagic)
	binary.LittleEndian.PutUint32(b[8:], logFormat)
	binary.LittleEndian.PutUint32(b[12:], uint32(pgsz))
	binary.LittleEndian.PutUint64(b[16:], base)
	binary.LittleEndian.PutUint64(b[24:], salt)
	binary.LittleEndian.PutUint64(b[56:], mix64(mixSeed, b[:56]))
	return b
}

// openLog opens (or creates) the log of the database at dbPath and installs the recovered commits in st. It
// returns the epoch of the last recovered commit (the base when there is none).
func openLog(dbPath string, pgsz int, st *store, mode os.FileMode, shared bool) (*wlog, uint64, int) {
	lg := &wlog{path: dbPath + "-mw", pgsz: pgsz, segBytes: defaultSegSize}
	lg.syncCond = sync.NewCond(&lg.syncMu)
	f, err := os.OpenFile(lg.path, os.O_RDWR|os.O_CREATE, mode)
	if err != nil {
		return nil, 0, rcCantOpen
	}
	// one process owns the log; in the multi-process mode every process holds it shared (so a single-process opener
	// is refused, and the other way round)
	locked := false
	if shared {
		locked = flockShared(f)
	} else {
		locked = lockFile(f) == nil
	}
	if !locked {
		f.Close()
		return nil, 0, rcBusy
	}
	lg.meta = f
	fi, err := f.Stat()
	if err != nil {
		lg.closeFiles()
		return nil, 0, rcIOErr
	}
	ids, err := listSegments(lg.path)
	if err != nil {
		lg.closeFiles()
		return nil, 0, rcIOErr
	}
	hdr := make([]byte, logMetaSize)
	valid := false
	if fi.Size() >= logMetaSize {
		if _, err := f.ReadAt(hdr, 0); err == nil {
			if string(hdr[:5]) == logMetaMagic[:5] && string(hdr[:8]) != logMetaMagic {
				lg.closeFiles()
				return nil, 0, rcCantOpen // another format of the log: never read, never dropped
			}
			valid = string(hdr[:8]) == logMetaMagic &&
				binary.LittleEndian.Uint64(hdr[56:]) == mix64(mixSeed, hdr[:56]) &&
				binary.LittleEndian.Uint32(hdr[8:]) == logFormat &&
				int(binary.LittleEndian.Uint32(hdr[12:])) == pgsz
			if string(hdr[:8]) == logMetaMagic && binary.LittleEndian.Uint32(hdr[8:]) != logFormat {
				lg.closeFiles()
				return nil, 0, rcCantOpen
			}
		}
	}
	if !valid {
		// A log whose header does not check out but with commits behind it is not a new log: they are not thrown away.
		for _, id := range ids {
			if fi, err := os.Stat(segPath(lg.path, id)); err == nil && fi.Size() > segHdrSize {
				lg.closeFiles()
				return nil, 0, rcCorrupt
			}
		}
		for _, id := range ids {
			os.Remove(segPath(lg.path, id))
		}
		var sb [8]byte
		rand.Read(sb[:])
		lg.salt = binary.LittleEndian.Uint64(sb[:])
		lg.base.Store(1)
		if err := f.Truncate(0); err != nil {
			lg.closeFiles()
			return nil, 0, rcIOErr
		}
		if rc := lg.writeMeta(1); rc != rcOK {
			lg.closeFiles()
			return nil, 0, rc
		}
		syncDir(lg.path)
		if rc := lg.newSegment(1); rc != rcOK {
			lg.closeFiles()
			return nil, 0, rc
		}
		return lg, 1, rcOK
	}
	base := binary.LittleEndian.Uint64(hdr[16:])
	lg.salt = binary.LittleEndian.Uint64(hdr[24:])
	lg.base.Store(base)
	last, rc := lg.replay(ids, base, st)
	if rc != rcOK {
		lg.closeFiles()
		return nil, 0, rc
	}
	if len(lg.segs) == 0 {
		next := uint32(1)
		if len(ids) > 0 {
			next = ids[len(ids)-1] + 1
		}
		if rc := lg.newSegment(next); rc != rcOK {
			lg.closeFiles()
			return nil, 0, rc
		}
	}
	return lg, last, rcOK
}

// replay installs the valid, contiguous prefix of the records newer than base, cuts a torn tail durably and removes
// the segments beyond it.
func (lg *wlog) replay(ids []uint32, base uint64, st *store) (uint64, int) {
	last := base
	var prev uint64
	stopped := false
	for _, id := range ids {
		p := segPath(lg.path, id)
		if stopped {
			os.Remove(p) // beyond a hole: never acknowledged
			continue
		}
		f, err := os.OpenFile(p, os.O_RDWR, 0)
		if err != nil {
			return 0, rcIOErr
		}
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			return 0, rcIOErr
		}
		size := fi.Size()
		h := make([]byte, segHdrSize)
		if size < segHdrSize {
			f.Close()
			os.Remove(p) // a segment whose creation did not finish
			stopped = true
			continue
		}
		if _, err := f.ReadAt(h, 0); err != nil {
			f.Close()
			return 0, rcIOErr
		}
		if string(h[:8]) != segMagic || binary.LittleEndian.Uint32(h[8:]) != id || int(binary.LittleEndian.Uint32(h[12:])) != lg.pgsz ||
			binary.LittleEndian.Uint64(h[16:]) != lg.salt || binary.LittleEndian.Uint64(h[24:]) != mix64(mixSeed, h[:24]) {
			f.Close()
			if size > segHdrSize {
				return 0, rcCorrupt
			}
			os.Remove(p)
			stopped = true
			continue
		}
		seg := &segment{id: id, f: f}
		off := int64(segHdrSize)
		rh := make([]byte, recHdrSize)
		var body []byte
		for {
			if off+recHdrSize > size {
				break
			}
			if _, err := f.ReadAt(rh, off); err != nil {
				return 0, rcIOErr
			}
			npages := binary.LittleEndian.Uint32(rh[4:])
			epoch := binary.LittleEndian.Uint64(rh[8:])
			dbsize := binary.LittleEndian.Uint32(rh[16:])
			if binary.LittleEndian.Uint32(rh[0:]) != recMagic || int(binary.LittleEndian.Uint32(rh[20:])) != lg.pgsz || npages == 0 || npages > 1<<24 {
				break
			}
			blen := int64(npages) * int64(4+lg.pgsz)
			if off+recHdrSize+blen > size {
				break
			}
			if int64(cap(body)) < blen {
				body = make([]byte, blen)
			}
			body = body[:blen]
			if _, err := f.ReadAt(body, off+recHdrSize); err != nil {
				return 0, rcIOErr
			}
			if binary.LittleEndian.Uint64(rh[32:]) != recChecksum(lg.salt, rh, body) {
				break
			}
			if prev != 0 && epoch != prev+1 {
				break
			}
			if epoch > base {
				if epoch != last+1 {
					break
				}
				pgnos := make([]uint32, npages)
				imgs := make([][]byte, npages)
				for i := range pgnos {
					e := body[i*(4+lg.pgsz):]
					pgnos[i] = binary.LittleEndian.Uint32(e)
					imgs[i] = e[4 : 4+lg.pgsz]
				}
				st.installRecovered(epoch, dbsize, pgnos, imgs)
				last = epoch
			}
			prev = epoch
			if epoch > seg.maxEpoch {
				seg.maxEpoch = epoch
			}
			off += recHdrSize + blen
		}
		if off < size {
			// the torn tail is cut durably: a record of it that survived a power failure could otherwise rejoin the
			// log behind new commits that reuse its epochs
			if err := f.Truncate(off); err != nil {
				return 0, rcIOErr
			}
			if err := f.Sync(); err != nil {
				return 0, rcIOErrFsync
			}
			stopped = true
		}
		seg.end = off
		seg.ready.Store(off)
		lg.segs = append(lg.segs, seg)
		lg.total.Add(off)
		lg.appended.Add(off) // (a recovered log waits for a compaction)
	}
	return last, rcOK
}

func recChecksum(salt uint64, hdr, body []byte) uint64 {
	var h [recHdrSize]byte
	copy(h[:], hdr)
	binary.LittleEndian.PutUint64(h[32:], 0)
	x := mix64(salt^mixSeed, h[:])
	return mix64(x, body)
}

func (lg *wlog) writeMeta(base uint64) int {
	if _, err := lg.meta.WriteAt(encodeMeta(lg.pgsz, base, lg.salt), 0); err != nil {
		return rcIOErrWrite
	}
	if err := fsyncFile(lg.meta); err != nil {
		return rcIOErrFsync
	}
	return rcOK
}

// newSegment creates segment id and makes it the current one (store.seqMu held, or before the database is used).
func (lg *wlog) newSegment(id uint32) int {
	p := segPath(lg.path, id)
	mode := os.FileMode(0o600)
	if fi, err := lg.meta.Stat(); err == nil {
		mode = fi.Mode().Perm()
	}
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return rcCantOpen
	}
	h := make([]byte, segHdrSize)
	copy(h, segMagic)
	binary.LittleEndian.PutUint32(h[8:], id)
	binary.LittleEndian.PutUint32(h[12:], uint32(lg.pgsz))
	binary.LittleEndian.PutUint64(h[16:], lg.salt)
	binary.LittleEndian.PutUint64(h[24:], mix64(mixSeed, h[:24]))
	if _, err := f.WriteAt(h, 0); err != nil {
		f.Close()
		os.Remove(p)
		return rcIOErrWrite
	}
	if err := fsyncFile(f); err != nil {
		f.Close()
		os.Remove(p)
		return rcIOErrFsync
	}
	syncDir(p)
	seg := &segment{id: id, f: f, end: segHdrSize}
	seg.ready.Store(segHdrSize)
	lg.segMu.Lock()
	lg.segs = append(lg.segs, seg)
	lg.segMu.Unlock()
	lg.total.Add(segHdrSize)
	return rcOK
}

func (lg *wlog) cur() *segment { return lg.segs[len(lg.segs)-1] }

func (lg *wlog) recordSize(n int) int64 { return recHdrSize + int64(n)*int64(4+lg.pgsz) }

// assign reserves the place of the record of epoch (store.seqMu held).
func (lg *wlog) assign(epoch uint64, n int) (*segment, int64, int) {
	size := lg.recordSize(n)
	seg := lg.cur()
	if lg.roll || (seg.end+size > lg.segBytes && seg.end > segHdrSize) {
		if rc := lg.newSegment(seg.id + 1); rc != rcOK {
			return nil, 0, rc
		}
		lg.roll = false
		seg = lg.cur()
	}
	off := seg.end
	if off+size > seg.ready.Load() {
		// the prefiller is behind: wait for it, then fill what is still missing here
		seg.fillMu.Lock()
		if short := off + size - seg.ready.Load(); short > 0 {
			if err := seg.prefill(max(short, prefillChunk)); err != nil {
				seg.fillMu.Unlock()
				return nil, 0, rcIOErrWrite
			}
		}
		seg.fillMu.Unlock()
	}
	seg.end += size
	if seg.ready.Load()-seg.end < prefillChunk/2 {
		seg.prefillBG()
	}
	if epoch > seg.maxEpoch {
		seg.maxEpoch = epoch
	}
	lg.total.Add(size)
	lg.appended.Add(size)
	return seg, off, rcOK
}

// unassign gives the place back if nothing was assigned after it (store.seqMu held).
func (lg *wlog) unassign(seg *segment, off int64, n int) bool {
	size := lg.recordSize(n)
	if lg.cur() != seg || seg.end != off+size {
		return false
	}
	seg.end = off
	lg.total.Add(-size)
	lg.appended.Add(-size)
	return true
}

// write writes the record of a commit at its place.
func (lg *wlog) write(seg *segment, off int64, epoch uint64, dbsize uint32, pgnos []uint32, imgs [][]byte) int {
	n := len(pgnos)
	buf := make([]byte, lg.recordSize(n))
	binary.LittleEndian.PutUint32(buf[0:], recMagic)
	binary.LittleEndian.PutUint32(buf[4:], uint32(n))
	binary.LittleEndian.PutUint64(buf[8:], epoch)
	binary.LittleEndian.PutUint32(buf[16:], dbsize)
	binary.LittleEndian.PutUint32(buf[20:], uint32(lg.pgsz))
	body := buf[recHdrSize:]
	for i, pg := range pgnos {
		e := body[i*(4+lg.pgsz):]
		binary.LittleEndian.PutUint32(e, pg)
		copy(e[4:4+lg.pgsz], imgs[i])
	}
	binary.LittleEndian.PutUint64(buf[32:], recChecksum(lg.salt, buf[:recHdrSize], body))
	if _, err := seg.f.WriteAt(buf, off); err != nil {
		return rcIOErrWrite
	}
	seg.dirty.Store(true)
	return rcOK
}

// sync returns when an fsync that began after the caller's write has completed (group commit).
func (lg *wlog) sync() int {
	lg.syncMu.Lock()
	defer lg.syncMu.Unlock()
	need := lg.syncStarted + 1
	for lg.syncDone < need {
		if lg.syncBroken {
			// a later fsync that succeeds (the kernel forgets the error) must not acknowledge a commit that sits
			// behind a record that may be lost
			return rcIOErrFsync
		}
		if lg.syncRunning {
			lg.syncCond.Wait()
			continue
		}
		lg.syncRunning = true
		lg.syncStarted++
		mine := lg.syncStarted
		lg.segMu.Lock()
		var files []*os.File
		for _, s := range lg.segs {
			if s.dirty.Swap(false) {
				files = append(files, s.f)
			}
		}
		lg.segMu.Unlock()
		lg.syncMu.Unlock()
		var err error
		for _, f := range files {
			if e := datasyncFile(f); e != nil {
				err = e
			}
		}
		lg.syncMu.Lock()
		lg.syncRunning = false
		lg.syncCond.Broadcast()
		if err != nil {
			lg.syncBroken = true
			return rcIOErrFsync
		}
		lg.syncDone = mine
		lg.nSyncs.Add(1)
	}
	return rcOK
}

func (lg *wlog) baseEpoch() uint64 { return lg.base.Load() }
func (lg *wlog) bytes() int64      { return lg.total.Load() }

// pending is the number of bytes placed since what the last compaction covered (the compaction trigger).
func (lg *wlog) pending() int64 { return lg.appended.Load() - lg.compactedMark.Load() }

// markCompacted: a compaction covered everything placed up to mark.
func (lg *wlog) markCompacted(mark int64) {
	for {
		cur := lg.compactedMark.Load()
		if mark <= cur || lg.compactedMark.CompareAndSwap(cur, mark) {
			return
		}
	}
}

// setBase durably records that the state up to base is in the database file.
func (lg *wlog) setBase(base uint64) int {
	if rc := lg.writeMeta(base); rc != rcOK {
		return rc
	}
	lg.base.Store(base)
	return rcOK
}

// dropUpTo removes the segments whose records are all <= base (store.seqMu held). The current segment is rolled
// over instead, so that the next compaction can remove it.
func (lg *wlog) dropUpTo(base uint64, quiescent bool) {
	lg.segMu.Lock()
	defer lg.segMu.Unlock()
	keep := lg.segs[:0]
	for i, s := range lg.segs {
		if i < len(lg.segs)-1 && s.maxEpoch <= base {
			s.f.Close()
			os.Remove(segPath(lg.path, s.id))
			lg.total.Add(-s.end)
			continue
		}
		keep = append(keep, s)
	}
	lg.segs = keep
	if c := lg.segs[len(lg.segs)-1]; c.maxEpoch <= base && c.end > segHdrSize {
		if quiescent {
			// nothing was assigned after base: no record is being written, the segment starts over
			// (the records stay in the file as garbage below ready: their epochs are <= base, and the recovery
			// stops at the first record that does not continue the log)
			lg.total.Add(segHdrSize - c.end)
			c.end, c.maxEpoch = segHdrSize, 0
		} else if c.end > minRollBytes {
			lg.roll = true
		}
	}
}

func (lg *wlog) closeFiles() {
	for _, s := range lg.segs {
		s.f.Close()
	}
	lg.segs = nil
	if lg.meta != nil {
		unlockFile(lg.meta)
		lg.meta.Close()
		lg.meta = nil
	}
}

// close closes the log; remove deletes its files (everything is in the database file).
func (lg *wlog) close(remove bool) {
	ids := make([]uint32, 0, len(lg.segs))
	for _, s := range lg.segs {
		ids = append(ids, s.id)
	}
	if remove {
		for _, id := range ids {
			os.Remove(segPath(lg.path, id))
		}
		os.Remove(lg.path)
	}
	lg.closeFiles()
}
