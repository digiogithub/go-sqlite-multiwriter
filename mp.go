package multiwriter

// The multi-process mode (URI mw_mp=1): several processes write the same database.
//
// The shared segmented log is the truth: every commit of every process is appended to it, once, by its publisher,
// under a publication lock that spans the processes; each process keeps its own page store and brings it up to date
// by reading, from the log, the records the others appended (before it takes a snapshot, and before it validates a
// commit). Nothing else about pages is shared. This is the design the original used before its shared index: the
// work of applying grows with the number of processes, which is fine for tens of them (agents, workers, a CLI and a
// server), and nothing in shared memory can be left half-made by a process that dies, except what the next
// publisher repairs from the log.
//
//	"<db>-mwlock"  the shared header (mapped by every process): committed epoch, compaction base, the cursor of the
//	               log, the commit in flight, the durable position, the registry of snapshots (one slot per
//	               connection: pid, snapshot); every process holds a shared flock on it (membership); on Linux its
//	               bytes 1..3 are also the cross-process locks (fcntl: the kernel releases them when a process dies)
//	"<db>-mwfcntl" the cross-process locks (bytes 1..3, fcntl) where flock and fcntl locks interact (macOS, the
//	               BSDs: a process's own shared flock would block its fcntl write lock on the same file forever)
//	"<db>-mw"      the meta header of the log (base epoch, salt), held shared by every process
//	"<db>-mw.N"    the segments of the log, written ahead of their end with zeros
//
// A publisher that dies inside the publication lock leaves at most one commit in flight (pend_epoch): the next one
// to take the lock finishes it if its record is complete in the log (it was not acknowledged, but it may have been
// seen), or takes it back if it is torn. A commit is visible to the other processes as soon as its record is in the
// log, and acknowledged only when the log is durable up to it (an acknowledged commit is never lost; a crash of the
// machine can lose one that was seen and not acknowledged, as in the original).

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	mpMagic      = 0x3130504d4f47574d // "MWGOMP01"
	mpSlots      = 2048
	mpSlotsOff   = 4096
	mpHeaderSize = mpSlotsOff + mpSlots*16
	mpSegBytes   = 32 << 20
	noSnap       = ^uint64(0)

	oMagic     = 0
	oPgsz      = 8
	oCommitted = 16 // the newest commit in the log (visible)
	oBase      = 24 // the state up to here is in the database file
	oCurSeg    = 32 // where the next record goes
	oCurEnd    = 40
	oPendEpoch = 48 // the commit in flight (0: none)
	oPendSeg   = 56
	oPendOff   = 64
	oSynced    = 72 // the log is durable up to here (segment << 40 | offset)
	oBroken    = 80 // an fsync of the log failed: no commit is acknowledged any more
	oSchema    = 88 // the epoch of the newest schema change
	oBaseSize  = 96 // database size in pages at the base
	oCompactT  = 104
	oReady     = 112
	oLogBytes  = 120 // bytes of records ever placed (monotonic): the compaction trigger is it minus oMark
	oSegMin    = 128 // the oldest segment that may exist
	oMark      = 136 // the value of oLogBytes that the last compaction covered
)

func u64At(m *shmMap, off int) *atomic.Uint64 { return (*atomic.Uint64)(unsafe.Pointer(&m.mem[off])) }

func logPos(seg uint32, end int64) uint64 { return uint64(seg)<<40 | uint64(end) }
func posSeg(p uint64) uint32              { return uint32(p >> 40) }
func posEnd(p uint64) int64               { return int64(p & (1<<40 - 1)) }

// separateByteLocks keeps the fcntl byte locks off the file that carries the membership flock. On Linux flock and
// fcntl locks are independent and the byte locks stay on "<db>-mwlock" (as before: processes of older versions
// sharing the database keep excluding each other); elsewhere (macOS, the BSDs) both kinds of lock live in the same
// per-file list, so a shared flock conflicts with an fcntl write lock of any owner, the holder's own included.
var separateByteLocks = runtime.GOOS != "linux"

type mpState struct {
	db    *mwDB
	lockF *os.File
	byteF *os.File // the file of the fcntl byte locks: lockF, or "<db>-mwfcntl" (separateByteLocks)
	shm   *shmMap
	path  string // the log ("<db>-mw")
	pgsz  int
	salt  uint64
	meta  *os.File

	pubMu   sync.Mutex    // the in-process side of the publication lock
	applyMu sync.Mutex    // the private store is brought up to date one thing at a time
	applied atomic.Uint64 // (written under applyMu) every commit up to here is in the private store
	rseg    uint32        // (applyMu) where the next record to apply starts
	roff    int64
	known   uint64 // (applyMu) the base the private store has taken into account
	p1      uint32 // (applyMu) the schema cookie of the newest page 1 applied
	segMax  map[uint32]uint64

	filesMu sync.Mutex
	bg      sync.WaitGroup // background segment preparation, waited for by close
	prep    atomic.Bool    // a preparation runs in this process (the fcntl lock only excludes the other processes)
	prepped atomic.Uint32  // the next segment is known to be ready up to this id
	files   map[uint32]*os.File
	syncMu  sync.Mutex

	slotMu sync.Mutex
	pid    uint64
}

func (m *mpState) u(off int) *atomic.Uint64 { return u64At(m.shm, off) }
func (m *mpState) committed() uint64        { return m.u(oCommitted).Load() }

// MARK: - open / close

var errMPUnsupported = errors.New("multiwriter: the multi-process mode is not supported on this platform")

// openMP attaches the database to the shared state (the first process creates it and recovers the log).
func (db *mwDB) openMP(path string, pgsz, reserved int, mode os.FileMode, rf *os.File, fileSize int64) int {
	if !mpSupported {
		return rcCantOpen
	}
	lf, err := os.OpenFile(path+"-mwlock", os.O_RDWR|os.O_CREATE, mode)
	if err != nil {
		return rcCantOpen
	}
	bf := lf
	if separateByteLocks {
		if bf, err = os.OpenFile(path+"-mwfcntl", os.O_RDWR|os.O_CREATE, mode); err != nil {
			lf.Close()
			return rcCantOpen
		}
	}
	m := &mpState{db: db, lockF: lf, byteF: bf, path: path + "-mw", pgsz: pgsz, files: map[uint32]*os.File{}, segMax: map[uint32]uint64{}, pid: uint64(os.Getpid())}
	first := flockTry(lf, true)
	if !first {
		if err := flockWait(lf, false); err != nil { // (waits for the first one to finish the header)
			m.closeLocks(false)
			return rcBusy
		}
	}
	shm, err := mapShared(lf, mpHeaderSize)
	if err != nil {
		m.closeLocks(false)
		return rcCantOpen
	}
	m.shm = shm
	st := newStore(pgsz, uint32(fileSize/int64(pgsz)))
	st.reserved = reserved
	st.baseLimit = 0 // (the file changes under a process when another one compacts: no cached copies of it)
	if first {
		clear(shm.mem)
		lg, last, rc := openLog(path, pgsz, st, mode, true) // recovery: the valid prefix of the log, the torn tail cut
		if rc != rcOK {
			m.close(false)
			return rc
		}
		m.salt, m.meta = lg.salt, lg.meta
		base := lg.baseEpoch()
		cur := lg.cur()
		// the current segment is written whole before anybody appends to it (an append inside the file needs no
		// change of its size to be durable: the fdatasync of a commit is cheaper)
		if fi, err := cur.f.Stat(); err == nil && fi.Size() < mpSegBytes {
			for off := max(cur.end, fi.Size()); off < mpSegBytes; off += int64(len(zeroChunk)) {
				if _, err := cur.f.WriteAt(zeroChunk[:min(int64(len(zeroChunk)), mpSegBytes-off)], off); err != nil {
					break
				}
			}
			fsyncFile(cur.f)
		}
		for _, s := range lg.segs {
			m.files[s.id] = s.f
			m.segMax[s.id] = s.maxEpoch
		}
		m.u(oMagic).Store(mpMagic)
		m.u(oPgsz).Store(uint64(pgsz))
		m.u(oCommitted).Store(last)
		m.u(oBase).Store(base)
		m.u(oCurSeg).Store(uint64(cur.id))
		m.u(oCurEnd).Store(uint64(cur.end))
		m.u(oSynced).Store(logPos(cur.id, cur.end))
		m.u(oBaseSize).Store(uint64(st.baseDBSize))
		m.u(oSegMin).Store(uint64(lg.segs[0].id))
		m.u(oLogBytes).Store(uint64(lg.bytes()))
		m.applied.Store(last)
		m.known, m.rseg, m.roff = base, cur.id, cur.end
		st.compactedEpoch = base
		m.u(oReady).Store(1)
		if err := flockWait(lf, false); err != nil { // downgrade: the others may come in, now that the header is complete
			m.close(false)
			return rcIOErr
		}
	} else {
		if m.u(oMagic).Load() != mpMagic || m.u(oReady).Load() != 1 || int(m.u(oPgsz).Load()) != pgsz {
			m.close(false)
			return rcCantOpen
		}
		if rc := m.openMeta(mode); rc != rcOK {
			m.close(false)
			return rc
		}
		base := m.u(oBase).Load()
		m.applied.Store(base)
		m.known = base
		st.compactedEpoch = base
		st.baseDBSize = uint32(m.u(oBaseSize).Load())
		if !m.locate(base) {
			m.close(false)
			return rcCorrupt
		}
	}
	db.store = st
	db.mp = m
	// what the others committed since the base (the first process has it all)
	m.applyMu.Lock()
	rc := m.applyTo(m.committed())
	m.applyMu.Unlock()
	if rc != rcOK {
		m.close(false)
		db.mp = nil
		return rc
	}
	db.epoch.Store(m.committed())
	db.nextEpoch.Store(m.committed())
	return rcOK
}

// openMeta reads the meta header of the log without recovering anything (another process owns the recovery).
func (m *mpState) openMeta(mode os.FileMode) int {
	f, err := os.OpenFile(m.path, os.O_RDWR, mode)
	if err != nil {
		return rcCantOpen
	}
	if !flockShared(f) {
		f.Close()
		return rcBusy
	}
	h := make([]byte, logMetaSize)
	if _, err := f.ReadAt(h, 0); err != nil || string(h[:8]) != logMetaMagic || binary.LittleEndian.Uint64(h[56:]) != mix64(mixSeed, h[:56]) {
		f.Close()
		return rcCorrupt
	}
	m.salt, m.meta = binary.LittleEndian.Uint64(h[24:]), f
	return rcOK
}

func (m *mpState) close(remove bool) {
	m.bg.Wait()
	m.filesMu.Lock()
	for id, f := range m.files {
		f.Close()
		if remove {
			os.Remove(segPath(m.path, id))
		}
	}
	m.files = map[uint32]*os.File{}
	m.filesMu.Unlock()
	if remove {
		if ids, err := listSegments(m.path); err == nil {
			for _, id := range ids {
				os.Remove(segPath(m.path, id))
			}
		}
		os.Remove(m.path)
		for _, p := range prepPaths(m.path) {
			os.Remove(p)
		}
	}
	if m.meta != nil {
		unlockFile(m.meta)
		m.meta.Close()
	}
	if m.shm != nil {
		m.shm.unmap()
	}
	m.closeLocks(remove)
}

// closeLocks closes (and with remove deletes) the lock files: closing drops the flock and every fcntl lock of the
// process on them.
func (m *mpState) closeLocks(remove bool) {
	if m.byteF != nil && m.byteF != m.lockF {
		if remove {
			os.Remove(m.byteF.Name())
		}
		m.byteF.Close()
	}
	if remove {
		os.Remove(m.lockF.Name())
	}
	m.lockF.Close()
}

// releaseMP: the last process to leave materialises everything into the database file and removes the shared files.
func (db *mwDB) releaseMP() {
	m := db.mp
	last := flockTry(m.lockF, true)
	clean := false
	if last && !db.failed.Load() && m.u(oBroken).Load() == 0 {
		if _, err := db.compactMP(); err == nil {
			clean = m.u(oBase).Load() == m.committed() && m.u(oPendEpoch).Load() == 0
		}
	}
	m.close(last && clean)
}

// MARK: - the registry of snapshots

func (m *mpState) slotAt(i int) (*atomic.Uint64, *atomic.Uint64) {
	return m.u(mpSlotsOff + i*16), m.u(mpSlotsOff + i*16 + 8)
}

// allocSlot takes a free slot (or one whose process is gone) for a connection; -1 when the registry is full.
func (m *mpState) allocSlot() int {
	m.slotMu.Lock()
	defer m.slotMu.Unlock()
	for i := 0; i < mpSlots; i++ {
		pid, snap := m.slotAt(i)
		p := pid.Load()
		if p != 0 && pidAlive(int32(p)) {
			continue
		}
		if pid.CompareAndSwap(p, m.pid) {
			snap.Store(noSnap)
			return i
		}
	}
	return -1
}

func (m *mpState) freeSlot(i int) {
	if i < 0 {
		return
	}
	pid, snap := m.slotAt(i)
	snap.Store(noSnap)
	pid.CompareAndSwap(m.pid, 0)
}

// pin registers a snapshot in the slot: the newest committed epoch, at or above the announced compaction target
// (a compaction that chose its target after the scan of the registry sees it; one that announced a target before
// makes the snapshot take a newer epoch).
func (m *mpState) pin(i int) uint64 {
	_, snap := m.slotAt(i)
	for {
		s := m.committed()
		snap.Store(s)
		if s >= m.u(oCompactT).Load() {
			return s
		}
	}
}

func (m *mpState) unpin(i int) {
	if i >= 0 {
		_, snap := m.slotAt(i)
		snap.Store(noSnap)
	}
}

// oldest is the oldest snapshot over every live process (the committed epoch when none is held).
func (m *mpState) oldest() uint64 {
	t := m.committed()
	for i := 0; i < mpSlots; i++ {
		pid, snap := m.slotAt(i)
		p, s := pid.Load(), snap.Load()
		if p == 0 || s == noSnap || s >= t {
			continue
		}
		if pidAlive(int32(p)) {
			t = s
		}
	}
	return t
}

// MARK: - the log

// prepPaths lists the prefilled segments, including a ".prep.tmp" left by a process killed while preparing one.
func prepPaths(base string) []string {
	m, _ := filepath.Glob(base + ".*.prep")
	t, _ := filepath.Glob(base + ".*.prep.tmp")
	return append(m, t...)
}

func (m *mpState) file(id uint32) (*os.File, error) {
	m.filesMu.Lock()
	defer m.filesMu.Unlock()
	if f := m.files[id]; f != nil {
		return f, nil
	}
	f, err := os.OpenFile(segPath(m.path, id), os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	m.files[id] = f
	return f, nil
}

func (m *mpState) forget(id uint32) {
	m.filesMu.Lock()
	if f := m.files[id]; f != nil {
		f.Close()
		delete(m.files, id)
	}
	m.filesMu.Unlock()
}

func (m *mpState) segHeader(id uint32) []byte {
	h := make([]byte, segHdrSize)
	copy(h, segMagic)
	binary.LittleEndian.PutUint32(h[8:], id)
	binary.LittleEndian.PutUint32(h[12:], uint32(m.pgsz))
	binary.LittleEndian.PutUint64(h[16:], m.salt)
	binary.LittleEndian.PutUint64(h[24:], mix64(mixSeed, h[:24]))
	return h
}

// roll starts segment id (publication lock held): the one prepared ahead if there is one, else a new file.
func (m *mpState) roll(id uint32) error {
	p := segPath(m.path, id)
	prep := p + ".prep"
	if err := os.Rename(prep, p); err != nil {
		f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			return err
		}
		f.Close()
	}
	f, err := m.file(id)
	if err != nil {
		return err
	}
	if _, err := f.WriteAt(m.segHeader(id), 0); err != nil {
		return err
	}
	if err := fsyncFile(f); err != nil {
		return err
	}
	syncDir(p)
	return nil
}

// prepareNext writes the next segment ahead (zeros, then renamed into place at the roll), outside the publication
// lock: a record then overwrites blocks that exist, and its fsync does not have to commit a new size of the file.
func (m *mpState) prepareNext() {
	cur := uint32(m.u(oCurSeg).Load())
	if m.prepped.Load() > cur {
		return
	}
	p := segPath(m.path, cur+1) + ".prep"
	if _, err := os.Stat(p); err == nil {
		m.prepped.Store(cur + 1)
		return
	}
	if _, err := os.Stat(segPath(m.path, cur+1)); err == nil {
		m.prepped.Store(cur + 1)
		return
	}
	if !m.prep.CompareAndSwap(false, true) {
		return
	}
	if !byteLock(m.byteF, lockPrefill, false) {
		m.prep.Store(false)
		return
	}
	m.bg.Add(1)
	go func() {
		defer m.bg.Done()
		defer m.prep.Store(false)
		defer byteUnlock(m.byteF, lockPrefill)
		tmp := p + ".tmp"
		f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			return
		}
		ok := true
		for off := int64(0); off < mpSegBytes && ok; off += int64(len(zeroChunk)) {
			_, err := f.WriteAt(zeroChunk, off)
			ok = err == nil
		}
		ok = ok && fsyncFile(f) == nil
		f.Close()
		if !ok || os.Rename(tmp, p) != nil {
			os.Remove(tmp)
		} else {
			m.prepped.Store(cur + 1)
		}
	}()
}

// readRecord reads the record at (seg, off): ok false when there is none there (the end of the segment, zeros, a
// torn record). check verifies the checksum (what a dead publisher left); records <= committed are complete.
func (m *mpState) readRecord(seg uint32, off int64, check bool) (epoch uint64, dbsize uint32, pgnos []uint32, imgs [][]byte, size int64, ok bool) {
	f, err := m.file(seg)
	if err != nil {
		return
	}
	h := make([]byte, recHdrSize)
	if n, _ := f.ReadAt(h, off); n < recHdrSize {
		return
	}
	npages := binary.LittleEndian.Uint32(h[4:])
	if binary.LittleEndian.Uint32(h[0:]) != recMagic || int(binary.LittleEndian.Uint32(h[20:])) != m.pgsz || npages == 0 || npages > 1<<24 {
		return
	}
	body := make([]byte, int64(npages)*int64(4+m.pgsz))
	if n, _ := f.ReadAt(body, off+recHdrSize); n < len(body) {
		return
	}
	if check && binary.LittleEndian.Uint64(h[32:]) != recChecksum(m.salt, h, body) {
		return
	}
	epoch, dbsize = binary.LittleEndian.Uint64(h[8:]), binary.LittleEndian.Uint32(h[16:])
	pgnos, imgs = make([]uint32, npages), make([][]byte, npages)
	for i := range pgnos {
		e := body[i*(4+m.pgsz):]
		pgnos[i] = binary.LittleEndian.Uint32(e)
		imgs[i] = e[4 : 4+m.pgsz]
	}
	return epoch, dbsize, pgnos, imgs, recHdrSize + int64(len(body)), true
}

// locate finds the record after base in the log (applyMu held, or before the database is used).
func (m *mpState) locate(base uint64) bool {
	ids, err := listSegments(m.path)
	if err != nil || len(ids) == 0 {
		return false
	}
	for _, id := range ids {
		off := int64(segHdrSize)
		for {
			e, _, _, _, size, ok := m.readRecord(id, off, false)
			if !ok {
				break
			}
			if e > base {
				m.rseg, m.roff = id, off
				return true
			}
			off += size
		}
		m.rseg, m.roff = id, off
	}
	return true // (nothing after base yet: the next record goes at the end of the last segment, or in the next one)
}

// applyTo brings the private store up to the commit `to` from the log (applyMu held).
func (m *mpState) applyTo(to uint64) int {
	if base := m.u(oBase).Load(); base > m.known {
		m.adoptBase(base)
	}
	relocated := false
	for m.applied.Load() < to {
		epoch, dbsize, pgnos, imgs, size, ok := m.readRecord(m.rseg, m.roff, false)
		if ok && epoch <= m.applied.Load() {
			m.roff += size // (already applied: the cursor was behind an adopted base)
			continue
		}
		if !ok || epoch != m.applied.Load()+1 {
			// the end of this segment: the record is at the head of the next one
			next := m.rseg + 1
			if e, _, _, _, _, ok2 := m.readRecord(next, segHdrSize, false); ok2 && e <= m.applied.Load()+1 {
				m.rseg, m.roff = next, segHdrSize
				continue
			}
			if base := m.u(oBase).Load(); base > m.applied.Load() {
				m.adoptBase(base) // (what we lacked was compacted and its segments deleted meanwhile)
				continue
			}
			if !relocated {
				// the cursor's segment may have been deleted by a compaction: search the log for the record
				relocated = true
				if m.locate(m.applied.Load()) {
					continue
				}
			}
			if debugOn {
				ids, _ := listSegments(m.path)
				e1, _, _, _, _, ok1 := m.readRecord(m.rseg, m.roff, false)
				e2, _, _, _, _, ok2 := m.readRecord(next, segHdrSize, false)
				debugf("mp[%d]: the record of epoch %d is not in the log (segment %d, offset %d: %d %v; next: %d %v) committed %d base %d cur %d/%d segs %v pend %d",
					m.pid, m.applied.Load()+1, m.rseg, m.roff, e1, ok1, e2, ok2, m.committed(), m.u(oBase).Load(), m.u(oCurSeg).Load(), m.u(oCurEnd).Load(), ids, m.u(oPendEpoch).Load())
			}
			return rcCorrupt
		}
		relocated = false
		m.install(epoch, dbsize, pgnos, imgs)
		m.segMax[m.rseg] = max(m.segMax[m.rseg], epoch)
		m.roff += size
	}
	return rcOK
}

// install puts one commit into the private store (applyMu held).
func (m *mpState) install(epoch uint64, dbsize uint32, pgnos []uint32, imgs [][]byte) {
	db := m.db
	db.store.installRecovered(epoch, dbsize, pgnos, imgs)
	for i, pg := range pgnos {
		if pg == 1 {
			c := be32(imgs[i][40:])
			if m.p1 != 0 && c != m.p1 {
				db.lastSchemaEpoch.Store(epoch)
				db.schemaGen.Add(1)
			}
			m.p1 = c
		}
	}
	m.applied.Store(epoch)
}

// adoptBase: another process compacted up to base. The private versions <= base are in the file now (no snapshot of
// this process is older: the compaction target is never above a registered snapshot).
func (m *mpState) adoptBase(base uint64) {
	st := m.db.store
	st.seqMu.Lock()
	st.compactedEpoch = base
	st.baseDBSize = uint32(m.u(oBaseSize).Load())
	d := 0
	for d < len(st.sizes) && st.sizes[d].epoch <= base {
		d++
	}
	st.sizes = append(st.sizes[:0], st.sizes[d:]...)
	st.seqMu.Unlock()
	if m.applied.Load() < base {
		// a lagging process: it lacks commits that are in the file only: everything it has is older
		st.dropAtOrBelow(base)
		m.applied.Store(base)
		m.locate(base) // (the cursor points into segments that may be deleted)
	}
	m.known = base
}

// MARK: - the publication lock

// lock takes the publication lock of every process, repairs what a publisher that died left, and brings the
// private store up to the committed state.
func (m *mpState) lock() int {
	m.pubMu.Lock()
	byteLock(m.byteF, lockPublish, true)
	m.applyMu.Lock()
	m.repair()
	if rc := m.applyTo(m.committed()); rc != rcOK {
		m.unlock()
		return rc
	}
	return rcOK
}

func (m *mpState) unlock() {
	m.applyMu.Unlock()
	byteUnlock(m.byteF, lockPublish)
	m.pubMu.Unlock()
}

// repair: the lock was released by the death of a publisher with a commit in flight. A complete record is finished
// (nobody acknowledged it, but nothing tells whether somebody saw it), a torn one taken back.
func (m *mpState) repair() {
	pe := m.u(oPendEpoch).Load()
	if pe == 0 {
		return
	}
	seg, off := uint32(m.u(oPendSeg).Load()), int64(m.u(oPendOff).Load())
	if pe == m.committed()+1 {
		if e, _, _, _, size, ok := m.readRecord(seg, off, true); ok && e == pe {
			m.u(oCurSeg).Store(uint64(seg))
			m.u(oCurEnd).Store(uint64(off + size))
			m.u(oCommitted).Store(pe)
			debugf("mp: finished the commit %d of a publisher that died", pe)
		} else {
			m.u(oCurSeg).Store(uint64(seg))
			m.u(oCurEnd).Store(uint64(off))
			if f, err := m.file(seg); err == nil {
				f.WriteAt(make([]byte, recHdrSize), off) // (the torn record must not be read as a record)
			}
			debugf("mp: took back the torn commit %d of a publisher that died", pe)
		}
	}
	m.u(oPendEpoch).Store(0)
}

// MARK: - publication

// publishMP validates a commit against the private store (brought up to date under the publication lock, which the
// lane holds), appends it to the shared log, installs it and makes it visible to every process.
func (db *mwDB) publishMP(l *lane, v *validation, pgnos []uint32, imgs [][]byte, wsDBSize, snapDBSize uint32) (uint64, int) {
	m := db.mp
	st := db.store
	if db.failed.Load() || m.u(oBroken).Load() != 0 {
		return 0, rcIOErr
	}
	n := len(pgnos)
	chains := make([]*chain, n)
	for i := range pgnos {
		chains[i] = st.chainGet(pgnos[i], true)
	}
	var ss stripeSet
	ss.add(pgnos)
	if v != nil {
		ss.add(v.readPgnos)
	}
	st.lockStripes(&ss)
	if v != nil {
		if rc := db.validateLocked(l, v, pgnos, chains); rc != rcOK {
			st.unlockStripes(&ss)
			return 0, rc
		}
	}
	st.unlockStripes(&ss)

	// ---- the next epoch, its record in the log
	epoch := m.committed() + 1
	newSize := st.dbsize(epoch - 1)
	if wsDBSize != snapDBSize {
		newSize = wsDBSize
	}
	seg, off := uint32(m.u(oCurSeg).Load()), int64(m.u(oCurEnd).Load())
	size := recHdrSize + int64(n)*int64(4+m.pgsz)
	if off+size > mpSegBytes && off > segHdrSize {
		if err := m.roll(seg + 1); err != nil {
			return 0, rcIOErrWrite
		}
		seg, off = seg+1, segHdrSize
		m.u(oCurSeg).Store(uint64(seg))
		m.u(oCurEnd).Store(uint64(off))
	}
	f, err := m.file(seg)
	if err != nil {
		return 0, rcIOErr
	}
	m.u(oPendSeg).Store(uint64(seg))
	m.u(oPendOff).Store(uint64(off))
	m.u(oPendEpoch).Store(epoch)
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf[0:], recMagic)
	binary.LittleEndian.PutUint32(buf[4:], uint32(n))
	binary.LittleEndian.PutUint64(buf[8:], epoch)
	binary.LittleEndian.PutUint32(buf[16:], newSize)
	binary.LittleEndian.PutUint32(buf[20:], uint32(m.pgsz))
	for i, pg := range pgnos {
		e := buf[recHdrSize+i*(4+m.pgsz):]
		binary.LittleEndian.PutUint32(e, pg)
		copy(e[4:4+m.pgsz], imgs[i])
	}
	binary.LittleEndian.PutUint64(buf[32:], recChecksum(m.salt, buf[:recHdrSize], buf[recHdrSize:]))
	if _, err := f.WriteAt(buf, off); err != nil {
		m.u(oPendEpoch).Store(0) // (nothing anybody can see was touched: this commit fails, the database does not)
		return 0, rcIOErrWrite
	}

	// ---- installed here, visible everywhere
	copies := make([][]byte, n)
	for i := range imgs {
		copies[i] = buf[recHdrSize+i*(4+m.pgsz)+4 : recHdrSize+(i+1)*(4+m.pgsz)]
	}
	m.install(epoch, newSize, pgnos, copies)
	m.segMax[seg] = max(m.segMax[seg], epoch)
	m.rseg, m.roff = seg, off+size
	m.u(oCurEnd).Store(uint64(off + size))
	m.u(oLogBytes).Add(uint64(size))
	m.u(oCommitted).Store(epoch)
	m.u(oPendEpoch).Store(0)
	if lim := db.logMaxBytes.Load(); lim > 0 && m.logBytes() > lim {
		db.kickCompactor()
	}
	if gi := db.gcInterval.Load(); gi > 0 && db.pubSinceGC.Add(1) >= gi {
		go db.gc() // (not under the publication lock)
	}
	db.nextEpoch.Store(epoch)
	db.epoch.Store(epoch)
	if l != nil {
		l.releaseHot()
	}
	if l != nil {
		l.mpSyncPos = logPos(seg, off+size)
	}
	db.nCommits.Add(1)
	db.nPagesPublished.Add(int64(n))
	return epoch, rcOK
}

// durable returns when the log is durable up to pos (segment << 40 | end): one fsync per process at a time covers
// every record written before it, of every process.
func (m *mpState) durable(pos uint64) int {
	synced := m.u(oSynced)
	if synced.Load() >= pos {
		return rcOK
	}
	m.syncMu.Lock()
	defer m.syncMu.Unlock()
	if synced.Load() >= pos {
		return rcOK
	}
	if m.u(oBroken).Load() != 0 {
		return rcIOErrFsync
	}
	target := logPos(uint32(m.u(oCurSeg).Load()), int64(m.u(oCurEnd).Load()))
	if target < pos {
		target = pos
	}
	for s := posSeg(synced.Load()); s <= posSeg(target); s++ {
		f, err := m.file(s)
		if err != nil {
			continue // (compacted and deleted meanwhile: nothing to make durable there)
		}
		if err := datasyncFile(f); err != nil {
			// the data it should have covered may never reach the disk, and a later fsync that succeeds (the kernel
			// forgets the error) must not acknowledge it
			m.u(oBroken).Store(1)
			return rcIOErrFsync
		}
	}
	for {
		cur := synced.Load()
		if cur >= target || synced.CompareAndSwap(cur, target) {
			break
		}
	}
	return rcOK
}

// MARK: - snapshots

// catchUp brings the private store up to the snapshot s that a lane registered.
func (m *mpState) catchUp(s uint64) int {
	if m.applied.Load() >= s {
		return rcOK
	}
	m.applyMu.Lock()
	defer m.applyMu.Unlock()
	return m.applyTo(s)
}

// MARK: - compaction

// compactMP: one process at a time writes the pages that no snapshot of any process holds back into the file.
func (db *mwDB) compactMP() (CompactResult, error) {
	var out CompactResult
	m := db.mp
	st := db.store
	if db.failed.Load() {
		return out, errFailed
	}
	db.compactMu.Lock()
	defer db.compactMu.Unlock()
	if !byteLock(m.byteF, lockCompact, false) {
		return out, nil // another process is compacting
	}
	defer byteUnlock(m.byteF, lockCompact)

	// no collection until the pages and the size of T are copied: it could drop the versions of T (a collection
	// only keeps the newest version at or below the oldest snapshot, which may move past T meanwhile)
	db.gcMu.Lock()
	gcHeld := true
	defer func() {
		if gcHeld {
			db.gcMu.Unlock()
		}
	}()

	// the target is chosen under the publication lock: announce it, then scan the registry
	if rc := m.lock(); rc != rcOK {
		return out, sqliteErr(rc)
	}
	cand := m.committed()
	mark := m.u(oLogBytes).Load() // (every record <= the target is placed)
	m.u(oCompactT).Store(cand)
	T := min(cand, m.oldest(), db.oldestActive())
	base := m.u(oBase).Load()
	pos := logPos(uint32(m.u(oCurSeg).Load()), int64(m.u(oCurEnd).Load()))
	m.unlock()
	if T <= base {
		return out, nil
	}
	// the pages go into the file only when the log has them on the disk
	if rc := m.durable(pos); rc != rcOK {
		return out, sqliteErr(rc)
	}
	pgsz := st.pgsz
	page := make([]byte, pgsz)
	st.listMu.Lock()
	dirty := st.dirty
	st.dirty = nil
	st.listMu.Unlock()
	var werr error
	for _, pgno := range dirty {
		c := st.chainGet(pgno, false)
		mu := st.stripeOf(pgno)
		mu.Lock()
		have := false
		if k := c.find(T); k >= 0 && c.v[k].epoch > base {
			copy(page, c.v[k].data)
			have = true
		}
		mu.Unlock()
		if have {
			if _, err := db.realFile.WriteAt(page, int64(pgno-1)*int64(pgsz)); err != nil {
				werr = err
				break
			}
			out.PagesWritten++
		}
	}
	sizePages := st.dbsize(T)
	db.gcMu.Unlock()
	gcHeld = false
	if werr == nil && sizePages > 0 {
		werr = db.realFile.Truncate(int64(sizePages) * int64(pgsz))
	}
	if werr == nil {
		werr = fsyncFile(db.realFile)
	}
	if werr == nil {
		if _, err := m.meta.WriteAt(encodeMeta(pgsz, T, m.salt), 0); err != nil {
			werr = err
		} else {
			werr = fsyncFile(m.meta)
		}
	}
	var again []uint32
	for _, pgno := range dirty {
		c := st.chainGet(pgno, false)
		mu := st.stripeOf(pgno)
		mu.Lock()
		if werr != nil || c.head() > T {
			again = append(again, pgno)
		} else {
			c.dirty = false
			if len(c.v) > 0 && !c.queued {
				c.queued = true
				st.listMu.Lock()
				st.cand = append(st.cand, pgno)
				st.listMu.Unlock()
			}
		}
		mu.Unlock()
	}
	st.listMu.Lock()
	st.dirty = append(st.dirty, again...)
	st.listMu.Unlock()
	if werr != nil {
		return out, werr
	}
	m.u(oBaseSize).Store(uint64(sizePages))
	m.u(oBase).Store(T)
	m.applyMu.Lock()
	m.adoptBase(T)
	// the segments that hold nothing newer than T are deleted (a lagging process finds what it lacks in the file)
	cur := uint32(m.u(oCurSeg).Load())
	var gone []uint32
	for id, mx := range m.segMax {
		if id < cur && mx <= T {
			gone = append(gone, id)
		}
	}
	m.applyMu.Unlock()
	sort.Slice(gone, func(i, j int) bool { return gone[i] < gone[j] })
	for _, id := range gone {
		os.Remove(segPath(m.path, id))
		m.forget(id)
		m.applyMu.Lock()
		delete(m.segMax, id)
		m.applyMu.Unlock()
	}
	for {
		cur := m.u(oMark).Load()
		if mark <= cur || m.u(oMark).CompareAndSwap(cur, mark) {
			break
		}
	}
	out.TargetEpoch = T
	out.VersionsFreed = db.gc()
	db.nCompactions.Add(1)
	db.nCompactedPages.Add(int64(out.PagesWritten))
	return out, nil
}

// logBytes: the bytes placed since what the last compaction covered (the compaction trigger; the size of the files
// is not: a segment is only deleted whole).
func (m *mpState) logBytes() int64 { return int64(m.u(oLogBytes).Load() - m.u(oMark).Load()) }
