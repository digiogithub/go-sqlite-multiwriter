package multiwriter

// Private write lanes. Each connection gets:
//   - its own in-memory "-wal" (no real file): SQLite believes it is the only writer, so there is no WAL write-lock
//     contention between connections;
//   - its own private wal-index (xShmMap memory) and lock calls that are always granted;
//   - a main-database file whose writes are swallowed (the real file is only ever written by the compactor).
//
// The physical write set of a transaction is the set of page numbers of the frames SQLite appended to the private
// WAL up to the commit frame. When the snapshot ends the private WAL is emptied and the wal-index header
// invalidated: SQLite's own recovery path then rebuilds an empty WAL and reports "changed", which resets the page
// cache. Every transaction therefore starts with a cold page cache; this is also what makes the set of pages read
// through xRead a complete read set.
//
// A transaction is the interval during which the connection holds a WAL read mark (SQLite takes it in the
// wal-index locks, which are this lane's own): the first read lock begins the snapshot, its release ends it.

import (
	"runtime"

	"modernc.org/libc"
	"sort"
	"sync/atomic"
	"time"
)

type TxState int

const (
	TxIdle TxState = iota
	TxActive
	TxPrepared
	TxCommitted
	TxAborted
)

type txInfo struct {
	txID          uint64
	snapshotEpoch uint64
	commitEpoch   uint64
	state         TxState
	isWriter      bool
	wsPages       int
}

type memWAL struct {
	buf        []byte
	size       int
	pgsz       int
	commitSeen bool
}

const (
	warmAfter        = 64 // read-only snapshots in a row before a connection keeps its page cache
	defaultHotCredit = 16
	shmRegionBytes   = 32768
	shmMaxRegions    = 1024
)

type lane struct {
	db *mwDB

	// options (URI parameters)
	readcheck bool
	noreloc   bool
	nomerge   bool
	noroute   bool
	hotCredit int
	syncLevel int // PRAGMA synchronous as observed on the connection (FULL = 2 by default)

	// the snapshot
	snapHeld    bool
	tx          txInfo
	writeLocked atomic.Bool // read by the schema barrier of other lanes
	triedWrite  bool

	// what the transaction read through xRead (page 1 excluded)
	rsBits     []byte
	rsList     []uint32
	readsSince uint32
	warm       bool
	warmEpoch  uint64

	// the private WAL and wal-index header
	wal      memWAL
	walBound bool
	shm0     []byte // the first 96 bytes of the wal-index (both copies of its header)

	// the write set captured at the commit frame
	wsPgnos    []uint32
	wsFrame    []int
	wsIndex    map[uint32]int
	wsDBSize   uint32
	commitBase int // first WAL frame of the current write transaction (>0: an earlier commit in this snapshot)

	// pages committed by this connection inside the current snapshot (sorted), and the epoch of each
	ownPg    []uint32
	ownEp    []uint64
	ownEpoch uint64
	poisoned bool // a commit of this snapshot was relocated: the snapshot takes no more commits

	dszEpoch uint64
	dszVal   uint32
	dszValid bool

	// hot spots
	consecAborts int
	retryCredit  int
	holdsHot     bool

	holdsReloc bool // this lane holds db.relocMu (released by publish right after its pages are installed)
	ddlActive  bool // this lane owns the schema barrier (released when its transaction ends)

	// the multi-process mode
	mpSlot    int    // the slot of the connection in the shared registry of snapshots
	mpHeld    bool   // the lane holds the publication lock of every process
	mpSyncPos uint64 // where the log must be durable for the last commit of the lane

	// the rebase (mw_rebase=1)
	rebaseOn    bool
	norebase    bool // this connection is itself a helper of the rebase: never recursive
	rbNoBackoff bool
	rbStreak    int
	rbSkip      int
	rb          *rbState
	// what the statements of the transaction read (the statement hook)
	rdCur     uintptr
	rdCurKind int
	rdDep     bool
	rdDB      uintptr
	rdChgBase int32
	rdCache   map[string]rdEnt
	cdb       atomic.Uintptr // the sqlite3* of the connection (dbLanes)
	curTLS    *libc.TLS      // the TLS of the call into the VFS that commits
}

func newLane(db *mwDB) *lane {
	return &lane{db: db, mpSlot: -1, readcheck: true, hotCredit: defaultHotCredit, syncLevel: 2, wsIndex: map[uint32]int{}, rdCache: map[string]rdEnt{}}
}

// MARK: - snapshots

func (l *lane) snapshot() uint64 {
	if l.snapHeld {
		return l.tx.snapshotEpoch
	}
	return l.db.visibleEpoch()
}

func (l *lane) snapshotBegin() {
	db := l.db
	if l.snapHeld {
		return
	}
	// Hot-spot escalation: a connection that recently lost a commit takes a per-database turn before its snapshot,
	// so the retries of a contended spot run one after the other on fresh state (bounded wait: 20 ms).
	if l.retryCredit > 0 && !l.holdsHot {
		for i := 0; i < 400; i++ {
			if db.hotHeld.CompareAndSwap(0, 1) {
				l.holdsHot = true
				db.nHotSerialised.Add(1)
				break
			}
			time.Sleep(50 * time.Microsecond)
		}
	}
	// The epoch is read while holding the registry mutex and the lane is registered before it is released: a
	// concurrent oldestActive either sees this lane or an epoch <= the one taken here. The GC relies on it.
	// (multi-process: the snapshot is registered in the shared registry, then this process applies what the others
	// committed up to it)
	db.mu.Lock()
	if db.mp != nil {
		l.tx.snapshotEpoch = db.mp.pin(l.mpSlot)
	} else {
		l.tx.snapshotEpoch = db.visibleEpoch()
	}
	db.active[l] = struct{}{}
	l.snapHeld = true
	db.mu.Unlock()
	if db.mp != nil {
		if rc := db.mp.catchUp(l.tx.snapshotEpoch); rc != rcOK {
			db.failed.Store(true)
		}
	}

	l.tx.txID++
	l.tx.commitEpoch = 0
	l.tx.state = TxActive
	l.tx.isWriter = false
	l.tx.wsPages = 0
	l.triedWrite = false
	if l.readsSince < ^uint32(0) {
		l.readsSince++
	}
	if l.warm && l.warmEpoch == l.tx.snapshotEpoch {
		// the same epoch as the read-only snapshot before: the cached pages are right and the read set stays
		l.warm = false
	} else {
		if l.warm && l.shm0 != nil {
			clear(l.shm0) // another epoch: SQLite resets its cache when it looks at the header again
		}
		l.warm = false
		for _, p := range l.rsList {
			l.rsBits[p] = 0
		}
		l.rsList = l.rsList[:0]
	}
	l.wsPgnos = l.wsPgnos[:0]
	l.dszValid = false
	db.nSnapshots.Add(1)
}

func (l *lane) snapshotEnd() {
	db := l.db
	if !l.snapHeld {
		return
	}
	db.mu.Lock()
	delete(db.active, l)
	l.snapHeld = false
	if db.mp != nil {
		db.mp.unpin(l.mpSlot)
	}
	l.releaseHot()
	if !l.tx.isWriter && l.retryCredit > 0 {
		l.retryCredit--
	}
	db.mu.Unlock()
	if l.ddlActive {
		l.ddlEnd()
	}
	if l.tx.state == TxActive {
		if l.tx.isWriter {
			l.tx.state = TxAborted
		} else {
			l.tx.state = TxCommitted
		}
	}
}

// reset drops the private WAL and invalidates the wal-index header: all zeros has isInit == 0, which SQLite treats
// as malformed: it takes the (private) write lock, recovers the (empty) WAL and reports changed, which resets its
// page cache.
func (l *lane) reset(keepHeader bool) {
	l.commitBase = 0
	l.ownPg, l.ownEp = l.ownPg[:0], l.ownEp[:0]
	l.poisoned = false
	l.wal.size = 0
	l.wal.commitSeen = false
	l.wal.pgsz = 0
	if !keepHeader && l.shm0 != nil {
		clear(l.shm0)
	}
}

// SQLite's wal-index lock slots: 0 = the write lock, 1 = checkpoint, 2 = recover, 3.. = read marks.
const (
	shmUnlock    = 1
	shmLock      = 2
	shmShared    = 4
	shmExclusive = 8
)

// onShmLock drives the lane from the locks SQLite takes in its (private) wal-index.
//   - a shared read mark is the snapshot; its release ends it;
//   - the first write of a transaction on an empty WAL swaps read mark 0 for another one while the write lock is
//     held: that unlock/lock pair is not the end of the snapshot;
//   - WAL recovery (run at the start of every transaction) takes and releases the write lock before any read mark
//     is held: not a writer.
func (l *lane) onShmLock(ofst, n, flags int32) int {
	if n != 1 {
		return rcOK
	}
	lock := flags&shmLock != 0
	switch {
	case ofst >= 3 && flags&shmShared != 0:
		if lock {
			l.snapshotBegin()
		} else if !l.writeLocked.Load() {
			// A connection that has not written for warmAfter snapshots keeps its page cache when the next snapshot
			// is at the same epoch (the wal-index header stays valid, so SQLite does not reset the cache).
			warm := !l.tx.isWriter && l.readsSince >= warmAfter && !l.triedWrite && len(l.wsPgnos) == 0 && l.tx.state == TxActive
			ep := l.tx.snapshotEpoch
			l.snapshotEnd()
			l.reset(warm)
			l.warm, l.warmEpoch = warm, ep
		}
	case ofst == 0 && flags&shmExclusive != 0 && l.snapHeld:
		if lock {
			l.triedWrite = true
			l.readsSince = 0
			// a snapshot older than the newest schema change can never publish: fail fast (retryable)
			if l.tx.snapshotEpoch < l.db.lastSchemaEpoch.Load() && !l.ddlActive {
				return rcBusySnapshot
			}
			// the schema barrier: no new write transaction while a schema change runs (SQLite's busy handler waits)
			if o := l.db.ddlOwner.Load(); o != nil && o != l {
				return rcBusy
			}
			l.writeLocked.Store(true)
			l.tx.isWriter = true
		} else if l.writeLocked.Load() {
			l.writeLocked.Store(false)
		}
	}
	return rcOK
}

// MARK: - the main database file

func (l *lane) rsMark(pgno uint32) {
	if int(pgno) >= len(l.rsBits) {
		n := len(l.rsBits)
		if n == 0 {
			n = 4096
		}
		for n <= int(pgno) {
			n *= 2
		}
		nb := make([]byte, n)
		copy(nb, l.rsBits)
		l.rsBits = nb
	}
	if l.rsBits[pgno] != 0 {
		return
	}
	l.rsBits[pgno] = 1
	l.rsList = append(l.rsList, pgno)
}

// mainRead serves reads page by page: the newest committed version <= the snapshot, else the real file.
func (l *lane) mainRead(dst []byte, off int64) int {
	if l.db.mp != nil && l.db.failed.Load() {
		return rcIOErrRead
	}
	st := l.db.store
	snap := l.snapshot()
	pgsz := int64(st.pgsz)
	for len(dst) > 0 {
		pgno := uint32(off/pgsz) + 1
		poff := int(off % pgsz)
		take := int(pgsz) - poff
		if take > len(dst) {
			take = len(dst)
		}
		if l.readcheck && l.snapHeld && pgno != 1 {
			l.rsMark(pgno)
		}
		if !st.read(pgno, snap, poff, dst[:take]) {
			n, err := l.db.realFile.ReadAt(dst[:take], off)
			if n < take {
				clear(dst[n:])
				if err != nil {
					return rcIOErrShort
				}
			}
			if poff == 0 && take == int(pgsz) {
				st.cacheBase(pgno, dst[:take])
			}
		}
		dst = dst[take:]
		off += int64(take)
	}
	return rcOK
}

func (l *lane) dbsizeAt(snap uint64) uint32 {
	if !l.dszValid || l.dszEpoch != snap {
		l.dszVal = l.db.store.dbsize(snap)
		l.dszEpoch = snap
		l.dszValid = true
	}
	return l.dszVal
}

func (l *lane) mainFileSize() int64 {
	return int64(l.dbsizeAt(l.snapshot())) * int64(l.db.store.pgsz)
}

// MARK: - the private WAL

func (l *lane) walRead(dst []byte, off int64) int {
	w := &l.wal
	if off >= int64(w.size) {
		clear(dst)
		return rcIOErrShort
	}
	n := copy(dst, w.buf[off:w.size])
	if n < len(dst) {
		clear(dst[n:])
		return rcIOErrShort
	}
	return rcOK
}

func (l *lane) walWrite(src []byte, off int64) int {
	w := &l.wal
	end := int(off) + len(src)
	if end > len(w.buf) {
		c := len(w.buf)
		if c == 0 {
			c = 65536
		}
		for c < end {
			c *= 2
		}
		nb := make([]byte, c)
		copy(nb, w.buf[:w.size])
		w.buf = nb
	}
	copy(w.buf[off:], src)
	if end > w.size {
		w.size = end
	}
	n := len(src)
	// WAL layout: a 32-byte header, then frames of (24-byte header + page). SQLite writes the header and the page
	// of a frame with two calls; the page write ends it.
	if off == 0 && n == 32 {
		w.pgsz = int(be32(w.buf[8:]))
		return rcOK
	}
	if n == 24 {
		return rcOK // a frame header, or a checksum rewrite of one
	}
	if w.commitSeen && w.pgsz != 0 && n < w.pgsz {
		return rcOK // padding after the commit frame
	}
	if w.pgsz == 0 || n != w.pgsz || off < 32 || (off-32)%int64(w.pgsz+24) != 24 {
		return rcIOErrWrite // unexpected layout: fail loudly
	}
	k := int((off - 32) / int64(w.pgsz+24))
	ntrunc := be32(w.buf[32+k*(w.pgsz+24)+4:])
	if ntrunc != 0 {
		return l.commitFrame(k, ntrunc) // the commit frame is complete
	}
	return rcOK
}

func (l *lane) walTruncate(size int64) {
	if int(size) < l.wal.size {
		l.wal.size = int(size)
	}
}

// commitFrame is called when the commit frame (frame index k, database size after the commit = dbsize) is complete.
func (l *lane) commitFrame(k int, dbsize uint32) int {
	w := &l.wal
	fs := w.pgsz + 24
	clear(l.wsIndex)
	l.wsPgnos, l.wsFrame = l.wsPgnos[:0], l.wsFrame[:0]
	l.wsDBSize = dbsize
	if k < l.commitBase {
		l.commitBase = 0 // the log was restarted: earlier frames are gone
	}
	for i := l.commitBase; i <= k; i++ {
		o := 32 + i*fs
		if o+fs > w.size {
			return rcIOErr
		}
		pg := be32(w.buf[o:])
		if j, ok := l.wsIndex[pg]; ok {
			l.wsFrame[j] = i // a later frame of the same page supersedes
			continue
		}
		l.wsIndex[pg] = len(l.wsPgnos)
		l.wsPgnos = append(l.wsPgnos, pg)
		l.wsFrame = append(l.wsFrame, i)
	}
	w.commitSeen = true
	l.tx.wsPages = len(l.wsPgnos)
	l.tx.state = TxPrepared
	rc := l.publish()
	if rc == rcOK {
		// The read snapshot may outlive this commit (a statement still stepping): the next commit starts after these
		// frames and must not count this commit's pages as somebody else's change.
		l.commitBase = k + 1
		l.dszVal, l.dszEpoch, l.dszValid = dbsize, l.tx.snapshotEpoch, true
		ce := l.tx.commitEpoch
		type pe struct {
			pg uint32
			ep uint64
		}
		t := make([]pe, 0, len(l.ownPg)+len(l.wsPgnos))
		for i := range l.ownPg {
			t = append(t, pe{l.ownPg[i], l.ownEp[i]})
		}
		for _, pg := range l.wsPgnos {
			t = append(t, pe{pg, ce})
		}
		// sorted by page; one entry per page, with the epoch of the commit that wrote it last
		sort.Slice(t, func(i, j int) bool {
			if t[i].pg != t[j].pg {
				return t[i].pg < t[j].pg
			}
			return t[i].ep > t[j].ep
		})
		l.ownPg, l.ownEp = l.ownPg[:0], l.ownEp[:0]
		for i := range t {
			if i == 0 || t[i].pg != t[i-1].pg {
				l.ownPg = append(l.ownPg, t[i].pg)
				l.ownEp = append(l.ownEp, t[i].ep)
			}
		}
		l.ownEpoch = ce
	}
	return rc
}

// schemaCookie is the schema cookie (page 1, bytes 40..43) as seen at the snapshot.
func (l *lane) schemaCookie() uint32 {
	var c [4]byte
	if !l.db.store.read(1, l.tx.snapshotEpoch, 40, c[:]) {
		l.db.realFile.ReadAt(c[:], 40)
	}
	return be32(c[:])
}

// publish publishes the captured write set as one atomic commit at a new epoch.
func (l *lane) publish() int {
	db := l.db
	if !l.snapHeld {
		return rcIOErr // a commit without a snapshot: a VFS stacked above does not forward the wal-index locks
	}
	if l.poisoned && len(l.ownPg) > 0 {
		// a commit of this snapshot was relocated: the next one would use the page numbers it had before
		l.tx.state = TxAborted
		db.nAborts.Add(1)
		return rcBusySnapshot
	}
	w := &l.wal
	fs := w.pgsz + 24
	imgs := make([][]byte, len(l.wsPgnos))
	var pg1 []byte
	for i := range l.wsPgnos {
		o := 32 + l.wsFrame[i]*fs + 24
		imgs[i] = w.buf[o : o+w.pgsz]
		if l.wsPgnos[i] == 1 {
			pg1 = imgs[i]
		}
	}
	snapSize := l.dbsizeAt(l.tx.snapshotEpoch) // (after a commit of this snapshot: the size it left)
	cookie := l.schemaCookie()
	// pages read but not written
	var ro []uint32
	if l.readcheck && len(l.rsList) > 0 {
		ro = make([]uint32, 0, len(l.rsList))
		for _, p := range l.rsList {
			if _, written := l.wsIndex[p]; !written {
				ro = append(ro, p)
			}
		}
	}
	v := &validation{snapshotEpoch: l.tx.snapshotEpoch, checkCookie: true, cookie: cookie, readPgnos: ro,
		ownPgnos: l.ownPg, ownEpochs: l.ownEp, ownEpoch: l.ownEpoch}
	sync := l.syncLevel >= 2
	// multi-process: ONE round of the publication lock per commit: the plain attempt and the relocation both run inside
	// it, against the state they publish on (the rebase replays without it)
	mpLocked := false
	if db.mp != nil {
		if rc := db.mp.lock(); rc != rcOK {
			l.tx.state = TxAborted
			return rc
		}
		mpLocked, l.mpHeld, l.mpSyncPos = true, true, 0
	}
	epoch, rc := db.publish(l, v, l.wsPgnos, imgs, l.wsDBSize, snapSize, sync)
	tryRebase := false
	if rc == errConflict && !l.noreloc {
		// Was the conflict only that other commits extended the file? Then the transaction's new pages are
		// renumbered above the new end and it commits.
		e, rr := l.relocate(v, l.wsPgnos, imgs, l.wsDBSize, snapSize, sync)
		if rr == rcOK {
			epoch, rc = e, rcOK
			l.poisoned = true
		} else if rr != errRelocNA {
			rc = rr
		} else {
			tryRebase = true
		}
	}
	if mpLocked {
		l.mpHeld = false
		db.mp.unlock()
		db.mp.prepareNext()
		if rc == rcOK && sync && l.mpSyncPos != 0 {
			rc = db.mp.durable(l.mpSyncPos) // (the commit is visible already: an fsync that fails fails every commit from now on)
			if rc != rcOK {
				db.failed.Store(true)
			}
		}
	}
	if tryRebase && l.canRebase(pg1, cookie) {
		// a physical conflict only on pages that it wrote: replay its row changes at the latest snapshot
		e, rb := l.rebase(l.curTLS, imgs, cookie)
		if rb == rcOK {
			epoch, rc = e, rcOK
			l.rbStreak = 0
			// the pages that were published are the helper's, not the images in this lane's WAL: another commit of
			// this snapshot would build on its own (as after a relocation)
			l.poisoned = true
		} else {
			// refused: the next conflicts of this connection are probably true ones too: they are not replayed,
			// 1, 3, 7... in a row
			if !l.rbNoBackoff {
				l.rbStreak = min(l.rbStreak+1, 5)
				l.rbSkip = 1<<l.rbStreak - 1
			}
			if rb != errConflict {
				rc = rb
			}
		}
	}
	switch rc {
	case rcOK:
		l.tx.commitEpoch = epoch
		l.tx.state = TxCommitted
		l.consecAborts = 0
		if l.retryCredit > 0 {
			l.retryCredit--
		}
		if pg1 != nil && be32(pg1[40:]) != cookie { // a schema change
			db.lastSchemaEpoch.Store(epoch)
			db.schemaGen.Add(1)
		}
		return rcOK
	case errConflict, errConflictSchema, errConflictRead:
		// The transaction is rolled back and the caller retries. A write-write conflict on the same pages repeats, so
		// a connection that keeps losing runs its next transactions serialised (not for a schema or a read conflict).
		l.tx.state = TxAborted
		db.nAborts.Add(1)
		l.consecAborts++
		if rc == errConflict && l.consecAborts >= 16 && !l.norebase {
			l.retryCredit = l.hotCredit
		}
		return rcBusySnapshot
	default:
		l.tx.state = TxAborted
		db.nAborts.Add(1)
		return rc
	}
}

// MARK: - the schema barrier
//
// A schema change (recognised by the statement hook before it runs) takes the barrier: one at a time, new write
// transactions are refused with SQLITE_BUSY (the busy handler retries), and the ones in flight are waited for. It is
// an optimisation of when the change runs (a CREATE INDEX reads the whole table and would otherwise lose to every
// concurrent insert); correctness never depends on it: a transaction that overlaps a schema change fails on the
// schema cookie.

const ddlWait = 2 * time.Second

func (l *lane) ddlBegin() {
	db := l.db
	if l.ddlActive {
		return
	}
	deadline := time.Now().Add(ddlWait)
	for !db.ddlOwner.CompareAndSwap(nil, l) {
		if time.Now().After(deadline) {
			return // give up waiting: the validation still protects us
		}
		time.Sleep(100 * time.Microsecond)
	}
	l.ddlActive = true
	// drain: wait for the write transactions of the others
	for time.Now().Before(deadline) {
		writers := 0
		db.mu.Lock()
		for o := range db.active {
			if o != l && o.writeLocked.Load() {
				writers++
			}
		}
		db.mu.Unlock()
		if writers == 0 && db.nextEpoch.Load() == db.epoch.Load() {
			return
		}
		time.Sleep(200 * time.Microsecond)
	}
}

func (l *lane) ddlEnd() {
	l.ddlActive = false
	l.db.ddlOwner.CompareAndSwap(l, nil)
}

// releaseHot gives the hot-spot turn back: when the lane's commit is visible (the next holder's snapshot sees it, no
// need to wait for its fsync), or at the end of the transaction.
func (l *lane) releaseHot() {
	if l.holdsHot {
		l.holdsHot = false
		l.db.hotHeld.Store(0)
	}
}

// yield lets the other goroutines run (used by spin waits).
func yield() { runtime.Gosched() }
