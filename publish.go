package multiwriter

// The commit protocol on top of the page store.

import (
	"sort"
	"time"
)

type validation struct {
	snapshotEpoch uint64
	checkCookie   bool // compare the schema cookie (page 1, bytes 40..43) seen at the snapshot
	cookie        uint32
	readPgnos     []uint32 // pages read but not written
	// pages this same snapshot already committed itself (sorted): their head version is our own commit, not a conflict
	ownPgnos  []uint32
	ownEpochs []uint64 // parallel to ownPgnos: the epoch each own page is validated against (ownEpoch when nil)
	ownEpoch  uint64
	adopt     bool // the images are private copies the store may keep
}

// snapFor is the epoch a page is validated against: the snapshot, or our own latest commit of it.
func (v *validation) snapFor(pgno uint32) uint64 {
	i := sort.Search(len(v.ownPgnos), func(i int) bool { return v.ownPgnos[i] >= pgno })
	if i < len(v.ownPgnos) && v.ownPgnos[i] == pgno {
		if v.ownEpochs != nil {
			return v.ownEpochs[i]
		}
		return v.ownEpoch
	}
	return v.snapshotEpoch
}

// readReal reads page pgno of the database file.
func (db *mwDB) readReal(pgno uint32, dst []byte) bool {
	if db.realFile == nil {
		return false
	}
	n, err := db.realFile.ReadAt(dst, int64(pgno-1)*int64(len(dst)))
	if n < len(dst) {
		clear(dst[n:])
	}
	return err == nil || n > 0
}

// publish validates a transaction against v and installs it atomically at a new epoch, writes its log record and
// makes it visible. It returns an internal conflict code, an SQLite error code or rcOK.
func (db *mwDB) publish(l *lane, v *validation, pgnos []uint32, imgs [][]byte, wsDBSize, snapDBSize uint32, sync bool) (uint64, int) {
	st := db.store
	if db.failed.Load() {
		return 0, rcIOErr
	}
	n := len(pgnos)
	copies := make([][]byte, n)
	chains := make([]*chain, n)
	for i := range pgnos {
		if v != nil && v.adopt {
			copies[i] = imgs[i]
		} else {
			copies[i] = make([]byte, st.pgsz)
			copy(copies[i], imgs[i])
		}
		chains[i] = st.chainGet(pgnos[i], true)
	}

	var ss stripeSet
	ss.add(pgnos)
	if v != nil {
		ss.add(v.readPgnos)
	}
	st.lockStripes(&ss)
	release := func() {
		st.unlockStripes(&ss)
		if l != nil && l.holdsReloc {
			l.holdsReloc = false
			db.relocMu.Unlock()
		}
	}

	// ---- validate
	if v != nil {
		rc := rcOK
		var used []uint32 // pages the transaction went through (read or wrote), sorted; built when first needed
		for _, rp := range v.readPgnos {
			c := st.chainGet(rp, false)
			if c.head() <= v.snapFor(rp) {
				continue
			}
			if l != nil && !l.noroute && c != nil && len(c.v) > 0 {
				// interior page changed: is what this transaction took from it (which child to follow) unchanged?
				if used == nil {
					used = make([]uint32, 0, len(v.readPgnos)+n)
					used = append(used, v.readPgnos...)
					used = append(used, pgnos...)
					sort.Slice(used, func(i, j int) bool { return used[i] < used[j] })
				}
				var old []byte
				if oi := c.find(v.snapFor(rp)); oi >= 0 {
					old = c.v[oi].data
				} else {
					buf := make([]byte, st.pgsz)
					if db.readReal(rp, buf) {
						old = buf
					}
				}
				if old != nil && interiorRoutesSame(old, c.v[len(c.v)-1].data, st.pgsz, st.reserved, used) {
					db.nReadsSaved.Add(1)
					continue
				}
			}
			db.nReadConflicts.Add(1)
			rc = errConflictRead
			break
		}
		if rc == rcOK {
			for i, pg := range pgnos {
				if chains[i].head() > v.snapFor(pg) {
					db.nPageConflicts.Add(1)
					rc = errConflict
					break
				}
			}
		}
		// a schema change invalidates everything the transaction did, even on other pages
		if rc == rcOK && v.checkCookie && st.p1Head.Load() > v.snapFor(1) && st.p1Cookie.Load() != v.cookie {
			db.nSchemaConflicts.Add(1)
			rc = errConflictSchema
		}
		if rc != rcOK {
			release()
			return 0, rc
		}
	}

	// ---- assign the epoch, the size record and the place in the log: the serialisation point
	st.seqMu.Lock()
	epoch := db.nextEpoch.Load() + 1
	lastSize := st.baseDBSize
	if k := len(st.sizes); k > 0 {
		lastSize = st.sizes[k-1].dbsize
	}
	// A transaction that did not change the file size must not roll the recorded size back to its (possibly stale)
	// view: growth and shrinking always write page 1, which is what conflicts.
	newSize := lastSize
	if wsDBSize != snapDBSize {
		newSize = wsDBSize
	}
	seg, off, lrc := db.log.assign(epoch, n)
	if lrc != rcOK {
		st.seqMu.Unlock()
		release()
		return 0, lrc
	}
	st.sizes = append(st.sizes, sizeRec{epoch, newSize})
	db.nextEpoch.Store(epoch) // assigned, NOT visible yet
	st.seqMu.Unlock()

	prevP1Head, prevP1Cookie := st.p1Head.Load(), st.p1Cookie.Load()
	wroteP1 := false
	var lp listPush
	for i, pg := range pgnos {
		st.install(pg, chains[i], epoch, copies[i], &lp)
		if pg == 1 {
			wroteP1 = true
			if l != nil && l.holdsReloc {
				// hand the new page 1 to the next relocation (it is the newest version)
				if db.p1Cache == nil {
					db.p1Cache = make([]byte, st.pgsz)
				}
				copy(db.p1Cache, copies[i])
				db.p1CacheEpoch = epoch
			}
		}
	}
	st.splice(&lp)
	release()

	// ---- outside the locks: persist, then become visible (in epoch order)
	if rc := db.log.write(seg, off, epoch, newSize, pgnos, imgs); rc != rcOK {
		// The record never made it. If nothing was assigned after us the commit is taken back cleanly; otherwise a
		// successor may already sit behind the hole: the database is failed until it is reopened.
		st.seqMu.Lock()
		latest := db.nextEpoch.Load() == epoch && db.log.unassign(seg, off, n)
		if latest {
			st.sizes = st.sizes[:len(st.sizes)-1]
			db.nextEpoch.Store(epoch - 1)
		} else {
			db.failed.Store(true)
		}
		st.seqMu.Unlock()
		if latest {
			var again stripeSet
			again.add(pgnos)
			st.lockStripes(&again)
			for _, pg := range pgnos {
				c := st.chainGet(pg, false)
				if k := len(c.v); k > 0 && c.v[k-1].epoch == epoch {
					c.v = c.v[:k-1]
					st.versions.Add(-1)
					st.bytes.Add(-int64(st.pgsz))
				}
			}
			if wroteP1 && st.p1Head.Load() == epoch {
				st.p1Cookie.Store(prevP1Cookie)
				st.p1Head.Store(prevP1Head)
			}
			if db.p1CacheEpoch == epoch {
				db.p1CacheEpoch = 0
			}
			st.unlockStripes(&again)
		}
		db.wakeAllVisibility()
		return 0, rc
	}
	if sync {
		if rc := db.log.sync(); rc != rcOK {
			db.failed.Store(true) // outcome uncertain: stop accepting commits
			db.wakeAllVisibility()
			return 0, rc
		}
	}
	if rc := db.makeVisible(epoch); rc != rcOK {
		return 0, rc
	}

	// the log only shrinks through compaction: ask the compactor, and throttle if it cannot keep up
	if lim := db.logMaxBytes.Load(); lim > 0 {
		if b := db.log.bytes(); b > lim {
			db.kickCompactor()
			if b > lim*16 {
				db.nBackpressure.Add(1)
				r := float64(b) / float64(lim*16)
				us := 500 * r * r * r
				if us > 50000 {
					us = 50000
				}
				time.Sleep(time.Duration(us) * time.Microsecond)
			}
		}
	}
	db.nCommits.Add(1)
	db.nPagesPublished.Add(int64(n))
	if gi := db.gcInterval.Load(); gi > 0 && db.pubSinceGC.Add(1) >= gi {
		db.gc()
	}
	return epoch, rcOK
}
