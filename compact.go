package multiwriter

// Compaction (checkpoint): materialises committed page versions into the database file so it becomes an ordinary
// SQLite database again. It never takes part in a publication: writers only wait for it on the short stripe locks.
//
// Target epoch T = the oldest snapshot in use (the visible epoch when none is): no reader can still need the old
// contents of a page that is overwritten, and any reader can use either the store or the file for state <= T.
//
// Crash-safe ordering:
//  1. write the newest version <= T of every page changed since the last base into the file, resize the file to its
//     size at T, fsync                                 [crash here: old base + log replay rewrites the same bytes]
//  2. record base = T in the log meta, fsync            [crash here: the records <= T are skipped]
//  3. publish compactedEpoch = T in the store (the GC may drop the versions), delete the segments <= T

import "errors"

type CompactResult struct {
	TargetEpoch   uint64
	PagesWritten  int
	VersionsFreed int64
}

var errFailed = errors.New("multiwriter: the database failed (I/O error); reopen it to recover")

func (db *mwDB) compact() (CompactResult, error) {
	if db.mp != nil {
		return db.compactMP()
	}
	var out CompactResult
	st := db.store
	if st == nil || db.log == nil {
		return out, nil
	}
	if db.failed.Load() {
		return out, errFailed
	}
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

	T := db.oldestActive()
	base := db.log.baseEpoch()
	if T <= base {
		return out, nil // nothing new to materialise (or pinned by an old reader)
	}
	// commits with synchronous < FULL are visible before they are on the disk: the log must have them before their
	// pages go into the file, or a power failure leaves the file ahead of the log
	if rc := db.log.sync(); rc != rcOK {
		return out, sqliteErr(rc)
	}
	pgsz := st.pgsz
	page := make([]byte, pgsz)

	// 1. write the pages: detach the dirty list, copy each page out under its stripe, write it holding nothing
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
		if !have {
			continue
		}
		if _, err := db.realFile.WriteAt(page, int64(pgno-1)*int64(pgsz)); err != nil {
			werr = err
			break
		}
		out.PagesWritten++
	}
	// the file size at T, from the in-header size of page 1 as of T
	var sizePages uint32
	if c1 := st.chainGet(1, false); c1 != nil {
		mu := st.stripeOf(1)
		mu.Lock()
		if k := c1.find(T); k >= 0 {
			sizePages = be32(c1.v[k].data[28:])
		}
		mu.Unlock()
	}
	if werr == nil && sizePages > 0 {
		werr = db.realFile.Truncate(int64(sizePages) * int64(pgsz))
	}
	if werr == nil {
		werr = fsyncFile(db.realFile)
	}
	// 2. the new base, durably
	if werr == nil {
		if rc := db.log.setBase(T); rc != rcOK {
			werr = sqliteErr(rc)
		}
	}
	// 3. settle the dirty list: clean pages drop out, pages with newer versions (or everything, on failure) stay
	var again, cand []uint32
	for _, pgno := range dirty {
		c := st.chainGet(pgno, false)
		mu := st.stripeOf(pgno)
		mu.Lock()
		if werr != nil || c.head() > T {
			again = append(again, pgno)
		} else {
			c.dirty = false
			// now in the file: a page written once never became a GC candidate; it is one now
			if len(c.v) > 0 && !c.queued {
				c.queued = true
				cand = append(cand, pgno)
			}
		}
		mu.Unlock()
	}
	st.listMu.Lock()
	st.dirty = append(st.dirty, again...)
	st.cand = append(st.cand, cand...)
	st.listMu.Unlock()
	if werr != nil {
		return out, werr
	}
	st.seqMu.Lock()
	st.compactedEpoch = T
	if sizePages > 0 {
		st.baseDBSize = sizePages
	}
	drop := 0 // size records <= T are subsumed by baseDBSize
	for drop < len(st.sizes) && st.sizes[drop].epoch <= T {
		drop++
	}
	st.sizes = append(st.sizes[:0], st.sizes[drop:]...)
	db.log.dropUpTo(T, db.nextEpoch.Load() == T)
	st.seqMu.Unlock()

	out.TargetEpoch = T
	out.VersionsFreed = db.gc()
	db.nCompactions.Add(1)
	db.nCompactedPages.Add(int64(out.PagesWritten))
	return out, nil
}
