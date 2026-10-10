package multiwriter

// Page relocation: a commit that conflicts only because other commits extended the file.
//
// Two transactions that start from the same snapshot and both append to the database (new leaves after a split, new
// overflow pages, new index pages) allocate the same new page numbers (dbsize+1, ...) and both rewrite page 1 (the
// in-header size). Physically they conflict, although what they changed is disjoint.
//
// Everything that refers to a page this transaction created lives in pages this transaction wrote (a page nobody has
// committed yet cannot be referenced by anything else), so the new pages can be renumbered above the current end of
// the file if the references are rewritten: child pointers and the right-most pointer of interior b-tree pages, the
// overflow pointer at the end of a cell's local payload, and the "next" pointer of an overflow page. Page 1 is merged:
// the latest page 1 with the change counter incremented and the in-header size extended.
//
// It refuses (the transaction is retried as before) unless every check holds: page 1 changed only in the change
// counter, the size and version-valid-for; the new pages are exactly the range (snapshot size, transaction size], not
// the lock-byte page; no page that existed at the snapshot conflicts, except interior table pages, which are merged
// three-way; every new page is referenced exactly once by the pages that were parsed; the schema cookie is unchanged.
// The result is published through the normal validation.

const (
	kNone     = 0
	kBtree    = 1
	kOverflow = 2
)

type relocRec struct {
	slot int
	off  int
	orig uint32
}

type rctx struct {
	lo, hi, delta uint32 // the old new-page range (lo, hi], and the offset to add
	pgsz, usable  int
	kind          []byte
	refs          []uint32
	queue         []uint32
	ok            bool
	rec           []relocRec // every rewritten reference
	curSlot       int
}

func (c *rctx) enqueue(p uint32, kind byte) {
	i := p - c.lo - 1
	c.refs[i]++
	if c.refs[i] > 1 || (c.kind[i] != kNone && c.kind[i] != kind) {
		c.ok = false
		return
	}
	c.kind[i] = kind
	c.queue = append(c.queue, p)
}

// patch rewrites the reference stored at pg[off:] if it points into the old new-page range.
func (c *rctx) patch(pg []byte, off int, kind byte) {
	p := be32(pg[off:])
	if p > c.lo && p <= c.hi {
		c.rec = append(c.rec, relocRec{c.curSlot, off, p})
		put32(pg[off:], p+c.delta)
		c.enqueue(p, kind)
	}
}

// patchBtreePage rewrites the references held by one b-tree page (a private copy).
func (c *rctx) patchBtreePage(pg []byte) {
	typ := pg[0]
	if typ != 0x02 && typ != 0x05 && typ != 0x0a && typ != 0x0d {
		c.ok = false
		return
	}
	interior := typ == 0x02 || typ == 0x05
	hdr := 8
	if interior {
		hdr = 12
	}
	ncell := be16(pg[3:])
	if hdr+2*ncell > c.pgsz {
		c.ok = false
		return
	}
	if interior {
		c.patch(pg, 8, kBtree)
	}
	U := c.usable
	minLocal := (U-12)*32/255 - 23
	maxLocal := (U-12)*64/255 - 23
	if typ == 0x0d {
		maxLocal = U - 35
	}
	for i := 0; i < ncell && c.ok; i++ {
		off := be16(pg[hdr+2*i:])
		if off < hdr+2*ncell || off+4 > c.pgsz {
			c.ok = false
			return
		}
		if typ == 0x05 { // interior table cell: child pointer + rowid, no payload
			c.patch(pg, off, kBtree)
			continue
		}
		q := off
		if typ == 0x02 {
			c.patch(pg, q, kBtree)
			q += 4
		}
		payload, n := varint(pg[q:c.pgsz])
		if n == 0 {
			c.ok = false
			return
		}
		q += n
		if typ == 0x0d {
			_, n = varint(pg[q:c.pgsz])
			if n == 0 {
				c.ok = false
				return
			}
			q += n
		}
		if payload > uint64(maxLocal) {
			local := minLocal + int((payload-uint64(minLocal))%uint64(U-4))
			if local > maxLocal {
				local = minLocal
			}
			if q+local+4 > c.pgsz {
				c.ok = false
				return
			}
			c.patch(pg, q+local, kOverflow)
		}
	}
}

// relocate publishes the transaction again with its new pages moved above the current end of the file. It returns
// rcOK if it committed, errRelocNA if the conflict is not a growth-only one, errConflict if the attempts were beaten
// by other extensions, or another code for a real error.
func (l *lane) relocate(v0 *validation, pgnos []uint32, imgs [][]byte, wsDBSize, snapDBSize uint32, sync bool) (uint64, int) {
	db := l.db
	st := db.store
	pgsz := st.pgsz
	if len(l.ownPg) != 0 || wsDBSize <= snapDBSize {
		return 0, errRelocNA
	}
	growth := wsDBSize - snapDBSize
	i1, nnew := -1, uint32(0)
	for i, p := range pgnos {
		if p == 1 {
			i1 = i
		}
		if p > snapDBSize {
			nnew++
		}
	}
	if i1 < 0 || nnew != growth {
		return 0, errRelocNA // page 1 must be written, and the new pages must be the whole range
	}
	for _, p := range pgnos {
		if p > snapDBSize && (p > wsDBSize || p-snapDBSize > growth) {
			return 0, errRelocNA
		}
	}
	// page 1 may differ from the snapshot's only in the change counter, the in-header size and version-valid-for
	snap1 := make([]byte, 100)
	if !st.read(1, l.tx.snapshotEpoch, 0, snap1) {
		if _, err := db.realFile.ReadAt(snap1, 0); err != nil {
			return 0, errRelocNA
		}
	}
	our1 := imgs[i1]
	for b := 0; b < 100; b++ {
		if (b >= 24 && b < 32) || (b >= 92 && b < 96) {
			continue
		}
		if snap1[b] != our1[b] {
			return 0, errRelocNA
		}
	}
	reserved := int(our1[20])
	pending := uint32(1073741824/pgsz) + 1 // the lock-byte page
	latest := make([]byte, pgsz)
	mbuf := make([]byte, 3*pgsz)

	for attempt := 0; attempt < 4; attempt++ {
		// pages that existed in the snapshot and have a newer committed version: an interior table page is merged,
		// anything else is a real conflict
		var cf []int
		for i, p := range pgnos {
			if p != 1 && p <= snapDBSize && st.headEpoch(p) > l.tx.snapshotEpoch {
				if imgs[i][0] == 0x05 && !l.nomerge {
					cf = append(cf, i)
				} else {
					return 0, errRelocNA
				}
			}
		}
		db.relocMu.Lock() // one relocation at a time: they would only beat each other
		l.holdsReloc = true
		unlock := func() {
			if l.holdsReloc {
				l.holdsReloc = false
				db.relocMu.Unlock()
			}
		}
		var eMerge uint64
		if db.p1Cache != nil && db.p1CacheEpoch != 0 && db.p1CacheEpoch == st.p1Head.Load() {
			copy(latest, db.p1Cache)
			eMerge = db.p1CacheEpoch
		} else if e, ok := st.headImage(1, latest); ok {
			eMerge = e
		} else {
			unlock()
			return 0, errRelocNA
		}
		cur, ctr := be32(latest[28:]), be32(latest[24:])
		newSize := cur + growth
		if be32(latest[92:]) != ctr || cur < snapDBSize || string(latest[40:44]) != string(snap1[40:44]) || string(latest[16:22]) != string(snap1[16:22]) {
			unlock()
			return 0, errRelocNA
		}
		if cur == snapDBSize {
			unlock()
			return 0, errRelocNA // nobody extended the file: the conflict is somewhere else
		}
		if (snapDBSize < pending && wsDBSize >= pending) || (cur < pending && newSize >= pending) {
			unlock()
			return 0, errRelocNA
		}
		delta := cur - snapDBSize
		c := &rctx{lo: snapDBSize, hi: wsDBSize, delta: delta, pgsz: pgsz, usable: pgsz - reserved, ok: true,
			kind: make([]byte, growth), refs: make([]uint32, growth)}
		nim := make([][]byte, len(pgnos))
		npg := make([]uint32, len(pgnos))
		slotOfNew := make([]int, growth)
		for i, p := range pgnos {
			nim[i] = make([]byte, pgsz)
			copy(nim[i], imgs[i])
			if p > snapDBSize {
				slotOfNew[p-snapDBSize-1] = i
			}
		}
		for i, p := range pgnos {
			if p != 1 && p <= snapDBSize && c.ok {
				c.curSlot = i
				c.patchBtreePage(nim[i])
			}
		}
		for qi := 0; qi < len(c.queue) && c.ok; qi++ { // the new pages, in reference order
			pq := c.queue[qi]
			slot := slotOfNew[pq-snapDBSize-1]
			c.curSlot = slot
			if c.kind[pq-snapDBSize-1] == kBtree {
				c.patchBtreePage(nim[slot])
			} else {
				c.patch(nim[slot], 0, kOverflow) // overflow page: its first 4 bytes point to the next one
			}
		}
		for k := uint32(0); k < growth && c.ok; k++ {
			if c.refs[k] != 1 {
				c.ok = false // each new page referenced exactly once
			}
		}
		if !c.ok {
			unlock()
			return 0, errRelocNA
		}
		for i, p := range pgnos {
			npg[i] = p
			if p > snapDBSize {
				npg[i] = p + delta
			}
		}
		// page 1: the latest one, counter + 1, size extended
		copy(nim[i1], latest)
		put32(nim[i1][24:], ctr+1)
		put32(nim[i1][92:], ctr+1)
		put32(nim[i1][28:], newSize)
		ownPg := []uint32{1}
		ownEp := []uint64{eMerge}
		nmerged := 0
		rc := rcOK
		for _, slot := range cf {
			theirs, basei, merged := mbuf[:pgsz], mbuf[pgsz:2*pgsz], mbuf[2*pgsz:]
			eh, ok := st.headImage(pgnos[slot], theirs)
			if !ok {
				rc = errRelocNA
				break
			}
			if eh <= l.tx.snapshotEpoch {
				continue
			}
			if !st.read(pgnos[slot], l.tx.snapshotEpoch, 0, basei) && !db.readReal(pgnos[slot], basei) {
				rc = errRelocNA
				break
			}
			if !interiorMerge(basei, nim[slot], theirs, pgsz, reserved, merged) {
				rc = errRelocNA
				break
			}
			copy(nim[slot], merged)
			ownPg = append(ownPg, pgnos[slot])
			ownEp = append(ownEp, eh)
			nmerged++
		}
		if rc != rcOK {
			unlock()
			return 0, rc
		}
		// insertion sort by page (page 1 and the merged pages were merged against the state at their epochs)
		for a := 1; a < len(ownPg); a++ {
			pg, ep := ownPg[a], ownEp[a]
			b := a - 1
			for b >= 0 && ownPg[b] > pg {
				ownPg[b+1], ownEp[b+1] = ownPg[b], ownEp[b]
				b--
			}
			ownPg[b+1], ownEp[b+1] = pg, ep
		}
		v := *v0
		v.ownPgnos, v.ownEpochs = ownPg, ownEp
		v.adopt = true
		epoch, prc := db.publish(l, &v, npg, nim, newSize, cur, sync) // (releases relocMu once its pages are installed)
		unlock()
		if prc == rcOK {
			db.nRelocations.Add(1)
			db.nMerges.Add(int64(nmerged))
			return epoch, rcOK
		}
		if prc != errConflict {
			return 0, prc
		}
		// another commit extended the file (or changed page 1) since we read it: read the latest again
	}
	return 0, errConflict
}
