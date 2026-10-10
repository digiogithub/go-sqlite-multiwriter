package multiwriter

// The net row changes of a write set, read from the page images: from the pages a transaction wrote and the pages
// the same numbers had at its snapshot, the rows of every table b-tree that were inserted, changed or deleted,
// without triggers or hooks. It decodes SQLite's file format directly. The net change over the whole write set makes
// splits and merges disappear (a row that moved between two written pages is no change); the freelist is walked
// because a page that the transaction frees is not rewritten, and a page on the freelist holds stale rows.
//
// Which table a page belongs to is found when it is needed: a page that the transaction allocated is listed by an
// interior page that it wrote; a page that existed at the snapshot is found by walking down from the root of every
// table with a key that the page holds. Whatever cannot be decoded exactly makes the result "unsupported": the
// caller refuses the commit as without the rebase.

import (
	"bytes"
	"sort"
)

type chgKind int

const (
	chgInsert chgKind = 1
	chgUpdate chgKind = 2
	chgDelete chgKind = 3
)

// rowChange is one row of the net change: the records are SQLite's own (header, serial types, values).
type rowChange struct {
	kind   chgKind
	tab    *tab
	rowid  int64
	oldRec []byte
	newRec []byte
}

type rdRow struct {
	root  uint32
	tab   *tab
	rowid int64
	rec   []byte
	key   []byte // (WITHOUT ROWID) the serial types and bytes of the key columns: the identity of the row
	bad   bool
	chain []uint32 // (new state) the overflow pages of the record
}

func pageKind(pg []byte, pgno uint32) byte {
	if pgno == 1 {
		return 'S'
	}
	switch pg[0] {
	case 0x0d:
		return 'L'
	case 0x05:
		return 'T'
	case 0x0a, 0x02:
		return 'I'
	}
	return 'O'
}

// snapPage reads page pgno at the transaction's snapshot; false if it does not exist there.
func (l *lane) snapPage(pgno uint32, dst []byte) bool {
	if pgno == 0 || pgno > l.dbsizeAt(l.tx.snapshotEpoch) {
		return false
	}
	if l.db.store.read(pgno, l.tx.snapshotEpoch, 0, dst) {
		return true
	}
	return l.db.readReal(pgno, dst)
}

type rowdiffCtx struct {
	l     *lane
	imgs  [][]byte
	cat   *catalog
	pgsz  int
	t1    []byte
	t2    []byte
	owner map[uint32]uint32 // cache of oldOwner
}

func (r *rowdiffCtx) wsImage(pgno uint32) []byte {
	if i, ok := r.l.wsIndex[pgno]; ok {
		return r.imgs[i]
	}
	return nil
}

// freelistOf: the freelist (trunk pages and the leaves they list) of the snapshot, or of the committed state of
// the transaction (newState: its images win).
func (r *rowdiffCtx) freelistOf(newState bool, page1 []byte) ([]uint32, bool) {
	var out []uint32
	tmp := make([]byte, r.pgsz)
	trunk := be32(page1[32:])
	for guard := 0; trunk != 0 && guard < 1000000; guard++ {
		var tp []byte
		if newState {
			tp = r.wsImage(trunk)
		}
		if tp == nil {
			if !r.l.snapPage(trunk, tmp) {
				return nil, false
			}
			tp = tmp
		}
		next, cnt := be32(tp), be32(tp[4:])
		if cnt > uint32(r.pgsz-8)/4 {
			return nil, false
		}
		out = append(out, trunk)
		for k := uint32(0); k < cnt; k++ {
			out = append(out, be32(tp[8+4*k:]))
		}
		trunk = next
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, true
}

func inSorted(s []uint32, v uint32) bool {
	i := sort.Search(len(s), func(i int) bool { return s[i] >= v })
	return i < len(s) && s[i] == v
}

// oldOwner: the table that owns a page that existed at the snapshot, by walking down from the root of every table
// with a key that the page holds.
func (r *rowdiffCtx) oldOwner(pgno uint32) uint32 {
	if o, ok := r.owner[pgno]; ok {
		return o
	}
	o := r.oldOwnerWalk(pgno)
	if o != 0 {
		r.owner[pgno] = o
	}
	return o
}

func (r *rowdiffCtx) oldOwnerWalk(pgno uint32) uint32 {
	pgsz := r.pgsz
	tmp, tmp2 := r.t1, r.t2
	if !r.l.snapPage(pgno, tmp) {
		return 0
	}
	nc := be16(tmp[3:])
	if nc == 0 {
		return 0
	}
	var off int
	switch tmp[0] {
	case 0x05:
		off = be16(tmp[12:])
	case 0x0d:
		off = be16(tmp[8:])
	default:
		return 0
	}
	if off+4 >= pgsz {
		return 0
	}
	var key uint64
	if tmp[0] == 0x0d {
		_, a := varint(tmp[off:pgsz])
		if a == 0 {
			return 0
		}
		k, b := varint(tmp[off+a : pgsz])
		if b == 0 {
			return 0
		}
		key = k
	} else {
		k, b := varint(tmp[off+4 : pgsz])
		if b == 0 {
			return 0
		}
		key = k
	}
	for t := range r.cat.tabs {
		tb := &r.cat.tabs[t]
		if !tb.ok || tb.withoutRowid || tb.root <= 1 {
			continue
		}
		page := tb.root
		for depth := 0; depth < 24 && page != 0; depth++ {
			if page == pgno {
				return tb.root
			}
			if !r.l.snapPage(page, tmp2) || tmp2[0] != 0x05 {
				break
			}
			n2 := be16(tmp2[3:])
			next := be32(tmp2[8:])
			for c := 0; c < n2; c++ {
				co := be16(tmp2[12+2*c:])
				if co+5 > pgsz {
					next = 0
					break
				}
				k, n := varint(tmp2[co+4 : pgsz])
				if n == 0 {
					next = 0
					break
				}
				if int64(key) <= int64(k) {
					next = be32(tmp2[co:])
					break
				}
			}
			page = next
		}
	}
	return 0
}

// fullPayload: the local part of a cell, then (if it spilled) the overflow chain from the images of the transaction
// (new state) or from its snapshot.
func (r *rowdiffCtx) fullPayload(newState bool, local []byte, total uint64, ovfl uint32, chain *[]uint32) ([]byte, bool) {
	buf := make([]byte, 0, total)
	buf = append(buf, local...)
	tmp := make([]byte, r.pgsz)
	for guard := 0; ovfl != 0 && uint64(len(buf)) < total && guard < 100000000; guard++ {
		var pg []byte
		if newState {
			pg = r.wsImage(ovfl)
		}
		if pg == nil {
			if !r.l.snapPage(ovfl, tmp) {
				return nil, false
			}
			pg = tmp
		}
		take := min(total-uint64(len(buf)), uint64(r.pgsz-4))
		if chain != nil {
			*chain = append(*chain, ovfl)
		}
		buf = append(buf, pg[4:4+take]...)
		ovfl = be32(pg)
	}
	return buf, uint64(len(buf)) == total
}

// rowsOf adds the rows of one table leaf page (0x0d).
func (r *rowdiffCtx) rowsOf(newState bool, pg []byte, rs *[]rdRow, root uint32) bool {
	tb := r.cat.byRoot(root)
	nc := be16(pg[3:])
	U := uint64(r.pgsz)
	M, X := (U-12)*32/255-23, U-35
	for i := 0; i < nc; i++ {
		off := be16(pg[8+2*i:])
		if off < 8 || off >= r.pgsz {
			return false
		}
		c := pg[off:r.pgsz]
		P, a := varint(c)
		if a == 0 {
			return false
		}
		rid, b := varint(c[a:])
		if b == 0 {
			return false
		}
		local, ovfl := P, uint32(0)
		if P > X {
			K := M + (P-M)%(U-4)
			local = M
			if K <= X {
				local = K
			}
			if a+b+int(local)+4 > len(c) {
				return false
			}
			ovfl = be32(c[a+b+int(local):])
		} else if a+b+int(local) > len(c) {
			return false
		}
		row := rdRow{root: root, tab: tb, rowid: int64(rid), rec: c[a+b : a+b+int(local)]}
		if ovfl != 0 {
			var ch *[]uint32
			if newState {
				ch = &row.chain
			}
			if full, ok := r.fullPayload(newState, row.rec, P, ovfl, ch); ok {
				row.rec = full
			} else {
				row.bad = true
			}
		}
		*rs = append(*rs, row)
	}
	return true
}

// rowsOfIndex adds the rows of one page of a WITHOUT ROWID table (leaf 0x0a or interior 0x02).
func (r *rowdiffCtx) rowsOfIndex(newState bool, pg []byte, rs *[]rdRow, root uint32) bool {
	tb := r.cat.byRoot(root)
	interior := pg[0] == 0x02
	hdr := 8
	if interior {
		hdr = 12
	}
	nc := be16(pg[3:])
	U := uint64(r.pgsz)
	M, X := (U-12)*32/255-23, (U-12)*64/255-23
	for i := 0; i < nc; i++ {
		off := be16(pg[hdr+2*i:])
		if off < hdr || off >= r.pgsz {
			return false
		}
		c := pg[off:r.pgsz]
		if interior {
			c = c[4:]
		}
		P, a := varint(c)
		if a == 0 {
			return false
		}
		local, ovfl := P, uint32(0)
		if P > X {
			K := M + (P-M)%(U-4)
			local = M
			if K <= X {
				local = K
			}
			if a+int(local)+4 > len(c) {
				return false
			}
			ovfl = be32(c[a+int(local):])
		} else if a+int(local) > len(c) {
			return false
		}
		row := rdRow{root: root, tab: tb, rec: c[a : a+int(local)]}
		if ovfl != 0 {
			var ch *[]uint32
			if newState {
				ch = &row.chain
			}
			if full, ok := r.fullPayload(newState, row.rec, P, ovfl, ch); ok {
				row.rec = full
			} else {
				row.bad = true
			}
		}
		if !row.bad { // the key
			hs, h := varint(row.rec)
			if h == 0 || hs > uint64(len(row.rec)) {
				row.bad = true
			} else {
				pos, hp, col := uint32(hs), uint32(h), 0
				var kb []byte
				for col < tb.npkRec && uint64(hp) < hs {
					t, k := varint(row.rec[hp:hs])
					if k == 0 {
						row.bad = true
						break
					}
					ln := typeLen(t)
					if int(pos+ln) > len(row.rec) {
						row.bad = true
						break
					}
					kb = append(kb, row.rec[hp:hp+uint32(k)]...)
					kb = append(kb, row.rec[pos:pos+ln]...)
					hp += uint32(k)
					pos += ln
					col++
				}
				if col < tb.npkRec {
					row.bad = true
				}
				row.key = kb
			}
		}
		*rs = append(*rs, row)
	}
	return true
}

func rowLess(p, q *rdRow) int {
	if p.root != q.root {
		if p.root < q.root {
			return -1
		}
		return 1
	}
	if p.tab != nil && p.tab.withoutRowid {
		if len(p.key) != len(q.key) {
			if len(p.key) < len(q.key) {
				return -1
			}
			return 1
		}
		return bytes.Compare(p.key, q.key)
	}
	switch {
	case p.rowid < q.rowid:
		return -1
	case p.rowid > q.rowid:
		return 1
	}
	return 0
}

// pageMap: page -> root.
type pageMap map[uint32]uint32

// sBuild lists, at the snapshot, every page of the trees of the WITHOUT ROWID tables (their interior pages are
// walked; the leaves are not read).
func (r *rowdiffCtx) sBuild(S pageMap) bool {
	tmp := r.t1
	reads := 0
	for t := range r.cat.tabs {
		tb := &r.cat.tabs[t]
		if !tb.withoutRowid || tb.root <= 1 {
			continue
		}
		S[tb.root] = tb.root
		depth := 0
		for pg := tb.root; depth < 40; depth++ { // how many levels of interior pages: down the right edge
			reads++
			if reads > 200000 || !r.l.snapPage(pg, tmp) {
				return false
			}
			if tmp[0] == 0x0a {
				break
			}
			if tmp[0] != 0x02 {
				return false
			}
			pg = be32(tmp[8:])
		}
		lvl := []uint32{tb.root}
		for d := 0; d < depth; d++ {
			var nxt []uint32
			for _, p := range lvl {
				reads++
				if reads > 200000 || !r.l.snapPage(p, tmp) || tmp[0] != 0x02 {
					return false
				}
				nc := be16(tmp[3:])
				for c := -1; c < nc; c++ {
					var ch uint32
					if c < 0 {
						ch = be32(tmp[8:])
					} else {
						off := be16(tmp[12+2*c:])
						if off+4 > r.pgsz {
							return false
						}
						ch = be32(tmp[off:])
					}
					S[ch] = tb.root
					if d+1 < depth {
						nxt = append(nxt, ch)
					}
				}
			}
			lvl = nxt
		}
	}
	return true
}

func (r *rowdiffCtx) isWRRoot(pg uint32) bool {
	t := r.cat.byRoot(pg)
	return t != nil && t.withoutRowid
}

// rowdiff computes the row changes of the write set; why is not "" when the write set holds something that is not
// decoded exactly.
func (l *lane) rowdiff(imgs [][]byte, cat *catalog) (out []rowChange, why string) {
	pgsz := l.db.store.pgsz
	r := &rowdiffCtx{l: l, imgs: imgs, cat: cat, pgsz: pgsz, t1: make([]byte, pgsz), t2: make([]byte, pgsz), owner: map[uint32]uint32{}}
	var oldr, newr []rdRow
	oldimg := make([]byte, pgsz)
	p1old := make([]byte, pgsz)
	if !l.snapPage(1, p1old) {
		return nil, "no page 1"
	}
	var fo, fn []uint32 // the freelist at the snapshot, and of the committed state
	if be32(p1old[36:]) > 0 {
		var ok bool
		if fo, ok = r.freelistOf(false, p1old); !ok {
			return nil, "the freelist cannot be read"
		}
	}
	{
		p1new := r.wsImage(1)
		touched := p1new != nil
		for _, pg := range l.wsPgnos {
			if touched || len(fo) == 0 {
				break
			}
			if inSorted(fo, pg) {
				touched = true
			}
		}
		if !touched {
			fn = fo
		} else {
			src := p1new
			if src == nil {
				src = p1old
			}
			var ok bool
			if fn, ok = r.freelistOf(true, src); !ok {
				return nil, "the freelist cannot be read"
			}
		}
	}
	// owners of the pages of the committed state that the interior pages written by the commit list (a few passes:
	// an interior page may itself be new)
	ov := pageMap{}
	S, cm := pageMap{}, pageMap{} // WITHOUT ROWID: page -> root at the snapshot, and child -> parent among the written interior pages
	sReady := false
	if cat.hasWR {
		for i, pgno := range l.wsPgnos {
			pg := imgs[i]
			if pgno == 1 || (pg[0] != 0x05 && pg[0] != 0x02) {
				continue
			}
			nc := be16(pg[3:])
			for c := -1; c < nc; c++ {
				var ch uint32
				if c < 0 {
					ch = be32(pg[8:])
				} else {
					off := be16(pg[12+2*c:])
					if off+4 > pgsz {
						continue
					}
					ch = be32(pg[off:])
				}
				cm[ch] = pgno
			}
		}
	}
	ensureS := func() bool {
		if cat.hasWR && !sReady {
			sReady = true
			if !r.sBuild(S) {
				why = "the pages of the WITHOUT ROWID tables cannot be listed"
				return false
			}
		}
		return true
	}
	newOwner := func(pgno uint32) uint32 {
		if cat.byRoot(pgno) != nil {
			return pgno
		}
		if o := ov[pgno]; o != 0 {
			return o
		}
		return r.oldOwner(pgno)
	}
	for pass := 0; pass < 3; pass++ {
		unresolved, before := 0, len(ov)
		for i, pgno := range l.wsPgnos {
			pg := imgs[i]
			if pgno == 1 || pg[0] != 0x05 {
				continue
			}
			root := newOwner(pgno)
			if root == 0 {
				unresolved++
				continue
			}
			nc := be16(pg[3:])
			for c := -1; c < nc; c++ {
				var ch uint32
				if c < 0 {
					ch = be32(pg[8:])
				} else {
					off := be16(pg[12+2*c:])
					if off+4 > pgsz {
						continue
					}
					ch = be32(pg[off:])
				}
				ov[ch] = root
			}
		}
		if unresolved == 0 || len(ov) == before {
			break
		}
	}
	snapSize := l.dbsizeAt(l.tx.snapshotEpoch)
	keepOld := func(pg []byte) []byte { cp := make([]byte, len(pg)); copy(cp, pg); return cp }
	for i, pgno := range l.wsPgnos {
		kn := pageKind(imgs[i], pgno)
		if kn == 'S' || kn == 'T' {
			continue
		}
		inFn, inFo := inSorted(fn, pgno), inSorted(fo, pgno)
		if kn == 'L' && !inFn {
			root := newOwner(pgno)
			if root == 0 {
				return nil, "a page of a table that the transaction wrote has no known owner"
			}
			if tb := cat.byRoot(root); tb == nil || !tb.ok {
				return nil, "a table that cannot be replayed was written"
			}
			if !r.rowsOf(true, imgs[i], &newr, root) {
				return nil, "a leaf page does not decode"
			}
		}
		if kn == 'I' && cat.hasWR && !inFn { // a page of a WITHOUT ROWID table, or of an index
			if !ensureS() {
				return nil, why
			}
			root, cur := uint32(0), pgno
			for g := 0; g < 64; g++ {
				if r.isWRRoot(cur) {
					root = cur
					break
				}
				par := cm[cur]
				if par == 0 {
					root = S[cur]
					break
				}
				pi := r.wsImage(par)
				if pi == nil || pi[0] != 0x02 {
					break
				}
				cur = par
			}
			if root != 0 {
				if tb := cat.byRoot(root); tb == nil || !tb.ok {
					return nil, "a table that cannot be replayed was written"
				}
				if !r.rowsOfIndex(true, imgs[i], &newr, root) {
					return nil, "a page of a WITHOUT ROWID table does not decode"
				}
			}
		}
		// the same page as the snapshot had it (a page that did not exist yet has no old rows)
		haveOld := pgno <= snapSize && l.snapPage(pgno, oldimg)
		if haveOld && cat.hasWR && pageKind(oldimg, pgno) == 'I' && !inFo {
			if !ensureS() {
				return nil, why
			}
			if root := S[pgno]; root != 0 {
				if tb := cat.byRoot(root); tb == nil || !tb.ok {
					return nil, "a table that cannot be replayed was written"
				}
				if !r.rowsOfIndex(false, keepOld(oldimg), &oldr, root) {
					return nil, "a page of a WITHOUT ROWID table does not decode"
				}
			}
		}
		if haveOld && pageKind(oldimg, pgno) == 'L' && !inFo {
			root := r.oldOwner(pgno)
			if root == 0 {
				return nil, "a page of a table that the transaction wrote has no known owner"
			}
			if tb := cat.byRoot(root); tb == nil || !tb.ok {
				return nil, "a table that cannot be replayed was written"
			}
			if !r.rowsOf(false, keepOld(oldimg), &oldr, root) {
				return nil, "a leaf page does not decode"
			}
		}
	}
	// pages the transaction freed (on the freelist now, not before) and did not rewrite: their old rows are gone
	for _, pg := range fn {
		if inSorted(fo, pg) || r.wsImage(pg) != nil || !l.snapPage(pg, oldimg) {
			continue
		}
		if pageKind(oldimg, pg) == 'I' && cat.hasWR {
			if !ensureS() {
				return nil, why
			}
			if wroot := S[pg]; wroot != 0 {
				if tb := cat.byRoot(wroot); tb == nil || !tb.ok {
					return nil, "a table that cannot be replayed was written"
				}
				if !r.rowsOfIndex(false, keepOld(oldimg), &oldr, wroot) {
					return nil, "a page of a WITHOUT ROWID table does not decode"
				}
			}
			continue
		}
		if pageKind(oldimg, pg) != 'L' {
			continue
		}
		root := r.oldOwner(pg)
		if root == 0 {
			return nil, "a page that the transaction freed has no known owner"
		}
		if tb := cat.byRoot(root); tb == nil || !tb.ok {
			return nil, "a table that cannot be replayed was written"
		}
		if !r.rowsOf(false, keepOld(oldimg), &oldr, root) {
			return nil, "a leaf page does not decode"
		}
	}
	// overflow pages written that no cell of the write set owns: an update that rewrites only the tail of a big value
	var nch []uint32
	for k := range newr {
		nch = append(nch, newr[k].chain...)
	}
	sort.Slice(nch, func(i, j int) bool { return nch[i] < nch[j] })
	for i, pg := range l.wsPgnos {
		if pg == 1 || pageKind(imgs[i], pg) != 'O' {
			continue
		}
		if inSorted(fn, pg) || inSorted(fo, pg) || inSorted(nch, pg) {
			continue
		}
		return nil, "an overflow page was written without its cell"
	}
	for i := range oldr {
		if oldr[i].bad {
			return nil, "a record does not decode"
		}
	}
	for i := range newr {
		if newr[i].bad {
			return nil, "a record does not decode"
		}
	}
	sort.Slice(oldr, func(i, j int) bool { return rowLess(&oldr[i], &oldr[j]) < 0 })
	sort.Slice(newr, func(i, j int) bool { return rowLess(&newr[i], &newr[j]) < 0 })
	clone := func(b []byte) []byte { return append([]byte(nil), b...) }
	i, j := 0, 0
	for i < len(oldr) || j < len(newr) {
		c := 0
		switch {
		case i >= len(oldr):
			c = 1
		case j >= len(newr):
			c = -1
		default:
			c = rowLess(&oldr[i], &newr[j])
		}
		switch {
		case c < 0:
			o := &oldr[i]
			out = append(out, rowChange{kind: chgDelete, tab: o.tab, rowid: o.rowid, oldRec: clone(o.rec)})
			i++
		case c > 0:
			w := &newr[j]
			out = append(out, rowChange{kind: chgInsert, tab: w.tab, rowid: w.rowid, newRec: clone(w.rec)})
			j++
		default:
			o, w := &oldr[i], &newr[j]
			if !bytes.Equal(o.rec, w.rec) {
				out = append(out, rowChange{kind: chgUpdate, tab: w.tab, rowid: w.rowid, oldRec: clone(o.rec), newRec: clone(w.rec)})
			}
			i++
			j++
		}
	}
	return out, ""
}
