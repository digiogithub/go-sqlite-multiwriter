package multiwriter

// Two refinements of page-level validation for interior b-tree pages.
//
// Routes. A transaction that read an interior page whose newer version still routes every child the transaction went
// through (same divider keys on both sides of the child) took the same path, so the read is still valid. Only for
// table b-trees: the cells of an index interior page are entries, and what a transaction read from one is not covered
// by the routes of its children.
//
// Merge. Two transactions that split different children of the same parent both rewrite the parent; the parent is a
// sorted list of (child, upper bound) pairs, so the two rewrites are merged three-way on that model, refusing any
// child changed by both sides.

import (
	"bytes"
	"sort"
)

type icell struct {
	child uint32
	key   []byte
}

type inode struct {
	typ   byte
	right uint32
	cells []icell
}

// parseInterior parses an interior page: the child pointer and the key bytes of every cell, plus the right-most child.
func parseInterior(pg []byte, pgsz, reserved int) (inode, bool) {
	var nd inode
	typ := pg[0]
	if typ != 0x02 && typ != 0x05 {
		return nd, false
	}
	ncell := be16(pg[3:])
	if 12+2*ncell > pgsz {
		return nd, false
	}
	nd.typ, nd.right = typ, be32(pg[8:])
	nd.cells = make([]icell, ncell)
	U := pgsz - reserved
	minLocal, maxLocal := (U-12)*32/255-23, (U-12)*64/255-23
	for i := 0; i < ncell; i++ {
		off := be16(pg[12+2*i:])
		if off < 12+2*ncell || off+5 > pgsz {
			return nd, false
		}
		cp := pg[off:]
		k := cp[4:]
		v, n := varint(k)
		if n == 0 {
			return nd, false
		}
		ln := n // table interior cell: the rowid varint is the whole key
		if typ == 0x02 {
			local := int(v)
			if v > uint64(maxLocal) {
				local = minLocal + int((v-uint64(minLocal))%uint64(U-4))
				if local > maxLocal {
					local = minLocal
				}
				local += 4
			}
			ln = n + local
		}
		if off+4+ln > pgsz {
			return nd, false
		}
		nd.cells[i] = icell{be32(cp), k[:ln]}
	}
	return nd, true
}

func (nd *inode) findChild(c uint32) int {
	for i := range nd.cells {
		if nd.cells[i].child == c {
			return i
		}
	}
	if nd.right == c {
		return len(nd.cells)
	}
	return -1
}

// interiorRoutesSame: the newer version nw of an interior table page routes every child of used (sorted) that old
// lists exactly as old did.
func interiorRoutesSame(old, nw []byte, pgsz, reserved int, used []uint32) bool {
	a, ok1 := parseInterior(old, pgsz, reserved)
	b, ok2 := parseInterior(nw, pgsz, reserved)
	if !ok1 || !ok2 || a.typ != 0x05 || b.typ != 0x05 {
		return false
	}
	any := false
	for i := 0; i <= len(a.cells); i++ {
		c := a.right
		if i < len(a.cells) {
			c = a.cells[i].child
		}
		k := sort.Search(len(used), func(k int) bool { return used[k] >= c })
		if k >= len(used) || used[k] != c {
			continue
		}
		any = true
		j := b.findChild(c)
		if j < 0 {
			return false
		}
		aLo, bLo, aHi, bHi := i > 0, j > 0, i < len(a.cells), j < len(b.cells)
		if aLo != bLo || aHi != bHi {
			return false
		}
		if aLo && !bytes.Equal(a.cells[i-1].key, b.cells[j-1].key) {
			return false
		}
		if aHi && !bytes.Equal(a.cells[i].key, b.cells[j].key) {
			return false
		}
	}
	return any
}

type mcell struct {
	child uint32
	key   int64
	inf   bool
	kb    []byte
}

func parseTableInterior(pg []byte, pgsz int) ([]mcell, bool) {
	if pg[0] != 0x05 {
		return nil, false
	}
	ncell := be16(pg[3:])
	if 12+2*ncell > pgsz {
		return nil, false
	}
	out := make([]mcell, 0, ncell+1)
	for i := 0; i < ncell; i++ {
		off := be16(pg[12+2*i:])
		if off < 12+2*ncell || off+5 > pgsz {
			return nil, false
		}
		cp := pg[off:]
		v, n := varint(cp[4:])
		if n == 0 {
			return nil, false
		}
		out = append(out, mcell{be32(cp), int64(v), false, cp[4 : 4+n]})
	}
	out = append(out, mcell{child: be32(pg[8:]), inf: true})
	return out, true
}

func sameHi(a, b *mcell) bool {
	return a.inf == b.inf && (a.inf || bytes.Equal(a.kb, b.kb))
}

func lessCell(a, b *mcell) bool {
	if a.inf != b.inf {
		return !a.inf
	}
	return a.key < b.key
}

func childMap(c []mcell) (map[uint32]int, bool) {
	m := make(map[uint32]int, len(c))
	for i := range c {
		if _, dup := m[c[i].child]; dup {
			return nil, false
		}
		m[c[i].child] = i
	}
	return m, true
}

// interiorMerge merges three-way an interior table page that two transactions rewrote from the same base. ours must
// already use final page numbers for its new children.
func interiorMerge(base, ours, theirs []byte, pgsz, reserved int, out []byte) bool {
	B, ok1 := parseTableInterior(base, pgsz)
	O, ok2 := parseTableInterior(ours, pgsz)
	H, ok3 := parseTableInterior(theirs, pgsz)
	if !ok1 || !ok2 || !ok3 {
		return false
	}
	mb, ok1 := childMap(B)
	mo, ok2 := childMap(O)
	mh, ok3 := childMap(H)
	if !ok1 || !ok2 || !ok3 {
		return false
	}
	// every child of the base must still be listed on both sides
	for i := range B {
		if _, ok := mo[B[i].child]; !ok {
			return false
		}
		if _, ok := mh[B[i].child]; !ok {
			return false
		}
	}
	M := make([]mcell, 0, len(O)+len(H))
	// ours, with theirs' change where only theirs changed the child
	for j := range O {
		c := O[j]
		ib, inB := mb[O[j].child]
		ih, inH := mh[O[j].child]
		if inB {
			changedO := !sameHi(&O[j], &B[ib])
			changedH := inH && !sameHi(&H[ih], &B[ib])
			if changedO && changedH {
				return false // both moved the same child's upper bound
			}
			if changedH {
				c = H[ih]
			}
		} else if inH {
			return false // a child new in ours cannot exist in theirs
		}
		M = append(M, c)
	}
	// children new in theirs
	for j := range H {
		if _, inB := mb[H[j].child]; !inB {
			if _, inO := mo[H[j].child]; inO {
				return false
			}
			M = append(M, H[j])
		}
	}
	sort.SliceStable(M, func(i, j int) bool { return lessCell(&M[i], &M[j]) })
	inf := 0
	for i := range M {
		if M[i].inf {
			inf++
		}
		if i > 0 && !M[i].inf && !lessCell(&M[i-1], &M[i]) {
			return false // keys strictly increasing
		}
	}
	if inf != 1 || !M[len(M)-1].inf {
		return false
	}
	// serialise: header, cell pointers, cells packed from the end of the usable area
	usable := pgsz - reserved
	ncell := len(M) - 1
	total := 0
	for i := 0; i < ncell; i++ {
		total += 4 + len(M[i].kb)
	}
	if 12+2*ncell+total > usable {
		return false // does not fit: the parent would have to split
	}
	clear(out[:pgsz])
	out[0] = 0x05
	out[3], out[4] = byte(ncell>>8), byte(ncell)
	content := usable - total
	out[5], out[6] = byte((content&0xffff)>>8), byte(content)
	put32(out[8:], M[ncell].child)
	pos := usable
	for i := 0; i < ncell; i++ {
		pos -= 4 + len(M[i].kb)
		put32(out[pos:], M[i].child)
		copy(out[pos+4:], M[i].kb)
		out[12+2*i], out[13+2*i] = byte(pos>>8), byte(pos)
	}
	return true
}
