package multiwriter

// The schema as the rebase needs it: for every table b-tree, how its records are laid out (which record column is
// which column) and whether the database can be replayed at all. Built from sqlite_schema as the transaction's
// snapshot has it: the rows are read from the pages, and SQLite itself parses the CREATE TABLE statements (in a
// scratch in-memory database), so generated columns, quoting and so on are never guessed.

import (
	"fmt"
	"strconv"
	"strings"

	sqlite3 "modernc.org/sqlite/lib"
)

// sqlVal is a value as sqlite3_column_type sees it.
type sqlVal struct {
	typ int
	i   int64
	d   float64
	b   []byte
}

type tab struct {
	root         uint32
	name         string
	ok           bool // false: its CREATE TABLE could not be understood, or it is an internal table: not replayed
	withoutRowid bool
	npkRec       int  // (WITHOUT ROWID) the key columns: the first record columns
	aliasPK      bool // INTEGER PRIMARY KEY: the key is the rowid (its record column holds NULL)
	nrec         int  // columns stored in a record
	aliasRec     int  // the record column of the rowid alias (-1: none)
	recName      []string
	recGen       []bool // a generated column: derived, not written, not compared
	noaff        []bool // no affinity: 1 and 1.0 are different values there
	dflt         []sqlVal
	fkParent     []string
	fkTo         []string // "" : the parent's primary key
	isParent     bool
	refmask      uint64 // the writable columns that foreign keys refer to
	rank         int    // 0 without foreign keys; parents before children
	pkName       []string
}

type catalog struct {
	tabs      []tab
	cookie    uint32
	rebasable bool
	hasWR     bool
	hasFK     bool
	maxrank   int
	why       string
}

func (c *catalog) byRoot(root uint32) *tab {
	for i := range c.tabs {
		if c.tabs[i].root == root {
			return &c.tabs[i]
		}
	}
	return nil
}

func (c *catalog) byName(name string) *tab {
	for i := range c.tabs {
		if strings.EqualFold(c.tabs[i].name, name) {
			return &c.tabs[i]
		}
	}
	return nil
}

func (c *catalog) index(t *tab) int {
	for i := range c.tabs {
		if &c.tabs[i] == t {
			return i
		}
	}
	return -1
}

// MARK: - records

var serialLen = [12]uint32{0, 1, 2, 3, 4, 6, 8, 8, 0, 0, 0, 0}

func typeLen(t uint64) uint32 {
	if t < 12 {
		return serialLen[t]
	}
	return uint32((t - 12) / 2)
}

// recColText returns a text or integer column of a record as a string.
func recColText(rec []byte, col int) (string, bool) {
	hs, hl := varint(rec)
	if hl == 0 || hs > uint64(len(rec)) {
		return "", false
	}
	pos, hp := uint32(hs), uint32(hl)
	for idx := 0; uint64(hp) < hs; idx++ {
		t, k := varint(rec[hp:hs])
		if k == 0 {
			return "", false
		}
		hp += uint32(k)
		l := typeLen(t)
		if idx == col {
			if int(pos+l) > len(rec) {
				return "", false
			}
			switch {
			case t >= 13 && t&1 == 1:
				return string(rec[pos : pos+l]), true
			case t >= 1 && t <= 6:
				v := int64(int8(rec[pos]))
				for b := uint32(1); b < l; b++ {
					v = v<<8 | int64(rec[pos+b])
				}
				return strconv.FormatInt(v, 10), true
			case t == 8:
				return "0", true
			case t == 9:
				return "1", true
			}
			return "", false
		}
		pos += l
	}
	return "", false
}

// cellPayload is the full payload of a table leaf cell at the snapshot: its local part, then the overflow chain.
func (l *lane) cellPayload(pg []byte, off int, pgsz int) ([]byte, bool) {
	P, a := varint(pg[off:])
	if a == 0 {
		return nil, false
	}
	_, b := varint(pg[off+a:])
	if b == 0 {
		return nil, false
	}
	U := uint64(pgsz)
	X, M := U-35, (U-12)*32/255-23
	local, ovfl := P, uint32(0)
	start := off + a + b
	if P > X {
		K := M + (P-M)%(U-4)
		local = M
		if K <= X {
			local = K
		}
		if start+int(local)+4 > pgsz {
			return nil, false
		}
		ovfl = be32(pg[start+int(local):])
	} else if start+int(local) > pgsz {
		return nil, false
	}
	buf := make([]byte, 0, P)
	buf = append(buf, pg[start:start+int(local)]...)
	tmp := make([]byte, pgsz)
	for ovfl != 0 && uint64(len(buf)) < P {
		if !l.snapPage(ovfl, tmp) {
			return nil, false
		}
		take := min(P-uint64(len(buf)), U-4)
		buf = append(buf, tmp[4:4+take]...)
		ovfl = be32(tmp)
	}
	return buf, uint64(len(buf)) == P
}

type schemaRow struct {
	typ, name, sql string
	root           uint32
}

func (l *lane) schemaCollect(pgno uint32, out *[]schemaRow, depth int) {
	pgsz := l.db.store.pgsz
	pg := make([]byte, pgsz)
	if depth > 8 || !l.snapPage(pgno, pg) {
		return
	}
	base := 0
	if pgno == 1 {
		base = 100
	}
	switch pg[base] {
	case 0x0d:
		nc := be16(pg[base+3:])
		for i := 0; i < nc; i++ {
			off := be16(pg[base+8+2*i:])
			if off >= pgsz {
				continue
			}
			rec, ok := l.cellPayload(pg, off, pgsz)
			if !ok {
				continue
			}
			typ, ok1 := recColText(rec, 0)
			name, ok2 := recColText(rec, 1)
			if !ok1 || !ok2 || (typ != "table" && typ != "trigger") {
				continue
			}
			root, _ := recColText(rec, 3)
			sql, _ := recColText(rec, 4)
			r, _ := strconv.ParseUint(root, 10, 32)
			*out = append(*out, schemaRow{typ, name, sql, uint32(r)})
		}
	case 0x05:
		nc := be16(pg[base+3:])
		for i := 0; i < nc; i++ {
			off := be16(pg[base+12+2*i:])
			if off+4 <= pgsz {
				l.schemaCollect(be32(pg[off:]), out, depth+1)
			}
		}
		l.schemaCollect(be32(pg[base+8:]), out, depth+1)
	}
}

func startsCI(s, p string) bool {
	s = strings.TrimLeft(s, " \n\t\r")
	return len(s) >= len(p) && strings.EqualFold(s[:len(p)], p)
}

// evalDefault evaluates the default expression of a column with SQLite (a constant).
func evalDefault(scratch *cconn, expr string, has bool) sqlVal {
	v := sqlVal{typ: colNull}
	if !has {
		return v
	}
	scratch.query("SELECT "+expr, func(s *cstmt) {
		v.typ = s.colType(0)
		switch v.typ {
		case colInteger:
			v.i = s.colInt64(0)
		case colFloat:
			v.d = s.colDouble(0)
		case colText:
			t, _ := s.colText(0)
			v.b = []byte(t)
		case colBlob:
			v.b = s.colBlob(0)
		}
	})
	return v
}

// typeHasNoAffinity: the BLOB affinity of a declared type (an empty type, or one with BLOB and none of INT, CHAR,
// CLOB, TEXT).
func typeHasNoAffinity(t string) bool {
	if t == "" {
		return true
	}
	u := strings.ToUpper(t)
	if strings.Contains(u, "INT") || strings.Contains(u, "CHAR") || strings.Contains(u, "CLOB") || strings.Contains(u, "TEXT") {
		return false
	}
	return strings.Contains(u, "BLOB")
}

func tabParse(scratch *cconn, r *schemaRow) tab {
	t := tab{root: r.root, name: r.name, aliasRec: -1}
	if r.sql == "" || !startsCI(r.sql, "CREATE TABLE") || startsCI(r.sql, "CREATE VIRTUAL") {
		return t
	}
	if len(r.name) >= 7 && strings.EqualFold(r.name[:7], "sqlite_") {
		return t
	}
	t.withoutRowid = strings.Contains(strings.ToUpper(r.sql), "WITHOUT ROWID")
	if scratch.exec(r.sql) != rcOK {
		return t
	}
	qn := quoteIdent(r.name)
	scratch.query("PRAGMA table_list("+qn+")", func(s *cstmt) {
		wr := -1
		for i := 0; i < s.colCount(); i++ {
			if s.colName(i) == "wr" {
				wr = i
			}
		}
		if nm, _ := s.colText(1); wr >= 0 && strings.EqualFold(nm, r.name) {
			t.withoutRowid = s.colInt64(wr) != 0
		}
	})
	type col struct {
		name, typ, dflt string
		hasDflt         bool
		pk, hidden      int
	}
	var cols []col
	scratch.query("PRAGMA table_xinfo("+qn+")", func(s *cstmt) {
		var c col
		c.name, _ = s.colText(1)
		c.typ, _ = s.colText(2)
		c.dflt, c.hasDflt = s.colText(4)
		c.pk, c.hidden = int(s.colInt64(5)), int(s.colInt64(6))
		cols = append(cols, c)
	})
	npk := 0
	for _, c := range cols {
		if c.pk > 0 {
			npk++
		}
	}
	if !t.withoutRowid && npk == 1 {
		for _, c := range cols {
			if c.pk == 1 && strings.EqualFold(c.typ, "INTEGER") {
				t.aliasPK = true
			}
		}
	}
	if t.aliasPK {
		// `x INTEGER PRIMARY KEY DESC` is not an alias of the rowid: SQLite makes an index for the key (origin "pk")
		scratch.query("PRAGMA index_list("+qn+")", func(s *cstmt) {
			if org, _ := s.colText(3); org == "pk" {
				t.aliasPK = false
			}
		})
	}
	// the order of the columns in the record: the declaration order; in a WITHOUT ROWID table the primary key
	// columns come first (in the order of the key), then the others
	var order []int
	if t.withoutRowid {
		for k := 1; k <= npk; k++ {
			for i, c := range cols {
				if c.pk == k {
					order = append(order, i)
				}
			}
		}
		t.npkRec = len(order)
		for i, c := range cols {
			if c.pk <= 0 {
				order = append(order, i)
			}
		}
		// the key must compare as bytes would: BINARY and ascending
		plain := npk > 0
		pkIndex := ""
		scratch.query("PRAGMA index_list("+qn+")", func(s *cstmt) {
			if org, _ := s.colText(3); org == "pk" {
				pkIndex, _ = s.colText(1)
			}
		})
		if pkIndex == "" {
			plain = false
		} else {
			scratch.query("PRAGMA index_xinfo("+quoteIdent(pkIndex)+")", func(s *cstmt) {
				if s.colInt64(5) != 0 {
					co, _ := s.colText(4)
					if s.colInt64(3) != 0 || !strings.EqualFold(co, "BINARY") {
						plain = false
					}
				}
			})
		}
		if !plain {
			return t
		}
	} else {
		for i := range cols {
			order = append(order, i)
		}
	}
	for _, i := range order {
		c := cols[i]
		if c.hidden == 2 {
			continue // generated, virtual: not stored
		}
		if t.aliasPK && c.pk == 1 {
			t.aliasRec = len(t.recName)
		}
		t.noaff = append(t.noaff, typeHasNoAffinity(c.typ))
		t.recName = append(t.recName, c.name)
		t.recGen = append(t.recGen, c.hidden == 3) // generated, stored: in the record, derived
		t.dflt = append(t.dflt, evalDefault(scratch, c.dflt, c.hasDflt && c.hidden != 3))
	}
	t.nrec = len(t.recName)
	for _, c := range cols {
		if c.pk > 0 {
			t.pkName = append(t.pkName, c.name)
		}
	}
	scratch.query("PRAGMA foreign_key_list("+qn+")", func(s *cstmt) {
		pt, _ := s.colText(2)
		to, _ := s.colText(4)
		t.fkParent = append(t.fkParent, pt)
		t.fkTo = append(t.fkTo, to)
	})
	t.ok = true
	return t
}

// writableBit is the bit of a column among the writable columns of a table (as changedMask counts them), -1 if none.
func (t *tab) writableBit(name string) int {
	bit := 0
	for i := 0; i < t.nrec; i++ {
		if t.recGen[i] || i == t.aliasRec {
			continue
		}
		if strings.EqualFold(t.recName[i], name) {
			return bit
		}
		bit++
	}
	return -1
}

// fkGraph: which tables are parents, which of their columns are referred to, and a rank (parents before children).
// A table that refers to itself, or a cycle, is not replayed.
func (c *catalog) fkGraph() {
	for i := range c.tabs {
		t := &c.tabs[i]
		for k := range t.fkParent {
			c.hasFK = true
			p := c.byName(t.fkParent[k])
			if p == nil {
				continue
			}
			if p == t {
				c.rebasable, c.why = false, "the database has a table with a foreign key to itself"
				return
			}
			p.isParent = true
			if t.fkTo[k] != "" {
				if b := p.writableBit(t.fkTo[k]); b >= 0 && b < 64 {
					p.refmask |= 1 << b
				}
			} else {
				for _, pn := range p.pkName {
					if b := p.writableBit(pn); b >= 0 && b < 64 {
						p.refmask |= 1 << b
					}
				}
			}
		}
	}
	for round := 0; round <= len(c.tabs); round++ {
		changed := false
		for i := range c.tabs {
			t := &c.tabs[i]
			r := 0
			for _, pn := range t.fkParent {
				if p := c.byName(pn); p != nil && p.rank+1 > r {
					r = p.rank + 1
				}
			}
			if r != t.rank {
				t.rank, changed = r, true
			}
		}
		if !changed {
			break
		}
		if round == len(c.tabs) {
			c.rebasable, c.why = false, "the foreign keys of the database are circular"
			return
		}
	}
	for i := range c.tabs {
		c.maxrank = max(c.maxrank, c.tabs[i].rank)
	}
}

// buildCatalog builds the schema at the lane's snapshot.
func (l *lane) buildCatalog() *catalog {
	c := &catalog{rebasable: true}
	pgsz := l.db.store.pgsz
	p1 := make([]byte, pgsz)
	if l.snapPage(1, p1) {
		c.cookie = be32(p1[40:])
		if be32(p1[56:]) != 1 {
			c.rebasable, c.why = false, "the text encoding is not UTF-8"
		} else if p1[20] != 0 {
			c.rebasable, c.why = false, "the pages have reserved bytes"
		}
	} else {
		c.rebasable, c.why = false, "no page 1"
	}
	var rows []schemaRow
	l.schemaCollect(1, &rows, 0)
	scratch, rc := copen(":memory:", sqlite3.SQLITE_OPEN_READWRITE|sqlite3.SQLITE_OPEN_CREATE, "")
	if rc != rcOK {
		c.rebasable, c.why = false, fmt.Sprintf("scratch database: %d", rc)
		return c
	}
	defer scratch.close()
	for i := range rows {
		r := &rows[i]
		if r.typ == "trigger" {
			c.rebasable, c.why = false, "the database has a trigger"
			continue
		}
		if startsCI(r.sql, "CREATE VIRTUAL") {
			c.rebasable, c.why = false, "the database has a virtual table"
		}
		t := tabParse(scratch, r)
		if t.withoutRowid {
			c.hasWR = true
		}
		c.tabs = append(c.tabs, t)
	}
	c.fkGraph()
	return c
}
