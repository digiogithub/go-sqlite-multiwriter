package multiwriter

// The rebase (URI mw_rebase=1). A commit whose pages conflict with newer commits only on pages that it wrote is not
// necessarily a logical conflict: two writers may change different rows of one page. Such a commit is not refused:
// its pages are discarded and its row changes (rowdiff.go) are replayed with ordinary SQL on a helper connection at
// the latest snapshot, and stock SQLite builds every page again. The original SQL is never run again.
//
// What is checked, so that the result is a state that a serial execution could have produced:
//   - the schema is the one the transaction ran with (the cookie);
//   - every row that the transaction changed or deleted is, at the latest snapshot, exactly what it was at the
//     transaction's snapshot (the whole row); otherwise it is a true conflict: SQLITE_BUSY_SNAPSHOT;
//   - a row that the transaction inserted is not there yet; the constraints are evaluated by SQLite on the rows as
//     they are now.
//
// The rows that the transaction read and did not change are checked by the statements: a transaction that ran a
// SELECT, or a statement that is not a point statement (by its bytecode), is not rebased (write skew).
//
// The replays are serialised and batched: the connections whose commit conflicted queue a request; the first becomes
// the leader and replays everything queued in ONE transaction of its helper, a savepoint for each request.

import (
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"modernc.org/libc"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	rebaseMaxAttempts = 200
	rebaseGateAfter   = 1 // lost attempts before the publication gate is closed for the next one
	rebaseBatch       = 64
)

// MARK: - what the statements of a transaction read

const (
	rdOther = 0
	rdDML   = 1
)

type rdEnt struct {
	point bool
	gen   uint64
}

func sqlSkip(s string) string {
	for {
		s = strings.TrimLeft(s, " \t\n\r\f")
		switch {
		case strings.HasPrefix(s, "--"):
			if i := strings.IndexByte(s, '\n'); i >= 0 {
				s = s[i:]
			} else {
				return ""
			}
		case strings.HasPrefix(s, "/*"):
			if i := strings.Index(s[2:], "*/"); i >= 0 {
				s = s[i+4:]
			} else {
				return ""
			}
		default:
			return s
		}
	}
}

func hasPrefixCI(s, p string) bool { return len(s) >= len(p) && strings.EqualFold(s[:len(p)], p) }

func stmtKind(sql string) int {
	s := sqlSkip(sql)
	if hasPrefixCI(s, "INSERT") || hasPrefixCI(s, "REPLACE") || hasPrefixCI(s, "UPDATE") || hasPrefixCI(s, "DELETE") {
		return rdDML
	}
	return rdOther
}

func isWordByte(c byte) bool {
	return c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func hasWord(sql, w string) bool {
	for i := 0; i+len(w) <= len(sql); i++ {
		if strings.EqualFold(sql[i:i+len(w)], w) && (i == 0 || !isWordByte(sql[i-1])) && (i+len(w) == len(sql) || !isWordByte(sql[i+len(w)])) {
			return true
		}
	}
	return false
}

func stmtReads(sql string) bool {
	s := sqlSkip(sql)
	for strings.HasPrefix(s, "(") {
		s = sqlSkip(s[1:])
	}
	return hasPrefixCI(s, "SELECT") || hasPrefixCI(s, "WITH") || hasPrefixCI(s, "VALUES")
}

// multirowInsert: an INSERT with several rows in VALUES is compiled with a coroutine, which the bytecode check refuses.
func multirowInsert(sql string) bool {
	s := sqlSkip(sql)
	if !hasPrefixCI(s, "INSERT") && !hasPrefixCI(s, "REPLACE") {
		return false
	}
	for i := strings.IndexByte(s, ')'); i >= 0; {
		q := sqlSkip(s[i+1:])
		if strings.HasPrefix(q, ",") && strings.HasPrefix(sqlSkip(q[1:]), "(") {
			return true
		}
		j := strings.IndexByte(s[i+1:], ')')
		if j < 0 {
			break
		}
		i += 1 + j
	}
	return false
}

type bcOp struct {
	op           string
	addr, p1, p2 int
}

var (
	bcDeny = toSet("SorterSort", "SorterNext", "SeekScan", "OpenEphemeral", "OpenAutoindex", "OpenPseudo", "VOpen", "VFilter", "VNext",
		"VUpdate", "VColumn", "Gosub", "BeginSubrtn", "InitCoroutine", "Yield", "Sort")
	// a constant expression computed once is a block guarded by Once that touches no table
	bcConst = toSet("Integer", "Int64", "Real", "String8", "String", "Blob", "Null", "Function", "PureFunc", "Copy", "SCopy", "IntCopy",
		"Variable", "Add", "Subtract", "Multiply", "Divide", "Remainder", "Concat", "Cast", "Affinity", "Not", "BitAnd", "BitOr",
		"ShiftLeft", "ShiftRight", "Negative", "AddImm", "Noop", "MustBeInt", "RealAffinity")
	// what a foreign key check may do inside a loop that scans the rows of the other table
	bcFKBody = toSet("Column", "Ne", "Eq", "Lt", "Le", "Gt", "Ge", "FkCounter", "Integer", "Copy", "SCopy", "Affinity", "IsNull", "NotNull",
		"Rowid", "IdxGT", "IdxGE", "IdxLT", "IdxLE", "DeferredSeek", "IdxRowid", "Goto", "MustBeInt", "Null", "String8", "Int64", "Real", "Variable")
)

func toSet(s ...string) map[string]bool {
	m := map[string]bool{}
	for _, x := range s {
		m[x] = true
	}
	return m
}

// bytecodeIsPoint: an INSERT ... VALUES, or an UPDATE/DELETE of one row found by its rowid or a unique index, that
// reads nothing else: no loop, no cursor opened for reading, no virtual table (what a foreign key adds is allowed).
func bytecodeIsPoint(tls *libc.TLS, db uintptr, sql string) bool {
	e, rc := prepareOn(tls, db, "EXPLAIN "+sql)
	if rc != rcOK {
		return false
	}
	var ops []bcOp
	point := true
	for e.step() == rcRow {
		op, _ := e.colText(1)
		if len(ops) >= 400 {
			point = false
			break
		}
		if len(ops) > 0 && op == "Init" {
			break // the sub-programs of foreign key actions follow, each with its own Init: replayed by SQLite
		}
		ops = append(ops, bcOp{op, int(e.colInt64(0)), int(e.colInt64(2)), int(e.colInt64(3))})
	}
	e.finalize()
	var rdcur []int
	for _, o := range ops {
		if o.op == "OpenRead" {
			rdcur = append(rdcur, o.p1)
		}
	}
	verifiedTo := -1
	for i := 0; point && i < len(ops); i++ {
		op := ops[i].op
		if bcDeny[op] {
			return false
		}
		if op == "Once" {
			for k := i + 1; k < len(ops) && ops[k].addr < ops[i].p2; k++ {
				if !bcConst[ops[k].op] {
					return false
				}
			}
			continue
		}
		if op == "Program" {
			continue
		}
		isRead := false
		for _, c := range rdcur {
			if c == ops[i].p1 {
				isRead = true
			}
		}
		start := op == "Rewind" || op == "Last" || (isRead && strings.HasPrefix(op, "Seek"))
		if start {
			j := -1
			for k := i + 1; k < len(ops); k++ {
				if (ops[k].op == "Next" || ops[k].op == "Prev") && ops[k].p1 == ops[i].p1 {
					j = k
					break
				}
			}
			if j < 0 {
				if strings.HasPrefix(op, "Seek") {
					continue // a lookup of one row
				}
				return false
			}
			if !isRead {
				return false // a loop over the table that is written: a scan
			}
			fkc := false
			for k := i + 1; k < j; k++ {
				if !bcFKBody[ops[k].op] {
					return false
				}
				if ops[k].op == "FkCounter" {
					fkc = true
				}
			}
			if !fkc {
				return false
			}
			verifiedTo = j
			i = j
			continue
		}
		if (op == "Next" || op == "Prev") && i > verifiedTo {
			return false
		}
	}
	return point
}

func (l *lane) stmtIsPoint(tls *libc.TLS, db uintptr, sql string) bool {
	gen := l.db.schemaGen.Load()
	if e, ok := l.rdCache[sql]; ok && e.gen == gen {
		return e.point
	}
	if multirowInsert(sql) {
		return false
	}
	p := bytecodeIsPoint(tls, db, sql)
	if len(l.rdCache) > 64 {
		clear(l.rdCache)
	}
	l.rdCache[sql] = rdEnt{p, gen}
	return p
}

// stmtNote is called by the statement hook of a mw_rebase=1 connection when a statement starts and ends.
func (l *lane) stmtNote(tls *libc.TLS, ending bool, st uintptr) {
	db := sqlite3.Xsqlite3_db_handle(tls, st)
	if !ending {
		if sqlite3.Xsqlite3_get_autocommit(tls, db) != 0 { // (this statement starts a transaction)
			l.rdDep = false
			l.rdChgBase = sqlite3.Xsqlite3_total_changes(tls, db)
		}
		sql := libc.GoString(sqlite3.Xsqlite3_sql(tls, st))
		if hasPrefixCI(sqlSkip(sql), "EXPLAIN") {
			return // (our own: the bytecode of a statement is looked at from inside the hook)
		}
		l.rdDB = db
		if stmtReads(sql) || hasWord(sql, "REPLACE") {
			l.rdDep = true // (REPLACE deletes rows that no change counter counts)
		}
		l.rdCur, l.rdCurKind = st, stmtKind(sql)
		if l.rdCurKind == rdDML && !l.rdDep && !l.stmtIsPoint(tls, db, sql) {
			l.rdDep = true
		}
		return
	}
	if l.rdCur != st {
		return
	}
	l.rdCur = 0
	if l.rdCurKind == rdDML && sqlite3.Xsqlite3_stmt_readonly(tls, st) == 0 && sqlite3.Xsqlite3_changes(tls, db) == 0 {
		l.rdDep = true // an UPDATE or DELETE that found nothing has read that the row is absent
	}
}

// readsUnchanged: did the transaction read rows that it may not have changed (then it is not rebased)? The hook is
// the connection's one trace callback: an application that installs its own replaces it, and then the statement that
// commits is not the one the hook saw: no rebase.
func (l *lane) readsUnchanged(tls *libc.TLS, nnet int) bool {
	if l.rdDep || l.rdCur == 0 || l.rdDB == 0 {
		return true
	}
	alive := false
	for st := sqlite3.Xsqlite3_next_stmt(tls, l.rdDB, 0); st != 0; st = sqlite3.Xsqlite3_next_stmt(tls, l.rdDB, st) {
		if st == l.rdCur && sqlite3.Xsqlite3_stmt_busy(tls, st) != 0 {
			alive = true
		}
	}
	if !alive {
		return true
	}
	// the changes SQLite counted must be exactly the net row changes found in the pages: a no-op UPDATE is counted
	// and writes no page (the committing statement itself is not counted yet)
	counted := int(uint32(sqlite3.Xsqlite3_total_changes(tls, l.rdDB)) - uint32(l.rdChgBase))
	if l.rdCurKind == rdDML {
		counted++
	}
	return counted != nnet
}

// canRebase: the connection asked for it, it is not a helper, it is the first commit of its snapshot, and it
// changed no schema and no field of the header the application can set.
func (l *lane) canRebase(pg1 []byte, cookie uint32) bool {
	if l.norebase || !l.rebaseOn || l.commitBase > 0 {
		return false
	}
	if l.rbSkip > 0 {
		l.rbSkip--
		return false
	}
	if pg1 != nil {
		if be32(pg1[40:]) != cookie {
			return false
		}
		h := make([]byte, 28)
		if !l.db.store.read(1, l.tx.snapshotEpoch, 44, h) {
			if _, err := l.db.realFile.ReadAt(h, 44); err != nil {
				return false
			}
		}
		if string(h) != string(pg1[44:72]) {
			return false
		}
	}
	return true
}

// MARK: - records

type recCols struct {
	ty  []uint64
	off []uint32
	ln  []uint32
	rec []byte
}

func parseRec(rec []byte) (recCols, bool) {
	r := recCols{rec: rec}
	hs, h := varint(rec)
	if h == 0 || hs > uint64(len(rec)) {
		return r, false
	}
	pos, hp := uint32(hs), uint32(h)
	for uint64(hp) < hs && len(r.ty) < 2048 {
		t, k := varint(rec[hp:hs])
		if k == 0 {
			return r, false
		}
		hp += uint32(k)
		l := typeLen(t)
		r.ty = append(r.ty, t)
		r.off = append(r.off, pos)
		r.ln = append(r.ln, l)
		pos += l
	}
	return r, int(pos) <= len(rec)
}

func (r *recCols) n() int { return len(r.ty) }

func bindCol(s *cstmt, idx int, r *recCols, col int) int32 {
	t, p, l := r.ty[col], r.rec[r.off[col]:], r.ln[col]
	switch {
	case t == 0:
		return s.bindNull(idx)
	case t == 8 || t == 9:
		return s.bindInt64(idx, int64(t-8))
	case t >= 1 && t <= 6:
		x := int64(int8(p[0]))
		for b := uint32(1); b < l; b++ {
			x = x<<8 | int64(p[b])
		}
		return s.bindInt64(idx, x)
	case t == 7:
		var u uint64
		for b := 0; b < 8; b++ {
			u = u<<8 | uint64(p[b])
		}
		return s.bindDouble(idx, *(*float64)(unsafe.Pointer(&u)))
	case t >= 12:
		return s.bindBytes(idx, p[:l], t&1 == 1)
	}
	return rcError
}

func putVarint(v uint64) []byte {
	var o [9]byte
	if v>>56 != 0 {
		o[8] = byte(v)
		v >>= 8
		for i := 7; i >= 0; i-- {
			o[i] = byte(v&0x7f) | 0x80
			v >>= 7
		}
		return o[:]
	}
	n := 1
	for t := v >> 7; t != 0; t >>= 7 {
		n++
	}
	for i := n - 1; i >= 0; i-- {
		o[i] = byte(v & 0x7f)
		if i != n-1 {
			o[i] |= 0x80
		}
		v >>= 7
	}
	return o[:n]
}

// valSerial: the serial type of a default and its content.
func valSerial(v *sqlVal) (uint64, []byte) {
	switch v.typ {
	case colInteger:
		x := v.i
		if x == 0 {
			return 8, nil
		}
		if x == 1 {
			return 9, nil
		}
		var nb uint64
		switch {
		case x >= -128 && x <= 127:
			nb = 1
		case x >= -32768 && x <= 32767:
			nb = 2
		case x >= -8388608 && x <= 8388607:
			nb = 3
		case x >= -2147483648 && x <= 2147483647:
			nb = 4
		case x >= -(1<<47) && x < 1<<47:
			nb = 5
		default:
			nb = 6
		}
		l := typeLen(nb)
		d := make([]byte, l)
		u := uint64(x)
		for i := int(l) - 1; i >= 0; i-- {
			d[i] = byte(u)
			u >>= 8
		}
		return nb, d
	case colFloat:
		u := *(*uint64)(unsafe.Pointer(&v.d))
		d := make([]byte, 8)
		for i := 7; i >= 0; i-- {
			d[i] = byte(u)
			u >>= 8
		}
		return 7, d
	case colText:
		return 13 + 2*uint64(len(v.b)), v.b
	case colBlob:
		return 12 + 2*uint64(len(v.b)), v.b
	}
	return 0, nil
}

// recPad completes a row written before an ALTER TABLE ADD COLUMN with the defaults of the columns it lacks, in
// SQLite's own encoding.
func recPad(t *tab, r *recCols) []byte {
	var hdr, data []byte
	for i := 0; i < t.nrec; i++ {
		if i < r.n() {
			hdr = append(hdr, putVarint(r.ty[i])...)
			data = append(data, r.rec[r.off[i]:r.off[i]+r.ln[i]]...)
		} else {
			ty, d := valSerial(&t.dflt[i])
			hdr = append(hdr, putVarint(ty)...)
			data = append(data, d...)
		}
	}
	hl := 1
	for len(putVarint(uint64(len(hdr)+hl))) != hl {
		hl = len(putVarint(uint64(len(hdr) + hl)))
	}
	out := append(putVarint(uint64(len(hdr)+hl)), hdr...)
	return append(out, data...)
}

// MARK: - the statements of a table on the helper

type updCache struct {
	mask uint64
	st   *cstmt
}

type tstmt struct {
	sel, ins, del *cstmt
	upd           []updCache
	ready         bool
}

func (t *tab) rowidName() string {
	for _, c := range []string{"rowid", "_rowid_", "oid"} {
		used := false
		for i := 0; i < t.nrec; i++ {
			if !t.recGen[i] && strings.EqualFold(t.recName[i], c) {
				used = true
			}
		}
		if !used {
			return c
		}
	}
	return ""
}

func itoa(i int) string { return strconv.Itoa(i) }

// wrPKConds: the condition that picks a WITHOUT ROWID row by its key (the parameter of a column is ?(2 + its place
// among the writable columns)).
func (t *tab) wrPKConds() string {
	var c []string
	k := 1
	for i := 0; i < t.nrec; i++ {
		if t.recGen[i] || i == t.aliasRec {
			continue
		}
		k++
		if i < t.npkRec {
			c = append(c, quoteIdent(t.recName[i])+"=?"+itoa(k))
		}
	}
	return strings.Join(c, " AND ")
}

func (ts *tstmt) prepare(h *cconn, t *tab) bool {
	rid := ""
	if !t.withoutRowid {
		if rid = t.rowidName(); rid == "" {
			return false
		}
	}
	var cols, marks []string
	conds := ""
	k := 1
	for i := 0; i < t.nrec; i++ {
		if t.recGen[i] || i == t.aliasRec {
			continue
		}
		k++
		q, pk := quoteIdent(t.recName[i]), "?"+itoa(k)
		cols = append(cols, q)
		marks = append(marks, pk)
		switch {
		case t.withoutRowid && i < t.npkRec:
			conds += " AND " + q + "=" + pk
		case t.noaff[i]: // (no affinity: 1 and 1.0 are IS-equal, and are different values)
			conds += " AND " + q + " COLLATE BINARY IS " + pk + " AND typeof(" + q + ") = typeof(" + pk + ")"
		default:
			conds += " AND " + q + " COLLATE BINARY IS " + pk
		}
	}
	tn := quoteIdent(t.name)
	var qs [3]string
	if t.withoutRowid {
		qs[0] = "SELECT 1 FROM " + tn + " WHERE 1" + conds
		qs[1] = "INSERT INTO " + tn + "(" + strings.Join(cols, ",") + ") VALUES(" + strings.Join(marks, ",") + ")"
		qs[2] = "DELETE FROM " + tn + " WHERE " + t.wrPKConds()
	} else {
		ins := "INSERT INTO " + tn + "(" + rid
		vals := "VALUES(?1"
		for i := range cols {
			ins += "," + cols[i]
			vals += "," + marks[i]
		}
		qs[0] = "SELECT 1 FROM " + tn + " WHERE " + rid + "=?1" + conds
		qs[1] = ins + ") " + vals + ")"
		qs[2] = "DELETE FROM " + tn + " WHERE " + rid + "=?1"
	}
	var rc int32
	if ts.sel, rc = h.prepare(qs[0]); rc != rcOK {
		return false
	}
	if ts.ins, rc = h.prepare(qs[1]); rc != rcOK {
		return false
	}
	if ts.del, rc = h.prepare(qs[2]); rc != rcOK {
		return false
	}
	ts.ready = true
	return true
}

func (ts *tstmt) finalize() {
	ts.sel.finalize()
	ts.ins.finalize()
	ts.del.finalize()
	for _, u := range ts.upd {
		u.st.finalize()
	}
	*ts = tstmt{}
}

const (
	bindAll = iota
	bindMask
	bindKey
)

func bindRow(s *cstmt, t *tab, rowid int64, r *recCols, mode int, only uint64) bool {
	s.reset()
	s.clear()
	s.bindInt64(1, rowid)
	k, bit := 1, 0
	for i := 0; i < t.nrec; i++ {
		if t.recGen[i] || i == t.aliasRec {
			continue
		}
		key := t.withoutRowid && i < t.npkRec
		take := mode == bindAll || (mode == bindMask && ((bit < 64 && only>>bit&1 == 1) || key)) || (mode == bindKey && key)
		bit++
		if !take {
			continue
		}
		k++
		if r != nil && bindCol(s, k, r, i) != rcOK {
			return false
		}
	}
	return true
}

// changedMask: the writable columns an update changes. An UPDATE that names a column that has not changed (an indexed
// one especially) writes the pages of its index: the transactions that only read them would be refused for it.
func changedMask(t *tab, o, n *recCols) (uint64, bool) {
	var mask uint64
	bit := 0
	for i := 0; i < t.nrec; i++ {
		if t.recGen[i] || i == t.aliasRec {
			continue
		}
		if bit >= 64 {
			return 0, false
		}
		if t.withoutRowid && i < t.npkRec {
			bit++
			continue
		}
		if o.ty[i] != n.ty[i] || o.ln[i] != n.ln[i] || string(o.rec[o.off[i]:o.off[i]+o.ln[i]]) != string(n.rec[n.off[i]:n.off[i]+n.ln[i]]) {
			mask |= 1 << bit
		}
		bit++
	}
	return mask, true
}

func (ts *tstmt) update(h *cconn, t *tab, mask uint64) *cstmt {
	for _, u := range ts.upd {
		if u.mask == mask {
			return u.st
		}
	}
	rid := t.rowidName()
	if rid == "" {
		rid = "rowid"
	}
	var sets []string
	bit, k := 0, 1
	if t.withoutRowid {
		k += t.npkRec // (a WITHOUT ROWID row is found by its key: ?2.. are the key columns, the new values follow)
	}
	for i := 0; i < t.nrec; i++ {
		if t.recGen[i] || i == t.aliasRec {
			continue
		}
		if mask>>bit&1 == 1 {
			k++
			sets = append(sets, quoteIdent(t.recName[i])+"=?"+itoa(k))
		}
		bit++
	}
	q := "UPDATE " + quoteIdent(t.name) + " SET " + strings.Join(sets, ",") + " WHERE "
	if t.withoutRowid {
		q += t.wrPKConds()
	} else {
		q += rid + "=?1"
	}
	st, rc := h.prepare(q)
	if rc != rcOK {
		return nil
	}
	if len(ts.upd) >= 8 {
		ts.upd[0].st.finalize()
		ts.upd = ts.upd[1:]
	}
	ts.upd = append(ts.upd, updCache{mask, st})
	return st
}

// MARK: - the group replay

type rbReq struct {
	lane   *lane
	chg    []rowChange
	cookie uint32
	fk     bool
	state  int // 0 waiting, 1 committed, 2 refused (provisional until done)
	done   bool
	epoch  uint64
}

type rbState struct {
	cat    *catalog
	ts     []tstmt
	sync   int
	helper *cconn
	hl     *lane
}

func (l *lane) rebaseFree() {
	if r := l.rb; r != nil {
		for i := range r.ts {
			r.ts[i].finalize()
		}
		if r.helper != nil {
			r.helper.close()
		}
		l.rb = nil
	}
}

func (l *lane) helperOpen() bool {
	R := l.rb
	if R.helper != nil {
		return true
	}
	u := "file:" + (&url.URL{Path: l.db.path}).EscapedPath() + "?mw=1&mw_norebase=1"
	if l.db.mp != nil {
		u += "&mw_mp=1"
	}
	h, rc := copen(u, sqlite3.SQLITE_OPEN_READWRITE|sqlite3.SQLITE_OPEN_URI|sqlite3.SQLITE_OPEN_NOMUTEX|sqlite3.SQLITE_OPEN_PRIVATECACHE, VFSName)
	if rc != rcOK {
		return false
	}
	sqlite3.Xsqlite3_busy_timeout(h.tls, h.db, 0)
	h.dbConfig(sqlite3.SQLITE_DBCONFIG_ENABLE_TRIGGER, 0) // (the effects of a trigger are rows of the write set)
	h.dbConfig(sqlite3.SQLITE_DBCONFIG_ENABLE_FKEY, 0)
	R.helper = h
	R.hl = laneOfDB(h.tls, h.db)
	return true
}

// applyChg applies one row change in the open transaction of the helper: rcOK replayed, errConflict a true
// conflict, rcBusy lost a race.
func applyChg(h *cconn, lt *tab, ts *tstmt, c *rowChange) int {
	if !ts.ready && !ts.prepare(h, lt) {
		return errConflict
	}
	var oldr, newr recCols
	var ok bool
	if c.oldRec != nil {
		if oldr, ok = parseRec(c.oldRec); !ok || oldr.n() > lt.nrec {
			return errConflict
		}
		if oldr.n() < lt.nrec { // a row written before an ALTER TABLE ADD COLUMN
			if oldr, ok = parseRec(recPad(lt, &oldr)); !ok {
				return errConflict
			}
		}
	}
	if c.newRec != nil {
		if newr, ok = parseRec(c.newRec); !ok || newr.n() > lt.nrec {
			return errConflict
		}
		if newr.n() < lt.nrec {
			if newr, ok = parseRec(recPad(lt, &newr)); !ok {
				return errConflict
			}
		}
	}
	if c.oldRec != nil { // it is the row that the transaction saw
		if !bindRow(ts.sel, lt, c.rowid, &oldr, bindAll, 0) {
			return errConflict
		}
		src := ts.sel.step()
		ts.sel.reset()
		if src&0xff == rcBusy {
			return rcBusy
		}
		if src != rcRow {
			return errConflict
		}
	}
	var src int32
	switch c.kind {
	case chgDelete:
		var r *recCols
		if lt.withoutRowid {
			r = &oldr
		}
		if !bindRow(ts.del, lt, c.rowid, r, bindKey, 0) {
			return errConflict
		}
		src = ts.del.step()
		ts.del.reset()
	case chgUpdate:
		mask, ok := changedMask(lt, &oldr, &newr)
		if !ok {
			return errConflict
		}
		if mask == 0 {
			return rcOK // (only a derived column changed)
		}
		if lt.isParent && mask&lt.refmask != 0 {
			return errConflict // (a key that a foreign key refers to changes: the ON UPDATE actions are not replayed)
		}
		us := ts.update(h, lt, mask)
		if us == nil || !bindRow(us, lt, c.rowid, &newr, bindMask, mask) {
			return errConflict
		}
		src = us.step()
		us.reset()
	default:
		if !bindRow(ts.ins, lt, c.rowid, &newr, bindAll, 0) {
			return errConflict
		}
		src = ts.ins.step()
		ts.ins.reset()
	}
	switch {
	case src == rcDone:
		return rcOK
	case src&0xff == rcBusy:
		return rcBusy
	}
	return errConflict
}

// replayReq replays one request: deletes, then updates, then inserts (a key that one frees and another takes). With
// foreign keys enforced: updates, deletes from the children to the parents, inserts from the parents to the children.
func replayReq(h *cconn, R *rbState, rq *rbReq) int {
	cat := R.cat
	if rq.fk {
		fl := make([]byte, len(cat.tabs))
		for i := range rq.chg {
			c := &rq.chg[i]
			lt := cat.byName(c.tab.name)
			if lt == nil {
				return errConflict
			}
			if lt.isParent {
				x := cat.index(lt)
				switch c.kind {
				case chgDelete:
					fl[x] |= 1
				case chgInsert:
					fl[x] |= 2
				}
				if fl[x] == 3 {
					return errConflict
				}
			}
		}
	}
	plain := [3]chgKind{chgDelete, chgUpdate, chgInsert}
	withFK := [3]chgKind{chgUpdate, chgDelete, chgInsert}
	for ph := 0; ph < 3; ph++ {
		phase := plain[ph]
		if rq.fk {
			phase = withFK[ph]
		}
		steps := 0
		if rq.fk && phase != chgUpdate {
			steps = cat.maxrank
		}
		for step := 0; step <= steps; step++ {
			rank := step
			if phase == chgDelete {
				rank = cat.maxrank - step
			}
			for i := range rq.chg {
				c := &rq.chg[i]
				if c.kind != phase {
					continue
				}
				lt := cat.byName(c.tab.name) // (the leader's own catalog: the same schema, the same cookie)
				if lt == nil || !lt.ok {
					return errConflict
				}
				if steps > 0 && lt.rank != rank {
					continue
				}
				if r := applyChg(h, lt, &R.ts[cat.index(lt)], c); r != rcOK {
					return r
				}
			}
		}
	}
	return rcOK
}

// groupOnce replays all the requests of a batch in one transaction at the snapshot the helper has now. rcOK: the
// transaction committed (or no request could be replayed: every state is final); rcBusy: lost the race, again.
func (l *lane) groupOnce(b []*rbReq) int {
	R := l.rb
	h := R.helper
	for _, rq := range b {
		rq.state = 0
	}
	fk := int32(0)
	if b[0].fk {
		fk = 1
	}
	h.dbConfig(sqlite3.SQLITE_DBCONFIG_ENABLE_FKEY, fk)
	if h.exec("BEGIN") != rcOK {
		return rcBusy
	}
	var cookie uint32
	got := false
	h.query("PRAGMA schema_version", func(s *cstmt) { cookie, got = uint32(s.colInt64(0)), true })
	if !got {
		h.exec("ROLLBACK")
		for _, rq := range b {
			rq.state = 2
		}
		return rcOK
	}
	ok, rc := 0, rcOK
	for _, rq := range b {
		if rq.cookie != cookie || rq.cookie != R.cat.cookie || rq.fk != b[0].fk {
			rq.state = 2
			continue
		}
		h.exec("SAVEPOINT g")
		r := replayReq(h, R, rq)
		if r == rcOK {
			h.exec("RELEASE g")
			rq.state = 1
			ok++
		} else if r == rcBusy {
			rc = rcBusy
			break
		} else { // a true conflict: this request alone is taken back
			h.exec("ROLLBACK TO g")
			h.exec("RELEASE g")
			rq.state = 2
		}
	}
	if rc == rcBusy || ok == 0 {
		h.exec("ROLLBACK")
		return rc
	}
	if crc := h.exec("COMMIT"); crc != rcOK {
		h.exec("ROLLBACK")
		if crc&0xff == rcBusy {
			return rcBusy
		}
		for _, rq := range b {
			if rq.state == 1 {
				rq.state = 2
			}
		}
		return rcOK
	}
	epoch := uint64(0)
	if R.hl != nil {
		epoch = R.hl.tx.commitEpoch
	}
	for _, rq := range b {
		if rq.state == 1 {
			if epoch != 0 {
				rq.epoch = epoch
			} else {
				rq.state = 2
			}
		}
	}
	return rcOK
}

func (l *lane) runBatch(b []*rbReq) {
	db := l.db
	R := l.rb
	if !l.helperOpen() {
		for _, rq := range b {
			rq.state = 2
		}
		return
	}
	// the helper commits the whole batch at the strongest level any of its connections asked for
	lvl := l.syncLevel
	for _, rq := range b {
		lvl = max(lvl, rq.lane.syncLevel)
	}
	if R.sync != lvl {
		R.helper.exec("PRAGMA synchronous=" + itoa(lvl))
		R.sync = lvl
	}
	attempt, closed := 0, false
	for ; attempt < rebaseMaxAttempts; attempt++ {
		if attempt == rebaseGateAfter && R.hl != nil {
			db.gateClose(R.hl) // the next attempt runs against a frozen state
			closed = true
		}
		if l.groupOnce(b) != rcBusy {
			break
		}
		db.nRebaseRetries.Add(1)
		if !closed {
			runtime.Gosched()
		}
	}
	if attempt == rebaseMaxAttempts {
		for _, rq := range b {
			rq.state = 2
		}
	}
	if closed {
		db.gateOpen()
	}
	done := int64(0)
	for _, rq := range b {
		if rq.state == 1 {
			done++
		}
	}
	db.nRebases.Add(done)
	if done > 1 {
		db.nRebaseGrouped.Add(done)
	}
}

// rebase: the commit of the lane conflicted on pages that it wrote: replay its row changes. rcOK: committed at the
// returned epoch (the lane's own pages are not published); errConflict: refused.
func (l *lane) rebase(tls *libc.TLS, imgs [][]byte, cookie uint32) (uint64, int) {
	db := l.db
	if l.rb == nil {
		l.rb = &rbState{sync: -1}
	}
	R := l.rb
	if R.cat == nil || R.cat.cookie != cookie {
		for i := range R.ts {
			R.ts[i].finalize()
		}
		R.cat = l.buildCatalog()
		R.ts = make([]tstmt, len(R.cat.tabs))
	}
	if !R.cat.rebasable || R.cat.cookie != cookie {
		debugf("rebase: catalog: %s", R.cat.why)
		db.nUnrebasable.Add(1)
		return 0, errConflict
	}
	chg, why := l.rowdiff(imgs, R.cat)
	if why != "" || len(chg) == 0 {
		debugf("rebase: rowdiff: %q, %d changes", why, len(chg))
		db.nUnrebasable.Add(1)
		return 0, errConflict
	}
	if l.readsUnchanged(tls, len(chg)) { // it read rows that it did not change: write skew, the application retries
		debugf("rebase: reads: dep=%v cur=%x changes=%d", l.rdDep, l.rdCur, len(chg))
		db.nUnrebasable.Add(1)
		return 0, errConflict
	}
	fk := false
	if R.cat.hasFK && l.rdDB != 0 {
		fk = dbConfigGet(tls, l.rdDB, sqlite3.SQLITE_DBCONFIG_ENABLE_FKEY) != 0
	}
	rq := &rbReq{lane: l, chg: chg, cookie: cookie, fk: fk}
	db.rbMu.Lock()
	db.rbQueue = append(db.rbQueue, rq)
	for !rq.done {
		if !db.rbLeader {
			db.rbLeader = true
			n := min(len(db.rbQueue), rebaseBatch)
			batch := append([]*rbReq(nil), db.rbQueue[:n]...)
			db.rbQueue = db.rbQueue[n:]
			db.rbMu.Unlock()
			l.runBatch(batch)
			db.rbMu.Lock()
			for _, x := range batch {
				x.done = true
			}
			db.rbLeader = false
			db.rbCond.Broadcast()
		} else {
			db.rbCond.Wait()
		}
	}
	db.rbMu.Unlock()
	if rq.state == 1 {
		return rq.epoch, rcOK
	}
	debugf("rebase: the replay was refused (a true conflict)")
	return 0, errConflict
}

func dbConfigGet(tls *libc.TLS, db uintptr, op int32) int32 {
	bp := libc.Xcalloc(tls, 1, 24)
	defer libc.Xfree(tls, bp)
	out := bp + 16
	sqlite3.Xsqlite3_db_config(tls, db, op, libc.VaList(bp, int32(-1), out))
	return *(*int32)(unsafe.Pointer(out))
}

// MARK: - the publication gate
//
// A replay of the rebase runs at the latest snapshot and can lose the race against ordinary commits over and over.
// After a loss it closes the gate: publishers already inside finish, new ones wait at the entrance, and the next
// attempt runs against a frozen state.

func (db *mwDB) gateEnter(l *lane) {
	for spins := 0; ; spins++ {
		if db.gateClosing.Load() == 0 || (l != nil && db.gateOwner.Load() == l) {
			db.gateActive.Add(1)
			if db.gateClosing.Load() == 0 || (l != nil && db.gateOwner.Load() == l) {
				return
			}
			db.gateActive.Add(-1)
		}
		runtime.Gosched()
	}
}

func (db *mwDB) gateExit() { db.gateActive.Add(-1) }

func (db *mwDB) gateClose(owner *lane) {
	db.gateOwner.Store(owner)
	db.gateClosing.Store(1)
	for db.gateActive.Load() > 0 {
		runtime.Gosched()
	}
	// ... and let every epoch already assigned become visible, so the next snapshot is the final state
	for db.epoch.Load() != db.nextEpoch.Load() && !db.failed.Load() {
		runtime.Gosched()
	}
}

func (db *mwDB) gateOpen() {
	db.gateClosing.Store(0)
	db.gateOwner.Store(nil)
}

// MARK: - connection -> lane

var dbLanes sync.Map // sqlite3* -> *lane

const fcntlLaneHandle = 0x4d570003

// laneOfDB returns the lane of the main database of a connection (nil: not opened with the engine).
func laneOfDB(tls *libc.TLS, db uintptr) *lane {
	if v, ok := dbLanes.Load(db); ok {
		return v.(*lane)
	}
	p := libc.Xcalloc(tls, 1, 8)
	defer libc.Xfree(tls, p)
	if sqlite3.Xsqlite3_file_control(tls, db, zMainName(), fcntlLaneHandle, p) != rcOK {
		return nil
	}
	f, ok := handles.Load(*(*uintptr)(unsafe.Pointer(p)))
	if !ok {
		return nil
	}
	l := f.(*mwFile).lane
	l.cdb.Store(db)
	dbLanes.Store(db, l)
	return l
}

var (
	zMainOnce sync.Once
	zMainPtr  uintptr
)

func zMainName() uintptr {
	zMainOnce.Do(func() { zMainPtr, _ = libc.CString("main") })
	return zMainPtr
}
