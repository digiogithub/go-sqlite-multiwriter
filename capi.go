package multiwriter

// A thin layer over the C API of SQLite (as modernc.org/sqlite translates it) for the connections the engine opens
// itself: the scratch database of the catalog and the helper connection of the rebase.

import (
	"unsafe"

	"modernc.org/libc"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	rcRow  = 100
	rcDone = 101

	colInteger = 1
	colFloat   = 2
	colText    = 3
	colBlob    = 4
	colNull    = 5
)

type cconn struct {
	tls *libc.TLS
	db  uintptr
}

// copen opens a connection with its own TLS. vfs "" is the default VFS.
func copen(name string, flags int32, vfs string) (*cconn, int32) {
	c := &cconn{tls: libc.NewTLS()}
	zn, _ := libc.CString(name)
	defer libc.Xfree(c.tls, zn)
	var zv uintptr
	if vfs != "" {
		zv, _ = libc.CString(vfs)
		defer libc.Xfree(c.tls, zv)
	}
	pp := libc.Xcalloc(c.tls, 1, 8)
	defer libc.Xfree(c.tls, pp)
	rc := sqlite3.Xsqlite3_open_v2(c.tls, zn, pp, flags, zv)
	c.db = *(*uintptr)(unsafe.Pointer(pp))
	if rc != rcOK {
		c.close()
		return nil, rc
	}
	return c, rcOK
}

func (c *cconn) close() {
	if c.db != 0 {
		sqlite3.Xsqlite3_close_v2(c.tls, c.db)
		c.db = 0
	}
	if c.tls != nil {
		c.tls.Close()
		c.tls = nil
	}
}

func (c *cconn) exec(sql string) int32 { return execC(c.tls, c.db, sql) }

func (c *cconn) dbConfig(op int32, v int32) int32 {
	bp := libc.Xmalloc(c.tls, 16)
	defer libc.Xfree(c.tls, bp)
	return sqlite3.Xsqlite3_db_config(c.tls, c.db, op, libc.VaList(bp, v, uintptr(0)))
}

func (c *cconn) prepare(sql string) (*cstmt, int32) { return prepareOn(c.tls, c.db, sql) }

type cstmt struct {
	tls *libc.TLS
	p   uintptr
}

func prepareOn(tls *libc.TLS, db uintptr, sql string) (*cstmt, int32) {
	zs, _ := libc.CString(sql)
	defer libc.Xfree(tls, zs)
	pp := libc.Xcalloc(tls, 1, 8)
	defer libc.Xfree(tls, pp)
	rc := sqlite3.Xsqlite3_prepare_v2(tls, db, zs, -1, pp, 0)
	p := *(*uintptr)(unsafe.Pointer(pp))
	if rc != rcOK || p == 0 {
		if p != 0 {
			sqlite3.Xsqlite3_finalize(tls, p)
		}
		if rc == rcOK {
			rc = rcError
		}
		return nil, rc
	}
	return &cstmt{tls, p}, rcOK
}

func (s *cstmt) step() int32 { return sqlite3.Xsqlite3_step(s.tls, s.p) }
func (s *cstmt) reset()      { sqlite3.Xsqlite3_reset(s.tls, s.p) }
func (s *cstmt) clear()      { sqlite3.Xsqlite3_clear_bindings(s.tls, s.p) }
func (s *cstmt) finalize() {
	if s != nil && s.p != 0 {
		sqlite3.Xsqlite3_finalize(s.tls, s.p)
		s.p = 0
	}
}

func (s *cstmt) bindInt64(i int, v int64) int32 {
	return sqlite3.Xsqlite3_bind_int64(s.tls, s.p, int32(i), v)
}
func (s *cstmt) bindDouble(i int, v float64) int32 {
	return sqlite3.Xsqlite3_bind_double(s.tls, s.p, int32(i), v)
}
func (s *cstmt) bindNull(i int) int32 { return sqlite3.Xsqlite3_bind_null(s.tls, s.p, int32(i)) }

// bindBytes binds text or a blob (copied: SQLITE_TRANSIENT).
func (s *cstmt) bindBytes(i int, b []byte, text bool) int32 {
	n := len(b)
	p := libc.Xmalloc(s.tls, libc.Tsize_t(n+1))
	if p == 0 {
		return rcNoMem
	}
	defer libc.Xfree(s.tls, p)
	copy(bytesAt(p, n), b)
	if text {
		return sqlite3.Xsqlite3_bind_text(s.tls, s.p, int32(i), p, int32(n), sqlite3.SQLITE_TRANSIENT)
	}
	return sqlite3.Xsqlite3_bind_blob(s.tls, s.p, int32(i), p, int32(n), sqlite3.SQLITE_TRANSIENT)
}

func (s *cstmt) colCount() int        { return int(sqlite3.Xsqlite3_column_count(s.tls, s.p)) }
func (s *cstmt) colType(i int) int    { return int(sqlite3.Xsqlite3_column_type(s.tls, s.p, int32(i))) }
func (s *cstmt) colInt64(i int) int64 { return sqlite3.Xsqlite3_column_int64(s.tls, s.p, int32(i)) }
func (s *cstmt) colDouble(i int) float64 {
	return sqlite3.Xsqlite3_column_double(s.tls, s.p, int32(i))
}
func (s *cstmt) colName(i int) string {
	return libc.GoString(sqlite3.Xsqlite3_column_name(s.tls, s.p, int32(i)))
}

// colText returns the text of a column ("", false for NULL).
func (s *cstmt) colText(i int) (string, bool) {
	z := sqlite3.Xsqlite3_column_text(s.tls, s.p, int32(i))
	if z == 0 {
		return "", false
	}
	n := int(sqlite3.Xsqlite3_column_bytes(s.tls, s.p, int32(i)))
	return string(bytesAt(z, n)), true
}

func (s *cstmt) colBlob(i int) []byte {
	z := sqlite3.Xsqlite3_column_blob(s.tls, s.p, int32(i))
	n := int(sqlite3.Xsqlite3_column_bytes(s.tls, s.p, int32(i)))
	out := make([]byte, n)
	if z != 0 && n > 0 {
		copy(out, bytesAt(z, n))
	}
	return out
}

// query runs sql and calls fn for every row.
func (c *cconn) query(sql string, fn func(*cstmt)) int32 {
	s, rc := c.prepare(sql)
	if rc != rcOK {
		return rc
	}
	defer s.finalize()
	for {
		rc = s.step()
		if rc != rcRow {
			break
		}
		fn(s)
	}
	if rc == rcDone {
		return rcOK
	}
	return rc
}

// quoteIdent quotes an SQL identifier ("%w" of sqlite3_mprintf).
func quoteIdent(s string) string {
	b := make([]byte, 0, len(s)+2)
	b = append(b, '"')
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			b = append(b, '"')
		}
		b = append(b, s[i])
	}
	return string(append(b, '"'))
}
