package multiwriter

// The VFS, registered in modernc.org/sqlite (SQLite compiled to Go: no cgo). It wraps the platform VFS of SQLite:
// the main file of a database opened with the engine (URI "vfs=multiwriter" or "mw=1") becomes a lane, and its
// "-wal" lives in memory; every other file (temporary files, a database opened without the engine, ATTACH, VACUUM
// INTO) is opened by the platform VFS as it is.
//
// SQLite calls these methods through C function pointers; in modernc a function pointer is a Go func value, so the
// methods are plain Go functions with the transpiled signatures.

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"modernc.org/libc"
	sqlite3 "modernc.org/sqlite/lib"
)

// VFSName is the name the VFS is registered under: open a database with "file:app.db?vfs=multiwriter".
const VFSName = "multiwriter"

// The layout of a file object of this VFS: [pMethods][handle][the platform file...]. A file this VFS does not wrap
// is the platform file itself, at the start of the buffer.
const fileHdr = 16

type mwFile struct {
	lane  *lane
	isWAL bool
	real  uintptr   // the platform file (main database only)
	shm   []uintptr // the private wal-index regions (C memory)
}

var (
	rootVFS  *sqlite3.Tsqlite3_vfs
	mwVFS    uintptr
	laneIO   sqlite3.Tsqlite3_io_methods
	walIO    sqlite3.Tsqlite3_io_methods
	regOnce  sync.Once
	regErr   error
	handles  sync.Map // handle -> *mwFile
	nextH    atomic.Uintptr
	tlsMu    sync.Mutex
	tlsLanes = map[*libc.TLS]*lane{} // the lane whose main file this connection touched last (pairs a new WAL with its lane)
)

func init() { Register() }

// Register registers the VFS (idempotent; it is done when the package is imported).
func Register() error {
	regOnce.Do(func() { regErr = register() })
	return regErr
}

func setFn[T any](dst *uintptr, f T) { *(*T)(unsafe.Pointer(dst)) = f }
func fnOf[T any](p uintptr) T        { return *(*T)(unsafe.Pointer(&struct{ uintptr }{p})) }

func bytesAt(p uintptr, n int) []byte { return unsafe.Slice((*byte)(unsafe.Pointer(p)), n) }

func register() error {
	tls := libc.NewTLS()
	defer tls.Close()
	if rc := sqlite3.Xsqlite3_initialize(tls); rc != rcOK {
		return &Error{Code: int(rc)}
	}
	root := sqlite3.Xsqlite3_vfs_find(tls, 0)
	if root == 0 {
		return &Error{Code: rcError}
	}
	rootVFS = (*sqlite3.Tsqlite3_vfs)(unsafe.Pointer(root))

	laneIO.FiVersion = 3
	setFn(&laneIO.FxClose, lmClose)
	setFn(&laneIO.FxRead, lmRead)
	setFn(&laneIO.FxWrite, lmWrite)
	setFn(&laneIO.FxTruncate, lmTruncate)
	setFn(&laneIO.FxSync, lmSync)
	setFn(&laneIO.FxFileSize, lmFileSize)
	setFn(&laneIO.FxLock, lmLock)
	setFn(&laneIO.FxUnlock, lmUnlock)
	setFn(&laneIO.FxCheckReservedLock, lmCheckReserved)
	setFn(&laneIO.FxFileControl, lmFileControl)
	setFn(&laneIO.FxSectorSize, lmSectorSize)
	setFn(&laneIO.FxDeviceCharacteristics, lmDeviceChar)
	setFn(&laneIO.FxShmMap, lmShmMap)
	setFn(&laneIO.FxShmLock, lmShmLock)
	setFn(&laneIO.FxShmBarrier, lmShmBarrier)
	setFn(&laneIO.FxShmUnmap, lmShmUnmap)
	setFn(&laneIO.FxFetch, lmFetch)
	setFn(&laneIO.FxUnfetch, lmUnfetch)

	walIO.FiVersion = 1
	setFn(&walIO.FxClose, walClose)
	setFn(&walIO.FxRead, walReadM)
	setFn(&walIO.FxWrite, walWriteM)
	setFn(&walIO.FxTruncate, walTruncateM)
	setFn(&walIO.FxSync, walSync)
	setFn(&walIO.FxFileSize, walFileSize)
	setFn(&walIO.FxLock, walNoLock)
	setFn(&walIO.FxUnlock, walNoLock)
	setFn(&walIO.FxCheckReservedLock, walCheckReserved)
	setFn(&walIO.FxFileControl, walFileControl)
	setFn(&walIO.FxSectorSize, walSectorSize)
	setFn(&walIO.FxDeviceCharacteristics, walDeviceChar)

	p := libc.Xcalloc(tls, 1, libc.Tsize_t(unsafe.Sizeof(sqlite3.Tsqlite3_vfs{})))
	if p == 0 {
		return &Error{Code: rcNoMem}
	}
	v := (*sqlite3.Tsqlite3_vfs)(unsafe.Pointer(p))
	*v = *rootVFS
	name, err := libc.CString(VFSName)
	if err != nil {
		return err
	}
	v.FzName = name
	v.FpNext = 0
	v.FpAppData = 0
	v.FszOsFile = rootVFS.FszOsFile + fileHdr
	setFn(&v.FxOpen, vfsOpen)
	setFn(&v.FxDelete, vfsDelete)
	if rc := sqlite3.Xsqlite3_vfs_register(tls, p, 0); rc != rcOK {
		return &Error{Code: int(rc)}
	}
	mwVFS = p
	return nil
}

// MARK: - helpers around the platform VFS

func rootOpen(tls *libc.TLS, zName, pFile uintptr, flags int32, pOut uintptr) int32 {
	return fnOf[func(*libc.TLS, uintptr, uintptr, uintptr, int32, uintptr) int32](rootVFS.FxOpen)(tls, uintptr(unsafe.Pointer(rootVFS)), zName, pFile, flags, pOut)
}

func methodsOf(pf uintptr) *sqlite3.Tsqlite3_io_methods {
	return (*sqlite3.Tsqlite3_io_methods)(unsafe.Pointer((*sqlite3.Tsqlite3_file)(unsafe.Pointer(pf)).FpMethods))
}

func realClose(tls *libc.TLS, pf uintptr) int32 {
	if (*sqlite3.Tsqlite3_file)(unsafe.Pointer(pf)).FpMethods == 0 {
		return rcOK
	}
	return fnOf[func(*libc.TLS, uintptr) int32](methodsOf(pf).FxClose)(tls, pf)
}

func realRead(tls *libc.TLS, pf, buf uintptr, n int32, off int64) int32 {
	return fnOf[func(*libc.TLS, uintptr, uintptr, int32, int64) int32](methodsOf(pf).FxRead)(tls, pf, buf, n, off)
}

func fileOf(p uintptr) *mwFile {
	h := *(*uintptr)(unsafe.Pointer(p + 8))
	f, _ := handles.Load(h)
	return f.(*mwFile)
}

func newHandle(p uintptr, f *mwFile, methods *sqlite3.Tsqlite3_io_methods) {
	h := nextH.Add(1)
	handles.Store(h, f)
	*(*uintptr)(unsafe.Pointer(p + 8)) = h
	(*sqlite3.Tsqlite3_file)(unsafe.Pointer(p)).FpMethods = uintptr(unsafe.Pointer(methods))
}

func dropHandle(p uintptr) {
	h := *(*uintptr)(unsafe.Pointer(p + 8))
	handles.Delete(h)
	(*sqlite3.Tsqlite3_file)(unsafe.Pointer(p)).FpMethods = 0
}

func noteLane(tls *libc.TLS, l *lane) {
	if l.walBound {
		return
	}
	tlsMu.Lock()
	tlsLanes[tls] = l
	tlsMu.Unlock()
}

func uriParam(tls *libc.TLS, zName uintptr, name string) (string, bool) {
	cn, _ := libc.CString(name)
	defer libc.Xfree(tls, cn)
	v := sqlite3.Xsqlite3_uri_parameter(tls, zName, cn)
	if v == 0 {
		return "", false
	}
	return libc.GoString(v), true
}

func uriBool(tls *libc.TLS, zName uintptr, name string, dflt bool) bool {
	v, ok := uriParam(tls, zName, name)
	if !ok {
		return dflt
	}
	switch strings.ToLower(v) {
	case "1", "true", "on", "yes":
		return true
	case "0", "false", "off", "no":
		return false
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return dflt
	}
	return n != 0
}

func uriInt(tls *libc.TLS, zName uintptr, name string, dflt int64) int64 {
	v, ok := uriParam(tls, zName, name)
	if !ok {
		return dflt
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return dflt
	}
	return n
}

// modeFor: 0 = the platform VFS as it is, 1 = the engine, -1 = a value of mw= that is not 0 or 1 (the open fails:
// a typo must not give stock SQLite).
func modeFor(tls *libc.TLS, zName uintptr, flags int32) int {
	if zName == 0 || flags&sqlite3.SQLITE_OPEN_MAIN_DB == 0 {
		return 0
	}
	v, ok := uriParam(tls, zName, "mw")
	if !ok {
		// vfs=multiwriter in the URI turns the engine on. Not every open through the VFS: the database that VACUUM
		// INTO or ATTACH opens has a plain name and stays as it is.
		if vfs, ok := uriParam(tls, zName, "vfs"); ok && vfs == VFSName {
			return 1
		}
		return 0
	}
	switch strings.ToLower(v) {
	case "0", "false", "off", "no":
		return 0
	case "1", "true", "on", "yes":
		return 1
	}
	return -1
}

// MARK: - the VFS

func vfsOpen(tls *libc.TLS, pVfs, zName, pFile uintptr, flags int32, pOut uintptr) int32 {
	// the WAL of a database opened with the engine lives in memory only
	if flags&sqlite3.SQLITE_OPEN_WAL != 0 && zName != 0 {
		name := libc.GoString(zName)
		if strings.HasSuffix(name, "-wal") && lookupDB(strings.TrimSuffix(name, "-wal")) != nil {
			tlsMu.Lock()
			l := tlsLanes[tls]
			tlsMu.Unlock()
			if l == nil || l.walBound || l.db.path+"-wal" != name {
				return rcCantOpen
			}
			l.walBound = true
			l.wal.size = 0
			newHandle(pFile, &mwFile{lane: l, isWAL: true}, &walIO)
			if pOut != 0 {
				*(*int32)(unsafe.Pointer(pOut)) = flags
			}
			return rcOK
		}
	}
	mode := modeFor(tls, zName, flags)
	if mode < 0 {
		return rcCantOpen
	}
	if mode == 0 {
		return rootOpen(tls, zName, pFile, flags, pOut)
	}
	path := libc.GoString(zName)
	if c, ok := uriParam(tls, zName, "cache"); ok && c == "shared" {
		return rcCantOpen // a shared cache shares one pager between connections: no private lanes
	}
	if flags&sqlite3.SQLITE_OPEN_READONLY != 0 && !isCleanWALDB(tls, path) {
		return rootOpen(tls, zName, pFile, flags, pOut) // a read-only connection cannot convert the file: it reads as stock does
	}
	if flags&sqlite3.SQLITE_OPEN_CREATE == 0 {
		if _, err := os.Stat(path); err != nil {
			return rcCantOpen
		}
	}
	if rc := ensureWALDB(tls, path); rc != rcOK {
		return rc
	}
	real := pFile + fileHdr
	if rc := rootOpen(tls, zName, real, flags, pOut); rc != rcOK {
		return rc
	}
	db := acquireDB(path)
	l := newLane(db)
	l.readcheck = uriBool(tls, zName, "mw_readcheck", true)
	l.noreloc = uriBool(tls, zName, "mw_noreloc", false)
	l.nomerge = uriBool(tls, zName, "mw_nomerge", false)
	l.noroute = uriBool(tls, zName, "mw_noroute", false)
	l.hotCredit = int(uriInt(tls, zName, "mw_hot_credit", defaultHotCredit))
	l.norebase = uriBool(tls, zName, "mw_norebase", false)
	l.rebaseOn = uriBool(tls, zName, "mw_rebase", false) && !l.norebase
	l.rbNoBackoff = !uriBool(tls, zName, "mw_rebase_backoff", true)
	if rc := openMain(tls, real, db, path); rc != rcOK {
		realClose(tls, real)
		releaseDB(db)
		return rc
	}
	if _, ok := uriParam(tls, zName, "mw_log_max_mb"); ok {
		db.logMaxBytes.Store(uriInt(tls, zName, "mw_log_max_mb", 32) << 20)
	}
	if _, ok := uriParam(tls, zName, "mw_gc"); ok {
		db.gcInterval.Store(uriInt(tls, zName, "mw_gc", 64))
	}
	if _, ok := uriParam(tls, zName, "mw_base_cache_mb"); ok {
		db.store.baseLimit = uriInt(tls, zName, "mw_base_cache_mb", 64) << 20
	}
	newHandle(pFile, &mwFile{lane: l, real: real}, &laneIO)
	noteLane(tls, l)
	registerHook(tls)
	return rcOK
}

// MARK: - the statement hook

const (
	traceStmt    = 0x01
	traceProfile = 0x02
)

var (
	hookOnce  sync.Once
	autoExtFn = connectionInit
	traceFn   = traceCallback
)

// registerHook installs, once, an auto-extension that puts the statement hook on every connection opened from then
// on (auto-extensions run right after xOpen inside sqlite3_open_v2, so the first engine open is in time for it).
func registerHook(tls *libc.TLS) {
	hookOnce.Do(func() {
		sqlite3.Xsqlite3_auto_extension(tls, *(*uintptr)(unsafe.Pointer(&autoExtFn)))
	})
}

// connectionInit puts the statement hook on a connection; on a mw_rebase=1 connection it also notes what the
// statements of a transaction read (ctx 1).
func connectionInit(tls *libc.TLS, db, pzErr, pApi uintptr) int32 {
	ctx := uintptr(0)
	if fn := sqlite3.Xsqlite3_db_filename(tls, db, zMainName()); fn != 0 {
		if uriBool(tls, fn, "mw_rebase", false) && !uriBool(tls, fn, "mw_norebase", false) {
			ctx = 1
		}
	}
	sqlite3.Xsqlite3_trace_v2(tls, db, traceStmt|traceProfile, *(*uintptr)(unsafe.Pointer(&traceFn)), ctx)
	return rcOK
}

// isDDL looks at the first keyword of the SQL text (after white space and comments) in C memory.
func isDDL(z uintptr) bool {
	if z == 0 {
		return false
	}
	at := func(i int) byte { return *(*byte)(unsafe.Pointer(z + uintptr(i))) }
	i := 0
	for {
		c := at(i)
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			i++
			continue
		case c == '-' && at(i+1) == '-':
			for at(i) != 0 && at(i) != '\n' {
				i++
			}
			continue
		case c == '/' && at(i+1) == '*':
			i += 2
			for at(i) != 0 && !(at(i) == '*' && at(i+1) == '/') {
				i++
			}
			if at(i) != 0 {
				i += 2
			}
			continue
		}
		break
	}
	var kw [8]byte
	n := 0
	for ; n < len(kw); n++ {
		c := at(i + n)
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		if c < 'A' || c > 'Z' {
			break
		}
		kw[n] = c
	}
	switch string(kw[:n]) {
	case "CREATE", "DROP", "ALTER", "REINDEX", "VACUUM":
		return true
	}
	return false
}

// traceCallback recognises a schema change: it raises the schema barrier before the statement runs, and gives it
// back when the statement ends if no snapshot is open (a change of a temporary object never opens one).
func traceCallback(tls *libc.TLS, typ uint32, ctx, p, x uintptr) int32 {
	if typ != traceStmt && typ != traceProfile {
		return 0
	}
	if ctx != 0 {
		if l := laneOfDB(tls, sqlite3.Xsqlite3_db_handle(tls, p)); l != nil {
			l.stmtNote(tls, typ == traceProfile, p)
		}
	}
	if !isDDL(sqlite3.Xsqlite3_sql(tls, p)) {
		return 0
	}
	l := laneOfDB(tls, sqlite3.Xsqlite3_db_handle(tls, p))
	if l == nil {
		return 0
	}
	if typ == traceStmt {
		l.ddlBegin()
	} else if l.ddlActive && !l.snapHeld {
		l.ddlEnd()
	}
	return 0
}

// openMain: the first lane of a database creates the page store from the real file's header and opens (or recovers)
// the log.
func openMain(tls *libc.TLS, real uintptr, db *mwDB, path string) int32 {
	h := libc.Xmalloc(tls, 100)
	defer libc.Xfree(tls, h)
	if rc := realRead(tls, real, h, 100, 0); rc != rcOK {
		return rc
	}
	hb := bytesAt(h, 100)
	if hb[52] != 0 || hb[53] != 0 || hb[54] != 0 || hb[55] != 0 {
		return rcCantOpen // auto_vacuum / incremental_vacuum: page relocation is not supported
	}
	pgsz := int(hb[16])<<8 | int(hb[17])
	if pgsz == 1 {
		pgsz = 65536
	}
	if pgsz < 512 || pgsz&(pgsz-1) != 0 {
		return rcNotADB
	}
	reserved := int(hb[20])
	db.openMu.Lock()
	defer db.openMu.Unlock()
	if db.store != nil {
		if db.store.pgsz != pgsz {
			return rcMisuse
		}
		return rcOK
	}
	fi, err := os.Stat(path)
	if err != nil {
		return rcCantOpen
	}
	rf, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return rcCantOpen
	}
	st := newStore(pgsz, uint32(fi.Size()/int64(pgsz)))
	st.reserved = reserved
	mode := fi.Mode().Perm() &^ 0o111
	lg, last, rc := openLog(path, pgsz, st, mode)
	if rc != rcOK {
		rf.Close()
		return int32(rc)
	}
	st.compactedEpoch = lg.baseEpoch()
	db.realFile = rf
	db.log = lg
	db.epoch.Store(last)
	db.nextEpoch.Store(last)
	db.store = st
	db.startCompactor()
	return rcOK
}

func vfsDelete(tls *libc.TLS, pVfs, zName uintptr, syncDir int32) int32 {
	if zName != 0 {
		name := libc.GoString(zName)
		if strings.HasSuffix(name, "-wal") && lookupDB(strings.TrimSuffix(name, "-wal")) != nil {
			return rcOK
		}
	}
	return fnOf[func(*libc.TLS, uintptr, uintptr, int32) int32](rootVFS.FxDelete)(tls, uintptr(unsafe.Pointer(rootVFS)), zName, syncDir)
}

// MARK: - WAL-mode bootstrap

// readHeader reads the first 100 bytes of the file through the platform VFS (closing a descriptor of the file
// drops every POSIX lock the process holds on it; the platform VFS keeps it open while others hold locks).
func readHeader(tls *libc.TLS, path string) ([]byte, bool) {
	pf := libc.Xcalloc(tls, 1, libc.Tsize_t(rootVFS.FszOsFile))
	defer libc.Xfree(tls, pf)
	zp, _ := libc.CString(path)
	defer libc.Xfree(tls, zp)
	var out [4]byte
	if rootOpen(tls, zp, pf, sqlite3.SQLITE_OPEN_READONLY|sqlite3.SQLITE_OPEN_MAIN_DB, uintptr(unsafe.Pointer(&out[0]))) != rcOK {
		return nil, false
	}
	defer realClose(tls, pf)
	buf := libc.Xmalloc(tls, 100)
	defer libc.Xfree(tls, buf)
	if realRead(tls, pf, buf, 100, 0) != rcOK {
		return nil, false
	}
	h := make([]byte, 100)
	copy(h, bytesAt(buf, 100))
	return h, true
}

func staleWAL(path string) bool {
	fi, err := os.Stat(path + "-wal")
	return err == nil && fi.Size() > 0
}

// isCleanWALDB: a database in WAL mode with no real -wal holding frames: what the engine can open without writing.
func isCleanWALDB(tls *libc.TLS, path string) bool {
	h, ok := readHeader(tls, path)
	return ok && h[18] == 2 && h[19] == 2 && !staleWAL(path)
}

// ensureWALDB converts a new or rollback-journal database to WAL mode with a private stock connection, and
// checkpoints a real -wal left over by stock use (its frames are invisible to the engine, and the compactor would
// later overwrite the file under it).
func ensureWALDB(tls *libc.TLS, path string) int32 {
	stale := staleWAL(path)
	if h, ok := readHeader(tls, path); ok && h[18] == 2 && h[19] == 2 && !stale {
		return rcOK
	}
	zp, _ := libc.CString(path)
	defer libc.Xfree(tls, zp)
	ppDb := libc.Xcalloc(tls, 1, 8)
	defer libc.Xfree(tls, ppDb)
	rc := sqlite3.Xsqlite3_open_v2(tls, zp, ppDb, sqlite3.SQLITE_OPEN_READWRITE|sqlite3.SQLITE_OPEN_CREATE|sqlite3.SQLITE_OPEN_PRIVATECACHE, rootVFS.FzName)
	c := *(*uintptr)(unsafe.Pointer(ppDb))
	if rc == rcOK {
		sqlite3.Xsqlite3_busy_timeout(tls, c, 5000) // (two openers of a new database may convert it at the same time)
		rc = execC(tls, c, "PRAGMA journal_mode=WAL")
		if rc == rcOK && stale {
			rc = execC(tls, c, "PRAGMA wal_checkpoint(TRUNCATE)")
		}
	}
	if c != 0 {
		sqlite3.Xsqlite3_close_v2(tls, c)
	}
	if rc != rcOK {
		return rc
	}
	// a checkpoint that another process blocks reports success, and a file that is not in WAL mode would have its
	// writes dropped by the lane: refuse to open rather than lose commits
	if h, ok := readHeader(tls, path); !ok || h[18] != 2 || h[19] != 2 || staleWAL(path) {
		return rcBusy
	}
	return rcOK
}

func execC(tls *libc.TLS, db uintptr, sql string) int32 {
	zs, _ := libc.CString(sql)
	defer libc.Xfree(tls, zs)
	return sqlite3.Xsqlite3_exec(tls, db, zs, 0, 0, 0)
}

// MARK: - the main database file of a lane

func lmClose(tls *libc.TLS, p uintptr) int32 {
	f := fileOf(p)
	l := f.lane
	l.snapshotEnd()
	for _, r := range f.shm {
		libc.Xfree(tls, r)
	}
	f.shm = nil
	l.shm0 = nil
	if l.ddlActive {
		l.ddlEnd()
	}
	l.rebaseFree()
	if db := l.cdb.Load(); db != 0 {
		dbLanes.CompareAndDelete(db, l)
	}
	tlsMu.Lock()
	for t, x := range tlsLanes {
		if x == l {
			delete(tlsLanes, t)
		}
	}
	tlsMu.Unlock()
	rc := realClose(tls, f.real)
	dropHandle(p)
	releaseDB(l.db)
	return rc
}

func lmRead(tls *libc.TLS, p, buf uintptr, n int32, off int64) int32 {
	f := fileOf(p)
	noteLane(tls, f.lane)
	return int32(f.lane.mainRead(bytesAt(buf, int(n)), off))
}

func lmWrite(tls *libc.TLS, p, buf uintptr, n int32, off int64) int32 {
	return rcOK // swallowed: only the compactor writes the real file
}

func lmTruncate(tls *libc.TLS, p uintptr, size int64) int32 { return rcOK }
func lmSync(tls *libc.TLS, p uintptr, flags int32) int32    { return rcOK }

func lmFileSize(tls *libc.TLS, p, out uintptr) int32 {
	f := fileOf(p)
	noteLane(tls, f.lane)
	*(*int64)(unsafe.Pointer(out)) = f.lane.mainFileSize()
	return rcOK
}

// File locks are virtualised above SHARED. A lane never writes the real file, so it needs no RESERVED, PENDING or
// EXCLUSIVE lock; taking them for real would make every closing connection's checkpoint attempt fail the opens of the
// others. The real SHARED lock is kept.
func lmLock(tls *libc.TLS, p uintptr, lvl int32) int32 {
	f := fileOf(p)
	noteLane(tls, f.lane)
	if lvl <= sqlite3.SQLITE_LOCK_SHARED {
		return fnOf[func(*libc.TLS, uintptr, int32) int32](methodsOf(f.real).FxLock)(tls, f.real, lvl)
	}
	return rcOK
}

func lmUnlock(tls *libc.TLS, p uintptr, lvl int32) int32 {
	if lvl >= sqlite3.SQLITE_LOCK_SHARED {
		return rcOK // back to SHARED: the real lock never left it
	}
	f := fileOf(p)
	return fnOf[func(*libc.TLS, uintptr, int32) int32](methodsOf(f.real).FxUnlock)(tls, f.real, lvl)
}

func lmCheckReserved(tls *libc.TLS, p, out uintptr) int32 {
	*(*int32)(unsafe.Pointer(out)) = 0
	return rcOK
}

func sqliteString(tls *libc.TLS, s string) uintptr {
	p := sqlite3.Xsqlite3_malloc(tls, int32(len(s)+1))
	if p != 0 {
		b := bytesAt(p, len(s)+1)
		copy(b, s)
		b[len(s)] = 0
	}
	return p
}

// autoVacuumIsNone: what SQLite makes of the value of PRAGMA auto_vacuum (only 1, 2, FULL and INCREMENTAL count).
func autoVacuumIsNone(z string) bool {
	switch strings.ToLower(z) {
	case "none":
		return true
	case "full", "incremental":
		return false
	}
	v, err := strconv.Atoi(z)
	if err != nil {
		return true
	}
	return v != 1 && v != 2
}

func lmFileControl(tls *libc.TLS, p uintptr, op int32, arg uintptr) int32 {
	f := fileOf(p)
	l := f.lane
	noteLane(tls, l)
	switch op {
	case fcntlLaneHandle:
		*(*uintptr)(unsafe.Pointer(arg)) = *(*uintptr)(unsafe.Pointer(p + 8))
		return rcOK
	case sqlite3.SQLITE_FCNTL_MMAP_SIZE:
		*(*int64)(unsafe.Pointer(arg)) = 0 // reads must go through xRead
		return rcOK
	case sqlite3.SQLITE_FCNTL_SIZE_HINT:
		return rcOK // the real file is never grown from a lane
	case sqlite3.SQLITE_FCNTL_PRAGMA:
		az := unsafe.Slice((*uintptr)(unsafe.Pointer(arg)), 3)
		if az[1] == 0 {
			return rcNotFound
		}
		name := strings.ToLower(libc.GoString(az[1]))
		val := ""
		if az[2] != 0 {
			val = libc.GoString(az[2])
		}
		fail := func(msg string) int32 {
			az[0] = sqliteString(tls, msg)
			return rcError
		}
		switch {
		case name == "journal_mode" && val != "" && !strings.EqualFold(val, "wal"):
			return fail("multiwriter: journal_mode=" + val + " is not supported (WAL only)")
		case name == "locking_mode" && strings.EqualFold(val, "exclusive"):
			return fail("multiwriter: locking_mode=EXCLUSIVE is not supported")
		case name == "auto_vacuum" && val != "" && !autoVacuumIsNone(val):
			return fail("multiwriter: auto_vacuum=" + val + " is not supported (NONE only)")
		case name == "synchronous" && val != "":
			lv, err := strconv.Atoi(val)
			switch strings.ToLower(val) {
			case "off":
				lv = 0
			case "normal":
				lv = 1
			case "full":
				lv = 2
			case "extra":
				lv = 3
			default:
				if err != nil {
					lv = l.syncLevel
				}
			}
			l.syncLevel = lv
			return rcNotFound // observed, not handled: SQLite still executes it
		case name == "mmap_size" && val != "":
			az[0] = sqliteString(tls, "0")
			return rcOK
		}
		return rcNotFound
	}
	return fnOf[func(*libc.TLS, uintptr, int32, uintptr) int32](methodsOf(f.real).FxFileControl)(tls, f.real, op, arg)
}

func lmSectorSize(tls *libc.TLS, p uintptr) int32 {
	f := fileOf(p)
	return fnOf[func(*libc.TLS, uintptr) int32](methodsOf(f.real).FxSectorSize)(tls, f.real)
}

func lmDeviceChar(tls *libc.TLS, p uintptr) int32 {
	f := fileOf(p)
	return fnOf[func(*libc.TLS, uintptr) int32](methodsOf(f.real).FxDeviceCharacteristics)(tls, f.real)
}

// The wal-index of a lane is private memory: no other connection can observe it.
func lmShmMap(tls *libc.TLS, p uintptr, page, size, extend int32, pp uintptr) int32 {
	f := fileOf(p)
	noteLane(tls, f.lane)
	if size != shmRegionBytes || page >= shmMaxRegions {
		return rcIOErrShmSize
	}
	for int(page) >= len(f.shm) {
		if extend == 0 {
			*(*uintptr)(unsafe.Pointer(pp)) = 0
			return rcOK
		}
		r := libc.Xcalloc(tls, 1, shmRegionBytes)
		if r == 0 {
			return rcNoMem
		}
		f.shm = append(f.shm, r)
		if len(f.shm) == 1 {
			f.lane.shm0 = bytesAt(r, 96)
		}
	}
	*(*uintptr)(unsafe.Pointer(pp)) = f.shm[page]
	return rcOK
}

// Locks are private: always granted. They still delimit the lane's transaction.
func lmShmLock(tls *libc.TLS, p uintptr, ofst, n, flags int32) int32 {
	return int32(fileOf(p).lane.onShmLock(ofst, n, flags))
}

func lmShmBarrier(tls *libc.TLS, p uintptr) {}

func lmShmUnmap(tls *libc.TLS, p uintptr, del int32) int32 { return rcOK } // the regions are freed with the lane

func lmFetch(tls *libc.TLS, p uintptr, off int64, n int32, pp uintptr) int32 {
	*(*uintptr)(unsafe.Pointer(pp)) = 0 // no mmap
	return rcOK
}

func lmUnfetch(tls *libc.TLS, p uintptr, off int64, q uintptr) int32 { return rcOK }

// MARK: - the private WAL (memory only)

func walClose(tls *libc.TLS, p uintptr) int32 {
	f := fileOf(p)
	f.lane.walBound = false
	f.lane.wal.size = 0
	dropHandle(p)
	return rcOK
}

func walReadM(tls *libc.TLS, p, buf uintptr, n int32, off int64) int32 {
	return int32(fileOf(p).lane.walRead(bytesAt(buf, int(n)), off))
}

func walWriteM(tls *libc.TLS, p, buf uintptr, n int32, off int64) int32 {
	l := fileOf(p).lane
	l.curTLS = tls
	return int32(l.walWrite(bytesAt(buf, int(n)), off))
}

func walTruncateM(tls *libc.TLS, p uintptr, size int64) int32 {
	fileOf(p).lane.walTruncate(size)
	return rcOK
}

func walSync(tls *libc.TLS, p uintptr, flags int32) int32 { return rcOK }

func walFileSize(tls *libc.TLS, p, out uintptr) int32 {
	*(*int64)(unsafe.Pointer(out)) = int64(fileOf(p).lane.wal.size)
	return rcOK
}

func walNoLock(tls *libc.TLS, p uintptr, lvl int32) int32 { return rcOK }

func walCheckReserved(tls *libc.TLS, p, out uintptr) int32 {
	*(*int32)(unsafe.Pointer(out)) = 0
	return rcOK
}

func walFileControl(tls *libc.TLS, p uintptr, op int32, arg uintptr) int32 { return rcNotFound }
func walSectorSize(tls *libc.TLS, p uintptr) int32                         { return 512 }
func walDeviceChar(tls *libc.TLS, p uintptr) int32                         { return 0 }
