package multiwriter

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"modernc.org/sqlite"
)

// Error is an SQLite result code returned by the engine itself (not through a connection).
type Error struct{ Code int }

func (e *Error) Error() string { return fmt.Sprintf("multiwriter: sqlite error %d", e.Code) }

func sqliteErr(rc int) error {
	if rc == rcOK {
		return nil
	}
	return &Error{Code: rc}
}

const (
	// CodeBusy is SQLITE_BUSY.
	CodeBusy = rcBusy
	// CodeBusySnapshot is SQLITE_BUSY_SNAPSHOT: the commit lost against a concurrent one; run the whole transaction again.
	CodeBusySnapshot = rcBusySnapshot
)

// IsBusySnapshot reports whether err is SQLITE_BUSY_SNAPSHOT or SQLITE_BUSY: the transaction must be run again
// (the whole transaction, not only the statement that failed).
func IsBusySnapshot(err error) bool {
	var se *sqlite.Error
	if errors.As(err, &se) {
		return se.Code() == rcBusySnapshot || se.Code()&0xff == rcBusy
	}
	var me *Error
	if errors.As(err, &me) {
		return me.Code&0xff == rcBusy
	}
	return false
}

// Options of a database opened with the engine. The zero value is the default.
type Options struct {
	// Synchronous is PRAGMA synchronous ("FULL" by default: every commit is on disk when it returns).
	Synchronous string
	// BusyTimeout is PRAGMA busy_timeout (5 s by default).
	BusyTimeout time.Duration
	// ReadCheck=false turns the validation of the pages a transaction read off: fewer retries, but write skew becomes
	// possible (mw_readcheck=0).
	DisableReadCheck bool
	// DurableVisibility makes a commit visible to other transactions only once it is on the disk, like stock SQLite
	// (mw_durable_visible=1). By default a commit is visible as soon as it is published and its own COMMIT returns
	// once it is durable: concurrent writers start from fresher snapshots and lose far fewer commits, but after a power
	// failure a read-only transaction may have seen a commit whose COMMIT never returned.
	DurableVisibility bool
	// Rebase replays a commit that lost only on shared pages (other rows of the same page) at the latest state
	// instead of refusing it (mw_rebase=1).
	Rebase bool
	// MultiProcess lets several processes write the database at the same time (mw_mp=1; every process must open it
	// so). Unix only.
	MultiProcess bool
	// LogMaxMB is the size of the log before it is compacted into the database file (32 MB by default).
	LogMaxMB int
	// MaxOpenConns limits the pool of database/sql (0: unlimited).
	MaxOpenConns int
	// Extra URI parameters (for example "mw_noreloc": "1").
	Params map[string]string
}

// DSN returns the modernc.org/sqlite data source name of a database opened with the engine.
func DSN(path string, o Options) string {
	abs, err := filepath.Abs(path)
	if err == nil {
		path = abs
	}
	q := url.Values{}
	q.Set("vfs", VFSName)
	if o.Rebase {
		q.Set("mw_rebase", "1")
	}
	if o.MultiProcess {
		q.Set("mw_mp", "1")
	}
	if o.DisableReadCheck {
		q.Set("mw_readcheck", "0")
	}
	if o.DurableVisibility {
		q.Set("mw_durable_visible", "1")
	}
	if o.LogMaxMB > 0 {
		q.Set("mw_log_max_mb", fmt.Sprint(o.LogMaxMB))
	}
	for k, v := range o.Params {
		q.Set(k, v)
	}
	sync := o.Synchronous
	if sync == "" {
		sync = "FULL"
	}
	bt := o.BusyTimeout
	if bt == 0 {
		bt = 5 * time.Second
	}
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous("+sync+")")
	q.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", bt.Milliseconds()))
	return "file:" + (&url.URL{Path: path}).EscapedPath() + "?" + q.Encode()
}

// Open opens a database with the engine through database/sql (driver "sqlite" of modernc.org/sqlite). Every
// connection of the pool is a writer of its own.
func Open(path string, o Options) (*sql.DB, error) {
	db, err := sql.Open("sqlite", DSN(path, o))
	if err != nil {
		return nil, err
	}
	if o.MaxOpenConns > 0 {
		db.SetMaxOpenConns(o.MaxOpenConns)
	}
	return db, nil
}

// RunTx runs fn in a transaction and runs it again, whole, while the commit loses against a concurrent one
// (SQLITE_BUSY_SNAPSHOT). fn may run several times: it must not have effects outside the transaction.
func RunTx(ctx context.Context, db *sql.DB, opts *sql.TxOptions, fn func(*sql.Tx) error) error {
	backoff := 50 * time.Microsecond
	for attempt := 0; ; attempt++ {
		tx, err := db.BeginTx(ctx, opts)
		if err != nil {
			if IsBusySnapshot(err) && attempt < 1000 {
				continue
			}
			return err
		}
		err = fn(tx)
		if err == nil {
			err = tx.Commit()
			if err == nil {
				return nil
			}
		} else {
			tx.Rollback()
		}
		if !IsBusySnapshot(err) || attempt >= 1000 {
			return err
		}
		if attempt > 2 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			if backoff < 5*time.Millisecond {
				backoff *= 2
			}
		}
	}
}

// Stats of a database open with the engine in this process.
type Stats struct {
	Epoch            uint64 // the newest visible commit
	BaseEpoch        uint64 // the state up to here is in the database file
	ActiveSnapshots  int
	OldestSnapshot   uint64
	Commits          int64
	Aborts           int64
	Snapshots        int64
	PageConflicts    int64
	ReadConflicts    int64
	SchemaConflicts  int64
	Relocations      int64
	Merges           int64
	ReadsSaved       int64
	HotSerialised    int64
	PagesPublished   int64
	Compactions      int64
	CompactedPages   int64
	LogBytes         int64
	LogSyncs         int64
	PageVersions     int64
	BytesRetained    int64
	VersionsReclaims int64
	GCRuns           int64
	Backpressure     int64
	Rebases          int64 // commits saved by the rebase
	RebasesGrouped   int64 // of them, in a group replay of two or more
	RebaseRetries    int64 // replays that lost a race and ran again
	Unrebasable      int64 // conflicts the rebase could not replay (refused as without it)
}

func dbFor(path string) *mwDB {
	path = strings.TrimPrefix(path, "file:")
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	if db := lookupDB(path); db != nil {
		return db
	}
	// SQLite names the database by its full path with the symbolic links resolved
	if real, err := filepath.EvalSymlinks(path); err == nil {
		return lookupDB(real)
	}
	return nil
}

// GetStats returns the statistics of a database open with the engine in this process.
func GetStats(path string) (Stats, bool) {
	db := dbFor(path)
	if db == nil || db.store == nil {
		return Stats{}, false
	}
	var s Stats
	s.Epoch = db.visibleEpoch()
	s.OldestSnapshot = db.oldestActive()
	db.mu.Lock()
	s.ActiveSnapshots = len(db.active)
	db.mu.Unlock()
	s.Commits, s.Aborts, s.Snapshots = db.nCommits.Load(), db.nAborts.Load(), db.nSnapshots.Load()
	s.PageConflicts, s.ReadConflicts, s.SchemaConflicts = db.nPageConflicts.Load(), db.nReadConflicts.Load(), db.nSchemaConflicts.Load()
	s.Relocations, s.Merges, s.ReadsSaved = db.nRelocations.Load(), db.nMerges.Load(), db.nReadsSaved.Load()
	s.HotSerialised, s.PagesPublished = db.nHotSerialised.Load(), db.nPagesPublished.Load()
	s.Compactions, s.CompactedPages, s.Backpressure = db.nCompactions.Load(), db.nCompactedPages.Load(), db.nBackpressure.Load()
	if db.log != nil {
		s.BaseEpoch, s.LogBytes, s.LogSyncs = db.log.baseEpoch(), db.log.bytes(), db.log.nSyncs.Load()
	}
	s.Rebases, s.RebasesGrouped, s.RebaseRetries, s.Unrebasable = db.nRebases.Load(), db.nRebaseGrouped.Load(), db.nRebaseRetries.Load(), db.nUnrebasable.Load()
	st := db.store
	s.PageVersions, s.BytesRetained, s.VersionsReclaims, s.GCRuns = st.versions.Load(), st.bytes.Load(), st.reclaimed.Load(), st.gcRuns.Load()
	return s, true
}

// Compact writes every committed page that no open snapshot holds back into the database file.
func Compact(path string) (CompactResult, error) {
	db := dbFor(path)
	if db == nil {
		return CompactResult{}, errors.New("multiwriter: the database is not open in this process")
	}
	return db.compact()
}
