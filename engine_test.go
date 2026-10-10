package multiwriter

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func openT(t *testing.T, path string, o Options) *sql.DB {
	t.Helper()
	db, err := Open(path, o)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func integrity(t *testing.T, db *sql.DB) {
	t.Helper()
	var ic string
	if err := db.QueryRow("PRAGMA integrity_check").Scan(&ic); err != nil || ic != "ok" {
		t.Fatalf("integrity_check: %q %v", ic, err)
	}
}

// openStock opens the file with the platform VFS only, as any SQLite would.
func openStock(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func sidecars(path string) []string {
	m, _ := filepath.Glob(path + "-mw*")
	return m
}

// Writers that each append to their own table: they extend the file at the same time, which is a physical conflict
// on page 1 and on the new page numbers that the relocation resolves.
func TestBulkOwnTables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bulk.db")
	db := openT(t, path, Options{})
	const W, TX, ROWS = 8, 40, 100
	for w := 0; w < W; w++ {
		mustExec(t, db, fmt.Sprintf("CREATE TABLE t%d(id INTEGER PRIMARY KEY, v TEXT)", w))
	}
	var wg sync.WaitGroup
	var retries atomic.Int64
	for w := 0; w < W; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < TX; i++ {
				err := RunTx(context.Background(), db, nil, func(tx *sql.Tx) error {
					for r := 0; r < ROWS; r++ {
						if _, err := tx.Exec(fmt.Sprintf("INSERT INTO t%d(v) VALUES(?)", w), strings.Repeat("x", 50+r)); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	for w := 0; w < W; w++ {
		var n int
		db.QueryRow(fmt.Sprintf("SELECT count(*) FROM t%d", w)).Scan(&n)
		if n != TX*ROWS {
			t.Fatalf("t%d: %d rows, want %d", w, n, TX*ROWS)
		}
	}
	integrity(t, db)
	s, _ := GetStats(path)
	t.Logf("commits=%d aborts=%d relocations=%d merges=%d pageConflicts=%d readConflicts=%d readsSaved=%d retries=%d",
		s.Commits, s.Aborts, s.Relocations, s.Merges, s.PageConflicts, s.ReadConflicts, s.ReadsSaved, retries.Load())
	if s.Relocations == 0 {
		t.Errorf("expected relocations")
	}
	db.Close()
	if sc := sidecars(path); len(sc) != 0 {
		t.Fatalf("sidecar files left after the last close: %v", sc)
	}
	st := openStock(t, path)
	defer st.Close()
	integrity(t, st)
	var n int
	st.QueryRow("SELECT count(*) FROM t3").Scan(&n)
	if n != TX*ROWS {
		t.Fatalf("stock read: %d rows", n)
	}
}

// Lost updates: every committed increment must be in the final value.
func TestHotCounters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hot.db")
	db := openT(t, path, Options{})
	defer db.Close()
	mustExec(t, db, "CREATE TABLE c(id INTEGER PRIMARY KEY, n INT)")
	for i := 0; i < 4; i++ {
		mustExec(t, db, "INSERT INTO c VALUES(?, 0)", i)
	}
	const W, N = 8, 150
	var done atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < W; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w)))
			for i := 0; i < N; i++ {
				id := r.Intn(4)
				err := RunTx(context.Background(), db, nil, func(tx *sql.Tx) error {
					_, err := tx.Exec("UPDATE c SET n = n + 1 WHERE id = ?", id)
					return err
				})
				if err != nil {
					t.Error(err)
					return
				}
				done.Add(1)
			}
		}(w)
	}
	wg.Wait()
	var sum int64
	db.QueryRow("SELECT sum(n) FROM c").Scan(&sum)
	if sum != done.Load() {
		t.Fatalf("sum %d, committed %d: lost updates", sum, done.Load())
	}
	integrity(t, db)
}

// Transfers between accounts read both rows and write both: the total never changes, and every reader sees a
// consistent total (snapshot isolation).
func TestTransfersInvariant(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bank.db")
	db := openT(t, path, Options{})
	defer db.Close()
	mustExec(t, db, "CREATE TABLE acct(id INTEGER PRIMARY KEY, bal INT, pad TEXT)")
	const A = 200
	for i := 0; i < A; i++ {
		mustExec(t, db, "INSERT INTO acct VALUES(?, 1000, ?)", i, strings.Repeat("p", 200))
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var bad atomic.Int64
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				var s int64
				if err := db.QueryRow("SELECT sum(bal) FROM acct").Scan(&s); err != nil {
					t.Error(err)
					return
				}
				if s != A*1000 {
					bad.Add(1)
				}
			}
		}()
	}
	var ww sync.WaitGroup
	for w := 0; w < 8; w++ {
		ww.Add(1)
		go func(w int) {
			defer ww.Done()
			r := rand.New(rand.NewSource(int64(w)))
			for i := 0; i < 150; i++ {
				a, b := r.Intn(A), r.Intn(A)
				if a == b {
					continue
				}
				err := RunTx(context.Background(), db, nil, func(tx *sql.Tx) error {
					var ba int
					if err := tx.QueryRow("SELECT bal FROM acct WHERE id = ?", a).Scan(&ba); err != nil {
						return err
					}
					amt := r.Intn(50)
					if _, err := tx.Exec("UPDATE acct SET bal = ? WHERE id = ?", ba-amt, a); err != nil {
						return err
					}
					_, err := tx.Exec("UPDATE acct SET bal = bal + ? WHERE id = ?", amt, b)
					return err
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	ww.Wait()
	close(stop)
	wg.Wait()
	var s int64
	db.QueryRow("SELECT sum(bal) FROM acct").Scan(&s)
	if s != A*1000 || bad.Load() != 0 {
		t.Fatalf("total %d, inconsistent reads %d", s, bad.Load())
	}
	integrity(t, db)
}

// Write skew: T1 reads A and writes B, T2 reads B and writes A, from the same snapshot. One of them must fail.
func TestWriteSkewRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "skew.db")
	db := openT(t, path, Options{MaxOpenConns: 2})
	defer db.Close()
	mustExec(t, db, "CREATE TABLE oncall(id INTEGER PRIMARY KEY, on_call INT, pad TEXT)")
	// rows on different pages
	mustExec(t, db, "INSERT INTO oncall VALUES(1, 1, ?)", strings.Repeat("a", 3000))
	mustExec(t, db, "INSERT INTO oncall VALUES(2, 1, ?)", strings.Repeat("b", 3000))
	ctx := context.Background()
	c1, _ := db.Conn(ctx)
	c2, _ := db.Conn(ctx)
	defer c1.Close()
	defer c2.Close()
	t1, _ := c1.BeginTx(ctx, nil)
	t2, _ := c2.BeginTx(ctx, nil)
	var n1, n2 int
	t1.QueryRow("SELECT sum(on_call) FROM oncall").Scan(&n1)
	t2.QueryRow("SELECT sum(on_call) FROM oncall").Scan(&n2)
	if n1 != 2 || n2 != 2 {
		t.Fatal(n1, n2)
	}
	if _, err := t1.Exec("UPDATE oncall SET on_call = 0 WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	if _, err := t2.Exec("UPDATE oncall SET on_call = 0 WHERE id = 2"); err != nil {
		t.Fatal(err)
	}
	e1 := t1.Commit()
	e2 := t2.Commit()
	if e1 != nil {
		t.Fatalf("first commit: %v", e1)
	}
	if e2 == nil || !IsBusySnapshot(e2) {
		t.Fatalf("second commit must be refused with BUSY_SNAPSHOT, got %v", e2)
	}
	t2.Rollback()
	c1.Close()
	c2.Close()
	var n int
	db.QueryRow("SELECT sum(on_call) FROM oncall").Scan(&n)
	if n != 1 {
		t.Fatalf("on call: %d", n)
	}
}

// Writers on different rows of different pages do not conflict at all.
func TestDisjointNoConflicts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "disj.db")
	db := openT(t, path, Options{})
	defer db.Close()
	mustExec(t, db, "CREATE TABLE r(id INTEGER PRIMARY KEY, n INT, pad TEXT)")
	for i := 0; i < 8; i++ {
		mustExec(t, db, "INSERT INTO r VALUES(?, 0, ?)", i, strings.Repeat("z", 3500)) // one row per page
	}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if err := RunTx(context.Background(), db, nil, func(tx *sql.Tx) error {
					_, err := tx.Exec("UPDATE r SET n = n + 1 WHERE id = ?", w)
					return err
				}); err != nil {
					t.Error(err)
				}
			}
		}(w)
	}
	wg.Wait()
	var s int
	db.QueryRow("SELECT sum(n) FROM r").Scan(&s)
	if s != 800 {
		t.Fatal(s)
	}
	st, _ := GetStats(path)
	t.Logf("aborts=%d pageConflicts=%d readConflicts=%d readsSaved=%d", st.Aborts, st.PageConflicts, st.ReadConflicts, st.ReadsSaved)
}

// A long reader keeps its snapshot while others commit and the log is compacted.
func TestLongReaderAndCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "long.db")
	db := openT(t, path, Options{LogMaxMB: 1})
	defer db.Close()
	mustExec(t, db, "CREATE TABLE k(id INTEGER PRIMARY KEY, v TEXT)")
	for i := 0; i < 100; i++ {
		mustExec(t, db, "INSERT INTO k(v) VALUES(?)", strings.Repeat("v", 500))
	}
	ctx := context.Background()
	rc, _ := db.Conn(ctx)
	rtx, _ := rc.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	var before int
	rtx.QueryRow("SELECT count(*) FROM k").Scan(&before)
	for i := 0; i < 2000; i++ {
		mustExec(t, db, "INSERT INTO k(v) VALUES(?)", strings.Repeat("w", 500))
	}
	Compact(path)
	var again int
	rtx.QueryRow("SELECT count(*) FROM k").Scan(&again)
	if again != before {
		t.Fatalf("snapshot moved: %d then %d", before, again)
	}
	rtx.Rollback()
	rc.Close()
	r, err := Compact(path)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := GetStats(path)
	t.Logf("compact %+v stats base=%d epoch=%d log=%d versions=%d", r, st.BaseEpoch, st.Epoch, st.LogBytes, st.PageVersions)
	if st.BaseEpoch != st.Epoch {
		t.Fatalf("base %d epoch %d", st.BaseEpoch, st.Epoch)
	}
	var n int
	db.QueryRow("SELECT count(*) FROM k").Scan(&n)
	if n != 2100 {
		t.Fatal(n)
	}
	integrity(t, db)
}

// Schema changes while others write: transactions that overlap one are refused and run again.
func TestDDLWhileWriting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ddl.db")
	db := openT(t, path, Options{})
	defer db.Close()
	mustExec(t, db, "CREATE TABLE base(id INTEGER PRIMARY KEY, v INT)")
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var ins atomic.Int64
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := RunTx(context.Background(), db, nil, func(tx *sql.Tx) error {
					_, err := tx.Exec("INSERT INTO base(v) VALUES(1)")
					return err
				}); err != nil {
					t.Error(err)
					return
				}
				ins.Add(1)
			}
		}()
	}
	for i := 0; i < 20; i++ {
		if err := RunTx(context.Background(), db, nil, func(tx *sql.Tx) error {
			_, err := tx.Exec(fmt.Sprintf("CREATE TABLE extra%d(a, b)", i))
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if err := RunTx(context.Background(), db, nil, func(tx *sql.Tx) error {
			_, err := tx.Exec(fmt.Sprintf("CREATE INDEX ix%d ON base(v)", i))
			return err
		}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	var n int64
	db.QueryRow("SELECT count(*) FROM base").Scan(&n)
	if n != ins.Load() {
		t.Fatalf("rows %d inserts %d", n, ins.Load())
	}
	var tabs int
	db.QueryRow("SELECT count(*) FROM sqlite_schema WHERE name LIKE 'extra%' OR name LIKE 'ix%'").Scan(&tabs)
	if tabs != 40 {
		t.Fatal(tabs)
	}
	integrity(t, db)
}

// Crash: a child process commits and dies without closing. Every acknowledged commit must be there after the
// recovery, and the file is an ordinary database once the last connection closes.
func TestCrashRecovery(t *testing.T) {
	if os.Getenv("MW_CRASH_CHILD") != "" {
		crashChild()
		return
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "crash.db")
	ack := filepath.Join(dir, "ack")
	cmd := exec.Command(os.Args[0], "-test.run", "^TestCrashRecovery$")
	cmd.Env = append(os.Environ(), "MW_CRASH_CHILD=1", "MW_CRASH_DB="+path, "MW_CRASH_ACK="+ack)
	out, err := cmd.CombinedOutput()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 7 {
		t.Fatalf("child: %v\n%s", err, out)
	}
	if len(sidecars(path)) == 0 {
		t.Fatal("the child left no log")
	}
	b, _ := os.ReadFile(ack)
	acked := strings.Count(string(b), "\n")
	db := openT(t, path, Options{})
	var n int
	if err := db.QueryRow("SELECT count(*) FROM t").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n < acked {
		t.Fatalf("recovered %d rows, %d were acknowledged", n, acked)
	}
	integrity(t, db)
	t.Logf("acknowledged %d, recovered %d", acked, n)
	mustExec(t, db, "INSERT INTO t(v) VALUES('after')")
	db.Close()
	st := openStock(t, path)
	defer st.Close()
	var m int
	st.QueryRow("SELECT count(*) FROM t").Scan(&m)
	if m != n+1 {
		t.Fatalf("stock sees %d, want %d", m, n+1)
	}
	integrity(t, st)
}

func crashChild() {
	path, ack := os.Getenv("MW_CRASH_DB"), os.Getenv("MW_CRASH_ACK")
	db, err := Open(path, Options{})
	if err != nil {
		os.Exit(2)
	}
	if _, err := db.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		os.Exit(3)
	}
	f, _ := os.Create(ack)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 150; i++ {
				err := RunTx(context.Background(), db, nil, func(tx *sql.Tx) error {
					_, err := tx.Exec("INSERT INTO t(v) VALUES(?)", strings.Repeat("c", 300))
					return err
				})
				if err != nil {
					os.Exit(4)
				}
				mu.Lock()
				f.WriteString("ok\n")
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	f.Sync()
	os.Exit(7) // no Close: the log holds the commits
}

// A second process cannot open a database another process has open with the engine.
func TestSecondProcessRefused(t *testing.T) {
	if os.Getenv("MW_SECOND_CHILD") != "" {
		db, _ := Open(os.Getenv("MW_SECOND_DB"), Options{})
		_, err := db.Exec("SELECT 1 FROM sqlite_schema")
		if err != nil {
			os.Exit(5)
		}
		os.Exit(0)
	}
	path := filepath.Join(t.TempDir(), "two.db")
	db := openT(t, path, Options{})
	defer db.Close()
	mustExec(t, db, "CREATE TABLE x(a)")
	cmd := exec.Command(os.Args[0], "-test.run", "^TestSecondProcessRefused$")
	cmd.Env = append(os.Environ(), "MW_SECOND_CHILD=1", "MW_SECOND_DB="+path)
	err := cmd.Run()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 5 {
		t.Fatalf("second process: %v", err)
	}
}

func TestUnsupportedPragmas(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.db")
	db := openT(t, path, Options{})
	defer db.Close()
	mustExec(t, db, "CREATE TABLE x(a)")
	if _, err := db.Exec("PRAGMA journal_mode=DELETE"); err == nil {
		t.Fatal("journal_mode=DELETE accepted")
	}
	if _, err := db.Exec("PRAGMA locking_mode=EXCLUSIVE"); err == nil {
		t.Fatal("locking_mode=EXCLUSIVE accepted")
	}
	var mode string
	db.QueryRow("PRAGMA journal_mode").Scan(&mode)
	if mode != "wal" {
		t.Fatal(mode)
	}
}

// Overflow pages, indexes, deletes and savepoints under concurrency, checked against a model.
func TestMixedWorkloadModel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mix.db")
	db := openT(t, path, Options{})
	defer db.Close()
	mustExec(t, db, "CREATE TABLE m(id INTEGER PRIMARY KEY, owner INT, payload BLOB, tag TEXT UNIQUE)")
	mustExec(t, db, "CREATE INDEX m_owner ON m(owner)")
	const W = 6
	models := make([]map[int64]int, W)
	var wg sync.WaitGroup
	for w := 0; w < W; w++ {
		models[w] = map[int64]int{}
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w) + 42))
			model := models[w]
			seq := 0
			for i := 0; i < 120; i++ {
				op := r.Intn(10)
				var ids []int64
				for id := range model {
					ids = append(ids, id)
				}
				size := 10 + r.Intn(6000) // some rows spill to overflow pages
				var newID int64
				var delID int64 = -1
				err := RunTx(context.Background(), db, nil, func(tx *sql.Tx) error {
					newID, delID = 0, -1
					switch {
					case op < 6 || len(ids) == 0:
						seq++
						res, err := tx.Exec("INSERT INTO m(owner, payload, tag) VALUES(?, randomblob(?), ?)", w, size, fmt.Sprintf("w%d-%d", w, seq))
						if err != nil {
							return err
						}
						newID, _ = res.LastInsertId()
					case op < 8:
						delID = ids[r.Intn(len(ids))]
						_, err := tx.Exec("DELETE FROM m WHERE id = ?", delID)
						return err
					default:
						id := ids[r.Intn(len(ids))]
						if _, err := tx.Exec("SAVEPOINT s"); err != nil {
							return err
						}
						if _, err := tx.Exec("UPDATE m SET payload = randomblob(?) WHERE id = ?", size, id); err != nil {
							return err
						}
						if _, err := tx.Exec("ROLLBACK TO s"); err != nil {
							return err
						}
						_, err := tx.Exec("RELEASE s")
						return err
					}
					return nil
				})
				if err != nil {
					t.Error(err)
					return
				}
				if newID != 0 {
					model[newID] = size
				}
				if delID >= 0 {
					delete(model, delID)
				}
			}
		}(w)
	}
	wg.Wait()
	for w := 0; w < W; w++ {
		rows, err := db.Query("SELECT id, length(payload) FROM m WHERE owner = ?", w)
		if err != nil {
			t.Fatal(err)
		}
		got := map[int64]int{}
		for rows.Next() {
			var id int64
			var n int
			rows.Scan(&id, &n)
			got[id] = n
		}
		rows.Close()
		if len(got) != len(models[w]) {
			t.Fatalf("owner %d: %d rows, model %d", w, len(got), len(models[w]))
		}
		for id, n := range models[w] {
			if got[id] != n {
				t.Fatalf("owner %d id %d: size %d, model %d", w, id, got[id], n)
			}
		}
	}
	integrity(t, db)
	s, _ := GetStats(path)
	t.Logf("commits=%d aborts=%d relocations=%d merges=%d", s.Commits, s.Aborts, s.Relocations, s.Merges)
}

// Writers that insert into their own key ranges of one table: their leaves are disjoint, but the splits all rewrite
// the same interior pages (merged three-way) and extend the file at the same time (relocated).
func TestSharedTableKeyRanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ranges.db")
	db := openT(t, path, Options{})
	mustExec(t, db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
	const W, TX, ROWS = 6, 30, 20
	for w := 0; w < W; w++ { // seed each range so the tree has interior pages with one child per range
		for i := 0; i < 20; i++ {
			mustExec(t, db, "INSERT INTO t VALUES(?, ?)", w*1_000_000+i, strings.Repeat("s", 400))
		}
	}
	var wg sync.WaitGroup
	for w := 0; w < W; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			next := w*1_000_000 + 1000
			for i := 0; i < TX; i++ {
				base := next
				err := RunTx(context.Background(), db, nil, func(tx *sql.Tx) error {
					for r := 0; r < ROWS; r++ {
						if _, err := tx.Exec("INSERT INTO t VALUES(?, ?)", base+r, strings.Repeat("v", 400)); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					t.Error(err)
					return
				}
				next += ROWS
			}
		}(w)
	}
	wg.Wait()
	var n int
	db.QueryRow("SELECT count(*) FROM t").Scan(&n)
	if n != W*20+W*TX*ROWS {
		t.Fatalf("rows %d", n)
	}
	integrity(t, db)
	s, _ := GetStats(path)
	t.Logf("commits=%d aborts=%d relocations=%d merges=%d pageConflicts=%d readConflicts=%d readsSaved=%d", s.Commits, s.Aborts, s.Relocations, s.Merges, s.PageConflicts, s.ReadConflicts, s.ReadsSaved)
	db.Close()
	st := openStock(t, path)
	defer st.Close()
	integrity(t, st)
}
