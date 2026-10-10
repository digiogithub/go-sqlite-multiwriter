package multiwriter

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Writers that each update their own row, all rows on one page: a page conflict that is not a row conflict.
func TestRebaseSharedPage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rb.db")
	db := openT(t, path, Options{Rebase: true})
	defer db.Close()
	mustExec(t, db, "CREATE TABLE r(id INTEGER PRIMARY KEY, n INT, note TEXT)")
	const W, N = 8, 150
	for i := 0; i < W; i++ {
		mustExec(t, db, "INSERT INTO r VALUES(?, 0, 'x')", i)
	}
	var retries atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < W; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < N; i++ {
				for {
					_, err := db.Exec("UPDATE r SET n = n + 1 WHERE id = ?", w)
					if err == nil {
						break
					}
					if !IsBusySnapshot(err) {
						t.Error(err)
						return
					}
					retries.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	for w := 0; w < W; w++ {
		var n int
		db.QueryRow("SELECT n FROM r WHERE id = ?", w).Scan(&n)
		if n != N {
			t.Fatalf("row %d: %d, want %d", w, n, N)
		}
	}
	integrity(t, db)
	s, _ := GetStats(path)
	t.Logf("commits=%d rebases=%d grouped=%d rebaseRetries=%d unrebasable=%d appRetries=%d", s.Commits, s.Rebases, s.RebasesGrouped, s.RebaseRetries, s.Unrebasable, retries.Load())
	if s.Rebases == 0 {
		t.Fatal("no commit was rebased")
	}
}

// Blind inserts of several writers into one table land on the same last leaf.
func TestRebaseInserts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rbi.db")
	db := openT(t, path, Options{Rebase: true})
	defer db.Close()
	mustExec(t, db, "CREATE TABLE ev(id INTEGER PRIMARY KEY, w INT, payload TEXT)")
	mustExec(t, db, "CREATE INDEX ev_w ON ev(w)")
	const W, N = 6, 120
	var wg sync.WaitGroup
	for w := 0; w < W; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < N; i++ {
				err := RunTx(context.Background(), db, nil, func(tx *sql.Tx) error {
					_, err := tx.Exec("INSERT INTO ev(w, payload) VALUES(?, ?)", w, fmt.Sprintf("%d-%d-%s", w, i, strings.Repeat("e", 40)))
					return err
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
		db.QueryRow("SELECT count(*) FROM ev WHERE w = ?", w).Scan(&n)
		if n != N {
			t.Fatalf("writer %d: %d rows", w, n)
		}
	}
	integrity(t, db)
	s, _ := GetStats(path)
	t.Logf("commits=%d aborts=%d rebases=%d relocations=%d unrebasable=%d page=%d read=%d schema=%d", s.Commits, s.Aborts, s.Rebases, s.Relocations, s.Unrebasable, s.PageConflicts, s.ReadConflicts, s.SchemaConflicts)
}

// A transaction that read the rows it decides on (SELECT, then UPDATE of its own row on the same page) must not be
// rebased: the two doctors on call.
func TestRebaseWriteSkewRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rbskew.db")
	db := openT(t, path, Options{Rebase: true, MaxOpenConns: 2})
	mustExec(t, db, "CREATE TABLE oncall(id INTEGER PRIMARY KEY, on_call INT)")
	mustExec(t, db, "INSERT INTO oncall VALUES(1, 1), (2, 1)") // one page
	ctx := context.Background()
	c1, _ := db.Conn(ctx)
	c2, _ := db.Conn(ctx)
	t1, _ := c1.BeginTx(ctx, nil)
	t2, _ := c2.BeginTx(ctx, nil)
	var n1, n2 int
	t1.QueryRow("SELECT sum(on_call) FROM oncall").Scan(&n1)
	t2.QueryRow("SELECT sum(on_call) FROM oncall").Scan(&n2)
	if _, err := t1.Exec("UPDATE oncall SET on_call = 0 WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	if _, err := t2.Exec("UPDATE oncall SET on_call = 0 WHERE id = 2"); err != nil {
		t.Fatal(err)
	}
	if err := t1.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := t2.Commit(); err == nil || !IsBusySnapshot(err) {
		t.Fatalf("second commit: %v", err)
	}
	t2.Rollback()
	c1.Close()
	c2.Close()
	var n int
	db.QueryRow("SELECT sum(on_call) FROM oncall").Scan(&n)
	if n != 1 {
		t.Fatalf("on call: %d", n)
	}
	db.Close()
}

// Two blind updates of the same row: a true conflict, never merged.
func TestRebaseTrueConflictRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rbtrue.db")
	db := openT(t, path, Options{Rebase: true, MaxOpenConns: 2, Params: map[string]string{"mw_rebase_backoff": "0"}})
	mustExec(t, db, "CREATE TABLE r(id INTEGER PRIMARY KEY, n INT)")
	mustExec(t, db, "INSERT INTO r VALUES(1, 10), (2, 20)")
	ctx := context.Background()
	c1, _ := db.Conn(ctx)
	c2, _ := db.Conn(ctx)
	t1, _ := c1.BeginTx(ctx, nil)
	t2, _ := c2.BeginTx(ctx, nil)
	t1.Exec("UPDATE r SET n = n + 1 WHERE id = 1")
	t2.Exec("UPDATE r SET n = n + 100 WHERE id = 1")
	if err := t1.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := t2.Commit(); err == nil || !IsBusySnapshot(err) {
		t.Fatalf("second commit: %v", err)
	}
	t2.Rollback()
	// different rows of the same page: rebased
	t1, _ = c1.BeginTx(ctx, nil)
	t2, _ = c2.BeginTx(ctx, nil)
	t1.Exec("UPDATE r SET n = n + 1 WHERE id = 1")
	t2.Exec("UPDATE r SET n = n + 1 WHERE id = 2")
	if err := t1.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := t2.Commit(); err != nil {
		t.Fatalf("disjoint rows: %v", err)
	}
	c1.Close()
	c2.Close()
	var a, b int
	db.QueryRow("SELECT n FROM r WHERE id = 1").Scan(&a)
	db.QueryRow("SELECT n FROM r WHERE id = 2").Scan(&b)
	if a != 12 || b != 21 {
		t.Fatalf("a=%d b=%d", a, b)
	}
	s, _ := GetStats(path)
	if s.Rebases != 1 {
		t.Fatalf("rebases %d", s.Rebases)
	}
	db.Close()
}

// An UPDATE that sets the value a row already has writes no page: the counted changes differ from the row changes,
// so the transaction is not rebased.
func TestRebaseNoopUpdateRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rbnoop.db")
	db := openT(t, path, Options{Rebase: true, MaxOpenConns: 2, Params: map[string]string{"mw_rebase_backoff": "0"}})
	mustExec(t, db, "CREATE TABLE r(id INTEGER PRIMARY KEY, n INT)")
	mustExec(t, db, "INSERT INTO r VALUES(3, 1000), (4, 1), (5, 5)")
	ctx := context.Background()
	c1, _ := db.Conn(ctx)
	c2, _ := db.Conn(ctx)
	ta, _ := c1.BeginTx(ctx, nil)
	tb, _ := c2.BeginTx(ctx, nil)
	ta.Exec("UPDATE r SET n = 1000 WHERE id = 3") // no-op: it "wrote" row 3
	ta.Exec("UPDATE r SET n = n + 1 WHERE id = 4")
	tb.Exec("UPDATE r SET n = 7 WHERE id = 3")
	if err := tb.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := ta.Commit(); err == nil || !IsBusySnapshot(err) {
		t.Fatalf("commit with a no-op update: %v", err)
	}
	ta.Rollback()
	c1.Close()
	c2.Close()
	db.Close()
}

// Many writers on one page with the rebase, including deletes and inserts, against a model.
func TestRebaseMixedModel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rbmix.db")
	db := openT(t, path, Options{Rebase: true})
	defer db.Close()
	mustExec(t, db, "CREATE TABLE kv(k INTEGER PRIMARY KEY, owner INT, v INT, s TEXT)")
	const W, N = 6, 100
	models := make([]map[int64]int, W)
	var wg sync.WaitGroup
	for w := 0; w < W; w++ {
		models[w] = map[int64]int{}
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			m := models[w]
			next := int64(w * 100000)
			for i := 0; i < N; i++ {
				var err error
				switch {
				case i%5 == 3 && len(m) > 0:
					var k int64
					for k = range m {
						break
					}
					err = RunTx(context.Background(), db, nil, func(tx *sql.Tx) error {
						_, e := tx.Exec("DELETE FROM kv WHERE k = ?", k)
						return e
					})
					if err == nil {
						delete(m, k)
					}
				case i%5 == 4 && len(m) > 0:
					var k int64
					for k = range m {
						break
					}
					err = RunTx(context.Background(), db, nil, func(tx *sql.Tx) error {
						_, e := tx.Exec("UPDATE kv SET v = v + 1 WHERE k = ?", k)
						return e
					})
					if err == nil {
						m[k]++
					}
				default:
					next++
					k := next
					err = RunTx(context.Background(), db, nil, func(tx *sql.Tx) error {
						_, e := tx.Exec("INSERT INTO kv VALUES(?, ?, 0, 'abc')", k, w)
						return e
					})
					if err == nil {
						m[k] = 0
					}
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	for w := 0; w < W; w++ {
		rows, _ := db.Query("SELECT k, v FROM kv WHERE owner = ?", w)
		got := map[int64]int{}
		for rows.Next() {
			var k int64
			var v int
			rows.Scan(&k, &v)
			got[k] = v
		}
		rows.Close()
		if len(got) != len(models[w]) {
			t.Fatalf("owner %d: %d rows, model %d", w, len(got), len(models[w]))
		}
		for k, v := range models[w] {
			if got[k] != v {
				t.Fatalf("owner %d k %d: %d model %d", w, k, got[k], v)
			}
		}
	}
	integrity(t, db)
	s, _ := GetStats(path)
	t.Logf("commits=%d aborts=%d rebases=%d grouped=%d unrebasable=%d", s.Commits, s.Aborts, s.Rebases, s.RebasesGrouped, s.Unrebasable)
}
