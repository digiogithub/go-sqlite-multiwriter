package multiwriter

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func TestSmoke(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.db")
	db, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY, w INT, v TEXT)"); err != nil {
		t.Fatal(err)
	}
	const W, N = 8, 200
	var wg sync.WaitGroup
	for w := 0; w < W; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < N; i++ {
				err := RunTx(context.Background(), db, nil, func(tx *sql.Tx) error {
					_, err := tx.Exec("INSERT INTO t(w, v) VALUES(?, ?)", w, fmt.Sprintf("row %d %d", w, i))
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
	var n int
	if err := db.QueryRow("SELECT count(*) FROM t").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != W*N {
		t.Fatalf("rows %d want %d", n, W*N)
	}
	var ic string
	db.QueryRow("PRAGMA integrity_check").Scan(&ic)
	if ic != "ok" {
		t.Fatal(ic)
	}
	s, _ := GetStats(path)
	t.Logf("%+v", s)
}
