package multiwriter

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func benchUpdates(b *testing.B, mw bool) {
	path := filepath.Join(b.TempDir(), "b.db")
	var db *sql.DB
	var err error
	if mw {
		db, err = Open(path, Options{})
	} else {
		db, err = sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)")
	}
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.Exec("CREATE TABLE r(id INTEGER PRIMARY KEY, n INT, pad TEXT)")
	for i := 0; i < 64; i++ {
		db.Exec("INSERT INTO r VALUES(?, 0, ?)", i, strings.Repeat("p", 100))
	}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := db.BeginTx(ctx, nil)
		if _, err := tx.Exec("UPDATE r SET n = n + 1 WHERE id = ?", i%64); err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdateStock(b *testing.B) { benchUpdates(b, false) }
func BenchmarkUpdateMW(b *testing.B)    { benchUpdates(b, true) }
