// mwbench compares the engine with stock SQLite (WAL, synchronous=FULL), both through modernc.org/sqlite.
//
//	go run ./cmd/mwbench -workload bulk -writers 1,4,16 -seconds 5
//
// Workloads: bulk (each writer inserts 100 rows per transaction into its own table), groups (each writer updates
// its own row; the rows share pages), hot (every writer increments one of 4 rows: true conflicts).
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	multiwriter "github.com/digiogithub/go-sqlite-multiwriter"
	_ "modernc.org/sqlite"
)

func main() {
	workload := flag.String("workload", "bulk", "bulk | groups | hot")
	writersF := flag.String("writers", "1,4,16", "comma-separated writer counts")
	seconds := flag.Float64("seconds", 5, "duration of each run")
	dir := flag.String("dir", "", "directory for the database files (default: a temporary one)")
	variants := flag.String("variants", "sqlite,mw", "sqlite and/or mw")
	flag.Parse()
	d := *dir
	if d == "" {
		var err error
		d, err = os.MkdirTemp(".", "mwbench-")
		if err != nil {
			panic(err)
		}
		defer os.RemoveAll(d)
	}
	fmt.Printf("%-8s %-7s %7s %10s %12s %9s %9s %9s\n", "variant", "workload", "writers", "tx/s", "retries/100", "p50 ms", "p99 ms", "p99.9 ms")
	for _, ws := range strings.Split(*writersF, ",") {
		w, _ := strconv.Atoi(ws)
		for _, v := range strings.Split(*variants, ",") {
			run(v, *workload, w, time.Duration(*seconds*float64(time.Second)), d)
		}
	}
}

func open(variant, path string, writers int) *sql.DB {
	var db *sql.DB
	var err error
	if variant == "mw" {
		db, err = multiwriter.Open(path, multiwriter.Options{})
	} else {
		db, err = sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(10000)&_txlock=immediate")
	}
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(writers + 1)
	db.SetMaxIdleConns(writers + 1)
	return db
}

func run(variant, workload string, writers int, dur time.Duration, dir string) {
	path, _ := filepath.Abs(filepath.Join(dir, fmt.Sprintf("%s-%s-%d.db", variant, workload, writers)))
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		os.Remove(p)
	}
	db := open(variant, path, writers)
	defer db.Close()
	ctx := context.Background()
	switch workload {
	case "bulk":
		for w := 0; w < writers; w++ {
			db.Exec(fmt.Sprintf("CREATE TABLE t%d(id INTEGER PRIMARY KEY, a INT, b TEXT)", w))
		}
	case "groups", "hot":
		db.Exec("CREATE TABLE r(id INTEGER PRIMARY KEY, n INT, pad TEXT)")
		for i := 0; i < writers || i < 4; i++ {
			db.Exec("INSERT INTO r VALUES(?, 0, ?)", i, strings.Repeat("p", 100))
		}
	}
	var commits, retries atomic.Int64
	lat := make([][]float64, writers)
	deadline := time.Now().Add(dur)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			conn, err := db.Conn(ctx)
			if err != nil {
				panic(err)
			}
			defer conn.Close()
			i := 0
			for time.Now().Before(deadline) {
				t0 := time.Now()
				for attempt := 0; ; attempt++ {
					err := txOnce(ctx, conn, workload, w, i)
					if err == nil {
						break
					}
					if !multiwriter.IsBusySnapshot(err) || attempt > 1000 {
						fmt.Fprintln(os.Stderr, "error:", err)
						return
					}
					retries.Add(1)
				}
				lat[w] = append(lat[w], float64(time.Since(t0).Microseconds())/1000)
				commits.Add(1)
				i++
			}
		}(w)
	}
	wg.Wait()
	var all []float64
	for _, l := range lat {
		all = append(all, l...)
	}
	sort.Float64s(all)
	q := func(p float64) float64 {
		if len(all) == 0 {
			return 0
		}
		return all[int(p*float64(len(all)-1))]
	}
	c := commits.Load()
	fmt.Printf("%-8s %-7s %7d %10.0f %12.2f %9.2f %9.2f %9.2f\n", variant, workload, writers, float64(c)/dur.Seconds(),
		100*float64(retries.Load())/float64(max(c, 1)), q(0.5), q(0.99), q(0.999))
}

func txOnce(ctx context.Context, conn *sql.Conn, workload string, w, i int) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	switch workload {
	case "bulk":
		stmt := fmt.Sprintf("INSERT INTO t%d(a, b) VALUES(?, ?)", w)
		for r := 0; r < 100; r++ {
			if _, err = tx.Exec(stmt, i*100+r, "payload-payload-payload-payload"); err != nil {
				break
			}
		}
	case "groups":
		_, err = tx.Exec("UPDATE r SET n = n + 1 WHERE id = ?", w)
	case "hot":
		_, err = tx.Exec("UPDATE r SET n = n + 1 WHERE id = ?", (w+i)%4)
	}
	if err != nil {
		tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		tx.Rollback()
	}
	return err
}
