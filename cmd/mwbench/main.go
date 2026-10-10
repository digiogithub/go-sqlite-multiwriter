// mwbench compares the engine with stock SQLite (WAL, synchronous=FULL), both through modernc.org/sqlite.
//
//	go run ./cmd/mwbench -workload bulk -writers 1,4,16 -seconds 5
//
// Variants: sqlite (stock, WAL, BEGIN IMMEDIATE and a busy timeout), mw (the engine), mwr (the engine with the rebase),
// mwp and mwpr (the engine in multi-process mode, without and with the rebase).
//
// -procs P spreads the writers over P processes (each one runs writers/P of them); the variants that cannot be shared
// by several processes (mw, mwr) are skipped then.
// Workloads: bulk (each writer inserts 100 rows per transaction into its own table), groups (each writer updates
// its own row; the rows share pages), hot (every writer increments one of 4 rows: true conflicts).
package main

import (
	"bufio"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/pprof"
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
	variants := flag.String("variants", "sqlite,mw,mwr", "sqlite, mw (the engine), mwr (with the rebase), mwp/mwpr (multi-process mode)")
	procs := flag.Int("procs", 1, "number of processes the writers are spread over")
	flag.BoolVar(&durableVisible, "durable-visible", false, "engine variants: a commit becomes visible only once durable")
	cpuprof := flag.String("cpuprofile", "", "write a CPU profile of this process to the file")
	flag.Parse()
	if *cpuprof != "" {
		f, err := os.Create(*cpuprof)
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	if os.Getenv("MWBENCH_CHILD") != "" {
		child()
		return
	}
	d := *dir
	if d == "" {
		var err error
		d, err = os.MkdirTemp(".", "mwbench-")
		if err != nil {
			panic(err)
		}
		defer os.RemoveAll(d)
	}
	fmt.Printf("%-8s %-7s %5s %7s %10s %12s %9s %9s %9s\n", "variant", "workload", "procs", "writers", "tx/s", "retries/100", "p50 ms", "p99 ms", "p99.9 ms")
	for _, ws := range strings.Split(*writersF, ",") {
		w, _ := strconv.Atoi(ws)
		for _, v := range strings.Split(*variants, ",") {
			if *procs > 1 && (v == "mw" || v == "mwr") {
				continue
			}
			run(v, *workload, w, *procs, time.Duration(*seconds*float64(time.Second)), d)
		}
	}
}

var durableVisible bool

func open(variant, path string, writers int) *sql.DB {
	var db *sql.DB
	var err error
	if strings.HasPrefix(variant, "mw") {
		db, err = multiwriter.Open(path, multiwriter.Options{
			Rebase:            strings.HasSuffix(variant, "r"),
			MultiProcess:      strings.HasPrefix(variant, "mwp"),
			DurableVisibility: durableVisible,
		})
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

func run(variant, workload string, writers, procs int, dur time.Duration, dir string) {
	path, _ := filepath.Abs(filepath.Join(dir, fmt.Sprintf("%s-%s-%d.db", variant, workload, writers)))
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		os.Remove(p)
	}
	db := open(variant, path, writers)
	defer db.Close()
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
	var c, r int64
	var all []float64
	t0 := time.Now().Add(100 * time.Millisecond)
	if procs <= 1 {
		ids := make([]int, writers)
		for i := range ids {
			ids[i] = i
		}
		c, r, all = work(db, workload, ids, t0, t0.Add(dur))
	} else {
		// the processes open the database first; the run starts for all of them at the same instant
		t0 = time.Now().Add(2 * time.Second)
		var mu sync.Mutex
		var wg sync.WaitGroup
		for p := 0; p < procs; p++ {
			var ids []string
			for w := p; w < writers; w += procs {
				ids = append(ids, strconv.Itoa(w))
			}
			if len(ids) == 0 {
				continue
			}
			cmd := exec.Command(os.Args[0])
			cmd.Env = append(os.Environ(), "MWBENCH_CHILD=1", "MWBENCH_VARIANT="+variant, "MWBENCH_WORKLOAD="+workload,
				"MWBENCH_DB="+path, "MWBENCH_IDS="+strings.Join(ids, ","), "MWBENCH_POOL="+strconv.Itoa(writers),
				"MWBENCH_START="+strconv.FormatInt(t0.UnixNano(), 10), "MWBENCH_END="+strconv.FormatInt(t0.Add(dur).UnixNano(), 10))
			cmd.Stderr = os.Stderr
			out, err := cmd.StdoutPipe()
			if err != nil {
				panic(err)
			}
			if err := cmd.Start(); err != nil {
				panic(err)
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				sc := bufio.NewScanner(out)
				sc.Buffer(nil, 1<<30)
				var lc, lr int64
				var ll []float64
				for sc.Scan() {
					f := strings.Fields(sc.Text())
					if len(f) == 3 && f[0] == "done" {
						lc, _ = strconv.ParseInt(f[1], 10, 64)
						lr, _ = strconv.ParseInt(f[2], 10, 64)
					} else if len(f) == 1 {
						v, _ := strconv.ParseFloat(f[0], 64)
						ll = append(ll, v)
					}
				}
				cmd.Wait()
				mu.Lock()
				c, r, all = c+lc, r+lr, append(all, ll...)
				mu.Unlock()
			}()
		}
		wg.Wait()
	}
	var chk string
	if err := db.QueryRow("PRAGMA quick_check").Scan(&chk); err != nil || chk != "ok" {
		fmt.Fprintf(os.Stderr, "%s %s %d: quick_check failed: %.200s %v\n", variant, workload, writers, chk, err)
	}
	sort.Float64s(all)
	q := func(p float64) float64 {
		if len(all) == 0 {
			return 0
		}
		return all[int(p*float64(len(all)-1))]
	}
	fmt.Printf("%-8s %-7s %5d %7d %10.0f %12.2f %9.2f %9.2f %9.2f\n", variant, workload, max(procs, 1), writers, float64(c)/dur.Seconds(),
		100*float64(r)/float64(max(c, 1)), q(0.5), q(0.99), q(0.999))
}

// child runs some writers of a multi-process run and prints its latencies, then "done <commits> <retries>".
func child() {
	var ids []int
	for _, s := range strings.Split(os.Getenv("MWBENCH_IDS"), ",") {
		w, _ := strconv.Atoi(s)
		ids = append(ids, w)
	}
	pool, _ := strconv.Atoi(os.Getenv("MWBENCH_POOL"))
	db := open(os.Getenv("MWBENCH_VARIANT"), os.Getenv("MWBENCH_DB"), pool)
	defer db.Close()
	if _, err := db.Exec("SELECT 1"); err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	ns := func(k string) time.Time { v, _ := strconv.ParseInt(os.Getenv(k), 10, 64); return time.Unix(0, v) }
	c, r, lat := work(db, os.Getenv("MWBENCH_WORKLOAD"), ids, ns("MWBENCH_START"), ns("MWBENCH_END"))
	w := bufio.NewWriter(os.Stdout)
	for _, l := range lat {
		fmt.Fprintf(w, "%.3f\n", l)
	}
	fmt.Fprintf(w, "done %d %d\n", c, r)
	w.Flush()
}

// work runs the writers ids from start to deadline: commits, retries and the latency of every transaction (ms).
func work(db *sql.DB, workload string, ids []int, start, deadline time.Time) (int64, int64, []float64) {
	ctx := context.Background()
	var commits, retries atomic.Int64
	lat := make([][]float64, len(ids))
	var wg sync.WaitGroup
	for k, w := range ids {
		wg.Add(1)
		go func(k, w int) {
			defer wg.Done()
			conn, err := db.Conn(ctx)
			if err != nil {
				panic(err)
			}
			defer conn.Close()
			time.Sleep(time.Until(start))
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
				lat[k] = append(lat[k], float64(time.Since(t0).Microseconds())/1000)
				commits.Add(1)
				i++
			}
		}(k, w)
	}
	wg.Wait()
	var all []float64
	for _, l := range lat {
		all = append(all, l...)
	}
	return commits.Load(), retries.Load(), all
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
