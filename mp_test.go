//go:build unix

package multiwriter

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func mpOpts() Options { return Options{MultiProcess: true} }

// child processes: MW_MP_CHILD=<role>, MW_MP_DB, MW_MP_ID, MW_MP_N, MW_MP_ACK
func TestMain(m *testing.M) {
	// MW_SEPARATE_BYTE_LOCKS=1 runs the multi-process tests (children included) with the fcntl byte locks on their
	// own file, the layout of the platforms where flock and fcntl locks interact, on any unix.
	if os.Getenv("MW_SEPARATE_BYTE_LOCKS") == "1" {
		separateByteLocks = true
	}
	if role := os.Getenv("MW_MP_CHILD"); role != "" {
		os.Exit(mpChild(role))
	}
	os.Exit(m.Run())
}

func mpChild(role string) int {
	path := os.Getenv("MW_MP_DB")
	id, _ := strconv.Atoi(os.Getenv("MW_MP_ID"))
	n, _ := strconv.Atoi(os.Getenv("MW_MP_N"))
	o := mpOpts()
	o.Rebase = os.Getenv("MW_MP_REBASE") != ""
	db, err := Open(path, o)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	defer db.Close()
	ctx := context.Background()
	switch role {
	case "work":
		// inserts into its own table, increments its own counter and a shared one
		for i := 0; i < n; i++ {
			err := RunTx(ctx, db, nil, func(tx *sql.Tx) error {
				if _, err := tx.Exec(fmt.Sprintf("INSERT INTO t%d(v) VALUES(?)", id), strings.Repeat("x", 100+i%300)); err != nil {
					return err
				}
				if _, err := tx.Exec("UPDATE c SET n = n + 1 WHERE id = ?", id); err != nil {
					return err
				}
				_, err := tx.Exec("UPDATE c SET n = n + 1 WHERE id = 1000")
				return err
			})
			if err != nil {
				fmt.Fprintln(os.Stderr, "child", id, err)
				return 3
			}
		}
		return 0
	case "loop":
		// increments its own row forever, acknowledging every commit in a file
		ack, _ := os.OpenFile(os.Getenv("MW_MP_ACK"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
		for {
			err := RunTx(ctx, db, nil, func(tx *sql.Tx) error {
				_, err := tx.Exec("UPDATE c SET n = n + 1 WHERE id = ?", id)
				return err
			})
			if err != nil {
				fmt.Fprintln(os.Stderr, "child", id, err)
				return 3
			}
			ack.WriteString("1\n")
		}
	case "bulk":
		// n writers, each inserting 100 rows per transaction into its own table (the file grows from every process)
		var wg sync.WaitGroup
		failed := make(chan error, n)
		deadline := time.Now().Add(2 * time.Second)
		for k := 0; k < n; k++ {
			wg.Add(1)
			go func(tab int) {
				defer wg.Done()
				for i := 0; time.Now().Before(deadline); i++ {
					err := RunTx(ctx, db, nil, func(tx *sql.Tx) error {
						for r := 0; r < 100; r++ {
							if _, err := tx.Exec(fmt.Sprintf("INSERT INTO t%d(v) VALUES(?)", tab), strings.Repeat("y", 30)); err != nil {
								return err
							}
						}
						return nil
					})
					if err != nil {
						failed <- fmt.Errorf("table %d, transaction %d: %w", tab, i, err)
						return
					}
				}
			}(id*n + k)
		}
		wg.Wait()
		close(failed)
		rc := 0
		for err := range failed {
			fmt.Fprintln(os.Stderr, "child", id, err)
			rc = 3
		}
		return rc
	case "bigloop":
		ack, _ := os.OpenFile(os.Getenv("MW_MP_ACK"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
		for i := 0; ; i++ {
			err := RunTx(ctx, db, nil, func(tx *sql.Tx) error {
				if _, err := tx.Exec(fmt.Sprintf("INSERT INTO t%d(v) VALUES(?)", id), strings.Repeat("z", 120000)); err != nil {
					return err
				}
				_, err := tx.Exec("UPDATE c SET n = n + 1 WHERE id = ?", id)
				return err
			})
			if err != nil {
				fmt.Fprintln(os.Stderr, "child", id, err)
				return 3
			}
			ack.WriteString("1\n")
		}
	case "hold":
		// keeps the database open (and a snapshot) until killed
		tx, _ := db.Begin()
		var x int
		tx.QueryRow("SELECT count(*) FROM c").Scan(&x)
		time.Sleep(time.Hour)
	}
	return 1
}

func spawn(t *testing.T, role, path string, id, n int, extra ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run", "^$")
	cmd.Env = append(os.Environ(), "MW_MP_CHILD="+role, "MW_MP_DB="+path, fmt.Sprintf("MW_MP_ID=%d", id), fmt.Sprintf("MW_MP_N=%d", n))
	cmd.Env = append(cmd.Env, extra...)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return cmd
}

func mpSetup(t *testing.T, path string, workers int) *sql.DB {
	db := openT(t, path, mpOpts())
	mustExec(t, db, "CREATE TABLE c(id INTEGER PRIMARY KEY, n INT, pad TEXT)")
	mustExec(t, db, "INSERT INTO c VALUES(1000, 0, '')")
	for w := 0; w < workers; w++ {
		mustExec(t, db, fmt.Sprintf("CREATE TABLE t%d(id INTEGER PRIMARY KEY, v TEXT)", w))
		mustExec(t, db, "INSERT INTO c VALUES(?, 0, ?)", w, strings.Repeat("p", 3000)) // (one row per page)
	}
	return db
}

// Several processes (and the parent) write the same database at the same time.
func TestMPProcesses(t *testing.T) {
	for _, rebase := range []bool{false, true} {
		t.Run(fmt.Sprintf("rebase=%v", rebase), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "mp.db")
			const P, N = 4, 150
			db := mpSetup(t, path, P+1)
			var extra []string
			if rebase {
				extra = append(extra, "MW_MP_REBASE=1")
			}
			var cmds []*exec.Cmd
			for p := 0; p < P; p++ {
				cmds = append(cmds, spawn(t, "work", path, p, N, extra...))
			}
			// the parent works too, through its own pool
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < N; i++ {
					if err := RunTx(context.Background(), db, nil, func(tx *sql.Tx) error {
						if _, err := tx.Exec(fmt.Sprintf("INSERT INTO t%d(v) VALUES('parent')", P)); err != nil {
							return err
						}
						if _, err := tx.Exec("UPDATE c SET n = n + 1 WHERE id = ?", P); err != nil {
							return err
						}
						_, err := tx.Exec("UPDATE c SET n = n + 1 WHERE id = 1000")
						return err
					}); err != nil {
						t.Error(err)
						return
					}
				}
			}()
			for _, c := range cmds {
				if err := c.Wait(); err != nil {
					t.Fatalf("child: %v", err)
				}
			}
			wg.Wait()
			for w := 0; w <= P; w++ {
				var rows, cnt int
				db.QueryRow(fmt.Sprintf("SELECT count(*) FROM t%d", w)).Scan(&rows)
				db.QueryRow("SELECT n FROM c WHERE id = ?", w).Scan(&cnt)
				if rows != N || cnt != N {
					t.Fatalf("writer %d: %d rows, counter %d, want %d", w, rows, cnt, N)
				}
			}
			var total int
			db.QueryRow("SELECT n FROM c WHERE id = 1000").Scan(&total)
			if total != (P+1)*N {
				t.Fatalf("shared counter %d, want %d (lost updates)", total, (P+1)*N)
			}
			integrity(t, db)
			s, _ := GetStats(path)
			t.Logf("this process: commits=%d aborts=%d relocations=%d rebases=%d compactions=%d", s.Commits, s.Aborts, s.Relocations, s.Rebases, s.Compactions)
			db.Close()
			if sc := sidecars(path); len(sc) != 0 {
				t.Fatalf("files left after the last process: %v", sc)
			}
			st := openStock(t, path)
			defer st.Close()
			integrity(t, st)
			db.QueryRow("SELECT n FROM c WHERE id = 1000").Scan(&total)
		})
	}
}

func countAcks(path string) int {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		n++
	}
	return n
}

// Processes killed with SIGKILL at any moment, also inside the publication lock: the others go on, every
// acknowledged commit is there, at most one commit per killed process is in doubt, and the file is intact.
func TestMPKilled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kill.db")
	const P = 4
	db := mpSetup(t, path, P)
	type worker struct {
		cmd   *exec.Cmd
		kills int
	}
	ws := make([]*worker, P)
	ack := func(p int) string { return filepath.Join(dir, fmt.Sprintf("ack%d", p)) }
	for p := 0; p < P; p++ {
		ws[p] = &worker{cmd: spawn(t, "loop", path, p, 0, "MW_MP_ACK="+ack(p))}
	}
	deadline := time.Now().Add(3 * time.Second)
	for k := 0; time.Now().Before(deadline); k++ {
		time.Sleep(time.Duration(60+k%5*20) * time.Millisecond)
		w := ws[k%P]
		w.cmd.Process.Signal(syscall.SIGKILL)
		w.cmd.Wait()
		w.kills++
		w.cmd = spawn(t, "loop", path, k%P, 0, "MW_MP_ACK="+ack(k%P))
	}
	for _, w := range ws {
		w.cmd.Process.Signal(syscall.SIGKILL)
		w.cmd.Wait()
		w.kills++
	}
	totalKills := 0
	for p := 0; p < P; p++ {
		var n int
		if err := db.QueryRow("SELECT n FROM c WHERE id = ?", p).Scan(&n); err != nil {
			t.Fatal(err)
		}
		acked := countAcks(ack(p))
		if n < acked || n > acked+ws[p].kills {
			t.Fatalf("process %d: counter %d, acknowledged %d, kills %d", p, n, acked, ws[p].kills)
		}
		totalKills += ws[p].kills
		t.Logf("process %d: counter %d, acknowledged %d, in doubt %d", p, n, acked, n-acked)
	}
	integrity(t, db)
	mustExec(t, db, "UPDATE c SET n = n + 1 WHERE id = 1000")
	db.Close()
	st := openStock(t, path)
	defer st.Close()
	integrity(t, st)
	t.Logf("%d kills", totalKills)
}

// Everything is killed: the next process alone recovers the log.
func TestMPAllKilledRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "all.db")
	const P = 3
	db := mpSetup(t, path, P)
	db.Close()
	var cmds []*exec.Cmd
	for p := 0; p < P; p++ {
		cmds = append(cmds, spawn(t, "loop", path, p, 0, "MW_MP_ACK="+filepath.Join(dir, fmt.Sprintf("ack%d", p))))
	}
	time.Sleep(1500 * time.Millisecond)
	for _, c := range cmds {
		c.Process.Signal(syscall.SIGKILL)
		c.Wait()
	}
	if len(sidecars(path)) == 0 {
		t.Fatal("no log left")
	}
	db = openT(t, path, mpOpts())
	for p := 0; p < P; p++ {
		var n int
		db.QueryRow("SELECT n FROM c WHERE id = ?", p).Scan(&n)
		acked := countAcks(filepath.Join(dir, fmt.Sprintf("ack%d", p)))
		if n < acked || n > acked+1 {
			t.Fatalf("process %d: counter %d, acknowledged %d", p, n, acked)
		}
	}
	integrity(t, db)
	db.Close()
	st := openStock(t, path)
	defer st.Close()
	integrity(t, st)
}

// A process that keeps an old snapshot holds compaction back; one that is killed does not.
func TestMPDeadReaderDoesNotPin(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pin.db")
	db := mpSetup(t, path, 1)
	defer db.Close()
	h := spawn(t, "hold", path, 0, 0)
	time.Sleep(500 * time.Millisecond)
	for i := 0; i < 50; i++ {
		mustExec(t, db, "UPDATE c SET n = n + 1 WHERE id = 0")
	}
	r1, _ := Compact(path)
	h.Process.Signal(syscall.SIGKILL)
	h.Wait()
	r2, err := Compact(path)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := GetStats(path)
	t.Logf("held: target %d; after the kill: target %d (epoch %d)", r1.TargetEpoch, r2.TargetEpoch, s.Epoch)
	if r2.TargetEpoch < s.Epoch {
		t.Fatalf("a dead process still holds compaction back: target %d, epoch %d", r2.TargetEpoch, s.Epoch)
	}
}

// One database, one mode: a single-process opener is refused while processes share it.
func TestMPModeMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mix.db")
	db := mpSetup(t, path, 1)
	defer db.Close()
	single, _ := Open(path, Options{})
	defer single.Close()
	if _, err := single.Exec("SELECT 1 FROM c"); err == nil {
		t.Fatal("a single-process opener was accepted while the database is open in the processes mode")
	}
}

// Every process grows the file at the same time (relocation of the new pages across processes).
func TestMPBulkGrowth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bulk.db")
	const P, W = 4, 4
	db := mpSetup(t, path, P*W)
	var cmds []*exec.Cmd
	for p := 0; p < P; p++ {
		cmds = append(cmds, spawn(t, "bulk", path, p, W))
	}
	for i, c := range cmds {
		if err := c.Wait(); err != nil {
			t.Errorf("child %d: %v", i, err)
		}
	}
	var res string
	if err := db.QueryRow("PRAGMA integrity_check").Scan(&res); err != nil || res != "ok" {
		if len(res) > 300 {
			res = res[:300]
		}
		t.Errorf("integrity_check: %q %v", res, err)
		db.Close()
		db = openT(t, path, Options{})
		db.QueryRow("PRAGMA integrity_check").Scan(&res)
		if len(res) > 300 {
			res = res[:300]
		}
		db.Close()
		if d := os.Getenv("MW_KEEP"); d != "" {
			b, _ := os.ReadFile(path)
			os.WriteFile(d, b, 0o644)
		}
		t.Fatalf("after reopening: %q", res)
	}
	for w := 0; w < P*W; w++ {
		var n int
		if err := db.QueryRow(fmt.Sprintf("SELECT count(*) FROM t%d", w)).Scan(&n); err != nil || n%100 != 0 {
			t.Fatalf("t%d: %d rows, %v", w, n, err)
		}
	}
}

func TestMPAllKilledBig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "big.db")
	const P = 3
	db := mpSetup(t, path, P)
	db.Close()
	var cmds []*exec.Cmd
	for p := 0; p < P; p++ {
		cmds = append(cmds, spawn(t, "bigloop", path, p, 0, "MW_MP_ACK="+filepath.Join(dir, fmt.Sprintf("ack%d", p))))
	}
	time.Sleep(3000 * time.Millisecond)
	for _, c := range cmds {
		c.Process.Signal(syscall.SIGKILL)
		c.Wait()
	}
	t.Logf("sidecars: %v", sidecars(path))
	db = openT(t, path, mpOpts())
	defer db.Close()
	for p := 0; p < P; p++ {
		var n, rows int
		db.QueryRow("SELECT n FROM c WHERE id = ?", p).Scan(&n)
		db.QueryRow(fmt.Sprintf("SELECT count(*) FROM t%d", p)).Scan(&rows)
		acked := countAcks(filepath.Join(dir, fmt.Sprintf("ack%d", p)))
		t.Logf("process %d: counter %d rows %d acknowledged %d", p, n, rows, acked)
		if n < acked || rows < acked {
			t.Errorf("process %d: counter %d rows %d, acknowledged %d", p, n, rows, acked)
		}
	}
	integrity(t, db)
}
