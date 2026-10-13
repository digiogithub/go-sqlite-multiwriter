# go-sqlite-multiwriter

Many concurrent writers on **one** SQLite database, in **pure Go** (no cgo).

A Go port of the engine of [sqliteai/sqlite-multiwriter](https://github.com/sqliteai/sqlite-multiwriter)
(article: [We solved SQLite's single-writer limitation](https://marcobambini.substack.com/p/we-solved-sqlites-single-writer-limitation)).
Like the original, it is a **VFS**: your SQL, your schema and your `.db` file do not change, and the file is a
plain SQLite database again once the last connection closes.

Sister project in Rust: [rust-sqlite-multiwriter](https://github.com/digiogithub/rust-sqlite-multiwriter).

It runs on [`modernc.org/sqlite`](https://gitlab.com/cznic/sqlite) (SQLite translated to Go), so it builds with
`CGO_ENABLED=0` for every platform modernc supports.

```go
import (
    multiwriter "github.com/digiogithub/go-sqlite-multiwriter"
)

db, err := multiwriter.Open("app.db", multiwriter.Options{}) // *sql.DB: every connection of the pool is a writer
...
err = multiwriter.RunTx(ctx, db, nil, func(tx *sql.Tx) error {
    _, err := tx.Exec("UPDATE account SET balance = balance + 10 WHERE id = ?", 7)
    return err
}) // retries the whole transaction when it loses against another commit (SQLITE_BUSY_SNAPSHOT)
```

It can also be used directly with the modernc driver: import the package (it registers the VFS) and open the URI
`file:app.db?vfs=multiwriter`.

## How it works

```
writer 1 --> private WAL --+
writer 2 --> private WAL --+--> validation and publication --> commit log --> .db file
writer 3 --> private WAL --+    (first committer wins)                       (compaction)
```

* **Private lanes.** Every connection has its own in-memory WAL and its own wal-index: SQLite believes it is the
  only writer, so nobody waits for the WAL write lock. Writes to the main file are discarded; only the compactor
  writes the `.db`.
* **Snapshots.** Every transaction reads a snapshot (an epoch). Committed pages are kept in a store of
  `(page, epoch)` versions with striped locks; a read takes the newest version ≤ its snapshot, or the page of the
  file.
* **Commit.** When the commit frame arrives it is validated: (1) the schema cookie did not change; (2) no page the
  transaction **read** has a newer version (prevents write skew; an interior table page whose routing did not
  change is forgiven); (3) no page it **wrote** has a newer version. On failure the transaction is refused with
  `SQLITE_BUSY_SNAPSHOT` and the application retries it.
* **Relocation.** When the only conflict is that other commits also grew the file (concurrent inserts), the new
  pages are renumbered above the current end, the references to them rewritten and page 1 merged.
* **Interior page merge.** Two transactions that split different leaves of the same parent are merged three-way
  on the parent.
* **Durability.** Every commit goes into a segmented log (`<db>-mw`, `<db>-mw.000001`...) with group commit
  (`synchronous=FULL` by default: the commit is on disk when it returns). The log is compacted into the `.db` in
  the background. After a crash, recovery replays the valid prefix of the log.
* **Visible, then durable.** A commit becomes visible to new snapshots as soon as its record is written, while
  its fsync runs; its own `COMMIT` returns once it is on the disk. A snapshot taken during that fsync would
  otherwise be stale and bound to lose on any shared page: with 16 writers on the same pages this alone is 3–4×
  the throughput. An acknowledged commit never depends on one that could be lost (a commit that read another one
  is acknowledged only after an fsync round that started after that one was written, and a failed fsync fails
  every later commit). The one difference with stock SQLite: after a power failure, a *read-only* transaction may
  have seen a commit whose `COMMIT` never returned. `mw_durable_visible=1` (`Options.DurableVisibility`) restores
  the strict order. The multi-process mode always works this way.
* **Hot-spot turn.** A connection that keeps losing takes a per-database turn before its next snapshot, so the
  retries of a contended spot run one after the other on fresh state. The turn is handed on as soon as the commit
  is visible, not after its fsync.
* **Rebase** (`mw_rebase=1`, optional). Two writers changing different rows can land on the same page. With the
  rebase, the losing commit is not refused: its row changes are decoded straight from the pages and replayed with
  ordinary SQL on a helper connection at the latest state (several requests batched into one transaction). If the
  row itself changed meanwhile (a real conflict) it is refused as before.
* **Schema barrier.** A `CREATE/DROP/ALTER/...` (detected with `sqlite3_trace_v2`) waits for the writers in flight
  and holds new ones back while it runs.
* **Multi-process** (`mw_mp=1`, optional, unix). Several processes write the same database. The shared log is the
  source of truth: every process applies the commits of the others into its private page store before a snapshot
  needs them. Publication is serialised across processes by an `fcntl` byte lock (released by the kernel if the
  holder dies; the next holder finishes a complete record or takes back a torn one). A shared mapped header holds
  the committed epoch, the log cursor and a registry of the open snapshots of every process (dead processes are
  ignored), so compaction never overwrites a page that somebody still reads.

Isolation: snapshot isolation with first-committer-wins per page, plus validation of the pages read.

## Port status

| Feature of the original | Status |
|---|---|
| Private lanes (private WAL + wal-index per connection) | ✅ |
| Version store, snapshots, read/write/schema validation | ✅ |
| Relocation of new pages, interior page merge, routing shortcut | ✅ |
| Durable log with group commit, crash recovery, compaction, GC | ✅ |
| DDL barrier, hot-spot turn, unsupported pragmas refused | ✅ |
| **Rebase** (`mw_rebase=1`): row decoding, catalog, "point" statements by bytecode, batched replay, publication gate | ✅ |
| **Multi-process** (`mw_mp=1`): shared log and header, cross-process publication lock, snapshot registry, crash repair | ✅ (unix) |

The `-mw*` files are not format-compatible with those of the C original (they need not be: they only exist while
the database is open; when the last connection closes everything is in the `.db`).

## Performance

`go run ./cmd/mwbench -workload bulk|groups|hot -writers 1,4,16` — 4-core machine, local disk,
`synchronous=FULL`, everything on modernc. `sqlite` = stock SQLite in WAL mode with `BEGIN IMMEDIATE` and
`busy_timeout`; `mw` = the engine; `mwr` = the engine with rebase. "retries" = application retries per 100
transactions.

| workload | writers | SQLite tx/s | mw tx/s | mwr tx/s | SQLite p99.9 | mw p99.9 | mwr p99.9 |
|---|---:|---:|---:|---:|---:|---:|---:|
| **bulk** (100 inserts/tx, own table) | 1 | **1,173** | 1,033 | 930 | 6 ms | 5 ms | 7 ms |
| | 4 | 1,106 | 1,761 | **1,825** | 632 ms | 17 ms | 17 ms |
| | 16 | 1,107 | **1,815** | 1,779 | 2,837 ms | 47 ms | 40 ms |
| **groups** (each its own row, same page) | 1 | **4,733** | 4,268 | 3,382 | 1.7 ms | 4 ms | 4 ms |
| | 4 | 4,531 | **8,364** (35 retries) | 6,755 (0 retries) | 2.5 ms | 4.8 ms | 5.8 ms |
| | 16 | 4,253 | **14,138** (128 retries) | 10,849 (0 retries) | 1,233 ms | 24 ms | 9.8 ms |
| **hot** (4 shared rows: real conflict) | 1 | **4,711** | 4,484 | 4,350 | 1.8 ms | 3.2 ms | 3.5 ms |
| | 4 | 4,209 | **8,157** (35 retries) | 6,783 | 3.8 ms | 4.8 ms | 6.7 ms |
| | 16 | 3,966 | **13,952** (124 retries) | 11,523 | 1,133 ms | 10 ms | 11 ms |

* With several writers throughput rises 1.6–3.5× and, above all, tail latency drops from seconds to
  milliseconds (SQLite serialises the writers behind a single lock).
* With a single writer the cost is close to SQLite (5–12 %: the engine's own record and fsync per commit).
* The rebase trades throughput for no retries at all: with commits visible before their fsync a retry is cheap and
  plain `mw` is faster; the rebase pays when a transaction is expensive to run again in the application.
* With `mw_durable_visible=1` (strict order) the contended workloads fall back to about SQLite's throughput
  (`groups`/`hot` with 16 writers: about 4,000 tx/s and 360 retries per 100 transactions); `bulk` does not change.

**Multi-process** (`go run ./cmd/mwbench -procs 4 -variants sqlite,mwp,mwpr ...`: the writers spread over 4
processes; `mwp` = the engine in multi-process mode, `mwpr` = the same with rebase; stock SQLite shares the database
between processes natively):

| workload | writers (4 processes) | SQLite tx/s | mwp tx/s | mwpr tx/s | SQLite p99.9 | mwp/mwpr p99.9 |
|---|---:|---:|---:|---:|---:|---:|
| **bulk** | 4 | 1,160 | **2,569** | 2,265 | 35 ms | 33 ms |
| | 16 | 1,315 | **2,973** | 2,854 | 3,742 ms | 47 ms |
| **groups** | 4 | 4,656 | 6,572 | **6,596** (0 retries) | 2.4 ms | 5.4 ms |
| | 16 | 4,391 | 8,408 (141 retries) | **9,113** (0 retries) | 1,232 ms | 34 ms |
| **hot** | 4 | 4,854 | **6,884** | 6,637 | 3.3 ms | 6.2 ms |
| | 16 | 4,314 | **8,385** | 8,279 | 1,332 ms | 34 ms |

* Across processes the engine doubles SQLite on growing tables and gives 1.4–2× on small transactions, with the
  tail latency in tens of milliseconds instead of seconds.
* In a single process the multi-process mode costs nothing measurable with one writer (3,991 vs 4,459 tx/s on
  `groups`, within the noise of the fsync); with many writers on the same pages the default mode is faster
  (16 writers, `hot`: 16,074 vs 10,374 tx/s), since its publication is not serialised by a lock.

## Multi-process

```go
db, err := multiwriter.Open("app.db", multiwriter.Options{MultiProcess: true})
```

Every process must open the database in multi-process mode (`mw_mp=1`); a process that opens it the other way
while it is open is refused. The first process to open recovers the log; the last one to close
compacts everything into the `.db` and removes the shared files (`<db>-mw*`, `<db>-mwlock`, and on macOS and the
BSDs `<db>-mwfcntl`, where the cross-process byte locks live because flock and fcntl locks interact there). A process killed at
any point (even in the middle of a commit) leaves nothing to repair by hand: the others carry on, and acknowledged
commits are never lost.

Each process keeps its own copy of the recent page versions, applied from the log on demand, so memory and the
cost of catching up grow with the write rate of the other processes. Up to 2048 snapshots can be open at once
over all processes.

## Options (URI)

| Parameter | Meaning |
|---|---|
| `vfs=multiwriter` | enables the engine (`mw=0` disables it for that database) |
| `mw_rebase=1` | replays a commit that lost only because it shared a page (`Options.Rebase`) |
| `mw_rebase_backoff=0` | replays every conflict (by default, after a refusal 1, 3, 7… are skipped so as not to waste work on hot rows) |
| `mw_mp=1` | multi-process mode (`Options.MultiProcess`) |
| `mw_durable_visible=1` | a commit becomes visible only once it is on the disk, like stock SQLite (`Options.DurableVisibility`) |
| `mw_readcheck=0` | do not validate the pages read: fewer retries, but allows write skew |
| `mw_log_max_mb` | log written since the last compaction that triggers the next one (32 MB) |
| `mw_base_cache_mb` | in-memory cache of the pages of the file (64 MB) |
| `mw_gc` | commits between two version collections (64) |
| `mw_noreloc`, `mw_nomerge`, `mw_noroute` | disable relocation / merge / routing shortcut (for measurement) |

## Limits

* WAL only; no `locking_mode=EXCLUSIVE` nor `auto_vacuum`. Not on network file systems.
* While it is open, the database must only be used through the engine (the `-mw*` log holds commits that are not
  in the `.db` yet).
* A transaction that fails with `SQLITE_BUSY_SNAPSHOT` must be retried **whole** (`RunTx` does it).
* A reader that never ends prevents compaction (as in the original).
* The rebase only applies to "blind" transactions: `INSERT ... VALUES` and `UPDATE`/`DELETE` of one row by its
  key, with no prior `SELECT` (a `SELECT` followed by an `UPDATE` is retried: it prevents write skew). It does not
  apply with DDL, triggers, virtual tables, `AUTOINCREMENT`, circular foreign keys, non-UTF-8 encodings, or when
  the application installs its own `sqlite3_trace_v2`. Two `INSERT`s without an explicit key into the same table
  pick the same `rowid` and count as a real conflict. `MW_DEBUG=1` shows why something is refused.
* Multi-process mode is unix only (Linux, macOS, BSD).

## Tests

```
go test ./...            # concurrency, invariants, write skew, DDL, crash and recovery, multi-process (kills included)
go test -race ./...
go run ./cmd/mwbench -workload bulk|groups|hot -writers 1,4,16
```

Apache 2.0 license (see `LICENSE` and `NOTICE`).
