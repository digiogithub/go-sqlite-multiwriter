package multiwriter

// Per-database state shared by every connection (lane) of this process that opened the same file. The registry is
// keyed by the full path SQLite hands to xOpen.

import (
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type mwDB struct {
	path string
	refs int // protected by registryMu

	openMu   sync.Mutex // the first lane creates the store and opens the log
	store    *store
	log      *wlog
	realFile *os.File // the database file: reads of pages the store does not hold, and compaction

	mu     sync.Mutex // the snapshot registry
	active map[*lane]struct{}

	epoch     atomic.Uint64 // last visible (committed and durable) epoch
	nextEpoch atomic.Uint64 // last assigned epoch (>= epoch; the difference is in flight)
	visMu     sync.Mutex
	visCond   *sync.Cond
	visParked atomic.Int32

	schemaGen       atomic.Uint64
	lastSchemaEpoch atomic.Uint64

	relocMu      sync.Mutex // serialises page relocations (they all chase the same end of the file)
	p1Cache      []byte     // the page 1 the last relocation installed, and its epoch (relocMu)
	p1CacheEpoch uint64

	hotHeld  atomic.Int32 // the hot-spot turn
	ddlOwner atomic.Pointer[lane]

	failed atomic.Bool // sticky I/O failure: no further commits until the database is reopened

	compactMu     sync.Mutex
	compactorKick chan struct{}
	compactorStop chan struct{}
	compactorDone chan struct{}
	logMaxBytes   atomic.Int64
	gcInterval    atomic.Int64
	pubSinceGC    atomic.Int64

	// statistics
	nCommits, nAborts, nSnapshots, nPageConflicts, nReadConflicts, nSchemaConflicts atomic.Int64
	nRelocations, nMerges, nReadsSaved, nCompactions, nCompactedPages, nLogSyncs    atomic.Int64
	nHotSerialised, nBackpressure, nPagesPublished                                  atomic.Int64
}

var (
	registryMu sync.Mutex
	registry   = map[string]*mwDB{}
)

func acquireDB(path string) *mwDB {
	registryMu.Lock()
	defer registryMu.Unlock()
	if db := registry[path]; db != nil {
		db.refs++
		return db
	}
	db := &mwDB{path: path, refs: 1, active: map[*lane]struct{}{}}
	db.visCond = sync.NewCond(&db.visMu)
	db.epoch.Store(1) // epoch 1 = the state found on disk at the first open
	db.nextEpoch.Store(1)
	db.schemaGen.Store(1)
	db.logMaxBytes.Store(32 << 20)
	db.gcInterval.Store(64)
	registry[path] = db
	return db
}

func lookupDB(path string) *mwDB {
	registryMu.Lock()
	defer registryMu.Unlock()
	return registry[path]
}

// releaseDB drops a reference. The last one stops the compactor, materialises everything into the real file (it is
// then an ordinary SQLite database again) and removes the log. A failed database keeps its log for recovery.
func releaseDB(db *mwDB) {
	registryMu.Lock()
	defer registryMu.Unlock()
	db.refs--
	if db.refs > 0 {
		return
	}
	delete(registry, db.path)
	db.stopCompactor()
	clean := false
	if db.store != nil && db.log != nil && !db.failed.Load() {
		if _, err := db.compact(); err == nil {
			clean = db.log.baseEpoch() == db.epoch.Load() && db.nextEpoch.Load() == db.epoch.Load()
		}
	}
	if db.log != nil {
		db.log.close(clean)
	}
	if db.realFile != nil {
		db.realFile.Close()
	}
}

// MARK: - snapshots

func (db *mwDB) visibleEpoch() uint64 { return db.epoch.Load() }

// oldestActive is the oldest snapshot in use (the visible epoch when none is).
func (db *mwDB) oldestActive() uint64 {
	db.mu.Lock()
	oldest := db.visibleEpoch()
	for l := range db.active {
		if l.tx.snapshotEpoch < oldest {
			oldest = l.tx.snapshotEpoch
		}
	}
	db.mu.Unlock()
	return oldest
}

// MARK: - visibility ordering

// makeVisible: epoch E becomes visible only after E-1 did, so readers never observe a hole.
func (db *mwDB) makeVisible(e uint64) int {
	if db.epoch.Load() >= e {
		return rcOK
	}
	for spin := 0; spin < 200 && db.epoch.Load() < e-1; spin++ {
		if db.failed.Load() {
			return rcIOErr
		}
		runtime.Gosched()
	}
	if db.epoch.Load() < e-1 {
		db.visMu.Lock()
		db.visParked.Add(1)
		for db.epoch.Load() < e-1 {
			if db.failed.Load() {
				db.visParked.Add(-1)
				db.visMu.Unlock()
				return rcIOErr
			}
			db.visCond.Wait()
		}
		db.visParked.Add(-1)
		db.visMu.Unlock()
	}
	db.epoch.CompareAndSwap(e-1, e)
	if db.visParked.Load() > 0 {
		db.visMu.Lock()
		db.visCond.Broadcast()
		db.visMu.Unlock()
	}
	return rcOK
}

func (db *mwDB) wakeAllVisibility() {
	db.visMu.Lock()
	db.visCond.Broadcast()
	db.visMu.Unlock()
}

// MARK: - garbage collection

// gc reclaims versions that no active or future snapshot can observe: a version is reclaimable when a newer one is
// visible to every snapshot that can still exist. If the newest version is in the real file already and old enough,
// the whole chain goes.
func (db *mwDB) gc() int64 {
	st := db.store
	if st == nil {
		return 0
	}
	db.pubSinceGC.Store(0)
	oldest := db.oldestActive()
	st.seqMu.Lock()
	compacted := st.compactedEpoch
	st.seqMu.Unlock()

	st.listMu.Lock()
	cand := st.cand
	st.cand = nil
	st.listMu.Unlock()

	var keep []uint32
	var reclaimed int64
	for _, pgno := range cand {
		c := st.chainGet(pgno, false)
		mu := st.stripeOf(pgno)
		if !mu.TryLock() {
			keep = append(keep, pgno) // a committer holds it: the next collection looks at it
			continue
		}
		i := c.find(oldest)
		drop := 0
		if i > 0 {
			drop = i
		}
		if n := len(c.v); n > 0 && c.v[n-1].epoch <= oldest && c.v[n-1].epoch <= compacted {
			drop = n
		}
		for j := 0; j < drop; j++ {
			if c.v[j].epoch == 0 {
				st.baseBytes.Add(-int64(st.pgsz))
			}
			c.v[j].data = nil
			st.bytes.Add(-int64(st.pgsz))
			st.versions.Add(-1)
			reclaimed++
		}
		if drop > 0 {
			c.v = append(c.v[:0], c.v[drop:]...)
		}
		if len(c.v) > 0 {
			keep = append(keep, pgno)
		} else {
			c.queued = false
			c.v = nil
		}
		mu.Unlock()
	}
	if len(keep) > 0 {
		st.listMu.Lock()
		st.cand = append(st.cand, keep...)
		st.listMu.Unlock()
	}
	// size records: keep the newest one visible to oldest and everything after it
	st.seqMu.Lock()
	si := -1
	for i := 0; i < len(st.sizes) && st.sizes[i].epoch <= oldest; i++ {
		si = i
	}
	if si > 0 {
		st.sizes = append(st.sizes[:0], st.sizes[si:]...)
	}
	st.seqMu.Unlock()
	st.reclaimed.Add(reclaimed)
	st.gcRuns.Add(1)
	return reclaimed
}

// MARK: - background compactor

// The compactor runs when the log outgrew logMaxBytes (publishers kick it), and checks once a second.
func (db *mwDB) startCompactor() {
	if db.compactorStop != nil {
		return
	}
	db.compactorKick = make(chan struct{}, 1)
	db.compactorStop = make(chan struct{})
	db.compactorDone = make(chan struct{})
	go func() {
		defer close(db.compactorDone)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-db.compactorStop:
				return
			case <-db.compactorKick:
			case <-t.C:
			}
			if db.log != nil && db.log.bytes() > db.logMaxBytes.Load() {
				db.compact()
			}
		}
	}()
}

func (db *mwDB) kickCompactor() {
	if db.compactorKick == nil {
		return
	}
	select {
	case db.compactorKick <- struct{}{}:
	default:
	}
}

func (db *mwDB) stopCompactor() {
	if db.compactorStop == nil {
		return
	}
	close(db.compactorStop)
	<-db.compactorDone
	db.compactorStop = nil
}
