package multiwriter

// The versioned committed page store.
//
// Every committed page image is kept as a version (page number, commit epoch) until the compactor has written it
// into the database file and no snapshot can still see an older one. A reader at snapshot S reads the newest
// version with an epoch <= S, else the page of the real file.
//
// Locking is striped: the chains of the pages with the same pgno % stripes share one mutex.
//   - readers lock one stripe, copy the bytes out and unlock (a version can never be freed under a reader);
//   - a publisher locks the stripes of every page it wrote or only read (ascending), validates, installs the new
//     versions invisibly (the visible epoch is not advanced yet) and unlocks. Commits on disjoint stripes run in
//     parallel; commits that touch a common page serialise their validate+install;
//   - epochs and log offsets are assigned under seqMu, a tiny critical section.

import (
	"sync"
	"sync/atomic"
)

const (
	numStripes       = 256
	chainBlockBits   = 12
	chainBlockSize   = 1 << chainBlockBits
	defaultBaseLimit = 64 << 20
)

type pageVersion struct {
	epoch uint64
	data  []byte
}

// chain holds the versions of one page in ascending epoch order. Protected by the page's stripe.
type chain struct {
	v      []pageVersion
	dirty  bool // in the dirty list (changed since the last compaction)
	queued bool // in the GC candidate list
}

func (c *chain) head() uint64 {
	if c == nil || len(c.v) == 0 {
		return 0
	}
	return c.v[len(c.v)-1].epoch
}

// find returns the index of the newest version with epoch <= snap, or -1.
func (c *chain) find(snap uint64) int {
	lo, hi, best := 0, len(c.v)-1, -1
	for lo <= hi {
		mid := (lo + hi) / 2
		if c.v[mid].epoch <= snap {
			best = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return best
}

type stripe struct {
	sync.Mutex
	_ [56]byte
}

type sizeRec struct {
	epoch  uint64
	dbsize uint32
}

type store struct {
	pgsz     int
	reserved int // bytes reserved at the end of every page (database header byte 20)

	stripes [numStripes]stripe
	dirMu   sync.Mutex
	dir     atomic.Pointer[[]*[chainBlockSize]chain]

	seqMu          sync.Mutex // epoch assignment, size records, base size, compacted epoch
	sizes          []sizeRec  // the database size (pages) as of an epoch, ascending
	baseDBSize     uint32     // pages in the real file at the compacted epoch
	compactedEpoch uint64     // state <= this epoch is in the real file

	listMu sync.Mutex
	dirty  []uint32 // pages changed since the last compaction
	cand   []uint32 // GC candidates (pages with more than one version, or compacted)

	p1Cookie atomic.Uint32 // schema cookie of the newest committed page 1 (stored before p1Head)
	p1Head   atomic.Uint64 // epoch of the newest committed page 1

	baseBytes atomic.Int64 // clean pages of the real file cached as epoch-0 versions
	baseLimit int64
	versions  atomic.Int64
	bytes     atomic.Int64
	allocated atomic.Int64
	reclaimed atomic.Int64
	gcRuns    atomic.Int64
}

func newStore(pgsz int, baseDBSize uint32) *store {
	s := &store{pgsz: pgsz, baseDBSize: baseDBSize, baseLimit: defaultBaseLimit}
	empty := make([]*[chainBlockSize]chain, 0)
	s.dir.Store(&empty)
	return s
}

func (s *store) stripeOf(pgno uint32) *stripe { return &s.stripes[pgno%numStripes] }

// chainGet returns the chain of pgno. Blocks are never freed or moved, so the pointer stays valid; its contents
// are protected by the page's stripe.
func (s *store) chainGet(pgno uint32, create bool) *chain {
	b := int(pgno >> chainBlockBits)
	dir := *s.dir.Load()
	if b < len(dir) && dir[b] != nil {
		return &dir[b][pgno&(chainBlockSize-1)]
	}
	if !create {
		return nil
	}
	s.dirMu.Lock()
	defer s.dirMu.Unlock()
	dir = *s.dir.Load()
	if b < len(dir) && dir[b] != nil {
		return &dir[b][pgno&(chainBlockSize-1)]
	}
	n := len(dir)
	if b >= n {
		n = b + 1
		if n < 2*len(dir) {
			n = 2 * len(dir)
		}
	}
	nd := make([]*[chainBlockSize]chain, n)
	copy(nd, dir)
	nd[b] = new([chainBlockSize]chain)
	s.dir.Store(&nd)
	return &nd[b][pgno&(chainBlockSize-1)]
}

// read copies bytes [poff, poff+len(dst)) of the newest version of pgno with epoch <= snap. False if none.
func (s *store) read(pgno uint32, snap uint64, poff int, dst []byte) bool {
	c := s.chainGet(pgno, false)
	if c == nil {
		return false
	}
	mu := s.stripeOf(pgno)
	mu.Lock()
	i := c.find(snap)
	if i >= 0 {
		copy(dst, c.v[i].data[poff:poff+len(dst)])
	}
	mu.Unlock()
	return i >= 0
}

// cacheBase remembers a page read from the real file as an epoch-0 version, so the next transaction (whose page
// cache is cold by design) copies it from memory. Only when the chain is empty; bounded by baseLimit.
func (s *store) cacheBase(pgno uint32, image []byte) {
	if s.baseBytes.Load()+int64(s.pgsz) > s.baseLimit {
		return
	}
	c := s.chainGet(pgno, true)
	cp := make([]byte, s.pgsz)
	copy(cp, image)
	mu := s.stripeOf(pgno)
	mu.Lock()
	if len(c.v) == 0 {
		c.v = append(c.v, pageVersion{0, cp})
		s.baseBytes.Add(int64(s.pgsz))
		s.versions.Add(1)
		s.bytes.Add(int64(s.pgsz))
	}
	mu.Unlock()
}

// dbsize returns the database size in pages as of snap.
func (s *store) dbsize(snap uint64) uint32 {
	s.seqMu.Lock()
	defer s.seqMu.Unlock()
	return s.dbsizeLocked(snap)
}

func (s *store) dbsizeLocked(snap uint64) uint32 {
	size := s.baseDBSize
	lo, hi, best := 0, len(s.sizes)-1, -1
	for lo <= hi {
		mid := (lo + hi) / 2
		if s.sizes[mid].epoch <= snap {
			best = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if best >= 0 {
		size = s.sizes[best].dbsize
	}
	return size
}

// headEpoch is the epoch of the newest committed version of pgno (0: none).
func (s *store) headEpoch(pgno uint32) uint64 {
	c := s.chainGet(pgno, false)
	if c == nil {
		return 0
	}
	mu := s.stripeOf(pgno)
	mu.Lock()
	e := c.head()
	mu.Unlock()
	return e
}

// headImage copies the newest committed image of pgno into dst and returns its epoch, atomically.
func (s *store) headImage(pgno uint32, dst []byte) (uint64, bool) {
	c := s.chainGet(pgno, false)
	if c == nil {
		return 0, false
	}
	mu := s.stripeOf(pgno)
	mu.Lock()
	defer mu.Unlock()
	if len(c.v) == 0 {
		return 0, false
	}
	copy(dst, c.v[len(c.v)-1].data)
	return c.v[len(c.v)-1].epoch, true
}

// install appends a version (caller holds the page's stripe). The pages that must join the lists are collected in
// lp and spliced in with one lock per commit.
func (s *store) install(pgno uint32, c *chain, epoch uint64, data []byte, lp *listPush) {
	if len(c.v) >= 1 && !c.queued {
		c.queued = true
		lp.cand = append(lp.cand, pgno)
	}
	if !c.dirty {
		c.dirty = true
		lp.dirty = append(lp.dirty, pgno)
	}
	if pgno == 1 {
		s.p1Cookie.Store(be32(data[40:]))
		s.p1Head.Store(epoch)
	}
	c.v = append(c.v, pageVersion{epoch, data})
	s.versions.Add(1)
	s.allocated.Add(1)
	s.bytes.Add(int64(s.pgsz))
}

type listPush struct{ cand, dirty []uint32 }

func (s *store) splice(lp *listPush) {
	if len(lp.cand) == 0 && len(lp.dirty) == 0 {
		return
	}
	s.listMu.Lock()
	s.cand = append(s.cand, lp.cand...)
	s.dirty = append(s.dirty, lp.dirty...)
	s.listMu.Unlock()
}

// installRecovered installs a commit read back from the log (single-threaded, before the database is used).
func (s *store) installRecovered(epoch uint64, dbsize uint32, pgnos []uint32, images [][]byte) {
	var lp listPush
	for i, pg := range pgnos {
		c := s.chainGet(pg, true)
		cp := make([]byte, s.pgsz)
		copy(cp, images[i])
		mu := s.stripeOf(pg)
		mu.Lock()
		s.install(pg, c, epoch, cp, &lp)
		mu.Unlock()
	}
	s.splice(&lp)
	s.seqMu.Lock()
	s.sizes = append(s.sizes, sizeRec{epoch, dbsize})
	s.seqMu.Unlock()
}

// stripeSet is a bitmap of stripes, locked in ascending order.
type stripeSet [numStripes / 64]uint64

func (ss *stripeSet) add(pgnos []uint32) {
	for _, p := range pgnos {
		i := p % numStripes
		ss[i/64] |= 1 << (i % 64)
	}
}

func (s *store) lockStripes(ss *stripeSet) {
	for w := 0; w < len(ss); w++ {
		for m := ss[w]; m != 0; m &= m - 1 {
			s.stripes[w*64+trailingZeros64(m)].Lock()
		}
	}
}

func (s *store) unlockStripes(ss *stripeSet) {
	for w := 0; w < len(ss); w++ {
		for m := ss[w]; m != 0; m &= m - 1 {
			s.stripes[w*64+trailingZeros64(m)].Unlock()
		}
	}
}

// dropAtOrBelow removes every version <= base (a process of the multi-process mode that lagged behind a compaction
// of another one: the file holds a newer state of every page than these versions).
func (s *store) dropAtOrBelow(base uint64) {
	dir := *s.dir.Load()
	for b, blk := range dir {
		if blk == nil {
			continue
		}
		for i := range blk {
			pgno := uint32(b<<chainBlockBits | i)
			mu := s.stripeOf(pgno)
			mu.Lock()
			c := &blk[i]
			k := 0
			for k < len(c.v) && c.v[k].epoch <= base {
				if c.v[k].epoch == 0 {
					s.baseBytes.Add(-int64(s.pgsz))
				}
				s.versions.Add(-1)
				s.bytes.Add(-int64(s.pgsz))
				k++
			}
			if k > 0 {
				c.v = append(c.v[:0], c.v[k:]...)
			}
			mu.Unlock()
		}
	}
}
