// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

var (
	errCompactionTransactExiting = errors.New("leveldb: compaction transact exiting")
)

type cStat struct {
	duration time.Duration
	read     int64
	write    int64
}

func (p *cStat) add(n *cStatStaging) {
	p.duration += n.duration
	p.read += n.read
	p.write += n.write
}

func (p *cStat) get() (duration time.Duration, read, write int64) {
	return p.duration, p.read, p.write
}

type cStatStaging struct {
	start    time.Time
	duration time.Duration
	on       bool
	read     int64
	write    int64
}

func (p *cStatStaging) startTimer() {
	if !p.on {
		p.start = time.Now()
		p.on = true
	}
}

func (p *cStatStaging) stopTimer() {
	if p.on {
		p.duration += time.Since(p.start)
		p.on = false
	}
}

type cStats struct {
	lk    sync.Mutex
	stats []cStat
	//SM
	mergedCompStats cStat
}

// SM
func (p *cStats) addmergedCompStat(n *cStatStaging) {
	p.lk.Lock()
	p.mergedCompStats.add(n)
	p.lk.Unlock()
}

func (p *cStats) addStat(level int, n *cStatStaging) {
	p.lk.Lock()
	if level >= len(p.stats) {
		newStats := make([]cStat, level+1)
		copy(newStats, p.stats)
		p.stats = newStats
	}
	p.stats[level].add(n)
	p.lk.Unlock()
}

func (p *cStats) getStat(level int) (duration time.Duration, read, write int64) {
	p.lk.Lock()
	defer p.lk.Unlock()
	if level < len(p.stats) {
		return p.stats[level].get()
	}
	return
}

func (db *DB) compactionError() {
	var err error
noerr:
	// No error.
	for {
		select {
		case err = <-db.compErrSetC:
			switch {
			case err == nil:
			case err == ErrReadOnly, errors.IsCorrupted(err):
				goto hasperr
			default:
				goto haserr
			}
		case <-db.closeC:
			return
		}
	}
haserr:
	// Transient error.
	for {
		select {
		case db.compErrC <- err:
		case err = <-db.compErrSetC:
			switch {
			case err == nil:
				goto noerr
			case err == ErrReadOnly, errors.IsCorrupted(err):
				goto hasperr
			default:
			}
		case <-db.closeC:
			return
		}
	}
hasperr:
	// Persistent error.
	for {
		select {
		case db.compErrC <- err:
		case db.compPerErrC <- err:
		case db.writeLockC <- struct{}{}:
			// Hold write lock, so that write won't pass-through.
			db.compWriteLocking = true
		case <-db.closeC:
			if db.compWriteLocking {
				// We should release the lock or Close will hang.
				<-db.writeLockC
			}
			return
		}
	}
}

type compactionTransactCounter int

func (cnt *compactionTransactCounter) incr() {
	*cnt++
}

type compactionTransactInterface interface {
	run(cnt *compactionTransactCounter) error
	revert() error
}

func (db *DB) compactionTransact(name string, t compactionTransactInterface) {
	//SM
	//fmt.Println("SM:db_path@", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0), "@", name, "@compactionTransact entered:version_id@", db.s.stVersion.id)
	//db.logf("SM: compactionTrasact entered:version_id@%v: /leveldb/db_compaction.go", db.s.stVersion.id)

	defer func() {
		if x := recover(); x != nil {
			if x == errCompactionTransactExiting {
				if err := t.revert(); err != nil {
					db.logf("%s revert error %q", name, err)
				}
			}
			panic(x)
		}
	}()

	const (
		backoffMin = 1 * time.Second
		backoffMax = 8 * time.Second
		backoffMul = 2 * time.Second
	)
	var (
		backoff  = backoffMin
		backoffT = time.NewTimer(backoff)
		lastCnt  = compactionTransactCounter(0)

		disableBackoff = db.s.o.GetDisableCompactionBackoff()
	)
	//SM
	//if name == "p@table@build" {
	//	disableBackoff = !disableBackoff
	//	}

	for n := 0; ; n++ {
		// Check whether the DB is closed.
		if db.isClosed() {
			db.logf("%s exiting", name)
			db.compactionExitTransact()
		} else if n > 0 {
			fmt.Printf("%s retrying N·%d\n", name, n)
			db.logf("%s retrying N·%d", name, n)
		}
		// Execute.
		cnt := compactionTransactCounter(0)
		err := t.run(&cnt)
		//fmt.Println("SM:run has ended and reached here!")
		if err != nil {
			//fmt.Printf("%s error I·%d %q", name, cnt, err)
			db.logf("%s error I·%d %q", name, cnt, err)
		}

		// Set compaction error status.
		select {
		case db.compErrSetC <- err:
		case perr := <-db.compPerErrC:
			if err != nil {
				db.logf("%s exiting (persistent error %q)", name, perr)
				db.compactionExitTransact()
			}
		case <-db.closeC:
			db.logf("%s exiting", name)
			db.compactionExitTransact()
		}
		if err == nil {
			return
		}
		if errors.IsCorrupted(err) {
			db.logf("%s exiting (corruption detected)", name)
			db.compactionExitTransact()
		}
		if !disableBackoff {
			// Reset backoff duration if counter is advancing.
			if cnt > lastCnt {
				backoff = backoffMin
				lastCnt = cnt
			}

			// Backoff.
			backoffT.Reset(backoff)
			if backoff < backoffMax {
				backoff *= backoffMul
				if backoff > backoffMax {
					backoff = backoffMax
				}
			}
			select {
			case <-backoffT.C:
			case <-db.closeC:
				db.logf("%s exiting", name)
				db.compactionExitTransact()
			}
		}
	}
}

type compactionTransactFunc struct {
	runFunc    func(cnt *compactionTransactCounter) error
	revertFunc func() error
}

func (t *compactionTransactFunc) run(cnt *compactionTransactCounter) error {
	return t.runFunc(cnt)
}

func (t *compactionTransactFunc) revert() error {
	if t.revertFunc != nil {
		return t.revertFunc()
	}
	return nil
}

func (db *DB) compactionTransactFunc(name string, run func(cnt *compactionTransactCounter) error, revert func() error) {
	db.compactionTransact(name, &compactionTransactFunc{run, revert})
}

func (db *DB) compactionExitTransact() {
	fmt.Println("compactionExitTransact entered..!!")
	panic(errCompactionTransactExiting)
}

func (db *DB) compactionCommit(name string, rec *sessionRecord) {
	//SM
	//fmt.Println("SM:db_path@", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0), "@", name, "@compactionCommit entered:version_id@", db.s.stVersion.id)

	db.compCommitLk.Lock()
	defer db.compCommitLk.Unlock() // Defer is necessary.
	db.compactionTransactFunc(name+"@commit", func(cnt *compactionTransactCounter) error {
		return db.s.commit(rec, true)
	}, nil)
}

/*
	func (db *DB) compactionCommit_m(name string, rec *sessionRecord) {
		//SM
		fmt.Println("SM:db_path@", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0), ":compactionCommit entered:version_id@", db.s.stVersion.id)

		db.compCommitLk.Lock()
		defer db.compCommitLk.Unlock() // Defer is necessary.
		db.compactionTransactFunc(name+"@commit", func(cnt *compactionTransactCounter) error {
			return db.s.commit_m(rec, true)
		}, nil)
	}
*/
func (db *DB) memCompaction() {
	mdb := db.getFrozenMem()
	if mdb == nil {
		return
	}
	defer mdb.decref()

	//fmt.Printf("memdb@flush N·%d S·%s\n", mdb.Len(), shortenb(mdb.Size()))
	db.logf("memdb@flush N·%d S·%s", mdb.Len(), shortenb(mdb.Size()))

	// Don't compact empty memdb.
	if mdb.Len() == 0 {
		db.logf("memdb@flush skipping")
		// drop frozen memdb
		db.dropFrozenMem()
		return
	}

	// Pause table compaction.
	resumeC := make(chan struct{})
	select {
	case db.tcompPauseC <- (chan<- struct{})(resumeC):
	case <-db.compPerErrC:
		close(resumeC)
		resumeC = nil
	case <-db.closeC:
		db.compactionExitTransact()
	}

	var (
		rec        = &sessionRecord{}
		stats      = &cStatStaging{}
		flushLevel int
	)

	// Generate tables.
	db.compactionTransactFunc("memdb@flush", func(cnt *compactionTransactCounter) (err error) {
		stats.startTimer()
		flushLevel, err = db.s.flushMemdb(rec, mdb.DB, db.memdbMaxLevel)
		stats.stopTimer()
		return
	}, func() error {
		for _, r := range rec.addedTables {
			db.logf("memdb@flush revert @%d", r.num)
			if err := db.s.stor.Remove(storage.FileDesc{Type: storage.TypeTable, Num: r.num}); err != nil {
				return err
			}
		}
		return nil
	})

	rec.setJournalNum(db.journalFd.Num)
	rec.setSeqNum(db.frozenSeq)

	// Commit.
	stats.startTimer()
	db.compactionCommit("memdb", rec)
	stats.stopTimer()

	//fmt.Printf("memdb@flush committed F·%d T·%v\n", len(rec.addedTables), stats.duration)
	db.logf("memdb@flush committed F·%d T·%v", len(rec.addedTables), stats.duration)

	// Save compaction stats
	for _, r := range rec.addedTables {
		stats.write += r.size
	}
	db.compStats.addStat(flushLevel, stats)
	atomic.AddUint32(&db.memComp, 1)

	// Drop frozen memdb.
	db.dropFrozenMem()

	// Resume table compaction.
	if resumeC != nil {
		select {
		case <-resumeC:
			close(resumeC)
		case <-db.closeC:
			db.compactionExitTransact()
		}
	}

	// Trigger table compaction.
	db.compTrigger(db.tcompCmdC)
}

type tableCompactionBuilder struct {
	db           *DB
	s            *session
	c            *compaction
	rec          *sessionRecord
	stat0, stat1 *cStatStaging

	snapHasLastUkey bool
	snapLastUkey    []byte
	snapLastSeq     uint64
	snapIter        int
	snapKerrCnt     int
	snapDropCnt     int

	kerrCnt int
	dropCnt int

	minSeq    uint64
	strict    bool
	tableSize int

	tw *tWriter
}

func (b *tableCompactionBuilder) appendKV(key, value []byte) error {
	// Create new table if not already.
	if b.tw == nil {
		// Check for pause event.
		if b.db != nil {
			select {
			case ch := <-b.db.tcompPauseC:
				b.db.pauseCompaction(ch)
			case <-b.db.closeC:
				b.db.compactionExitTransact()
			default:
			}
		}

		// Create new table.
		var err error
		b.tw, err = b.s.tops.create(b.tableSize)
		if err != nil {
			fmt.Println("error occured while creating new table!!! here was the point!!!!")
			return err
		}
	}

	// Write key/value into table.
	return b.tw.append(key, value)
}

func (b *tableCompactionBuilder) needFlush() bool {
	return b.tw.tw.BytesLen() >= b.tableSize
}

func (b *tableCompactionBuilder) flush() error {
	t, err := b.tw.finish()
	if err != nil {
		fmt.Println("error occured in b.tw.finish")
		return err
	}
	b.rec.addTableFile(b.c.sourceLevel+1, t)
	b.stat1.write += t.size
	b.s.logf("table@build created L%d@%d N·%d S·%s %q:%q", b.c.sourceLevel+1, t.fd.Num, b.tw.tw.EntriesLen(), shortenb(int(t.size)), t.imin, t.imax)
	b.tw = nil
	return nil
}

func (b *tableCompactionBuilder) cleanup() {
	if b.tw != nil {
		b.tw.drop()
		b.tw = nil
	}
}

func (b *tableCompactionBuilder) run(cnt *compactionTransactCounter) error {
	//SM
	//fmt.Println("SM: run entered: /leveldb/db_compaction.go")
	b.s.log("SM: run entered: /leveldb/db_compaction.go")

	snapResumed := b.snapIter > 0
	hasLastUkey := b.snapHasLastUkey // The key might has zero length, so this is necessary.
	lastUkey := append([]byte{}, b.snapLastUkey...)
	lastSeq := b.snapLastSeq
	b.kerrCnt = b.snapKerrCnt
	b.dropCnt = b.snapDropCnt
	// Restore compaction state.
	b.c.restore()

	defer b.cleanup()

	b.stat1.startTimer()
	defer b.stat1.stopTimer()

	iter := b.c.newIterator()
	defer iter.Release()
	for i := 0; iter.Next(); i++ {
		// Incr transact counter.
		cnt.incr()

		// Skip until last state.
		if i < b.snapIter {
			continue
		}

		resumed := false
		if snapResumed {
			resumed = true
			snapResumed = false
		}

		ikey := iter.Key()
		ukey, seq, kt, kerr := parseInternalKey(ikey)

		if kerr == nil {
			shouldStop := !resumed && b.c.shouldStopBefore(ikey)

			if !hasLastUkey || b.s.icmp.uCompare(lastUkey, ukey) != 0 {
				// First occurrence of this user key.

				// Only rotate tables if ukey doesn't hop across.
				if b.tw != nil && (shouldStop || b.needFlush()) {
					if err := b.flush(); err != nil {
						fmt.Println()
						fmt.Println("error occured during flush at the middle")
						return err
					}

					// Creates snapshot of the state.
					b.c.save()
					b.snapHasLastUkey = hasLastUkey
					b.snapLastUkey = append(b.snapLastUkey[:0], lastUkey...)
					b.snapLastSeq = lastSeq
					b.snapIter = i
					b.snapKerrCnt = b.kerrCnt
					b.snapDropCnt = b.dropCnt
				}

				hasLastUkey = true
				lastUkey = append(lastUkey[:0], ukey...)
				lastSeq = keyMaxSeq
			}

			switch {
			case lastSeq <= b.minSeq:
				// Dropped because newer entry for same user key exist
				fallthrough // (A)
			case kt == keyTypeDel && seq <= b.minSeq && b.c.baseLevelForKey(lastUkey):
				// For this user key:
				// (1) there is no data in higher levels
				// (2) data in lower levels will have larger seq numbers
				// (3) data in layers that are being compacted here and have
				//     smaller seq numbers will be dropped in the next
				//     few iterations of this loop (by rule (A) above).
				// Therefore this deletion marker is obsolete and can be dropped.
				lastSeq = seq
				b.dropCnt++
				continue
			default:
				lastSeq = seq
			}
		} else {
			if b.strict {
				return kerr
			}

			// Don't drop corrupted keys.
			hasLastUkey = false
			lastUkey = lastUkey[:0]
			lastSeq = keyMaxSeq
			b.kerrCnt++
		}

		if err := b.appendKV(ikey, iter.Value()); err != nil {
			fmt.Println("error occured while appendingKV")
			return err
		}
	}

	if err := iter.Error(); err != nil {
		fmt.Println("erro occured while iteration")
		return err
	}

	// Finish last table.
	if b.tw != nil && !b.tw.empty() {
		return b.flush()
	}
	return nil
}

func (b *tableCompactionBuilder) revert() error {
	for _, at := range b.rec.addedTables {
		b.s.logf("table@build revert @%d", at.num)
		if err := b.s.stor.Remove(storage.FileDesc{Type: storage.TypeTable, Num: at.num}); err != nil {
			return err
		}
	}
	return nil
}

// SM
func (db *DB) tableCompaction(c *compaction, noTrivial bool) {
	defer c.release()
	//SM
	//fmt.Printf("SM:db_path@%s@tableCompaction-entered@version_id@%d", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0), db.s.stVersion.id)
	db.s.logf("SM:db_path@%s:tableCompaction entered:/leveldb/db_compaction.go", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0))

	rec := &sessionRecord{}
	rec.addCompPtr(c.sourceLevel, c.imax)

	if !noTrivial && c.trivial() {
		t := c.levels[0][0]
		db.logf("table@move L%d@%d -> L%d", c.sourceLevel, t.fd.Num, c.sourceLevel+1)
		rec.delTable(c.sourceLevel, t.fd.Num)
		rec.addTableFile(c.sourceLevel+1, t)
		db.compactionCommit("table-move", rec)
		return
	}
	// SM
	//var stats [2]cStatStaging
	for i, tables := range c.levels {
		for _, t := range tables {
			// SM
			c.stats[i].read += t.size
			// Insert deleted tables into record
			rec.delTable(c.sourceLevel+i, t.fd.Num)
		}
	}

	//sourceSize := int(c.stats[0].read + c.stats[1].read)
	sourceSize := int(c.stats[0].read + c.stats[1].read)
	minSeq := db.minSeq()

	//SM
	//fmt.Printf("SM:db_path@%s@table-compaction L%d·%d -> L%d·%d S·%s Q·%d\n", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0), c.sourceLevel, len(c.levels[0]), c.sourceLevel+1, len(c.levels[1]), shortenb(sourceSize), minSeq)
	db.logf("SM:db_path@%s: table@compaction L%d·%d -> L%d·%d S·%s Q·%d", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0), c.sourceLevel, len(c.levels[0]), c.sourceLevel+1, len(c.levels[1]), shortenb(sourceSize), minSeq)

	// SM
	c.compBuilder = &tableCompactionBuilder{
		db:  db,
		s:   db.s,
		c:   c,
		rec: rec,

		stat1: &c.stats[1],

		minSeq:    minSeq,
		strict:    db.s.o.GetStrict(opt.StrictCompaction),
		tableSize: db.s.o.GetCompactionTableSize(c.sourceLevel + 1),
	}
	db.compactionTransact("table@build", c.compBuilder)

	// SM
	// changed :: stats => c.stats
	// Commit.

	//c.stats[1].startTimer()
	c.stats[1].startTimer()
	db.compactionCommit("table", rec)
	c.stats[1].stopTimer()
	//c.stats[1].stopTimer()

	resultSize := int(c.stats[1].write)

	//fmt.Printf("SM:db_path@%s@table-compaction-committed F%s S%s Ke·%d D·%d T·%v\n", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0), sint(len(rec.addedTables)-len(rec.deletedTables)), sshortenb(resultSize-sourceSize), c.compBuilder.kerrCnt, c.compBuilder.dropCnt, c.stats[1].duration)
	db.logf("SM:db_path@%s: table@compaction committed F%s S%s Ke·%d D·%d T·%v", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0), sint(len(rec.addedTables)-len(rec.deletedTables)), sshortenb(resultSize-sourceSize), c.compBuilder.kerrCnt, c.compBuilder.dropCnt, c.stats[1].duration)

	// Save compaction stats
	for i := range c.stats {
		db.compStats.addStat(c.sourceLevel+1, &c.stats[i])
	}
	switch c.typ {
	case level0Compaction:
		atomic.AddUint32(&db.level0Comp, 1)
	case nonLevel0Compaction:
		atomic.AddUint32(&db.nonLevel0Comp, 1)
	case seekCompaction:
		atomic.AddUint32(&db.seekComp, 1)
	}
}

func (db *DB) ptableCompaction(c *compaction, noTrivial bool) {

	//SM
	//fmt.Printf("SM:db_path@%s@ptableCompaction-entered@version-id@%d", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0), db.s.stVersion.id)
	db.s.logf("SM:db_path@%s:ptableCompaction entered:/leveldb/db_compaction.go", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0))

	rec := &sessionRecord{}
	rec.addCompPtr(c.sourceLevel, c.imax)

	if !noTrivial && c.trivial() {
		t := c.levels[0][0]
		db.logf("table@move L%d@%d -> L%d", c.sourceLevel, t.fd.Num, c.sourceLevel+1)
		rec.delTable(c.sourceLevel, t.fd.Num)
		rec.addTableFile(c.sourceLevel+1, t)
		defer db.pCompCloseW.Done()
		c.compBuilder = &tableCompactionBuilder{
			db:    db,
			s:     db.s,
			c:     c,
			rec:   rec,
			stat1: &c.stats[1],

			strict:    db.s.o.GetStrict(opt.StrictCompaction),
			tableSize: db.s.o.GetCompactionTableSize(c.sourceLevel + 1),
		}
		//db.compactionCommit("table-move", rec)
		return
	}
	// SM
	// var stats [2]cStatStaging

	defer db.pCompCloseW.Done()

	for i, tables := range c.levels {
		for _, t := range tables {
			// SM
			c.stats[i].read += t.size
			// Insert deleted tables into record
			rec.delTable(c.sourceLevel+i, t.fd.Num)
		}
	}
	sourceSize := int(c.stats[0].read + c.stats[1].read)

	minSeq := db.minSeq()

	//SM
	//fmt.Printf("SM:db_path@%s@table-pcompaction@L%d·%d -> L%d·%d S·%s Q·%d\n", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0), c.sourceLevel, len(c.levels[0]), c.sourceLevel+1, len(c.levels[1]), shortenb(sourceSize), minSeq)
	db.logf("SM:db_path@%s: table-pcompaction L%d·%d -> L%d·%d S·%s Q·%d", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0), c.sourceLevel, len(c.levels[0]), c.sourceLevel+1, len(c.levels[1]), shortenb(sourceSize), minSeq)

	// SM

	c.compBuilder = &tableCompactionBuilder{
		db:    db,
		s:     db.s,
		c:     c,
		rec:   rec,
		stat1: &c.stats[1],

		minSeq:    minSeq,
		strict:    db.s.o.GetStrict(opt.StrictCompaction),
		tableSize: db.s.o.GetCompactionTableSize(c.sourceLevel + 1),
	}
	db.compactionTransact("p-table-build", c.compBuilder)
	//fmt.Printf("SM:db_path@%s@compactionTransact-done", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0))

	// SM
	// changed :: stats => c.stats
	// Commit.
	/*
		c.stats[1].startTimer()
		db.compactionCommit("table", rec)
		c.stats[1].stopTimer()

		resultSize := int(c.stats[1].write)

		db.logf("SM:db_path@%s: table@compaction committed F%s S%s Ke·%d D·%d T·%v", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0), sint(len(rec.addedTables)-len(rec.deletedTables)), sshortenb(resultSize-sourceSize), c.compBuilder.kerrCnt, c.compBuilder.dropCnt, c.stats[1].duration)

		// Save compaction stats
		for i := range c.stats {
			db.compStats.addStat(c.sourceLevel+1, &c.stats[i])
		}
		switch c.typ {
		case level0Compaction:
			atomic.AddUint32(&db.level0Comp, 1)
		case nonLevel0Compaction:
			atomic.AddUint32(&db.nonLevel0Comp, 1)
		case seekCompaction:
			atomic.AddUint32(&db.seekComp, 1)
		}
	*/
}

func (db *DB) tableRangeCompaction(level int, umin, umax []byte) error {
	db.logf("table@compaction range L%d %q:%q", level, umin, umax)
	if level >= 0 {
		if c := db.s.getCompactionRange(level, umin, umax, true); c != nil {
			db.tableCompaction(c, true)
		}
	} else {
		// Retry until nothing to compact.
		for {
			compacted := false

			// Scan for maximum level with overlapped tables.
			v := db.s.version()
			m := 1
			for i := m; i < len(v.levels); i++ {
				tables := v.levels[i]
				if tables.overlaps(db.s.icmp, umin, umax, false) {
					m = i
				}
			}
			v.release()

			for level := 0; level < m; level++ {
				if c := db.s.getCompactionRange(level, umin, umax, false); c != nil {
					db.tableCompaction(c, true)
					compacted = true
				}
			}

			if !compacted {
				break
			}
		}
	}

	return nil
}

func (db *DB) tableAutoCompaction() (err error) {
	// SM
	db.s.logf("SM:db_path@%s@tableAutoCompaction-entered@version_id@%v", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0), db.s.stVersion.id)
	//fmt.Printf("SM:db_path@%s@tableAutoCompaction-entered", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0))

	// SM
	if cs := db.s.pickCompaction(); cs != nil {

		// drop compaction with lesser cScore
		// which has overlapping sstable as a compaction range with higher one
		cs = db.dropIfOverlaps(cs)

		if len(cs) == 1 {
			start := time.Now()
			fmt.Printf("SM db_path %s table-compaction-start sourceLevel %d \n", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0), cs[0].sourceLevel)

			db.tableCompaction(cs[0], false)
			defer func() {
				elapsed := time.Since(start).Microseconds()
				db.cTotalElapsed += elapsed
				fmt.Printf("SM db_path %s table-compaction-ended sourceLevel %d comp-duration %d total-comp-duration %d\n",
					reflect.ValueOf(db.s.stor.Storage).Elem().Field(0), cs[0].sourceLevel, elapsed, db.cTotalElapsed)
			}()
		} else {
			start := time.Now()
			fmt.Printf("SM db_path %s table-compaction-start sourceLevel ", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0))
			for i, c := range cs {
				if i == len(cs)-1 {
					fmt.Printf("%d \n", c.sourceLevel)
				} else {
					fmt.Printf("%d", c.sourceLevel)
				}
			}

			db.pCompCloseW.Add(len(cs))
			for _, c := range cs {
				//fmt.Printf("SM:db_path@%s@ptableCompaction-triggered-time@%d", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0), i)
				// do compaction without trivial
				go db.ptableCompaction(c, false)
			}
			db.pCompCloseW.Wait()
			//fmt.Printf("SM:db_path@%s@all-ptableCompaction-done", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0))

			// merge sessionRecords for commit
			var recs = make([]*sessionRecord, len(cs))
			var lmadded int
			var lmdeleted int
			//var stats = make([]*[2]cStatStaging, len(cs))

			for i, c := range cs {
				rec := c.compBuilder.rec
				recs[i] = rec
				lmadded += len(recs[i].addedTables)
				lmdeleted += len(recs[i].deletedTables)
				//stats = append(stats, &c.stats)
			}

			var madded = make([]atRecord, lmadded)
			var mdeleted = make([]dtRecord, lmdeleted)

			var stagedAddedTablesLen int
			var stagedDeletedTablesLen int

			for _, rec := range recs {
				for j, t := range rec.addedTables {
					//fmt.Println(i, "th record::addedTable::tnum=", t.num, "level=", t.level, "tsize=", t.size, "imin=", t.imin, "imax=", t.imax)
					madded[j+stagedAddedTablesLen] = t
				}
				stagedAddedTablesLen += len(rec.addedTables)
				for j, t := range rec.deletedTables {
					//fmt.Println(i, "th record::deletedTables::tnum=", t.num, "level=", t.level)
					mdeleted[j+stagedDeletedTablesLen] = t
				}
				stagedDeletedTablesLen += len(rec.deletedTables)
			}

			mrec := &sessionRecord{
				addedTables:   madded,
				deletedTables: mdeleted,
			}
			/*
				nv := db.s.version().spawn(mrec, true)

				// abandon useless version id to prevent blocking version processing loop.
				defer func() {
					if err != nil {
						db.s.abandon <- nv.id
						db.s.logf("commit@abandon useless vid D%d", nv.id)
					}
				}()

				if db.s.manifest == nil {
					// manifest journal writer not yet created, create one
					err = db.s.newManifest(mrec, nv)
				} else {
					err = db.s.flushManifest(mrec)
				}

				if err == nil {

				}
			*/
			mc := &mergedCompaction{
				s: db.s,
				//v:    db.s.version(),
				cs:   cs,
				stat: cStatStaging{},
			}

			defer func() {
				mc.cs[0].release()
				for _, c := range cs {
					if c == cs[0] {
						continue
					}
					c.released = true
				}

				elapsed := time.Since(start).Microseconds()
				db.cTotalElapsed += elapsed
				fmt.Printf("SM db_path %s table-compaction-ended comp-duration %d total-comp-duration %d\n",
					reflect.ValueOf(db.s.stor.Storage).Elem().Field(0), elapsed, db.cTotalElapsed)
			}()

			for _, c := range mc.cs {
				for _, stat := range c.stats {
					mc.stat.read += stat.read
				}
				mc.stat.write += c.stats[1].write
				mc.kerrCnt += c.compBuilder.kerrCnt
				mc.dropCnt += c.compBuilder.dropCnt
			}

			mc.stat.startTimer()
			db.compactionCommit("merged-table", mrec)
			mc.stat.stopTimer()

			sourceSize := int(mc.stat.read)
			resultSize := int(mc.stat.write)

			//fmt.Printf("SM:db_path@%s@table-pcompaction-committed F%s S%s Ke·%d D·%d T·%v\n", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0), sint(len(mrec.addedTables)-len(mrec.deletedTables)), sshortenb(resultSize-sourceSize), mc.kerrCnt, mc.dropCnt, mc.stat.duration)
			db.logf("SM:db_path@%s@table-pcompaction-committed F%s S%s Ke·%d D·%d T·%v", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0), sint(len(mrec.addedTables)-len(mrec.deletedTables)), sshortenb(resultSize-sourceSize), mc.kerrCnt, mc.dropCnt, mc.stat.duration)

			// Save compaction stats
			db.compStats.addmergedCompStat(&mc.stat)

			for _, c := range mc.cs {
				switch c.typ {
				case level0Compaction:
					atomic.AddUint32(&db.level0Comp, 1)
				case nonLevel0Compaction:
					atomic.AddUint32(&db.nonLevel0Comp, 1)
				case seekCompaction:
					atomic.AddUint32(&db.seekComp, 1)
				}
			}
		}
	}
	return
}

// SM
func (db *DB) dropIfOverlaps(cs []*compaction) []*compaction {

	if len(cs) == 1 {
		return cs
	}

	// 1. sort cs(compactions) ascending, sourcelevel upward(e.g. 0 -> 4)
	sort.Slice(cs, func(i, j int) bool {
		return cs[i].sourceLevel < cs[j].sourceLevel
	})

	// 2. get pairs of compactions having contiguous sourceLevel
	var cpairs [][2]*compaction
	for i := 1; i < len(cs); i++ {
		if cs[i-1].sourceLevel == cs[i].sourceLevel-1 {
			cpairs = append(cpairs, [2]*compaction{cs[i-1], cs[i]})
		}
	}

	// 3. check whether there are duplicated sst table
	// within overlapped level of two compaction having contiguous sourceLevel
	for _, cpair := range cpairs {
		var duplicates []int64
		m := make(map[int64]bool)

		for _, tFile := range cpair[0].levels[1] {
			m[tFile.fd.Num] = true
		}

		for _, tFile := range cpair[1].levels[0] {
			if m[tFile.fd.Num] {
				duplicates = append(duplicates, tFile.fd.Num)
			}
		}

		// 4. if duplicated sstable exists, drop the one which has lesser cScore
		if len(duplicates) != 0 {
			if cpair[0].cScore >= cpair[1].cScore {
				for i, c := range cs {
					if c == cpair[1] {
						cs = append(cs[:i], cs[i+1:]...)
					}
				}
			} else {
				for i, c := range cs {
					if c == cpair[0] {
						cs = append(cs[:i], cs[i+1:]...)
					}
				}
			}
		}
	}
	return cs
}

func (db *DB) tableNeedCompaction() bool {
	//SM
	db.logf("SM:db_path@%s:tableNeedCompaction entered", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0))

	v := db.s.version()
	defer v.release()
	return v.needCompaction()
}

// resumeWrite returns an indicator whether we should resume write operation if enough level0 files are compacted.
func (db *DB) resumeWrite() bool {
	v := db.s.version()
	defer v.release()
	if v.tLen(0) < db.s.o.GetWriteL0PauseTrigger() {
		return true
	}
	return false
}

func (db *DB) pauseCompaction(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	case <-db.closeC:
		db.compactionExitTransact()
	}
}

type cCmd interface {
	ack(err error)
}

type cAuto struct {
	// Note for table compaction, an non-empty ackC represents it's a compaction waiting command.
	ackC chan<- error
}

func (r cAuto) ack(err error) {
	if r.ackC != nil {
		defer func() {
			recover()
		}()
		r.ackC <- err
	}
}

type cRange struct {
	level    int
	min, max []byte
	ackC     chan<- error
}

func (r cRange) ack(err error) {
	if r.ackC != nil {
		defer func() {
			recover()
		}()
		r.ackC <- err
	}
}

// This will trigger auto compaction but will not wait for it.
func (db *DB) compTrigger(compC chan<- cCmd) {
	select {
	case compC <- cAuto{}:
	default:
	}
}

// This will trigger auto compaction and/or wait for all compaction to be done.
func (db *DB) compTriggerWait(compC chan<- cCmd) (err error) {
	ch := make(chan error)
	defer close(ch)
	// Send cmd.
	select {
	case compC <- cAuto{ch}:
	case err = <-db.compErrC:
		return
	case <-db.closeC:
		return ErrClosed
	}
	// Wait cmd.
	select {
	case err = <-ch:
	case err = <-db.compErrC:
	case <-db.closeC:
		return ErrClosed
	}
	return err
}

// Send range compaction request.
func (db *DB) compTriggerRange(compC chan<- cCmd, level int, min, max []byte) (err error) {
	ch := make(chan error)
	defer close(ch)
	// Send cmd.
	select {
	case compC <- cRange{level, min, max, ch}:
	case err := <-db.compErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}
	// Wait cmd.
	select {
	case err = <-ch:
	case err = <-db.compErrC:
	case <-db.closeC:
		return ErrClosed
	}
	return err
}

func (db *DB) mCompaction() {
	var x cCmd

	defer func() {
		if x := recover(); x != nil {
			if x != errCompactionTransactExiting {
				panic(x)
			}
		}
		if x != nil {
			x.ack(ErrClosed)
		}
		db.closeW.Done()
	}()

	for {
		select {
		case x = <-db.mcompCmdC:
			switch x.(type) {
			case cAuto:
				db.memCompaction()
				x.ack(nil)
				x = nil
			default:
				panic("leveldb: unknown command")
			}
		case <-db.closeC:
			return
		}
	}
}

func (db *DB) tCompaction() {
	//SM
	//db.log("SM: tCompaction entered: /leveldb/db_compaction.go")
	//fmt.Printf("SM:db_path@%s@tCompaction-entered", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0))
	db.logf("SM:db_path@%s:tCompaction entered:/leveldb/db_compaction.go", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0))

	var (
		x     cCmd
		waitQ []cCmd
	)

	defer func() {
		if x := recover(); x != nil {
			if x != errCompactionTransactExiting {
				panic(x)
			}
		}
		for i := range waitQ {
			waitQ[i].ack(ErrClosed)
			waitQ[i] = nil
		}
		if x != nil {
			x.ack(ErrClosed)
		}
		db.closeW.Done()
	}()

	for {
		if db.tableNeedCompaction() {
			select {
			case x = <-db.tcompCmdC:
			case ch := <-db.tcompPauseC:
				db.pauseCompaction(ch)
				continue
			case <-db.closeC:
				return
			default:
			}
			// Resume write operation as soon as possible.
			if len(waitQ) > 0 && db.resumeWrite() {
				for i := range waitQ {
					waitQ[i].ack(nil)
					waitQ[i] = nil
				}
				waitQ = waitQ[:0]
			}
		} else {
			for i := range waitQ {
				waitQ[i].ack(nil)
				waitQ[i] = nil
			}
			waitQ = waitQ[:0]
			select {
			case x = <-db.tcompCmdC:
			case ch := <-db.tcompPauseC:
				db.pauseCompaction(ch)
				continue
			case <-db.closeC:
				return
			}
		}
		if x != nil {
			switch cmd := x.(type) {
			case cAuto:
				if cmd.ackC != nil {
					// Check the write pause state before caching it.
					if db.resumeWrite() {
						x.ack(nil)
					} else {
						waitQ = append(waitQ, x)
					}
				}
			case cRange:
				x.ack(db.tableRangeCompaction(cmd.level, cmd.min, cmd.max))
			default:
				panic("leveldb: unknown command")
			}
			x = nil
		}
		//SM
		//v := db.s.version()
		//fmt.Printf("SM:db_path@%s@table-auto-compaction-started", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0))
		//start := time.Now()
		db.tableAutoCompaction()
		//end := time.Since(start)
		//fmt.Printf("SM:db_path@%s@table-auto-compaction-ended", reflect.ValueOf(db.s.stor.Storage).Elem().Field(0))
		//v.release()
	}
}
