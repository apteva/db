package main

import (
	"fmt"
	"path/filepath"

	csqlite "crawshaw.io/sqlite"
	gosqlite "zombiezen.com/go/sqlite"
)

// stmt is the common surface of the two bindings' prepared statements.
type stmt interface {
	BindBytes(param int, value []byte)
	BindInt64(param int, value int64)
	BindText(param int, value string)
	BindFloat(param int, value float64)
	Step() (bool, error)
	Reset() error
	ColumnLen(col int) int
	ColumnBytes(col int, buf []byte) int
	ColumnText(col int) string
	ColumnFloat(col int) float64
	ColumnCount() int
}

type conn interface {
	Prep(query string) stmt
	Close() error
}

type cconn struct{ c *csqlite.Conn }

func (c cconn) Prep(q string) stmt { return c.c.Prep(q) }
func (c cconn) Close() error       { return c.c.Close() }

type gconn struct{ c *gosqlite.Conn }

func (c gconn) Prep(q string) stmt { return c.c.Prep(q) }
func (c gconn) Close() error       { return c.c.Close() }

// sqliteKV runs a WITHOUT ROWID table in WAL mode with a large page cache.
// txn controls how many inserts share one transaction.
type sqliteKV struct {
	c             conn
	ins, sel      stmt
	begin, commit stmt
	txn, inTxn    int
	open          bool
	mmap          bool
	reopen        func() (conn, error)
}

// openSQLiteCFsync is synchronous=FULL with SQLite's default fullfsync=OFF:
// one fsync(2) per commit, the same primitive as the engine's SyncOS mode.
func openSQLiteCFsync(dir string) (kv, error) {
	k, err := openSQLiteC(dir, true, 1)
	if err != nil {
		return nil, err
	}
	st := k.(*sqliteKV).c.Prep("PRAGMA fullfsync=OFF")
	if _, err := st.Step(); err != nil {
		return nil, err
	}
	st.Reset()
	return k, nil
}

// openSQLiteCMmap is the bulk variant with SQLite's own file mapping enabled,
// the counterpart of the engine reading through a mapped file.
func openSQLiteCMmap(dir string, txn int) (kv, error) {
	k, err := openSQLiteC(dir, false, txn)
	if err != nil {
		return nil, err
	}
	s := k.(*sqliteKV)
	st := s.c.Prep("PRAGMA mmap_size=1073741824")
	if _, err := st.Step(); err != nil {
		return nil, err
	}
	st.Reset()
	s.mmap = true
	return k, nil
}

func openSQLiteC(dir string, sync bool, txn int) (kv, error) {
	open := func() (conn, error) {
		c, err := csqlite.OpenConn(filepath.Join(dir, "db.sqlite"),
			csqlite.SQLITE_OPEN_READWRITE|csqlite.SQLITE_OPEN_CREATE|csqlite.SQLITE_OPEN_WAL|csqlite.SQLITE_OPEN_NOMUTEX)
		if err != nil {
			return nil, err
		}
		return cconn{c}, nil
	}
	return newSQLiteKV(open, sync, txn)
}

func openSQLiteGo(dir string, sync bool, txn int) (kv, error) {
	open := func() (conn, error) {
		c, err := gosqlite.OpenConn(filepath.Join(dir, "db.sqlite"),
			gosqlite.OpenReadWrite|gosqlite.OpenCreate|gosqlite.OpenWAL|gosqlite.OpenNoMutex)
		if err != nil {
			return nil, err
		}
		return gconn{c}, nil
	}
	return newSQLiteKV(open, sync, txn)
}

// NewReader opens a second connection; SQLite in WAL mode serves concurrent
// readers that way. Each connection gets its own page cache.
func (s *sqliteKV) NewReader() (reader, error) {
	if err := s.endTxn(); err != nil {
		return nil, err
	}
	c, err := s.reopen()
	if err != nil {
		return nil, err
	}
	pragmas := []string{fmt.Sprintf("PRAGMA cache_size=-%d", *flagCache*1024)}
	if s.mmap {
		pragmas = append(pragmas, "PRAGMA mmap_size=1073741824")
	}
	for _, q := range pragmas {
		st := c.Prep(q)
		if _, err := st.Step(); err != nil {
			return nil, err
		}
		st.Reset()
	}
	r := &sqliteKV{c: c, txn: 1}
	r.sel = c.Prep("SELECT v FROM kv WHERE k = ?")
	return r, nil
}

func newSQLiteKV(open func() (conn, error), sync bool, txn int) (kv, error) {
	c, err := open()
	if err != nil {
		return nil, err
	}
	syncMode := "OFF"
	if sync {
		syncMode = "FULL"
	}
	// Go's os.File.Sync (used by the engine and Pebble) issues F_FULLFSYNC on
	// macOS; make SQLite pay the same price in sync mode.
	for _, q := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=" + syncMode,
		"PRAGMA fullfsync=" + map[bool]string{true: "ON", false: "OFF"}[sync],
		fmt.Sprintf("PRAGMA cache_size=-%d", *flagCache*1024),
		"PRAGMA temp_store=MEMORY",
		"CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID",
	} {
		s := c.Prep(q)
		if _, err := s.Step(); err != nil {
			return nil, err
		}
		s.Reset()
	}
	s := &sqliteKV{c: c, txn: txn, reopen: open}
	s.ins = c.Prep("INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)")
	s.sel = c.Prep("SELECT v FROM kv WHERE k = ?")
	s.begin = c.Prep("BEGIN")
	s.commit = c.Prep("COMMIT")
	return s, nil
}

func (s *sqliteKV) Insert(k, v []byte) error {
	if s.txn > 1 && !s.open {
		if _, err := s.begin.Step(); err != nil {
			return err
		}
		s.begin.Reset()
		s.open = true
	}
	s.ins.BindBytes(1, k)
	s.ins.BindBytes(2, v)
	if _, err := s.ins.Step(); err != nil {
		return err
	}
	if err := s.ins.Reset(); err != nil {
		return err
	}
	if s.open {
		s.inTxn++
		if s.inTxn >= s.txn {
			return s.endTxn()
		}
	}
	return nil
}

func (s *sqliteKV) endTxn() error {
	if !s.open {
		return nil
	}
	if _, err := s.commit.Step(); err != nil {
		return err
	}
	s.commit.Reset()
	s.open, s.inTxn = false, 0
	return nil
}

func (s *sqliteKV) Find(k, dst []byte) ([]byte, bool, error) {
	if err := s.endTxn(); err != nil {
		return nil, false, err
	}
	s.sel.BindBytes(1, k)
	row, err := s.sel.Step()
	if err != nil {
		return nil, false, err
	}
	if !row {
		s.sel.Reset()
		return dst[:0], false, nil
	}
	n := s.sel.ColumnLen(0)
	if cap(dst) < n {
		dst = make([]byte, n)
	}
	dst = dst[:n]
	s.sel.ColumnBytes(0, dst)
	s.sel.Reset()
	return dst, true, nil
}

func (s *sqliteKV) Settle() error {
	if err := s.endTxn(); err != nil {
		return err
	}
	st := s.c.Prep("PRAGMA wal_checkpoint(TRUNCATE)")
	if _, err := st.Step(); err != nil {
		return err
	}
	return st.Reset()
}

func (s *sqliteKV) Close() error {
	if err := s.endTxn(); err != nil {
		return err
	}
	return s.c.Close()
}

// sqliteTables keeps n WITHOUT ROWID tables in one file and routes each key
// by its last byte, autocommitting every insert like sqlite-c does.
type sqliteTables struct {
	c        conn
	ins, sel []stmt
	reopen   func() (conn, error)
}

func openSQLiteTables(dir string, n int) (kv, error) {
	k, err := openSQLiteC(dir, false, 1)
	if err != nil {
		return nil, err
	}
	s := k.(*sqliteKV)
	t := &sqliteTables{c: s.c, reopen: s.reopen}
	for i := 0; i < n; i++ {
		st := s.c.Prep(fmt.Sprintf("CREATE TABLE IF NOT EXISTS t%d (k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID", i))
		if _, err := st.Step(); err != nil {
			return nil, err
		}
		st.Reset()
		t.ins = append(t.ins, s.c.Prep(fmt.Sprintf("INSERT OR REPLACE INTO t%d (k, v) VALUES (?, ?)", i)))
		t.sel = append(t.sel, s.c.Prep(fmt.Sprintf("SELECT v FROM t%d WHERE k = ?", i)))
	}
	return t, nil
}

func (t *sqliteTables) pick(k []byte) int { return int(k[len(k)-1]) % len(t.ins) }

func (t *sqliteTables) Insert(k, v []byte) error {
	ins := t.ins[t.pick(k)]
	ins.BindBytes(1, k)
	ins.BindBytes(2, v)
	if _, err := ins.Step(); err != nil {
		return err
	}
	return ins.Reset()
}

func (t *sqliteTables) Find(k, dst []byte) ([]byte, bool, error) {
	sel := t.sel[t.pick(k)]
	sel.BindBytes(1, k)
	row, err := sel.Step()
	if err != nil {
		return nil, false, err
	}
	if !row {
		sel.Reset()
		return dst[:0], false, nil
	}
	n := sel.ColumnLen(0)
	if cap(dst) < n {
		dst = make([]byte, n)
	}
	dst = dst[:n]
	sel.ColumnBytes(0, dst)
	sel.Reset()
	return dst, true, nil
}

func (t *sqliteTables) NewReader() (reader, error) {
	c, err := t.reopen()
	if err != nil {
		return nil, err
	}
	st := c.Prep(fmt.Sprintf("PRAGMA cache_size=-%d", *flagCache*1024))
	if _, err := st.Step(); err != nil {
		return nil, err
	}
	st.Reset()
	r := &sqliteTables{c: c}
	for i := range t.sel {
		r.sel = append(r.sel, c.Prep(fmt.Sprintf("SELECT v FROM t%d WHERE k = ?", i)))
	}
	r.ins = make([]stmt, len(t.ins)) // pick needs the count only
	return r, nil
}

func (t *sqliteTables) Settle() error {
	st := t.c.Prep("PRAGMA wal_checkpoint(TRUNCATE)")
	if _, err := st.Step(); err != nil {
		return err
	}
	return st.Reset()
}

func (t *sqliteTables) Close() error { return t.c.Close() }

func (s *sqliteKV) Iterate() (int, error) {
	if err := s.endTxn(); err != nil {
		return 0, err
	}
	st := s.c.Prep("SELECT k, v FROM kv ORDER BY k")
	n := 0
	for {
		ok, err := st.Step()
		if err != nil {
			return n, err
		}
		if !ok {
			break
		}
		n++
	}
	return n, st.Reset()
}

func (t *sqliteTables) Iterate() (int, error) {
	n := 0
	for i := range t.ins {
		st := t.c.Prep(fmt.Sprintf("SELECT k, v FROM t%d ORDER BY k", i))
		for {
			ok, err := st.Step()
			if err != nil {
				return n, err
			}
			if !ok {
				break
			}
			n++
		}
		st.Reset()
	}
	return n, nil
}
