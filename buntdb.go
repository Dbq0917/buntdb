// Package buntdb implements a low-level in-memory key/value store in pure Go.
// It persists to disk, is ACID compliant, and uses locking for multiple
// readers and a single writer. Bunt is ideal for projects that need a
// dependable database, and favor speed over data size.
package buntdb

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/btree"
	"github.com/tidwall/gjson"
	"github.com/tidwall/grect"
	"github.com/tidwall/match"
	"github.com/tidwall/rtred"
)

var (
	// ErrTxNotWritable is returned when performing a write operation on a
	// read-only transaction.
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	ErrTxClosed = errors.New("tx closed")

	// ErrNotFound is returned when an item or index is not in the database.
	ErrNotFound = errors.New("not found")

	// ErrInvalid is returned when the database file is an invalid format.
	ErrInvalid = errors.New("invalid database")

	// ErrDatabaseClosed is returned when the database is closed.
	ErrDatabaseClosed = errors.New("database closed")

	// ErrIndexExists is returned when an index already exists in the database.
	ErrIndexExists = errors.New("index exists")

	// ErrInvalidOperation is returned when an operation cannot be completed.
	ErrInvalidOperation = errors.New("invalid operation")

	// ErrInvalidSyncPolicy is returned for an invalid SyncPolicy value.
	ErrInvalidSyncPolicy = errors.New("invalid sync policy")

	// ErrShrinkInProcess is returned when a shrink operation is in-process.
	ErrShrinkInProcess = errors.New("shrink is in-process")

	// ErrPersistenceActive is returned when post-loading data from an database
	// not opened with Open(":memory:").
	ErrPersistenceActive = errors.New("persistence active")

	// ErrTxIterating is returned when Set or Delete are called while iterating.
	ErrTxIterating = errors.New("tx is iterating")

	// ErrTxConflict is returned when an optimistic transaction cannot be
	// committed because another transaction committed first.
	ErrTxConflict = errors.New("tx conflict")

	// ErrConflict is an alias for ErrTxConflict.
	ErrConflict = ErrTxConflict

	// ErrSerializationFailure is an alias for ErrTxConflict.
	ErrSerializationFailure = ErrTxConflict
)

const useAbsEx = true

// DB represents a collection of key-value pairs that persist on disk.
// Transactions are used for all forms of data access to the DB.
type DB struct {
	mu         sync.RWMutex      // protects config, file, and lifecycle fields
	snapshotMu sync.RWMutex      // protects published snapshot and version
	writeMu    sync.Mutex        // serializes wait-on-conflict writers
	file       *os.File          // the underlying file
	buf        []byte            // a buffer to write to
	keys       *btree.BTree      // a tree of all item ordered by key
	exps       *btree.BTree      // a tree of items ordered by expiration
	idxs       map[string]*index // the index trees.
	insIdxs    []*index          // a reuse buffer for gathering indexes
	flushes    int               // a count of the number of disk flushes
	closed     bool              // set when the database has been closed
	config     Config            // the database configuration
	persist    bool              // do we write to disk
	version    uint64            // the current committed snapshot version
	shrinking  bool              // when an aof shrink is in-process.
	lastaofsz  int               // the size of the last shrink aof size
}

// IsolationLevel controls which committed version a transaction is allowed to
// observe.
type IsolationLevel uint8

const (
	// ReadUncommitted is the lowest SQL-style level. BuntDB writers keep
	// their changes private until commit, so this behaves like ReadCommitted
	// for committed-data reads and can never expose a dirty value from
	// another transaction.
	ReadUncommitted IsolationLevel = 4

	// ReadCommitted refreshes a read-only transaction to the latest committed
	// snapshot before each statement.
	ReadCommitted IsolationLevel = 1

	// RepeatableRead pins a consistent snapshot for the whole transaction.
	RepeatableRead IsolationLevel = 2

	// SnapshotIsolation pins a transaction to the snapshot that was current
	// when the transaction began.
	SnapshotIsolation = RepeatableRead

	// SerializableIsolation pins a snapshot and checks for a conflicting
	// write at commit time. It is the same as SnapshotIsolation for read-only
	// transactions.
	SerializableIsolation IsolationLevel = 3
)

const (
	// LevelReadUncommitted is an alias for ReadUncommitted.
	LevelReadUncommitted = ReadUncommitted
	// LevelReadCommitted is an alias for ReadCommitted.
	LevelReadCommitted = ReadCommitted
	// LevelRepeatableRead is an alias for RepeatableRead.
	LevelRepeatableRead = RepeatableRead
	// LevelSnapshot is an alias for SnapshotIsolation.
	LevelSnapshot = SnapshotIsolation
	// LevelSerializable is an alias for SerializableIsolation.
	LevelSerializable = SerializableIsolation
	// IsolationReadUncommitted is an alias for ReadUncommitted.
	IsolationReadUncommitted = ReadUncommitted
	// IsolationReadCommitted is an alias for ReadCommitted.
	IsolationReadCommitted = ReadCommitted
	// IsolationRepeatableRead is an alias for RepeatableRead.
	IsolationRepeatableRead = RepeatableRead
	// IsolationSnapshot is an alias for SnapshotIsolation.
	IsolationSnapshot = SnapshotIsolation
	// IsolationSerializable is an alias for SerializableIsolation.
	IsolationSerializable = SerializableIsolation

	// IsolationLevel* names are aliases for callers that prefer the type
	// name as a constant prefix.
	IsolationLevelReadUncommitted = ReadUncommitted
	IsolationLevelReadCommitted   = ReadCommitted
	IsolationLevelRepeatableRead  = RepeatableRead
	IsolationLevelSnapshot        = SnapshotIsolation
	IsolationLevelSerializable    = SerializableIsolation

	// LevelSnapshotIsolation is an alias for SnapshotIsolation.
	LevelSnapshotIsolation = SnapshotIsolation
)

// ConflictResolution is an alternate name for ConflictPolicy.
type ConflictResolution = ConflictPolicy

// ConflictPolicy controls what a writable transaction does when its starting
// snapshot has been superseded by another committed transaction.
type ConflictPolicy uint8

const (
	// ConflictWait serializes writable transactions from begin to completion.
	// Writers never observe a conflict under this policy.
	ConflictWait ConflictPolicy = iota

	// ConflictAbort rolls back the transaction and returns ErrTxConflict when
	// another transaction has committed since the snapshot was taken.
	ConflictAbort

	// ConflictOverwrite applies this transaction's key changes to the latest
	// committed snapshot. Concurrent changes to keys not written by this
	// transaction are retained, and this transaction's values win for keys it
	// did write.
	ConflictOverwrite
)

const (
	// ConflictLastWriterWins is an alias for ConflictOverwrite.
	ConflictLastWriterWins = ConflictOverwrite
	// ConflictLastWriteWins is an alias for ConflictOverwrite.
	ConflictLastWriteWins = ConflictOverwrite

	// ResolutionWait is an alias for ConflictWait.
	ResolutionWait = ConflictWait
	// ResolutionAbort is an alias for ConflictAbort.
	ResolutionAbort = ConflictAbort
	// ResolutionOverwrite is an alias for ConflictOverwrite.
	ResolutionOverwrite = ConflictOverwrite
	// ResolutionLastWriterWins is an alias for ConflictLastWriterWins.
	ResolutionLastWriterWins = ConflictLastWriterWins

	// ConflictPolicy* names are aliases that include the type name.
	ConflictPolicyWait       = ConflictWait
	ConflictPolicyAbort      = ConflictAbort
	ConflictPolicyOverwrite  = ConflictOverwrite
	ConflictPolicyLastWriter = ConflictLastWriterWins
	ConflictPolicyLastWrite  = ConflictLastWriteWins
)

// TxOptions configures the isolation and write-conflict behavior of a
// transaction. The zero value selects SnapshotIsolation with ConflictWait.
type TxOptions struct {
	Isolation IsolationLevel

	// Conflict selects the write/write conflict policy.
	Conflict ConflictPolicy

	// ConflictPolicy is an alternate name for Conflict. When both are set,
	// Conflict takes precedence.
	ConflictPolicy ConflictPolicy

	// Resolution is an alternate name for Conflict. When Conflict or
	// ConflictPolicy is non-zero, those fields take precedence.
	Resolution ConflictResolution

	// ConflictResolution is another alternate name for Conflict.
	ConflictResolution ConflictResolution
}

// TransactionOptions is an alternate name for TxOptions.
type TransactionOptions = TxOptions

// IsolationOptions is an alternate name for TxOptions.
type IsolationOptions = TxOptions

// TxOption mutates transaction options.
type TxOption func(*TxOptions)

// WithIsolation sets the isolation level for BeginTx, ViewTx, or UpdateTx.
func WithIsolation(level IsolationLevel) TxOption {
	return func(opts *TxOptions) {
		opts.Isolation = level
	}
}

// WithConflict sets the conflict policy for BeginTx or UpdateTx.
func WithConflict(policy ConflictPolicy) TxOption {
	return func(opts *TxOptions) {
		opts.Conflict = policy
	}
}

// WithConflictResolution sets the conflict resolution policy.
func WithConflictResolution(policy ConflictResolution) TxOption {
	return func(opts *TxOptions) {
		opts.ConflictResolution = policy
	}
}

func normalizeTxOptions(opts *TxOptions) TxOptions {
	nopts := TxOptions{
		Isolation: SnapshotIsolation,
		Conflict:  ConflictWait,
	}
	if opts == nil {
		return nopts
	}
	switch opts.Isolation {
	case ReadUncommitted, ReadCommitted, RepeatableRead, SerializableIsolation:
		nopts.Isolation = opts.Isolation
	}
	policy := opts.Conflict
	if policy == ConflictWait && opts.ConflictPolicy != ConflictWait {
		policy = opts.ConflictPolicy
	}
	if policy == ConflictWait && opts.Resolution != ConflictWait {
		policy = opts.Resolution
	}
	if policy == ConflictWait && opts.ConflictResolution != ConflictWait {
		policy = opts.ConflictResolution
	}
	switch policy {
	case ConflictWait, ConflictAbort, ConflictOverwrite:
		nopts.Conflict = policy
	}
	return nopts
}

// SyncPolicy represents how often data is synced to disk.
type SyncPolicy int

const (
	// Never is used to disable syncing data to disk.
	// The faster and less safe method.
	Never SyncPolicy = 0
	// EverySecond is used to sync data to disk every second.
	// It's pretty fast and you can lose 1 second of data if there
	// is a disaster.
	// This is the recommended setting.
	EverySecond = 1
	// Always is used to sync data after every write to disk.
	// Slow. Very safe.
	Always = 2
)

// Config represents database configuration options. These
// options are used to change various behaviors of the database.
type Config struct {
	// SyncPolicy adjusts how often the data is synced to disk.
	// This value can be Never, EverySecond, or Always.
	// The default is EverySecond.
	SyncPolicy SyncPolicy

	// AutoShrinkPercentage is used by the background process to trigger
	// a shrink of the aof file when the size of the file is larger than the
	// percentage of the result of the previous shrunk file.
	// For example, if this value is 100, and the last shrink process
	// resulted in a 100mb file, then the new aof file must be 200mb before
	// a shrink is triggered.
	AutoShrinkPercentage int

	// AutoShrinkMinSize defines the minimum size of the aof file before
	// an automatic shrink can occur.
	AutoShrinkMinSize int

	// AutoShrinkDisabled turns off automatic background shrinking
	AutoShrinkDisabled bool

	// OnExpired is used to custom handle the deletion option when a key
	// has been expired.
	OnExpired func(keys []string)

	// OnExpiredSync will be called inside the same transaction that is
	// performing the deletion of expired items. If OnExpired is present then
	// this callback will not be called. If this callback is present, then the
	// deletion of the timed-out item is the explicit responsibility of this
	// callback.
	OnExpiredSync func(key, value string, tx *Tx) error
}

// exctx is a simple b-tree context for ordering by expiration.
type exctx struct {
	db *DB
}

// Open opens a database at the provided path.
// If the file does not exist then it will be created automatically.
func Open(path string) (*DB, error) {
	db := &DB{}
	// initialize trees and indexes
	db.keys = btreeNew(lessCtx(nil))
	db.exps = btreeNew(lessCtx(&exctx{db}))
	db.idxs = make(map[string]*index)
	// initialize default configuration
	db.config = Config{
		SyncPolicy:           EverySecond,
		AutoShrinkPercentage: 100,
		AutoShrinkMinSize:    32 * 1024 * 1024,
	}
	// turn off persistence for pure in-memory
	db.persist = path != ":memory:"
	if db.persist {
		var err error
		// hardcoding 0666 as the default mode.
		db.file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
		// load the database from disk
		if err := db.load(); err != nil {
			// close on error, ignore close error
			_ = db.file.Close()
			return nil, err
		}
	}
	// start the background manager.
	go db.backgroundManager()
	return db, nil
}

// Close releases all database resources.
// All transactions must be closed before closing the database.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	db.closed = true
	db.snapshotMu.Lock()
	defer db.snapshotMu.Unlock()
	if db.persist {
		db.file.Sync() // do a sync but ignore the error
		if err := db.file.Close(); err != nil {
			return err
		}
	}
	// Let's release all references to nil. This will help both with debugging
	// late usage panics and it provides a hint to the garbage collector
	db.keys, db.exps, db.idxs, db.file = nil, nil, nil, nil
	return nil
}

// Save writes a point-in-time snapshot of the database to a writer. The
// database lock is held only while creating the copy-on-write view; writes can
// continue while the snapshot is being serialized. This can be used for
// snapshots and backups for pure in-memory databases using the ":memory:".
// Databases that persist to disk can also be snapshotted by copying the
// database file.
func (db *DB) Save(wr io.Writer) error {
	var err error
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrDatabaseClosed
	}
	db.snapshotMu.Lock()
	snapshot := db.keys.Copy()
	db.snapshotMu.Unlock()
	db.mu.RUnlock()
	now := time.Now()
	// use a buffered writer and flush every 4MB
	var buf []byte
	// iterated through every item in the database and write to the buffer
	btreeAscend(snapshot, func(item interface{}) bool {
		dbi := item.(*dbItem)
		buf = dbi.writeSetTo(buf, now)
		if len(buf) > 1024*1024*4 {
			// flush when buffer is over 4MB
			_, err = wr.Write(buf)
			if err != nil {
				return false
			}
			buf = buf[:0]
		}
		return true
	})
	if err != nil {
		return err
	}
	// one final flush
	if len(buf) > 0 {
		_, err = wr.Write(buf)
		if err != nil {
			return err
		}
	}
	return nil
}

// Load loads commands from reader. This operation blocks all reads and writes.
// Note that this can only work for fully in-memory databases opened with
// Open(":memory:").
func (db *DB) Load(rd io.Reader) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.persist {
		// cannot load into databases that persist to disk
		return ErrPersistenceActive
	}
	if _, err := db.readLoad(rd, time.Now()); err != nil {
		return err
	}
	db.snapshotMu.Lock()
	db.version++
	db.snapshotMu.Unlock()
	return nil
}

// index represents a b-tree or r-tree index and also acts as the
// b-tree/r-tree context for itself.
type index struct {
	btr     *btree.BTree                           // contains the items
	rtr     *rtred.RTree                           // contains the items
	name    string                                 // name of the index
	pattern string                                 // a required key pattern
	less    func(a, b string) bool                 // less comparison function
	rect    func(item string) (min, max []float64) // rect from string function
	db      *DB                                    // the origin database
	opts    IndexOptions                           // index options
}

// match matches the pattern to the key
func (idx *index) match(key string) bool {
	if idx.pattern == "*" {
		return true
	}
	if idx.opts.CaseInsensitiveKeyMatching {
		for i := 0; i < len(key); i++ {
			if key[i] >= 'A' && key[i] <= 'Z' {
				key = strings.ToLower(key)
				break
			}
		}
	}
	return match.Match(key, idx.pattern)
}

// clearCopy creates a copy of the index, but with an empty dataset.
func (idx *index) clearCopy() *index {
	// copy the index meta information
	nidx := &index{
		name:    idx.name,
		pattern: idx.pattern,
		db:      idx.db,
		less:    idx.less,
		rect:    idx.rect,
		opts:    idx.opts,
	}
	// initialize with empty trees
	if nidx.less != nil {
		nidx.btr = btreeNew(lessCtx(nidx))
	}
	if nidx.rect != nil {
		nidx.rtr = rtred.New(nidx)
	}
	return nidx
}

// rebuild rebuilds the index from the provided key tree.
func (idx *index) rebuild(keys *btree.BTree) {
	// initialize trees
	if idx.less != nil {
		idx.btr = btreeNew(lessCtx(idx))
	}
	if idx.rect != nil {
		idx.rtr = rtred.New(idx)
	}
	// iterate through all keys and fill the index
	btreeAscend(keys, func(item interface{}) bool {
		dbi := item.(*dbItem)
		if !idx.match(dbi.key) {
			// does not match the pattern, continue
			return true
		}
		if idx.less != nil {
			idx.btr.Set(dbi)
		}
		if idx.rect != nil {
			idx.rtr.Insert(dbi)
		}
		return true
	})
}

// CreateIndex builds a new index and populates it with items.
// The items are ordered in an b-tree and can be retrieved using the
// Ascend* and Descend* methods.
// An error will occur if an index with the same name already exists.
//
// When a pattern is provided, the index will be populated with
// keys that match the specified pattern. This is a very simple pattern
// match where '*' matches on any number characters and '?' matches on
// any one character.
// The less function compares if string 'a' is less than string 'b'.
// It allows for indexes to create custom ordering. It's possible
// that the strings may be textual or binary. It's up to the provided
// less function to handle the content format and comparison.
// There are some default less function that can be used such as
// IndexString, IndexBinary, etc.
func (db *DB) CreateIndex(name, pattern string,
	less ...func(a, b string) bool) error {
	return db.Update(func(tx *Tx) error {
		return tx.CreateIndex(name, pattern, less...)
	})
}

// ReplaceIndex builds a new index and populates it with items.
// The items are ordered in an b-tree and can be retrieved using the
// Ascend* and Descend* methods.
// If a previous index with the same name exists, that index will be deleted.
func (db *DB) ReplaceIndex(name, pattern string,
	less ...func(a, b string) bool) error {
	return db.Update(func(tx *Tx) error {
		err := tx.CreateIndex(name, pattern, less...)
		if err != nil {
			if err == ErrIndexExists {
				err := tx.DropIndex(name)
				if err != nil {
					return err
				}
				return tx.CreateIndex(name, pattern, less...)
			}
			return err
		}
		return nil
	})
}

// CreateSpatialIndex builds a new index and populates it with items.
// The items are organized in an r-tree and can be retrieved using the
// Intersects method.
// An error will occur if an index with the same name already exists.
//
// The rect function converts a string to a rectangle. The rectangle is
// represented by two arrays, min and max. Both arrays may have a length
// between 1 and 20, and both arrays must match in length. A length of 1 is a
// one dimensional rectangle, and a length of 4 is a four dimension rectangle.
// There is support for up to 20 dimensions.
// The values of min must be less than the values of max at the same dimension.
// Thus min[0] must be less-than-or-equal-to max[0].
// The IndexRect is a default function that can be used for the rect
// parameter.
func (db *DB) CreateSpatialIndex(name, pattern string,
	rect func(item string) (min, max []float64)) error {
	return db.Update(func(tx *Tx) error {
		return tx.CreateSpatialIndex(name, pattern, rect)
	})
}

// ReplaceSpatialIndex builds a new index and populates it with items.
// The items are organized in an r-tree and can be retrieved using the
// Intersects method.
// If a previous index with the same name exists, that index will be deleted.
func (db *DB) ReplaceSpatialIndex(name, pattern string,
	rect func(item string) (min, max []float64)) error {
	return db.Update(func(tx *Tx) error {
		err := tx.CreateSpatialIndex(name, pattern, rect)
		if err != nil {
			if err == ErrIndexExists {
				err := tx.DropIndex(name)
				if err != nil {
					return err
				}
				return tx.CreateSpatialIndex(name, pattern, rect)
			}
			return err
		}
		return nil
	})
}

// DropIndex removes an index.
func (db *DB) DropIndex(name string) error {
	return db.Update(func(tx *Tx) error {
		return tx.DropIndex(name)
	})
}

// Indexes returns a list of index names.
func (db *DB) Indexes() ([]string, error) {
	var names []string
	var err = db.View(func(tx *Tx) error {
		var err error
		names, err = tx.Indexes()
		return err
	})
	return names, err
}

// ReadConfig returns the database configuration.
func (db *DB) ReadConfig(config *Config) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	*config = db.config
	return nil
}

// SetConfig updates the database configuration.
func (db *DB) SetConfig(config Config) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	switch config.SyncPolicy {
	default:
		return ErrInvalidSyncPolicy
	case Never, EverySecond, Always:
	}
	db.config = config
	return nil
}

// insertIntoDatabase inserts an item into the supplied trees and indexes. If a
// previous item with the same key already exists, that item is replaced and
// returned.
func insertIntoDatabase(keys, exps *btree.BTree, idxs map[string]*index,
	insIdxs *[]*index, item *dbItem,
) *dbItem {
	var pdbi *dbItem
	// Generate a list of indexes that this item will be inserted in to.
	idxList := *insIdxs
	for _, idx := range idxs {
		if idx.match(item.key) {
			idxList = append(idxList, idx)
		}
	}
	prev := keys.Set(item)
	if prev != nil {
		// A previous item was removed from the keys tree. Let's
		// fully delete this item from all indexes.
		pdbi = prev.(*dbItem)
		if pdbi.opts != nil && pdbi.opts.ex {
			// Remove it from the expires tree.
			exps.Delete(pdbi)
		}
		for _, idx := range idxList {
			if idx.btr != nil {
				// Remove it from the btree index.
				idx.btr.Delete(pdbi)
			}
			if idx.rtr != nil {
				// Remove it from the rtree index.
				idx.rtr.Remove(pdbi)
			}
		}
	}
	if item.opts != nil && item.opts.ex {
		// The new item has eviction options. Add it to the
		// expires tree
		exps.Set(item)
	}
	for i, idx := range idxList {
		if idx.btr != nil {
			// Add new item to btree index.
			idx.btr.Set(item)
		}
		if idx.rtr != nil {
			// Add new item to rtree index.
			idx.rtr.Insert(item)
		}
		// clear the index
		idxList[i] = nil
	}
	// reuse the index list slice
	*insIdxs = idxList[:0]
	// we must return the previous item to the caller.
	return pdbi
}

// deleteFromDatabase removes an item from the supplied trees and indexes.
func deleteFromDatabase(keys, exps *btree.BTree, idxs map[string]*index,
	item *dbItem,
) *dbItem {
	var pdbi *dbItem
	prev := keys.Delete(item)
	if prev != nil {
		pdbi = prev.(*dbItem)
		if pdbi.opts != nil && pdbi.opts.ex {
			// Remove it from the exipres tree.
			exps.Delete(pdbi)
		}
		for _, idx := range idxs {
			if !idx.match(pdbi.key) {
				continue
			}
			if idx.btr != nil {
				// Remove it from the btree index.
				idx.btr.Delete(pdbi)
			}
			if idx.rtr != nil {
				// Remove it from the rtree index.
				idx.rtr.Remove(pdbi)
			}
		}
	}
	return pdbi
}

// insertIntoDatabase performs inserts an item in to the database and updates
// all indexes.
func (db *DB) insertIntoDatabase(item *dbItem) *dbItem {
	return insertIntoDatabase(db.keys, db.exps, db.idxs, &db.insIdxs, item)
}

// deleteFromDatabase removes an item from the database and indexes.
func (db *DB) deleteFromDatabase(item *dbItem) *dbItem {
	return deleteFromDatabase(db.keys, db.exps, db.idxs, item)
}

// backgroundManager runs continuously in the background and performs various
// operations such as removing expired items and syncing to disk.
func (db *DB) backgroundManager() {
	flushes := 0
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		var shrink bool
		// Open a standard view. This will take a full lock of the
		// database thus allowing for access to anything we need.
		var onExpired func([]string)
		var expired []*dbItem
		var onExpiredSync func(key, value string, tx *Tx) error
		err := db.Update(func(tx *Tx) error {
			onExpired = db.config.OnExpired
			if onExpired == nil {
				onExpiredSync = db.config.OnExpiredSync
			}
			if db.persist && !db.config.AutoShrinkDisabled {
				pos, err := db.file.Seek(0, 1)
				if err != nil {
					return err
				}
				aofsz := int(pos)
				if aofsz > db.config.AutoShrinkMinSize {
					prc := float64(db.config.AutoShrinkPercentage) / 100.0
					shrink = aofsz > db.lastaofsz+int(float64(db.lastaofsz)*prc)
				}
			}
			// produce a list of expired items that need removing
			btreeAscendLessThan(tx.state.exps, &dbItem{
				opts: &dbItemOpts{ex: true, exat: time.Now()},
			}, func(item interface{}) bool {
				expired = append(expired, item.(*dbItem))
				return true
			})
			if onExpired == nil && onExpiredSync == nil {
				for _, itm := range expired {
					if _, err := tx.Delete(itm.key); err != nil {
						// it's ok to get a "not found" because the
						// 'Delete' method reports "not found" for
						// expired items.
						if err != ErrNotFound {
							return err
						}
					}
				}
			} else if onExpiredSync != nil {
				for _, itm := range expired {
					if err := onExpiredSync(itm.key, itm.val, tx); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err == ErrDatabaseClosed {
			break
		}

		// send expired event, if needed
		if onExpired != nil && len(expired) > 0 {
			keys := make([]string, 0, 32)
			for _, itm := range expired {
				keys = append(keys, itm.key)
			}
			onExpired(keys)
		}

		// execute a disk sync, if needed
		func() {
			db.mu.Lock()
			defer db.mu.Unlock()
			if db.persist && db.config.SyncPolicy == EverySecond &&
				flushes != db.flushes {
				_ = db.file.Sync()
				flushes = db.flushes
			}
		}()
		if shrink {
			if err = db.Shrink(); err != nil {
				if err == ErrDatabaseClosed {
					break
				}
			}
		}
	}
}

// Sync will synchronously persist the database file to disk. This
// should only be used when [SyncPolicy] is set to [Never].
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	if !db.persist {
		return nil
	}
	return db.file.Sync()
}

// Shrink will make the database file smaller by removing redundant
// log entries. This operation does not block the database.
func (db *DB) Shrink() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrDatabaseClosed
	}
	if !db.persist {
		// The database was opened with ":memory:" as the path.
		// There is no persistence, and no need to do anything here.
		db.mu.Unlock()
		return nil
	}
	if db.shrinking {
		// The database is already in the process of shrinking.
		db.mu.Unlock()
		return ErrShrinkInProcess
	}
	db.shrinking = true
	defer func() {
		db.mu.Lock()
		db.shrinking = false
		db.mu.Unlock()
	}()
	fname := db.file.Name()
	tmpname := fname + ".tmp"
	// the endpos is used to return to the end of the file when we are
	// finished writing all of the current items.
	endpos, err := db.file.Seek(0, 2)
	if err != nil {
		return err
	}
	db.mu.Unlock()
	time.Sleep(time.Second / 4) // wait just a bit before starting
	f, err := os.Create(tmpname)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
		_ = os.RemoveAll(tmpname)
	}()

	// we are going to read items in as chunks as to not hold up the database
	// for too long.
	var buf []byte
	pivot := ""
	done := false
	for !done {
		err := func() error {
			db.mu.RLock()
			defer db.mu.RUnlock()
			if db.closed {
				return ErrDatabaseClosed
			}
			done = true
			var n int
			now := time.Now()
			btreeAscendGreaterOrEqual(db.keys, &dbItem{key: pivot},
				func(item interface{}) bool {
					dbi := item.(*dbItem)
					// 1000 items or 64MB buffer
					if n > 1000 || len(buf) > 64*1024*1024 {
						pivot = dbi.key
						done = false
						return false
					}
					buf = dbi.writeSetTo(buf, now)
					n++
					return true
				},
			)
			if len(buf) > 0 {
				if _, err := f.Write(buf); err != nil {
					return err
				}
				buf = buf[:0]
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	// We reached this far so all of the items have been written to a new tmp
	// There's some more work to do by appending the new line from the aof
	// to the tmp file and finally swap the files out.
	return func() error {
		// We're wrapping this in a function to get the benefit of a defered
		// lock/unlock.
		db.mu.Lock()
		defer db.mu.Unlock()
		if db.closed {
			return ErrDatabaseClosed
		}
		// We are going to open a new version of the aof file so that we do
		// not change the seek position of the previous. This may cause a
		// problem in the future if we choose to use syscall file locking.
		aof, err := os.Open(fname)
		if err != nil {
			return err
		}
		defer func() { _ = aof.Close() }()
		if _, err := aof.Seek(endpos, 0); err != nil {
			return err
		}
		// Just copy all of the new commands that have occurred since we
		// started the shrink process.
		if _, err := io.Copy(f, aof); err != nil {
			return err
		}
		// Close all files
		if err := aof.Close(); err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		if err := db.file.Close(); err != nil {
			return err
		}
		// Any failures below here are really bad. So just panic.
		if err := renameFile(tmpname, fname); err != nil {
			panicErr(err)
		}
		db.file, err = os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			panicErr(err)
		}
		pos, err := db.file.Seek(0, 2)
		if err != nil {
			return err
		}
		db.lastaofsz = int(pos)
		return nil
	}()
}

func panicErr(err error) error {
	panic(fmt.Errorf("buntdb: %w", err))
}

func renameFile(src, dest string) error {
	var err error
	if err = os.Rename(src, dest); err != nil {
		if runtime.GOOS == "windows" {
			if err = os.Remove(dest); err == nil {
				err = os.Rename(src, dest)
			}
		}
	}
	return err
}

// readLoad reads from the reader and loads commands into the database.
// modTime is the modified time of the reader, should be no greater than
// the current time.Now().
// Returns the number of bytes of the last command read and the error if any.
func (db *DB) readLoad(rd io.Reader, modTime time.Time) (n int64, err error) {
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()
	totalSize := int64(0)
	data := make([]byte, 4096)
	parts := make([]string, 0, 8)
	r := bufio.NewReader(rd)
	for {
		// peek at the first byte. If it's a 'nul' control character then
		// ignore it and move to the next byte.
		c, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return totalSize, err
		}
		if c == 0 {
			// ignore nul control characters
			n += 1
			continue
		}
		if err := r.UnreadByte(); err != nil {
			return totalSize, err
		}

		// read a single command.
		// first we should read the number of parts that the of the command
		cmdByteSize := int64(0)
		line, err := r.ReadBytes('\n')
		if err != nil {
			return totalSize, err
		}
		if line[0] != '*' {
			return totalSize, ErrInvalid
		}
		cmdByteSize += int64(len(line))

		// convert the string number to and int
		var n int
		if len(line) == 4 && line[len(line)-2] == '\r' {
			if line[1] < '0' || line[1] > '9' {
				return totalSize, ErrInvalid
			}
			n = int(line[1] - '0')
		} else {
			if len(line) < 5 || line[len(line)-2] != '\r' {
				return totalSize, ErrInvalid
			}
			for i := 1; i < len(line)-2; i++ {
				if line[i] < '0' || line[i] > '9' {
					return totalSize, ErrInvalid
				}
				n = n*10 + int(line[i]-'0')
			}
		}
		// read each part of the command.
		parts = parts[:0]
		for i := 0; i < n; i++ {
			// read the number of bytes of the part.
			line, err := r.ReadBytes('\n')
			if err != nil {
				return totalSize, err
			}
			if line[0] != '$' {
				return totalSize, ErrInvalid
			}
			cmdByteSize += int64(len(line))
			// convert the string number to and int
			var n int
			if len(line) == 4 && line[len(line)-2] == '\r' {
				if line[1] < '0' || line[1] > '9' {
					return totalSize, ErrInvalid
				}
				n = int(line[1] - '0')
			} else {
				if len(line) < 5 || line[len(line)-2] != '\r' {
					return totalSize, ErrInvalid
				}
				for i := 1; i < len(line)-2; i++ {
					if line[i] < '0' || line[i] > '9' {
						return totalSize, ErrInvalid
					}
					n = n*10 + int(line[i]-'0')
				}
			}
			// resize the read buffer
			if len(data) < n+2 {
				dataln := len(data)
				for dataln < n+2 {
					dataln *= 2
				}
				data = make([]byte, dataln)
			}
			if _, err = io.ReadFull(r, data[:n+2]); err != nil {
				return totalSize, err
			}
			if data[n] != '\r' || data[n+1] != '\n' {
				return totalSize, ErrInvalid
			}
			// copy string
			parts = append(parts, string(data[:n]))
			cmdByteSize += int64(n + 2)
		}
		// finished reading the command

		if len(parts) == 0 {
			continue
		}
		if (parts[0][0] == 's' || parts[0][0] == 'S') &&
			(parts[0][1] == 'e' || parts[0][1] == 'E') &&
			(parts[0][2] == 't' || parts[0][2] == 'T') {
			// SET
			if len(parts) < 3 || len(parts) == 4 || len(parts) > 5 {
				return totalSize, ErrInvalid
			}
			if len(parts) == 5 {
				arg := strings.ToLower(parts[3])
				if arg != "ex" && arg != "ae" {
					return totalSize, ErrInvalid
				}
				ex, err := strconv.ParseInt(parts[4], 10, 64)
				if err != nil {
					return totalSize, err
				}
				var exat time.Time
				now := time.Now()
				if arg == "ex" {
					dur := (time.Duration(ex) * time.Second) - now.Sub(modTime)
					exat = now.Add(dur)
				} else {
					exat = time.Unix(ex, 0)
				}
				if exat.After(now) {
					db.insertIntoDatabase(&dbItem{
						key: parts[1],
						val: parts[2],
						opts: &dbItemOpts{
							ex:   true,
							exat: exat,
						},
					})
				} else {
					db.deleteFromDatabase(&dbItem{
						key: parts[1],
					})
				}
			} else {
				db.insertIntoDatabase(&dbItem{key: parts[1], val: parts[2]})
			}
		} else if (parts[0][0] == 'd' || parts[0][0] == 'D') &&
			(parts[0][1] == 'e' || parts[0][1] == 'E') &&
			(parts[0][2] == 'l' || parts[0][2] == 'L') {
			// DEL
			if len(parts) != 2 {
				return totalSize, ErrInvalid
			}
			db.deleteFromDatabase(&dbItem{key: parts[1]})
		} else if (parts[0][0] == 'f' || parts[0][0] == 'F') &&
			strings.ToLower(parts[0]) == "flushdb" {
			db.keys = btreeNew(lessCtx(nil))
			db.exps = btreeNew(lessCtx(&exctx{db}))
			db.idxs = make(map[string]*index)
		} else {
			return totalSize, ErrInvalid
		}
		totalSize += cmdByteSize
	}
}

// load reads entries from the append only database file and fills the database.
// The file format uses the Redis append only file format, which is and a series
// of RESP commands. For more information on RESP please read
// http://redis.io/topics/protocol. The only supported RESP commands are DEL and
// SET.
func (db *DB) load() error {
	fi, err := db.file.Stat()
	if err != nil {
		return err
	}
	n, err := db.readLoad(db.file, fi.ModTime())
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			// The db file has ended mid-command, which is allowed but the
			// data file should be truncated to the end of the last valid
			// command
			if err := db.file.Truncate(n); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	if _, err := db.file.Seek(n, 0); err != nil {
		return err
	}
	var estaofsz int
	db.keys.Walk(func(items []interface{}) {
		for _, v := range items {
			estaofsz += v.(*dbItem).estAOFSetSize()
		}
	})
	db.lastaofsz += estaofsz
	return nil
}

// managed calls a block of code that is fully contained in a transaction.
// This method is intended to be wrapped by Update and View
func (db *DB) managed(writable bool, fn func(tx *Tx) error) (err error) {
	return db.managedOptions(writable, nil, fn)
}

func (db *DB) managedOptions(writable bool, opts *TxOptions,
	fn func(tx *Tx) error,
) (err error) {
	var tx *Tx
	tx, err = db.BeginWithOptions(writable, opts)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			// The caller returned an error. We must rollback.
			_ = tx.Rollback()
			return
		}
		if writable {
			// Everything went well. Lets Commit()
			err = tx.Commit()
		} else {
			// read-only transaction can only roll back.
			err = tx.Rollback()
		}
	}()
	tx.funcd = true
	defer func() {
		tx.funcd = false
	}()
	err = fn(tx)
	return
}

// View executes a function within a managed read-only transaction.
// When a non-nil error is returned from the function that error will be return
// to the caller of View().
//
// Executing a manual commit or rollback from inside the function will result
// in a panic.
func (db *DB) View(fn func(tx *Tx) error) error {
	return db.managed(false, fn)
}

// ViewWithOptions executes a read-only transaction using the provided
// isolation options. A read-only transaction never blocks a writer.
func (db *DB) ViewWithOptions(opts *TxOptions,
	fn func(tx *Tx) error,
) error {
	return db.managedOptions(false, opts, fn)
}

// Update executes a function within a managed read/write transaction.
// The transaction has been committed when no error is returned.
// In the event that an error is returned, the transaction will be rolled back.
// When a non-nil error is returned from the function, the transaction will be
// rolled back and the that error will be return to the caller of Update().
//
// Executing a manual commit or rollback from inside the function will result
// in a panic.
func (db *DB) Update(fn func(tx *Tx) error) error {
	return db.managed(true, fn)
}

// UpdateWithOptions executes a read/write transaction using the provided
// isolation and conflict options. Writes are applied to the transaction's
// private snapshot and published with a short lock at commit time.
func (db *DB) UpdateWithOptions(opts *TxOptions,
	fn func(tx *Tx) error,
) error {
	return db.managedOptions(true, opts, fn)
}

// ViewTx executes a read-only transaction with functional options.
func (db *DB) ViewTx(fn func(tx *Tx) error, options ...TxOption) error {
	opts := &TxOptions{}
	for _, option := range options {
		option(opts)
	}
	return db.managedOptions(false, opts, fn)
}

// UpdateTx executes a read/write transaction with functional options.
func (db *DB) UpdateTx(fn func(tx *Tx) error, options ...TxOption) error {
	opts := &TxOptions{}
	for _, option := range options {
		option(opts)
	}
	return db.managedOptions(true, opts, fn)
}

// ViewWithIsolation is a convenience wrapper around ViewWithOptions.
func (db *DB) ViewWithIsolation(level IsolationLevel,
	fn func(tx *Tx) error,
) error {
	return db.ViewWithOptions(&TxOptions{Isolation: level}, fn)
}

// UpdateWithIsolation is a convenience wrapper around UpdateWithOptions.
func (db *DB) UpdateWithIsolation(level IsolationLevel,
	fn func(tx *Tx) error,
) error {
	return db.UpdateWithOptions(&TxOptions{Isolation: level}, fn)
}

// get return an item or nil if not found.
func (db *DB) get(key string) *dbItem {
	item := db.keys.Get(&dbItem{key: key})
	if item != nil {
		return item.(*dbItem)
	}
	return nil
}

// Tx represents a transaction on the database. This transaction can either be
// read-only or read/write. Read-only transactions can be used for retrieving
// values for keys and iterating through keys and values. Read/write
// transactions can set and delete keys.
//
// All transactions must be committed or rolled-back when done.
type Tx struct {
	db          *DB             // the underlying database.
	origin      *DB             // db pointer retained for lock cleanup
	writable    bool            // when false mutable operations fail.
	funcd       bool            // when true Commit and Rollback panic.
	wc          *txWriteContext // context for writable transactions.
	state       *txState        // the transaction's stable data view.
	baseVersion uint64          // the snapshot version this transaction began with
	isolation   IsolationLevel
	conflict    ConflictPolicy
	serialized  bool // true when this writer holds db.writeMu
}

type txState struct {
	keys    *btree.BTree
	exps    *btree.BTree
	idxs    map[string]*index
	insIdxs []*index
}

type txWriteContext struct {
	changes     map[string]*dbItem // final value for each changed key; nil deletes
	indexOps    map[string]*index  // final index state; nil means dropped
	deleteAll   bool               // flushdb was requested
	itercount   int                // stack of iterators
	indexChange bool               // an index was created or dropped
}

// DeleteAll deletes all items from the database.
func (tx *Tx) DeleteAll() error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return ErrTxIterating
	}

	oldIdxs := tx.state.idxs
	// Reset the private trees. Other transactions continue to read their own
	// snapshots while this transaction is being built.
	tx.state.keys = btreeNew(lessCtx(nil))
	tx.state.exps = btreeNew(lessCtx(&exctx{tx.db}))
	tx.state.idxs = make(map[string]*index)
	tx.state.insIdxs = nil

	// finally re-create the indexes
	for name, idx := range oldIdxs {
		tx.state.idxs[name] = idx.clearCopy()
	}

	// always clear out the commits
	tx.wc.changes = make(map[string]*dbItem)
	tx.wc.deleteAll = true

	return nil
}

// Begin opens a new transaction.
// Multiple read-only transactions can be opened at the same time but there can
// only be one read/write transaction at a time. Attempting to open a read/write
// transactions while another one is in progress will result in blocking until
// the current read/write transaction is completed.
//
// All transactions must be closed by calling Commit() or Rollback() when done.
func (db *DB) Begin(writable bool) (*Tx, error) {
	return db.BeginWithOptions(writable, nil)
}

// BeginWithOptions opens a transaction with explicit isolation and conflict
// options. Read-only transactions take a stable snapshot without taking a lock
// on the live key tree. Writable transactions modify a private copy-on-write
// snapshot and publish it atomically at commit time.
func (db *DB) BeginWithOptions(writable bool, opts *TxOptions) (*Tx, error) {
	nopts := normalizeTxOptions(opts)
	tx := &Tx{
		db:        db,
		origin:    db,
		writable:  writable,
		isolation: nopts.Isolation,
		conflict:  nopts.Conflict,
	}
	if writable && tx.conflict == ConflictWait {
		db.writeMu.Lock()
		tx.serialized = true
	}
	db.mu.RLock()
	if db.closed {
		tx.unlock()
		db.mu.RUnlock()
		return nil, ErrDatabaseClosed
	}
	if writable {
		db.snapshotMu.Lock()
		tx.baseVersion = db.version
		tx.state = db.cloneStateLocked()
		db.snapshotMu.Unlock()
		tx.wc = &txWriteContext{}
		tx.wc.changes = make(map[string]*dbItem)
		tx.wc.indexOps = make(map[string]*index)
	} else {
		// Copy() is constant-time and copy-on-write, but it updates the
		// source tree's COW token. Hold the write side of snapshotMu just
		// long enough to create independent transaction trees.
		db.snapshotMu.Lock()
		tx.baseVersion = db.version
		tx.state = db.readStateLocked()
		db.snapshotMu.Unlock()
	}
	db.mu.RUnlock()
	return tx, nil
}

// BeginTx opens a transaction using functional options.
func (db *DB) BeginTx(writable bool, options ...TxOption) (*Tx, error) {
	opts := &TxOptions{}
	for _, option := range options {
		option(opts)
	}
	return db.BeginWithOptions(writable, opts)
}

// IsolationLevel returns the transaction's isolation level.
func (tx *Tx) IsolationLevel() IsolationLevel {
	return tx.isolation
}

// ConflictPolicy returns the transaction's conflict policy.
func (tx *Tx) ConflictPolicy() ConflictPolicy {
	return tx.conflict
}

// unlock releases any writer serialization slot held by the transaction.
func (tx *Tx) unlock() {
	if tx.serialized {
		tx.origin.writeMu.Unlock()
		tx.serialized = false
	}
}

// refresh advances a ReadCommitted read-only transaction to the latest
// committed snapshot.
func (tx *Tx) refresh() {
	if tx.writable {
		return
	}
	if tx.isolation != ReadCommitted && tx.isolation != ReadUncommitted {
		return
	}
	db := tx.db
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return
	}
	db.snapshotMu.Lock()
	tx.state = db.readStateLocked()
	tx.baseVersion = db.version
	db.snapshotMu.Unlock()
}

func (tx *Tx) get(key string) *dbItem {
	item := tx.state.keys.Get(&dbItem{key: key})
	if item != nil {
		return item.(*dbItem)
	}
	return nil
}

func copyIndexForWrite(idx *index, keys *btree.BTree) *index {
	nidx := &index{
		name:    idx.name,
		pattern: idx.pattern,
		db:      idx.db,
		less:    idx.less,
		rect:    idx.rect,
		opts:    idx.opts,
	}
	if idx.btr != nil {
		nidx.btr = idx.btr.Copy()
	}
	if idx.rtr != nil {
		nidx.rtr = rtred.New(nidx)
		btreeAscend(keys, func(item interface{}) bool {
			dbi := item.(*dbItem)
			if nidx.match(dbi.key) {
				nidx.rtr.Insert(dbi)
			}
			return true
		})
	}
	return nidx
}

func copyIndexForRead(idx *index) *index {
	nidx := &index{
		name:    idx.name,
		pattern: idx.pattern,
		db:      idx.db,
		less:    idx.less,
		rect:    idx.rect,
		opts:    idx.opts,
	}
	if idx.btr != nil {
		nidx.btr = idx.btr.Copy()
	}
	// Published R-trees are never mutated: writers replace them with freshly
	// built trees at commit time. Sharing is therefore a stable snapshot and
	// avoids copying a spatial index for every report.
	nidx.rtr = idx.rtr
	return nidx
}

func (db *DB) readStateLocked() *txState {
	state := &txState{
		keys: db.keys.Copy(),
		exps: db.exps.Copy(),
		idxs: make(map[string]*index, len(db.idxs)),
	}
	for name, idx := range db.idxs {
		state.idxs[name] = copyIndexForRead(idx)
	}
	return state
}

func (db *DB) cloneStateLocked() *txState {
	state := &txState{
		keys: db.keys.Copy(),
		exps: db.exps.Copy(),
		idxs: make(map[string]*index, len(db.idxs)),
	}
	for name, idx := range db.idxs {
		state.idxs[name] = copyIndexForWrite(idx, state.keys)
	}
	return state
}

// rebaseWriteSetLocked applies this transaction's logical changes to the
// latest committed snapshot. Other writers' changes to untouched keys remain
// visible.
func (tx *Tx) rebaseWriteSetLocked() *txState {
	state := tx.db.cloneStateLocked()
	for name, template := range tx.wc.indexOps {
		if template == nil {
			delete(state.idxs, name)
			continue
		}
		// The template contains this transaction's stale trees. Rebuild an
		// empty index with the same definition from the latest keyspace so
		// committed changes from the conflicting writer are not lost.
		idx := template.clearCopy()
		idx.rebuild(state.keys)
		state.idxs[name] = idx
	}
	for key, item := range tx.wc.changes {
		deleteFromDatabase(state.keys, state.exps, state.idxs,
			&dbItem{key: key})
		if item != nil {
			insertIntoDatabase(state.keys, state.exps, state.idxs,
				&state.insIdxs, item)
		}
	}
	return state
}

func (tx *Tx) writeAOF() error {
	db := tx.db
	if !db.persist || (!tx.wc.deleteAll && len(tx.wc.changes) == 0) {
		return nil
	}
	db.buf = db.buf[:0]
	if tx.wc.deleteAll {
		db.buf = append(db.buf, "*1\r\n$7\r\nflushdb\r\n"...)
	}
	now := time.Now()
	for key, item := range tx.wc.changes {
		if item == nil {
			db.buf = (&dbItem{key: key}).writeDeleteTo(db.buf)
		} else {
			db.buf = item.writeSetTo(db.buf, now)
		}
	}
	n, err := db.file.Write(db.buf)
	if err != nil {
		if n > 0 {
			// There was a partial write to disk. Remove the tail so that a
			// later load does not observe a half-written command.
			pos, seekErr := db.file.Seek(-int64(n), 1)
			if seekErr != nil {
				panicErr(seekErr)
			}
			if truncErr := db.file.Truncate(pos); truncErr != nil {
				panicErr(truncErr)
			}
		}
		return err
	}
	if db.config.SyncPolicy == Always {
		_ = db.file.Sync()
	}
	db.flushes++
	return nil
}

// Commit writes all changes to disk.
// An error is returned when a write error occurs, or when a Commit() is called
// from a read-only transaction.
func (tx *Tx) Commit() error {
	if tx.funcd {
		panic("managed tx commit not allowed")
	}
	if tx.db == nil {
		tx.unlock()
		return ErrTxClosed
	} else if !tx.writable {
		tx.unlock()
		return ErrTxNotWritable
	}
	db := tx.db
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		tx.unlock()
		tx.db = nil
		return ErrDatabaseClosed
	}

	changed := tx.wc.deleteAll || len(tx.wc.changes) > 0 ||
		tx.wc.indexChange
	if !changed {
		db.mu.Unlock()
		tx.unlock()
		tx.db = nil
		return nil
	}

	db.snapshotMu.Lock()
	conflict := tx.baseVersion != db.version
	if conflict && tx.conflict == ConflictAbort {
		db.snapshotMu.Unlock()
		db.mu.Unlock()
		tx.unlock()
		tx.db = nil
		return ErrTxConflict
	}
	publish := tx.state
	if conflict && !tx.wc.deleteAll &&
		(tx.conflict == ConflictOverwrite || tx.conflict == ConflictWait) {
		publish = tx.rebaseWriteSetLocked()
	}
	db.snapshotMu.Unlock()

	if err := tx.writeAOF(); err != nil {
		db.mu.Unlock()
		tx.unlock()
		tx.db = nil
		return err
	}

	db.snapshotMu.Lock()
	db.keys = publish.keys
	db.exps = publish.exps
	db.idxs = publish.idxs
	db.version++
	db.snapshotMu.Unlock()
	db.mu.Unlock()
	tx.unlock()
	tx.db = nil
	return nil
}

// Rollback closes the transaction and reverts all mutable operations that
// were performed on the transaction such as Set() and Delete().
//
// Read-only transactions can only be rolled back, not committed.
func (tx *Tx) Rollback() error {
	if tx.funcd {
		panic("managed tx rollback not allowed")
	}
	if tx.db == nil {
		tx.unlock()
		return ErrTxClosed
	}
	// unlock the database for more transactions.
	tx.unlock()
	// Clear the db field to disable this transaction from future use.
	tx.db = nil
	return nil
}

// dbItemOpts holds various meta information about an item.
type dbItemOpts struct {
	ex   bool      // does this item expire?
	exat time.Time // when does this item expire?
}
type dbItem struct {
	key, val string      // the binary key and value
	opts     *dbItemOpts // optional meta information
	keyless  bool        // keyless item for scanning
}

// estIntSize returns the string representions size.
// Has the same result as len(strconv.Itoa(x)).
func estIntSize(x int) int {
	n := 1
	if x < 0 {
		n++
		x *= -1
	}
	for x >= 10 {
		n++
		x /= 10
	}
	return n
}

func estArraySize(count int) int {
	return 1 + estIntSize(count) + 2
}

func estBulkStringSize(s string) int {
	return 1 + estIntSize(len(s)) + 2 + len(s) + 2
}

// estAOFSetSize returns an estimated number of bytes that this item will use
// when stored in the aof file.
func (dbi *dbItem) estAOFSetSize() int {
	var n int
	if dbi.opts != nil && dbi.opts.ex {
		n += estArraySize(5)
		n += estBulkStringSize("set")
		n += estBulkStringSize(dbi.key)
		n += estBulkStringSize(dbi.val)
		n += estBulkStringSize("ex")
		n += estBulkStringSize("99") // estimate two byte bulk string
	} else {
		n += estArraySize(3)
		n += estBulkStringSize("set")
		n += estBulkStringSize(dbi.key)
		n += estBulkStringSize(dbi.val)
	}
	return n
}

func appendArray(buf []byte, count int) []byte {
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(count), 10)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendBulkString(buf []byte, s string) []byte {
	buf = append(buf, '$')
	buf = strconv.AppendInt(buf, int64(len(s)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, s...)
	buf = append(buf, '\r', '\n')
	return buf
}

// writeSetTo writes an item as a single SET record to the a bufio Writer.
func (dbi *dbItem) writeSetTo(buf []byte, now time.Time) []byte {
	if dbi.opts != nil && dbi.opts.ex {
		buf = appendArray(buf, 5)
		buf = appendBulkString(buf, "set")
		buf = appendBulkString(buf, dbi.key)
		buf = appendBulkString(buf, dbi.val)
		if useAbsEx {
			ex := dbi.opts.exat.Unix()
			buf = appendBulkString(buf, "ae")
			buf = appendBulkString(buf, strconv.FormatUint(uint64(ex), 10))
		} else {
			ex := dbi.opts.exat.Sub(now) / time.Second
			buf = appendBulkString(buf, "ex")
			buf = appendBulkString(buf, strconv.FormatUint(uint64(ex), 10))
		}
	} else {
		buf = appendArray(buf, 3)
		buf = appendBulkString(buf, "set")
		buf = appendBulkString(buf, dbi.key)
		buf = appendBulkString(buf, dbi.val)
	}
	return buf
}

// writeSetTo writes an item as a single DEL record to the a bufio Writer.
func (dbi *dbItem) writeDeleteTo(buf []byte) []byte {
	buf = appendArray(buf, 2)
	buf = appendBulkString(buf, "del")
	buf = appendBulkString(buf, dbi.key)
	return buf
}

// expired evaluates id the item has expired. This will always return false when
// the item does not have `opts.ex` set to true.
func (dbi *dbItem) expired() bool {
	return dbi.opts != nil && dbi.opts.ex && time.Now().After(dbi.opts.exat)
}

// MaxTime from http://stackoverflow.com/questions/25065055#32620397
// This is a long time in the future. It's an imaginary number that is
// used for b-tree ordering.
var maxTime = time.Unix(1<<63-62135596801, 999999999)

// expiresAt will return the time when the item will expire. When an item does
// not expire `maxTime` is used.
func (dbi *dbItem) expiresAt() time.Time {
	if dbi.opts == nil || !dbi.opts.ex {
		return maxTime
	}
	return dbi.opts.exat
}

// Less determines if a b-tree item is less than another. This is required
// for ordering, inserting, and deleting items from a b-tree. It's important
// to note that the ctx parameter is used to help with determine which
// formula to use on an item. Each b-tree should use a different ctx when
// sharing the same item.
func (dbi *dbItem) Less(dbi2 *dbItem, ctx interface{}) bool {
	switch ctx := ctx.(type) {
	case *exctx:
		// The expires b-tree formula
		if dbi2.expiresAt().After(dbi.expiresAt()) {
			return true
		}
		if dbi.expiresAt().After(dbi2.expiresAt()) {
			return false
		}
	case *index:
		if ctx.less != nil {
			// Using an index
			if ctx.less(dbi.val, dbi2.val) {
				return true
			}
			if ctx.less(dbi2.val, dbi.val) {
				return false
			}
		}
	}
	// Always fall back to the key comparison. This creates absolute uniqueness.
	if dbi.keyless {
		return false
	} else if dbi2.keyless {
		return true
	}
	return dbi.key < dbi2.key
}

func lessCtx(ctx interface{}) func(a, b interface{}) bool {
	return func(a, b interface{}) bool {
		return a.(*dbItem).Less(b.(*dbItem), ctx)
	}
}

// Rect converts a string to a rectangle.
// An invalid rectangle will cause a panic.
func (dbi *dbItem) Rect(ctx interface{}) (min, max []float64) {
	switch ctx := ctx.(type) {
	case *index:
		return ctx.rect(dbi.val)
	}
	return nil, nil
}

// SetOptions represents options that may be included with the Set() command.
type SetOptions struct {
	// Expires indicates that the Set() key-value will expire
	Expires bool
	// TTL is how much time the key-value will exist in the database
	// before being evicted. The Expires field must also be set to true.
	// TTL stands for Time-To-Live.
	TTL time.Duration
}

// GetLess returns the less function for an index. This is handy for
// doing ad-hoc compares inside a transaction.
// Returns ErrNotFound if the index is not found or there is no less
// function bound to the index
func (tx *Tx) GetLess(index string) (func(a, b string) bool, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	tx.refresh()
	idx, ok := tx.state.idxs[index]
	if !ok || idx.less == nil {
		return nil, ErrNotFound
	}
	return idx.less, nil
}

// GetRect returns the rect function for an index. This is handy for
// doing ad-hoc searches inside a transaction.
// Returns ErrNotFound if the index is not found or there is no rect
// function bound to the index
func (tx *Tx) GetRect(index string) (func(s string) (min, max []float64),
	error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	tx.refresh()
	idx, ok := tx.state.idxs[index]
	if !ok || idx.rect == nil {
		return nil, ErrNotFound
	}
	return idx.rect, nil
}

// Set inserts or replaces an item in the database based on the key.
// The opt params may be used for additional functionality such as forcing
// the item to be evicted at a specified time. When the return value
// for err is nil the operation succeeded. When the return value of
// replaced is true, then the operaton replaced an existing item whose
// value will be returned through the previousValue variable.
// The results of this operation will not be available to other
// transactions until the current transaction has successfully committed.
//
// Only a writable transaction can be used with this operation.
// This operation is not allowed during iterations such as Ascend* & Descend*.
func (tx *Tx) Set(key, value string, opts *SetOptions) (previousValue string,
	replaced bool, err error) {
	if tx.db == nil {
		return "", false, ErrTxClosed
	} else if !tx.writable {
		return "", false, ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return "", false, ErrTxIterating
	}
	item := &dbItem{key: key, val: value}
	if opts != nil {
		if opts.Expires {
			// The caller is requesting that this item expires. Convert the
			// TTL to an absolute time and bind it to the item.
			item.opts = &dbItemOpts{ex: true, exat: time.Now().Add(opts.TTL)}
		}
	}
	prev := insertIntoDatabase(tx.state.keys, tx.state.exps, tx.state.idxs,
		&tx.state.insIdxs, item)
	if prev != nil && !prev.expired() {
		previousValue, replaced = prev.val, true
	}
	tx.wc.changes[key] = item
	return previousValue, replaced, nil
}

// Get returns a value for a key. If the item does not exist or if the item
// has expired then ErrNotFound is returned. If ignoreExpired is true, then
// the found value will be returned even if it is expired.
func (tx *Tx) Get(key string, ignoreExpired ...bool) (val string, err error) {
	if tx.db == nil {
		return "", ErrTxClosed
	}
	tx.refresh()
	var ignore bool
	if len(ignoreExpired) != 0 {
		ignore = ignoreExpired[0]
	}
	item := tx.get(key)
	if item == nil || (item.expired() && !ignore) {
		// The item does not exists or has expired. Let's assume that
		// the caller is only interested in items that have not expired.
		return "", ErrNotFound
	}
	return item.val, nil
}

// Delete removes an item from the database based on the item's key. If the item
// does not exist or if the item has expired then ErrNotFound is returned.
//
// Only a writable transaction can be used for this operation.
// This operation is not allowed during iterations such as Ascend* & Descend*.
func (tx *Tx) Delete(key string) (val string, err error) {
	if tx.db == nil {
		return "", ErrTxClosed
	} else if !tx.writable {
		return "", ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return "", ErrTxIterating
	}
	item := deleteFromDatabase(tx.state.keys, tx.state.exps, tx.state.idxs,
		&dbItem{key: key})
	if item == nil {
		return "", ErrNotFound
	}
	tx.wc.changes[key] = nil
	// Even though the item has been deleted, we still want to check
	// if it has expired. An expired item should not be returned.
	if item.expired() {
		// The item exists in the tree, but has expired. Let's assume that
		// the caller is only interested in items that have not expired.
		return "", ErrNotFound
	}
	return item.val, nil
}

// TTL returns the remaining time-to-live for an item.
// A negative duration will be returned for items that do not have an
// expiration.
func (tx *Tx) TTL(key string) (time.Duration, error) {
	if tx.db == nil {
		return 0, ErrTxClosed
	}
	tx.refresh()
	item := tx.get(key)
	if item == nil {
		return 0, ErrNotFound
	} else if item.opts == nil || !item.opts.ex {
		return -1, nil
	}
	dur := time.Until(item.opts.exat)
	if dur < 0 {
		return 0, ErrNotFound
	}
	return dur, nil
}

// scan iterates through a specified index and calls user-defined iterator
// function for each item encountered.
// The desc param indicates that the iterator should descend.
// The gt param indicates that there is a greaterThan limit.
// The lt param indicates that there is a lessThan limit.
// The index param tells the scanner to use the specified index tree. An
// empty string for the index means to scan the keys, not the values.
// The start and stop params are the greaterThan, lessThan limits. For
// descending order, these will be lessThan, greaterThan.
// An error will be returned if the tx is closed or the index is not found.
func (tx *Tx) scan(desc, gt, lt bool, index, start, stop string,
	iterator func(key, value string) bool) error {
	if tx.db == nil {
		return ErrTxClosed
	}
	tx.refresh()
	// wrap a btree specific iterator around the user-defined iterator.
	iter := func(item interface{}) bool {
		dbi := item.(*dbItem)
		if dbi.expired() {
			return true
		}
		return iterator(dbi.key, dbi.val)
	}
	var tr *btree.BTree
	if index == "" {
		// empty index means we will use the keys tree.
		tr = tx.state.keys
	} else {
		idx := tx.state.idxs[index]
		if idx == nil {
			// index was not found. return error
			return ErrNotFound
		}
		tr = idx.btr
		if tr == nil {
			return nil
		}
	}
	// create some limit items
	var itemA, itemB *dbItem
	if gt || lt {
		if index == "" {
			itemA = &dbItem{key: start}
			itemB = &dbItem{key: stop}
		} else {
			itemA = &dbItem{val: start}
			itemB = &dbItem{val: stop}
			if desc {
				itemA.keyless = true
				itemB.keyless = true
			}
		}
	}
	// execute the scan on the underlying tree.
	if tx.wc != nil {
		tx.wc.itercount++
		defer func() {
			tx.wc.itercount--
		}()
	}
	if desc {
		if gt {
			if lt {
				btreeDescendRange(tr, itemA, itemB, iter)
			} else {
				btreeDescendGreaterThan(tr, itemA, iter)
			}
		} else if lt {
			btreeDescendLessOrEqual(tr, itemA, iter)
		} else {
			btreeDescend(tr, iter)
		}
	} else {
		if gt {
			if lt {
				btreeAscendRange(tr, itemA, itemB, iter)
			} else {
				btreeAscendGreaterOrEqual(tr, itemA, iter)
			}
		} else if lt {
			btreeAscendLessThan(tr, itemA, iter)
		} else {
			btreeAscend(tr, iter)
		}
	}
	return nil
}

// Match returns true if the specified key matches the pattern. This is a very
// simple pattern matcher where '*' matches on any number characters and '?'
// matches on any one character.
func Match(key, pattern string) bool {
	return match.Match(key, pattern)
}

// AscendKeys allows for iterating through keys based on the specified pattern.
func (tx *Tx) AscendKeys(pattern string,
	iterator func(key, value string) bool) error {
	if pattern == "" {
		return nil
	}
	if pattern[0] == '*' {
		if pattern == "*" {
			return tx.Ascend("", iterator)
		}
		return tx.Ascend("", func(key, value string) bool {
			if match.Match(key, pattern) {
				if !iterator(key, value) {
					return false
				}
			}
			return true
		})
	}
	min, max := match.Allowable(pattern)
	return tx.AscendGreaterOrEqual("", min, func(key, value string) bool {
		if key > max {
			return false
		}
		if match.Match(key, pattern) {
			if !iterator(key, value) {
				return false
			}
		}
		return true
	})
}

// DescendKeys allows for iterating through keys based on the specified pattern.
func (tx *Tx) DescendKeys(pattern string,
	iterator func(key, value string) bool) error {
	if pattern == "" {
		return nil
	}
	if pattern[0] == '*' {
		if pattern == "*" {
			return tx.Descend("", iterator)
		}
		return tx.Descend("", func(key, value string) bool {
			if match.Match(key, pattern) {
				if !iterator(key, value) {
					return false
				}
			}
			return true
		})
	}
	min, max := match.Allowable(pattern)
	return tx.DescendLessOrEqual("", max, func(key, value string) bool {
		if key < min {
			return false
		}
		if match.Match(key, pattern) {
			if !iterator(key, value) {
				return false
			}
		}
		return true
	})
}

// Ascend calls the iterator for every item in the database within the range
// [first, last], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) Ascend(index string,
	iterator func(key, value string) bool) error {
	return tx.scan(false, false, false, index, "", "", iterator)
}

// AscendGreaterOrEqual calls the iterator for every item in the database within
// the range [pivot, last], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendGreaterOrEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(false, true, false, index, pivot, "", iterator)
}

// AscendLessThan calls the iterator for every item in the database within the
// range [first, pivot), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendLessThan(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(false, false, true, index, pivot, "", iterator)
}

// AscendRange calls the iterator for every item in the database within
// the range [greaterOrEqual, lessThan), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendRange(index, greaterOrEqual, lessThan string,
	iterator func(key, value string) bool) error {
	return tx.scan(
		false, true, true, index, greaterOrEqual, lessThan, iterator,
	)
}

// Descend calls the iterator for every item in the database within the range
// [last, first], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) Descend(index string,
	iterator func(key, value string) bool) error {
	return tx.scan(true, false, false, index, "", "", iterator)
}

// DescendGreaterThan calls the iterator for every item in the database within
// the range [last, pivot), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendGreaterThan(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(true, true, false, index, pivot, "", iterator)
}

// DescendLessOrEqual calls the iterator for every item in the database within
// the range [pivot, first], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendLessOrEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(true, false, true, index, pivot, "", iterator)
}

// DescendRange calls the iterator for every item in the database within
// the range [lessOrEqual, greaterThan), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendRange(index, lessOrEqual, greaterThan string,
	iterator func(key, value string) bool) error {
	return tx.scan(
		true, true, true, index, lessOrEqual, greaterThan, iterator,
	)
}

// AscendEqual calls the iterator for every item in the database that equals
// pivot, until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	var err error
	var less func(a, b string) bool
	if index != "" {
		less, err = tx.GetLess(index)
		if err != nil {
			return err
		}
	}
	return tx.AscendGreaterOrEqual(index, pivot, func(key, value string) bool {
		if less == nil {
			if key != pivot {
				return false
			}
		} else if less(pivot, value) {
			return false
		}
		return iterator(key, value)
	})
}

// DescendEqual calls the iterator for every item in the database that equals
// pivot, until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	var err error
	var less func(a, b string) bool
	if index != "" {
		less, err = tx.GetLess(index)
		if err != nil {
			return err
		}
	}
	return tx.DescendLessOrEqual(index, pivot, func(key, value string) bool {
		if less == nil {
			if key != pivot {
				return false
			}
		} else if less(value, pivot) {
			return false
		}
		return iterator(key, value)
	})
}

// rect is used by Intersects and Nearby
type rect struct {
	min, max []float64
}

func (r *rect) Rect(ctx interface{}) (min, max []float64) {
	return r.min, r.max
}

// Nearby searches for rectangle items that are nearby a target rect.
// All items belonging to the specified index will be returned in order of
// nearest to farthest.
// The specified index must have been created by AddIndex() and the target
// is represented by the rect string. This string will be processed by the
// same bounds function that was passed to the CreateSpatialIndex() function.
// An invalid index will return an error.
// The dist param is the distance of the bounding boxes. In the case of
// simple 2D points, it's the distance of the two 2D points squared.
func (tx *Tx) Nearby(index, bounds string,
	iterator func(key, value string, dist float64) bool) error {
	if tx.db == nil {
		return ErrTxClosed
	}
	if index == "" {
		// cannot search on keys tree. just return nil.
		return nil
	}
	// // wrap a rtree specific iterator around the user-defined iterator.
	iter := func(item rtred.Item, dist float64) bool {
		dbi := item.(*dbItem)
		return iterator(dbi.key, dbi.val, dist)
	}
	tx.refresh()
	idx := tx.state.idxs[index]
	if idx == nil {
		// index was not found. return error
		return ErrNotFound
	}
	if idx.rtr == nil {
		// not an r-tree index. just return nil
		return nil
	}
	// execute the nearby search
	var min, max []float64
	if idx.rect != nil {
		min, max = idx.rect(bounds)
	}
	// set the center param to false, which uses the box dist calc.
	idx.rtr.KNN(&rect{min, max}, false, iter)
	return nil
}

// Intersects searches for rectangle items that intersect a target rect.
// The specified index must have been created by AddIndex() and the target
// is represented by the rect string. This string will be processed by the
// same bounds function that was passed to the CreateSpatialIndex() function.
// An invalid index will return an error.
func (tx *Tx) Intersects(index, bounds string,
	iterator func(key, value string) bool) error {
	if tx.db == nil {
		return ErrTxClosed
	}
	if index == "" {
		// cannot search on keys tree. just return nil.
		return nil
	}
	// wrap a rtree specific iterator around the user-defined iterator.
	iter := func(item rtred.Item) bool {
		dbi := item.(*dbItem)
		return iterator(dbi.key, dbi.val)
	}
	tx.refresh()
	idx := tx.state.idxs[index]
	if idx == nil {
		// index was not found. return error
		return ErrNotFound
	}
	if idx.rtr == nil {
		// not an r-tree index. just return nil
		return nil
	}
	// execute the search
	var min, max []float64
	if idx.rect != nil {
		min, max = idx.rect(bounds)
	}
	idx.rtr.Search(&rect{min, max}, iter)
	return nil
}

// Len returns the number of items in the database
func (tx *Tx) Len() (int, error) {
	if tx.db == nil {
		return 0, ErrTxClosed
	}
	tx.refresh()
	return tx.state.keys.Len(), nil
}

// IndexOptions provides an index with additional features or
// alternate functionality.
type IndexOptions struct {
	// CaseInsensitiveKeyMatching allow for case-insensitive
	// matching on keys when setting key/values.
	CaseInsensitiveKeyMatching bool
}

// CreateIndex builds a new index and populates it with items.
// The items are ordered in an b-tree and can be retrieved using the
// Ascend* and Descend* methods.
// An error will occur if an index with the same name already exists.
//
// When a pattern is provided, the index will be populated with
// keys that match the specified pattern. This is a very simple pattern
// match where '*' matches on any number characters and '?' matches on
// any one character.
// The less function compares if string 'a' is less than string 'b'.
// It allows for indexes to create custom ordering. It's possible
// that the strings may be textual or binary. It's up to the provided
// less function to handle the content format and comparison.
// There are some default less function that can be used such as
// IndexString, IndexBinary, etc.
func (tx *Tx) CreateIndex(name, pattern string,
	less ...func(a, b string) bool) error {
	return tx.createIndex(name, pattern, less, nil, nil)
}

// CreateIndexOptions is the same as CreateIndex except that it allows
// for additional options.
func (tx *Tx) CreateIndexOptions(name, pattern string,
	opts *IndexOptions,
	less ...func(a, b string) bool) error {
	return tx.createIndex(name, pattern, less, nil, opts)
}

// CreateSpatialIndex builds a new index and populates it with items.
// The items are organized in an r-tree and can be retrieved using the
// Intersects method.
// An error will occur if an index with the same name already exists.
//
// The rect function converts a string to a rectangle. The rectangle is
// represented by two arrays, min and max. Both arrays may have a length
// between 1 and 20, and both arrays must match in length. A length of 1 is a
// one dimensional rectangle, and a length of 4 is a four dimension rectangle.
// There is support for up to 20 dimensions.
// The values of min must be less than the values of max at the same dimension.
// Thus min[0] must be less-than-or-equal-to max[0].
// The IndexRect is a default function that can be used for the rect
// parameter.
func (tx *Tx) CreateSpatialIndex(name, pattern string,
	rect func(item string) (min, max []float64)) error {
	return tx.createIndex(name, pattern, nil, rect, nil)
}

// CreateSpatialIndexOptions is the same as CreateSpatialIndex except that
// it allows for additional options.
func (tx *Tx) CreateSpatialIndexOptions(name, pattern string,
	opts *IndexOptions,
	rect func(item string) (min, max []float64)) error {
	return tx.createIndex(name, pattern, nil, rect, nil)
}

// createIndex is called by CreateIndex() and CreateSpatialIndex()
func (tx *Tx) createIndex(name string, pattern string,
	lessers []func(a, b string) bool,
	rect func(item string) (min, max []float64),
	opts *IndexOptions,
) error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return ErrTxIterating
	}
	if name == "" {
		// cannot create an index without a name.
		// an empty name index is designated for the main "keys" tree.
		return ErrIndexExists
	}
	// check if an index with that name already exists.
	if _, ok := tx.state.idxs[name]; ok {
		// index with name already exists. error.
		return ErrIndexExists
	}
	// genreate a less function
	var less func(a, b string) bool
	switch len(lessers) {
	default:
		// multiple less functions specified.
		// create a compound less function.
		less = func(a, b string) bool {
			for i := 0; i < len(lessers)-1; i++ {
				if lessers[i](a, b) {
					return true
				}
				if lessers[i](b, a) {
					return false
				}
			}
			return lessers[len(lessers)-1](a, b)
		}
	case 0:
		// no less function
	case 1:
		less = lessers[0]
	}
	var sopts IndexOptions
	if opts != nil {
		sopts = *opts
	}
	if sopts.CaseInsensitiveKeyMatching {
		pattern = strings.ToLower(pattern)
	}
	// intialize new index
	idx := &index{
		name:    name,
		pattern: pattern,
		less:    less,
		rect:    rect,
		db:      tx.db,
		opts:    sopts,
	}
	idx.rebuild(tx.state.keys)
	// save the index
	tx.state.idxs[name] = idx
	tx.wc.indexOps[name] = idx
	tx.wc.indexChange = true
	return nil
}

// DropIndex removes an index.
func (tx *Tx) DropIndex(name string) error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return ErrTxIterating
	}
	if name == "" {
		// cannot drop the default "keys" index
		return ErrInvalidOperation
	}
	idx, ok := tx.state.idxs[name]
	if !ok {
		return ErrNotFound
	}
	// delete from the map.
	// this is all that is needed to delete an index.
	_ = idx
	delete(tx.state.idxs, name)
	tx.wc.indexOps[name] = nil
	tx.wc.indexChange = true
	return nil
}

// Indexes returns a list of index names.
func (tx *Tx) Indexes() ([]string, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	tx.refresh()
	names := make([]string, 0, len(tx.state.idxs))
	for name := range tx.state.idxs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// Rect is helper function that returns a string representation
// of a rect. IndexRect() is the reverse function and can be used
// to generate a rect from a string.
func Rect(min, max []float64) string {
	r := grect.Rect{Min: min, Max: max}
	return r.String()
}

// Point is a helper function that converts a series of float64s
// to a rectangle for a spatial index.
func Point(coords ...float64) string {
	return Rect(coords, coords)
}

// IndexRect is a helper function that converts string to a rect.
// Rect() is the reverse function and can be used to generate a string
// from a rect.
func IndexRect(a string) (min, max []float64) {
	r := grect.Get(a)
	return r.Min, r.Max
}

// IndexString is a helper function that return true if 'a' is less than 'b'.
// This is a case-insensitive comparison. Use the IndexBinary() for comparing
// case-sensitive strings.
func IndexString(a, b string) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] >= 'A' && a[i] <= 'Z' {
			if b[i] >= 'A' && b[i] <= 'Z' {
				// both are uppercase, do nothing
				if a[i] < b[i] {
					return true
				} else if a[i] > b[i] {
					return false
				}
			} else {
				// a is uppercase, convert a to lowercase
				if a[i]+32 < b[i] {
					return true
				} else if a[i]+32 > b[i] {
					return false
				}
			}
		} else if b[i] >= 'A' && b[i] <= 'Z' {
			// b is uppercase, convert b to lowercase
			if a[i] < b[i]+32 {
				return true
			} else if a[i] > b[i]+32 {
				return false
			}
		} else {
			// neither are uppercase
			if a[i] < b[i] {
				return true
			} else if a[i] > b[i] {
				return false
			}
		}
	}
	return len(a) < len(b)
}

// IndexBinary is a helper function that returns true if 'a' is less than 'b'.
// This compares the raw binary of the string.
func IndexBinary(a, b string) bool {
	return a < b
}

// IndexInt is a helper function that returns true if 'a' is less than 'b'.
func IndexInt(a, b string) bool {
	ia, _ := strconv.ParseInt(a, 10, 64)
	ib, _ := strconv.ParseInt(b, 10, 64)
	return ia < ib
}

// IndexUint is a helper function that returns true if 'a' is less than 'b'.
// This compares uint64s that are added to the database using the
// Uint() conversion function.
func IndexUint(a, b string) bool {
	ia, _ := strconv.ParseUint(a, 10, 64)
	ib, _ := strconv.ParseUint(b, 10, 64)
	return ia < ib
}

// IndexFloat is a helper function that returns true if 'a' is less than 'b'.
// This compares float64s that are added to the database using the
// Float() conversion function.
func IndexFloat(a, b string) bool {
	ia, _ := strconv.ParseFloat(a, 64)
	ib, _ := strconv.ParseFloat(b, 64)
	return ia < ib
}

// IndexJSON provides for the ability to create an index on any JSON field.
// When the field is a string, the comparison will be case-insensitive.
// It returns a helper function used by CreateIndex.
func IndexJSON(path string) func(a, b string) bool {
	return func(a, b string) bool {
		return gjson.Get(a, path).Less(gjson.Get(b, path), false)
	}
}

// IndexJSONCaseSensitive provides for the ability to create an index on
// any JSON field.
// When the field is a string, the comparison will be case-sensitive.
// It returns a helper function used by CreateIndex.
func IndexJSONCaseSensitive(path string) func(a, b string) bool {
	return func(a, b string) bool {
		return gjson.Get(a, path).Less(gjson.Get(b, path), true)
	}
}

// Desc is a helper function that changes the order of an index.
func Desc(less func(a, b string) bool) func(a, b string) bool {
	return func(a, b string) bool { return less(b, a) }
}

//// Wrappers around btree Ascend/Descend

func bLT(tr *btree.BTree, a, b interface{}) bool { return tr.Less(a, b) }
func bGT(tr *btree.BTree, a, b interface{}) bool { return tr.Less(b, a) }

// func bLTE(tr *btree.BTree, a, b interface{}) bool { return !tr.Less(b, a) }
// func bGTE(tr *btree.BTree, a, b interface{}) bool { return !tr.Less(a, b) }

// Ascend

func btreeAscend(tr *btree.BTree, iter func(item interface{}) bool) {
	tr.Ascend(nil, iter)
}

func btreeAscendLessThan(tr *btree.BTree, pivot interface{},
	iter func(item interface{}) bool,
) {
	tr.Ascend(nil, func(item interface{}) bool {
		return bLT(tr, item, pivot) && iter(item)
	})
}

func btreeAscendGreaterOrEqual(tr *btree.BTree, pivot interface{},
	iter func(item interface{}) bool,
) {
	tr.Ascend(pivot, iter)
}

func btreeAscendRange(tr *btree.BTree, greaterOrEqual, lessThan interface{},
	iter func(item interface{}) bool,
) {
	tr.Ascend(greaterOrEqual, func(item interface{}) bool {
		return bLT(tr, item, lessThan) && iter(item)
	})
}

// Descend

func btreeDescend(tr *btree.BTree, iter func(item interface{}) bool) {
	tr.Descend(nil, iter)
}

func btreeDescendGreaterThan(tr *btree.BTree, pivot interface{},
	iter func(item interface{}) bool,
) {
	tr.Descend(nil, func(item interface{}) bool {
		return bGT(tr, item, pivot) && iter(item)
	})
}

func btreeDescendRange(tr *btree.BTree, lessOrEqual, greaterThan interface{},
	iter func(item interface{}) bool,
) {
	tr.Descend(lessOrEqual, func(item interface{}) bool {
		return bGT(tr, item, greaterThan) && iter(item)
	})
}

func btreeDescendLessOrEqual(tr *btree.BTree, pivot interface{},
	iter func(item interface{}) bool,
) {
	tr.Descend(pivot, iter)
}

func btreeNew(less func(a, b interface{}) bool) *btree.BTree {
	// Using NewNonConcurrent because we're managing our own locks.
	return btree.NewNonConcurrent(less)
}
