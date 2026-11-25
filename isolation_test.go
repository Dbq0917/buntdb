package buntdb

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

func btreeRootID(t *testing.T, tree any) uintptr {
	t.Helper()
	base := reflect.ValueOf(tree).Elem().FieldByName("base")
	if !base.IsValid() || base.IsNil() {
		t.Fatal("btree base is unavailable")
	}
	root := base.Elem().FieldByName("root")
	if !root.IsValid() || root.IsNil() {
		t.Fatal("btree root is unavailable")
	}
	return root.Pointer()
}

func openIsolationTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestIsolationSnapshotReadDoesNotBlockWriter(t *testing.T) {
	db := openIsolationTestDB(t)
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("hot", "0", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	readerEntered := make(chan struct{})
	releaseReader := make(chan struct{})
	readerErr := make(chan error, 1)

	go func() {
		readerErr <- db.ViewWithOptions(&TxOptions{
			Isolation: SnapshotIsolation,
		}, func(tx *Tx) error {
			first, err := tx.Get("hot")
			if err != nil {
				return err
			}
			close(readerEntered)
			<-releaseReader

			var scanned string
			err = tx.Ascend("", func(key, value string) bool {
				if key == "hot" {
					scanned = value
				}
				return true
			})
			if err != nil {
				return err
			}
			if scanned != first {
				return fmt.Errorf("scan saw %q after first value %q", scanned, first)
			}
			last, err := tx.Get("hot")
			if err != nil {
				return err
			}
			if last != first {
				return fmt.Errorf("snapshot changed from %q to %q", first, last)
			}
			return nil
		})
	}()

	select {
	case <-readerEntered:
	case <-time.After(time.Second):
		t.Fatal("reader did not start")
	}

	writerDone := make(chan error, 1)
	go func() {
		writerDone <- db.Update(func(tx *Tx) error {
			_, _, err := tx.Set("hot", "1", nil)
			return err
		})
	}()

	select {
	case err := <-writerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		close(releaseReader)
		t.Fatal("writer was blocked by a long snapshot read")
	}

	close(releaseReader)
	if err := <-readerErr; err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error {
		value, err := tx.Get("hot")
		if err != nil {
			return err
		}
		if value != "1" {
			return fmt.Errorf("committed value = %q, want %q", value, "1")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestIsolationSnapshotConcurrentSameKey(t *testing.T) {
	db := openIsolationTestDB(t)
	const fillers = 500
	if err := db.Update(func(tx *Tx) error {
		if _, _, err := tx.Set("hot", "0", nil); err != nil {
			return err
		}
		for i := 0; i < fillers; i++ {
			if _, _, err := tx.Set(fmt.Sprintf("key:%04d", i), "v", nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var writers sync.WaitGroup
	writers.Add(1)
	go func() {
		defer writers.Done()
		for i := 1; i <= 300; i++ {
			if err := db.Update(func(tx *Tx) error {
				_, _, err := tx.Set("hot", strconv.Itoa(i), nil)
				return err
			}); err != nil {
				t.Errorf("write %d: %v", i, err)
				return
			}
			if i%20 == 0 {
				runtime.Gosched()
			}
		}
	}()

	var readers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for i := 0; i < 50; i++ {
				err := db.ViewTx(func(tx *Tx) error {
					first, err := tx.Get("hot")
					if err != nil {
						return err
					}
					for pass := 0; pass < 5; pass++ {
						var scanned string
						if err := tx.Ascend("", func(key, value string) bool {
							if key == "hot" {
								scanned = value
							}
							return true
						}); err != nil {
							return err
						}
						if scanned != first {
							return fmt.Errorf("pass %d scanned %q, first was %q", pass, scanned, first)
						}
						runtime.Gosched()
					}
					last, err := tx.Get("hot")
					if err != nil {
						return err
					}
					if last != first {
						return fmt.Errorf("read changed from %q to %q", first, last)
					}
					return nil
				}, WithIsolation(SnapshotIsolation))
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}

	readers.Wait()
	writers.Wait()
}

func TestIsolationSnapshotSeesOneAtomicMultiKeyVersion(t *testing.T) {
	db := openIsolationTestDB(t)
	const keyCount = 24

	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < keyCount; i++ {
			if _, _, err := tx.Set(fmt.Sprintf("v:%02d", i), "0", nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		defer close(stop)
		for version := 1; version <= 120; version++ {
			value := strconv.Itoa(version)
			if err := db.Update(func(tx *Tx) error {
				for i := 0; i < keyCount; i++ {
					if _, _, err := tx.Set(fmt.Sprintf("v:%02d", i), value, nil); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Error(err)
				return
			}
		}
	}()

	var readers sync.WaitGroup
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				err := db.View(func(tx *Tx) error {
					version := ""
					count := 0
					var inconsistent error
					scanErr := tx.AscendKeys("v:*", func(key, value string) bool {
						if version == "" {
							version = value
						}
						if value != version {
							inconsistent = fmt.Errorf("key %s has version %q, want %q", key, value, version)
							return false
						}
						count++
						return true
					})
					if scanErr != nil {
						return scanErr
					}
					if inconsistent != nil {
						return inconsistent
					}
					if count != keyCount {
						return fmt.Errorf("scanned %d keys, want %d", count, keyCount)
					}
					return nil
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}

	writer.Wait()
	readers.Wait()
}

func TestIsolationSnapshotDoesNotCopyKeyspace(t *testing.T) {
	db := openIsolationTestDB(t)
	if err := db.CreateIndex("vals", "shadow:*", IndexString); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("shadow:%04d", i)
			if _, _, err := tx.Set(key, strconv.Itoa(i), nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	if got, want := btreeRootID(t, tx.state.keys), btreeRootID(t, db.keys); got != want {
		t.Fatalf("key snapshot root = %x, live root = %x; expected shared COW root", got, want)
	}
	txIndex, ok := tx.state.idxs["vals"]
	if !ok || txIndex.btr == nil {
		t.Fatal("snapshot index is missing")
	}
	liveIndex := db.idxs["vals"]
	if got, want := btreeRootID(t, txIndex.btr), btreeRootID(t, liveIndex.btr); got != want {
		t.Fatalf("index snapshot root = %x, live root = %x; expected shared COW root", got, want)
	}
}

func TestIsolationReaderNotBlockedByOpenWriter(t *testing.T) {
	db := openIsolationTestDB(t)
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("committed", "yes", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	writer, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := writer.Set("uncommitted", "maybe", nil); err != nil {
		t.Fatal(err)
	}

	readerDone := make(chan error, 1)
	go func() {
		readerDone <- db.View(func(tx *Tx) error {
			if _, err := tx.Get("committed"); err != nil {
				return err
			}
			if _, err := tx.Get("uncommitted"); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("uncommitted read error = %v, want ErrNotFound", err)
			}
			return nil
		})
	}()

	select {
	case err := <-readerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reader was blocked by an open writer")
	}
	if err := writer.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestIsolationReadCommittedRefreshesStatements(t *testing.T) {
	db := openIsolationTestDB(t)
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("key", "old", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	snapshot, err := db.BeginWithOptions(false, &TxOptions{
		Isolation: RepeatableRead,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = snapshot.Rollback() }()

	committed, err := db.BeginWithOptions(false, &TxOptions{
		Isolation: ReadCommitted,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = committed.Rollback() }()

	if value, err := snapshot.Get("key"); err != nil || value != "old" {
		t.Fatalf("snapshot initial = %q, %v", value, err)
	}
	if value, err := committed.Get("key"); err != nil || value != "old" {
		t.Fatalf("read-committed initial = %q, %v", value, err)
	}

	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("key", "new", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	if value, err := snapshot.Get("key"); err != nil || value != "old" {
		t.Fatalf("snapshot after write = %q, %v; want old", value, err)
	}
	if value, err := committed.Get("key"); err != nil || value != "new" {
		t.Fatalf("read-committed after write = %q, %v; want new", value, err)
	}
}

func TestIsolationConflictAbort(t *testing.T) {
	db := openIsolationTestDB(t)
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("seed", "value", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	opts := &TxOptions{Conflict: ConflictAbort}
	tx1, err := db.BeginWithOptions(true, opts)
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := db.BeginWithOptions(true, opts)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := tx1.Set("a", "1", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tx2.Set("b", "2", nil); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}

	err = tx2.Commit()
	if !errors.Is(err, ErrTxConflict) {
		t.Fatalf("commit error = %v, want %v", err, ErrTxConflict)
	}

	if err := db.View(func(tx *Tx) error {
		if _, err := tx.Get("a"); err != nil {
			return err
		}
		if _, err := tx.Get("b"); !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("b error = %v, want ErrNotFound", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestIsolationConflictOverwriteRebasesChangesAndIndex(t *testing.T) {
	db := openIsolationTestDB(t)
	if err := db.CreateIndex("vals", "*", IndexString); err != nil {
		t.Fatal(err)
	}

	opts := &TxOptions{Conflict: ConflictOverwrite}
	tx1, err := db.BeginWithOptions(true, opts)
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := db.BeginWithOptions(true, opts)
	if err != nil {
		t.Fatal(err)
	}

	if err := tx2.CreateIndex("vals2", "*", IndexString); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tx1.Set("concurrent", "one", nil); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}

	if _, _, err := tx2.Set("from-tx2", "two", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tx2.Set("concurrent", "tx2-wins", nil); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}

	err = db.View(func(tx *Tx) error {
		value, err := tx.Get("concurrent")
		if err != nil {
			return err
		}
		if value != "tx2-wins" {
			return fmt.Errorf("concurrent = %q, want tx2-wins", value)
		}

		seen := map[string]string{}
		if err := tx.Ascend("vals2", func(key, value string) bool {
			seen[key] = value
			return true
		}); err != nil {
			return err
		}
		if seen["concurrent"] != "tx2-wins" || seen["from-tx2"] != "two" {
			return fmt.Errorf("rebased index missing entries: %#v", seen)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestIsolationConflictWaitSerializesWriters(t *testing.T) {
	db := openIsolationTestDB(t)
	tx1, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	secondBegan := make(chan *Tx, 1)
	go func() {
		tx, err := db.Begin(true)
		if err != nil {
			t.Error(err)
			return
		}
		secondBegan <- tx
	}()

	select {
	case tx := <-secondBegan:
		_ = tx.Rollback()
		_ = tx1.Rollback()
		t.Fatal("second writer began while first writer was still open")
	case <-time.After(100 * time.Millisecond):
	}

	if err := tx1.Rollback(); err != nil {
		t.Fatal(err)
	}
	select {
	case tx := <-secondBegan:
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("second writer did not begin after rollback")
	}
}

func TestIsolationConflictWaitRebasesConcurrentOptimisticWrite(t *testing.T) {
	db := openIsolationTestDB(t)

	waiting, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	optimistic, err := db.BeginWithOptions(true, &TxOptions{
		Conflict: ConflictOverwrite,
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := waiting.Set("waiting", "mine", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := optimistic.Set("optimistic", "committed", nil); err != nil {
		t.Fatal(err)
	}
	if err := optimistic.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := waiting.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error {
		waitingValue, err := tx.Get("waiting")
		if err != nil {
			return err
		}
		optimisticValue, err := tx.Get("optimistic")
		if err != nil {
			return err
		}
		if waitingValue != "mine" || optimisticValue != "committed" {
			return fmt.Errorf("values = %q/%q", waitingValue, optimisticValue)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
