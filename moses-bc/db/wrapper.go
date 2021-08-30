package db

import (
	"context"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/paulmatencio/s3c/gLog"
	"os"
	"time"
)

const (
	// Ref: https://godoc.org/github.com/dgraph-io/badger#DB.RunValueLogGC
	badgerDiscardRatio = 0.5
	// Default BadgerDB GC interval
	badgerGCInterval = 10 * time.Minute
)

type (
	// DB defines an embedded key/value store database interface.
	DB interface {
		Get(namespace, key []byte) (value []byte, err error)
		Set(namespace, key, value []byte) error
		SetWithTTL(namespace, key, value []byte, ttl time.Duration) error
		SetWithMeta(namespace, key, value []byte, meta byte) error
		Delete(namespace,key []byte) error
		Has(namespace, key []byte) (bool, error)
		List(namespace,prefix []byte) (error)
		Close() error
	}

	// BadgerDB is a wrapper around a BadgerDB backend database that implements
	// the DB interface.
	BadgerDB struct {
		db         *badger.DB
		ctx        context.Context
		cancelFunc context.CancelFunc
		logger     *badger.Logger
	}
)

// NewBadgerDB returns a new initialized BadgerDB database implementing the DB
// interface. If the database cannot be initialized, an error will be returned.

func NewBadgerDB(dataDir string, logger *badger.Logger) (*BadgerDB, error) {

	if err := os.MkdirAll(dataDir, 0774); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions(dataDir)
	opts.Logger = nil //  disable logging
	opts.SyncWrites = true
	badgerDB, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	bdb := &BadgerDB{
		db:     badgerDB,
		logger: logger,
	}
	bdb.ctx, bdb.cancelFunc = context.WithCancel(context.Background())
	//start Garbage collector with a go routine
	go bdb.runGC()
	return bdb, nil
}


func (bdb *BadgerDB) Get(namespace, key []byte) (value []byte, err error) {

	var (
		item  *badger.Item
	)
	err = bdb.db.View(func(txn *badger.Txn) error {
		if item, err = txn.Get(namespaceKey(namespace, []byte(key))); err == nil {
			value, err = item.ValueCopy(nil)
		}
		return err
	})
	return value, err
}


func (bdb *BadgerDB) Set(namespace, key, value []byte) error {

	return bdb.db.Update(func(txn *badger.Txn) error {
		err := txn.SetEntry(badger.NewEntry(namespaceKey(namespace, []byte(key)), []byte(value)))
		return err
	})
}

func (bdb *BadgerDB) SetWithTTL(namespace, key, value []byte, ttl time.Duration) error {

	return bdb.db.Update(func(txn *badger.Txn) error {
		err := txn.SetEntry(badger.NewEntry(namespaceKey(namespace, []byte(key)), []byte(value)).WithTTL(ttl))
		return err
	})
}

func (bdb *BadgerDB) SetWithMeta(namespace, key, value []byte, meta byte) error {

	return bdb.db.Update(func(txn *badger.Txn) error {
		err := txn.SetEntry(badger.NewEntry(namespaceKey(namespace, []byte(key)), []byte(value)).WithMeta(meta))
		return err
	})
}

func (bdb *BadgerDB) Delete (namespace, key[]byte)  error {

	return bdb.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(namespaceKey(namespace, []byte(key)))
		return err
	})
}



func (bdb *BadgerDB) Has(namespace, key []byte) (ok bool, err error) {
	_, err = bdb.Get(namespace, key)
	switch err {
	case badger.ErrKeyNotFound:
		ok, err = false, nil
	case nil:
		ok, err = true, nil
	}
	return ok,err
}

// Close implements the DB interface. It closes the connection to the underlying
// BadgerDB database as well as invoking the context's cancel function.
func (bdb *BadgerDB) Close() error {
	bdb.cancelFunc()
	return bdb.db.Close()
}

// runGC triggers the garbage collection for the BadgerDB backend database. It
// should be run in a goroutine.
func (bdb *BadgerDB) runGC() {
	ticker := time.NewTicker(badgerGCInterval)
	for {
		select {
		case <-ticker.C:
			err := bdb.db.RunValueLogGC(badgerDiscardRatio)
			if err != nil {
				// don't report error when GC didn't result in any cleanup
				if err == badger.ErrNoRewrite {
					// bdb.logger.Debugf("no BadgerDB GC occurred: %v", err)
					gLog.Error.Printf("no BadgerDB GC occurred: %v", err)
				} else {
					gLog.Error.Printf("failed to GC BadgerDB: %v", err)
					// bdb.logger.Errorf("failed to GC BadgerDB: %v", err)
				}
			}
		case <-bdb.ctx.Done():
			return
		}
	}
}


/*
	Prefix scans
 */
func (bdb *BadgerDB) List(namespace, prefix []byte)  (error) {

	return  bdb.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		pref := namespaceKey(namespace, prefix)
		// pref := []byte(prefix)
		for it.Seek(pref); it.ValidForPrefix(pref); it.Next() {
			var err error
			item := it.Item()
			k := item.Key()
			err = item.Value(func(v []byte) error {
				gLog.Info.Printf("key=%s, value=%s\n", k, v)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

}

// badgerNamespaceKey returns a composite key used for lookup and storage for a
// given namespace and key.
func namespaceKey(namespace, key []byte) []byte {
	prefix := []byte(fmt.Sprintf("%s/", namespace))
	return append(prefix, key...)
}
