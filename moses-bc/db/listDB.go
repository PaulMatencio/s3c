package db

import (
	"fmt"
	"github.com/dgraph-io/badger"
)

func List(db *badger.DB, prefix string) (error){

	return  db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		// prefix := []byte("1234")
		pref := []byte(prefix)
		for it.Seek(pref); it.ValidForPrefix(pref); it.Next() {
			var err error
			item := it.Item()
			k := item.Key()
			err = item.Value(func(v []byte) error {
				fmt.Printf("key=%s, value=%s\n", k, v)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}
