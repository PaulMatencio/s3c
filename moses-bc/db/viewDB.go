package db


import (
	"fmt"
	"github.com/dgraph-io/badger/v3"
)

func viewBadgerDB(db *badger.DB,keys []string) (error) {

	err := db.View (
		func(txn *badger.Txn) error {
			var err error
			for _,k := range keys {

				if item, err := txn.Get([]byte(k)); err != nil {
					return err
				} else {
					if val, err := item.ValueCopy(nil); err != nil {
						return err
					} else {
						fmt.Printf("Key:%s Value:%s\n", k,string(val))
					}
				}
			}
			return err
		})
	return err

}