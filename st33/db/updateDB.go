package db

import "github.com/dgraph-io/badger"

func UpdateBadgerDB(db *badger.DB, updates map[string]string ) (error){

	err := db.Update(

		func(txn *badger.Txn) error {
			txn = db.NewTransaction(true)

			for k,v := range updates {
				if err := txn.Set([]byte(k),[]byte(v)); err == badger.ErrTxnTooBig {
					_ = txn.Commit()
					txn = db.NewTransaction(true)
					_ = txn.Set([]byte(k),[]byte(v))
				}
			}
			_ = txn.Commit()

			return nil
		})

	return err

}