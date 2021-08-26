package db
import "github.com/dgraph-io/badger/v3"

func UpdateBadgerDB(db *badger.DB, updates map[string]string ) (err error){

	err = db.Update(

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

func Set(db *badger.DB, key string, value string) (err error) {
	return db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry([]byte(key), []byte(value))
		err = txn.SetEntry(e)
		return err
	})
}