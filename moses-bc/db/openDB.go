package db

import (
	"github.com/dgraph-io/badger/v3"
)

func OpenBadgerDB(dir string, logLevel int ) (*badger.DB,error)  {

	opts := badger.DefaultOptions(dir)
	if logLevel == 5  {
		// add debug options here
	}
	return badger.Open(opts)
}

