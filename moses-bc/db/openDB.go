package db

import "github.com/dgraph-io/badger/v3"

func OpenBadgerDB(dir string, logLevel int ) (*badger.DB,error)  {

	opts := badger.DefaultOptions("./")
	if logLevel == 5  {
		// add debug options here
	}
	opts.Dir = dir
	opts.ValueDir = dir
	return badger.Open(opts)
}