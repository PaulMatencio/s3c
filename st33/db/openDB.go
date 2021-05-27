package db

import "github.com/dgraph-io/badger"

func OpenBadgerDB(dir string ) (*badger.DB,error)  {

	opts := badger.DefaultOptions("./")
	opts.Dir = dir
	opts.ValueDir = dir
	return badger.Open(opts)
}


