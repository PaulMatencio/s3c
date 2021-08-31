package datatype

import (
	"encoding/json"
	"github.com/paulmatencio/s3c/moses-bc/db"
	"time"
)

type (
	// BC defines the backupContext interface.
	BC interface {
		New()
		SetMarker(marker string)
		GetMarker()  string
		SetNextIndex(nextIndex int)
		GetNextIndex() int
		ReadBbdns (ns []byte, key []byte, Bdb *db.BadgerDB) (err error)
		WriteBdb(ns []byte, key []byte, Bdb *db.BadgerDB) (err error)
	}

	// BackupContext to save the  context of a backup. It is used for resuming a backup after a failure
	// See --resume flag
	BackupContext struct {
		SrcBucket     string        `json:"src-bucket"`
		TgtBucket     string        `json:"tgt-bucket"`
		SrcUrl        string        `json:"src-url"`
		Env           string        `json:"env"`
		Driver        string        `json:"driver"`
		Prefix        string        `json:"prefix"`
		Marker        string        `json:"marker"`
		ToDate        string        `json:"to-date"`
		FromDate      string        `json:"from-date"`
		Infile        string        `json:"input-file"`
		IBucket       string        `json:"input-bucket"`
		DbDir         string        `json:"database-directory"`
		DbName        string        `json:"database-name"`
		Maxloop       int           `json:"max-loop"`
		MaxPage       int           `json:"max-page"`
		MaxKey        int64         `json:"max-key""`
		MaxPageSize   int64         `json:"max-page-size"`
		MaxVersions   int           `json:"max-versions"`
		NameSpace     string        `json:"name-space"`
		Logit         bool          `json:"log-it"`
		Check         bool          `json:"check"`
		CtimeOut      time.Duration `json:"context-time-out"`
		NextMarker    string        `json:"next-marker"`
		NextIndex     int            `json:"next-index"`
		BackupIntance int           `json:"backup-instance"`
	}
)

func (*BackupContext) New() *BackupContext {
	var c BackupContext
	return &c
}

func (bc *BackupContext) SetMarker(marker string) {
	bc.Marker = marker
}

func (bc *BackupContext) GetMarker() string {
	return bc.Marker
}

func (bc *BackupContext) SetNextIndex(nextIndex int) {
	bc.NextIndex = nextIndex
}

func (bc *BackupContext) GetNextIndex() int {
	return bc.NextIndex
}

func (bc *BackupContext) ReadBbd(ns []byte, key []byte, Bdb *db.BadgerDB) (err error) {
	var value []byte
	value, err = Bdb.Get(ns, key)
	if err == nil {
		err = json.Unmarshal(value, bc)
	}
	return
}

func (bc *BackupContext) WriteBdb(ns []byte, key []byte, Bdb *db.BadgerDB) (err error) {
	/*  the value comes from c */
	var value []byte
	value, err = json.Marshal(bc)
	err = Bdb.Set(ns, key, value)
	return
}

func (bc *BackupContext) Get() (*BackupContext) {
	return bc
}


func (bc *BackupContext) Clear() {
	Clear(bc)
}


