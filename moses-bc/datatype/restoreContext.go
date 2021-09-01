package datatype

import (
	"encoding/json"
	"github.com/paulmatencio/s3c/moses-bc/db"
	"time"
)

type (

	RC interface {
		New()
		SetMarker(marker string)
		GetMarker()  string
		SetNextIndex(nextIndex int)
		GetNextIndex() int
		ReadBbdns (ns []byte, key []byte, Bdb *db.BadgerDB) (err error)
		WriteBdb(ns []byte, key []byte, Bdb *db.BadgerDB) (err error)
	}


	RestoreContext struct {
	SrcBucket      string        `json:"src-bucket"`
	TgtBucket      string        `json:"tgt-bucket"`
	IndBucket      string        `json:"index-bucket"`
	TgtUrl         string        `json:"tgt-url"`
	TgtEnv         string        `json:"tgt-env"`
	TgtDriver      string        `json:"tgt-driver"`
	Prefix         string        `json:"prefix"`
	Key            string        `json:"key"`
	Marker         string        `json:"marker"`
	ToDate         string        `json:"to-date"`
	FromDate       string        `json:"from-date"`
	Infile         string        `json:"input-file"`
	IBucket        string        `json:"input-bucket"`
	DbDir          string        `json:"database-directory"`
	DbName         string        `json:"database-name"`
	Maxloop        int           `json:"max-loop"`
	MaxPage        int           `json:"max-page"`
	MaxKey         int64         `json:"max-key""`
	MaxPageSize    int64         `json:"max-page-size"`
	MaxVersions    int           `json:"max-versions"`
	NameSpace      string        `json:"name-space"`
	Logit          bool          `json:"log-it"`
	Check          bool          `json:"check"`
	Replace        bool          `json:"replace"`
	VersionId      string        `json:"version-id"`
	ReIndex        bool          `json:"re-index"`
	Tos3           bool          `json:"to-s3"`
	CtimeOut       time.Duration `json:"context-time-out"`
	NextMarker     string        `json:"next-marker"`
	NextIndex      int       `json:"next-index"`
	RestoreIntance int           `json:"restore-instance"`
}
)

func (*RestoreContext) New() *RestoreContext {
	var rc RestoreContext
	return &rc
}

func (rc *RestoreContext) SetMarker(marker string) {
	if rc != nil {
		rc.Marker = marker
	}
}

func (rc *RestoreContext) ReadBbd(ns []byte, key []byte, Bdb *db.BadgerDB) (err error) {
	var value []byte
	value, err = Bdb.Get(ns, key)
	if err == nil {
		err = json.Unmarshal(value, rc)
	}
	return
}

func (rc *RestoreContext) WriteBdb(ns []byte, key []byte, Bdb *db.BadgerDB) (err error) {
	/*  the value comes from c */
	var value []byte
	value, err = json.Marshal(rc)
	err = Bdb.Set(ns, key, value)
	return
}

func (rc *RestoreContext) SetNextIndex(nextIndex int) {
	if rc != nil {
		rc.NextIndex = nextIndex
	}
}

func (rc *RestoreContext) GetNextIndex() (nextindex int) {
	if rc != nil {
		nextindex =  rc.NextIndex
	}
	return
}