package datatype

import (
	"encoding/json"
	"github.com/paulmatencio/s3c/moses-bc/db"
	"time"
)

type RestoreContext struct {
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
	RestoreIntance int           `json:"restore-instance"`
}

func (*RestoreContext) New() *RestoreContext {
	var c RestoreContext
	return &c
}

func (c *RestoreContext) SetMarker(marker string) {
	c.Marker = marker
}

func (c *RestoreContext) ReadBbd(ns []byte, key []byte, Bdb *db.BadgerDB) (err error) {
	var value []byte
	value, err = Bdb.Get(ns, key)
	if err == nil {
		err = json.Unmarshal(value, c)
	}
	return
}

func (c *RestoreContext) WriteBdb(ns []byte, key []byte, Bdb *db.BadgerDB) (err error) {
	/*  the value comes from c */
	var value []byte
	value, err = json.Marshal(c)
	err = Bdb.Set(ns, key, value)
	return
}
