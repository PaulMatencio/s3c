
package datatype

import (
	"encoding/json"
	// "encoding/json"
	// "github.com/paulmatencio/s3c/moses-bc/db"
	"github.com/paulmatencio/s3c/moses-bc/db"
	"time"
)
const (
	TTL time.Duration = 720 * time.Hour /* hours */
)

type ErrorContext struct {

	Type      string        `json:"type"`
	Message   string        `json:"message"`
	Time      time.Time     `json:"date-time"`
}

func (*ErrorContext) New() *ErrorContext {
	var e ErrorContext
	return &e
}

func (e *ErrorContext) WriteBdb(ns []byte, key []byte, value []byte, Bdb *db.BadgerDB) (err error) {
	v := []byte{}
	e.Message = string(value)
	e.Time = time.Now()

	if v,err = json.Marshal(e); err == nil {
		err = Bdb.SetWithTTL(ns, key, v,TTL)
	} else {
		return
	}
	return
}

