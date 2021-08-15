package lib

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3/sc/cmd"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"sync"
	"time"
)

type LogBackup struct {
	Method  string `json:"method,omitempty"`
	Incremental bool `json:"incremental,omitempty"`
	Key     string `json:"document-id"`
	Bucket  string `json:"bucket-name"`
	Pages   int    `json:"number-pages"`
	Size    int64  `json:"document-size"`
	Pubdate string `json:"publication-date,omitempty"`
	Loaddate string `json:"load-date,omitempty"`
	Errors  int    `json:"number-errors"`
}

type LogRequest struct {
	Service   *s3.S3
	Bucket    string
	LogBackup []*LogBackup
	Ctimeout  time.Duration
}

func (logb *LogBackup) Logit( request datatype.PutObjRequest3,incr bool, timeout time.Duration) (error, string) {

	var (
		meta       = map[string]*string{}
		err1 error = nil
		msg  string
	)
	request.Key = GetCurrentDate() + "/" + logb.Method + "/" + logb.Key
	if !incr &&  len(logb.Loaddate) >= 8 {
		request.Key = ToDate(logb.Loaddate) + "/" + logb.Method + "/" + logb.Key
	}
	if jsoni, err := json.Marshal(logb); err == nil {
		jsonb := string(jsoni)
		meta["usermd"] = &jsonb
		request.Metadata = meta
		if _, err := api.PutObjectWithContext(timeout, request); err != nil {
			err1 = errors.New(fmt.Sprintf("Failed to log backup of %s to %s", request.Key, request.Bucket))
		} else {
			msg = fmt.Sprintf("The backup of %s is logged in bucket %s", request.Key, request.Bucket)
		}
	} else {
		err1 = errors.New(fmt.Sprintf("Error converting structure to Json %v", err))
	}
	return err1, msg
}


func Logit(logReq LogRequest) {

	req := datatype.PutObjRequest3{
		Service: logReq.Service,
		Bucket:  logReq.Bucket,
		Buffer:  bytes.NewBuffer([]byte{}),
	}

	wg := sync.WaitGroup{}
	for _, logb := range logReq.LogBackup {
		wg.Add(1)
		go func(*LogBackup, datatype.PutObjRequest3) {
			defer wg.Done()
			if err, msg := logb.Logit(req,logb.Incremental, logReq.Ctimeout); err != nil {
				gLog.Error.Printf("%v", err)
			} else {
				gLog.Trace.Printf("%s", msg)
			}
		}(logb, req)
	}
	wg.Wait()
}

/*
	current date
    format YYYY/MM/DD
*/
func GetCurrentDate() string {
	d := fmt.Sprintln(time.Now().Format(cmd.ISOLayout))
	return fmt.Sprintf("%s/%s/%s", d[0:4], d[5:7], d[8:10])
}

func ToDate(date string) string {
	mydate := date[0:4]+"-"+date[4:6]+"-" + date[6:8]
	if _,err := time.Parse(cmd.ISOLayout,mydate); err == nil {
		return date[0:4] + "/" + date[4:6] + "/" + date[6:8]
	} else {
		return GetCurrentDate()
	}
}

