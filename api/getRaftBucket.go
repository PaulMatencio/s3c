package api

import (
	"encoding/json"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"io/ioutil"
	"net/http"
	"time"
)

func GetRaftBucket(url string, bucket string) (error,*datatype.RaftBucket) {
	var (
		rb      datatype.RaftBucket
		req     = "buckets/" + bucket
	)
	url = url + "/_/" + req
	gLog.Trace.Printf("GetRaft Bucket url: %s\t Retry number: %d", url, retryNumber)
	for i := 1; i <= retryNumber; i++ {
		if response, err := http.Get(url); err == nil {
			gLog.Trace.Printf("Response: %v", response)
			if response.StatusCode == 200 {
				defer response.Body.Close()
				if contents, err := ioutil.ReadAll(response.Body); err == nil {
					json.Unmarshal(contents, &rb)
				}  else {
					gLog.Error.Printf("Status: %d %s",response.StatusCode,response.Status)
				}
				break
			} else {
				gLog.Error.Printf("Status: %d %s",response.StatusCode,response.Status)
			}
		} else {
			gLog.Error.Printf("Error: %v - number of retries: %d" , err, i )
			time.Sleep(waitTime * time.Millisecond)
		}
	}
	return err,&rb
}

func GetRaftBuckets(url string) (error,[]string) {
	var (
		rl  []string
		req = "buckets"
		err error
	)
	url  = url + "/_/" + req
	for i := 1; i <= retryNumber; i++ {
		if response, err := http.Get(url); err == nil {
			gLog.Trace.Printf("Response: %v",response)
			if response.StatusCode == 200 {
				defer response.Body.Close()
				if contents, err := ioutil.ReadAll(response.Body); err == nil {
					json.Unmarshal(contents,&rl)
				}
			}else {
				gLog.Error.Printf("Status: %d %s",response.StatusCode,response.Status)
			}
			break
		} else {
			gLog.Error.Printf("Error: %v - number of retries: %d" , err, i )
			time.Sleep(waitTime * time.Millisecond)
		}
	}
	return err,rl
}

func GetRaftBuckets_v2(url string) (error,[]string) {
	var (
		buckets  []string
		req = "buckets"
		// err error
		res Resp
	)
	url  = url + "/_/" + req
	gLog.Trace.Printf("GetRaft bucket url: %s",url)
	for i := 1; i <= retryNumber; i++ {
		if res =doGet(url,buckets); res.Err == nil {
			if res.Status == 200 {
				b:= *res.Result
				buckets = b.([]string)
				break
			} else {
				gLog.Error.Printf("Status: %d",res.Status)
			}
		} else {
			gLog.Error.Printf("Error: %v - number of retries: %d" , res.Err, i )
			time.Sleep(waitTime * time.Millisecond)
		}
	}
	return res.Err,buckets
}