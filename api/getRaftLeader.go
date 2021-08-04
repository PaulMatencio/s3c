package api

import (
	"encoding/json"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"io/ioutil"
	"net/http"
	"time"
)

func GetRaftLeader(client *http.Client, url string) (error,*datatype.RaftLeader) {
	var (
		req = "raft/leader"
		err error
		rl  datatype.RaftLeader
	)

	url  = url + "/_/" + req
	gLog.Trace.Printf("GetRaft Leader url: %s",url)
	for i := 1; i <= retryNumber; i++ {
		if response, err := client.Get(url); err == nil {
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
	return err,&rl
}

func GetRaftLeaderV2(client *http.Client,url string) (error,datatype.RaftLeader) {
	var (
		req = "raft/leader"
		// err error
		rl  datatype.RaftLeader
		res Resp
	)
	url  = url + "/_/" + req
	gLog.Trace.Printf("GetRaft Leader url: %s",url)
	for i := 1; i <= retryNumber; i++ {
		if res = doGet(url,rl); res.Err == nil {
			if res.Status == 200 {
				b := *res.Result
				rl = b.(datatype.RaftLeader)
				break
			}
		} else {
			gLog.Error.Printf("Error: %v - number of retries: %d" , res.Err, i )
			time.Sleep(waitTime * time.Millisecond)
		}
	}
	return res.Err,rl
}
