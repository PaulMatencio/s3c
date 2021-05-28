package api

import (
	"encoding/json"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

func GetRaftSession(url string, sessionId int) (error,*datatype.RaftSessionInfo) {
	var (
		rb      datatype.RaftSessionInfo
		req     = "raft_sessions"
		id = strconv.Itoa(sessionId)
	)

	url = url + "/_/" + req +"/"+ id + "/info"
	gLog.Trace.Printf("GetRaft Session url: %s\t Retry number: %d", url, retryNumber)
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
			}
		} else {
			gLog.Error.Printf("Error: %v - number of retries: %d" , err, i )
			time.Sleep(waitTime * time.Millisecond)
		}
	}
	// fmt.Printf("%v",rb)
	return err,&rb
}

func GetRaftSessionsV2(url string) (error,*datatype.RaftSessions) {
	var (
		raftSessions    datatype.RaftSessions
		req = "raft_sessions"
		res Resp
	)
	url  = url + "/_/" + req
	gLog.Trace.Printf("GetRaft Sessions url: %s\t Retry number: %d",url,retryNumber)
	for i := 1; i <= retryNumber; i++ {
		if res = doGet(url,raftSessions); res.Err == nil {
			if res.Status == 200 {
				b:= *res.Result
				raftSessions = b.(datatype.RaftSessions)
				break
			}else {
				gLog.Error.Printf("Status: %d %s",res.Status)
			}
		} else {
			gLog.Error.Printf("Error: %v - number of retries: %d" , res.Err, i )
			time.Sleep(waitTime * time.Millisecond)
		}
	}
	return res.Err,&raftSessions
}