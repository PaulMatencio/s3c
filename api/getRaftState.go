package api

import (
	"encoding/json"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"io/ioutil"
	"net/http"
	"time"
)

func GetRaftState(client *http.Client,url string) (error,*datatype.RaftState) {
	var (
		req = "raft/state"
		err error
		rl  datatype.RaftState
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
