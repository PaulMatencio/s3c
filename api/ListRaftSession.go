package api

import (
	"encoding/json"
	datatype "github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"io/ioutil"
	"net/http"
	"time"
)
type Resp struct {
	Result *interface{}
	Err    error
	Status int
}
var (
	err            error
	// waitTime       = utils.GetWaitTime(*viper.GetViper())
	waitTime time.Duration = 200
	// retryNumber  = utils.GetRetryNumber(*viper.GetViper());
	retryNumber = 3
)

func ListRaftSessions(url string) (error,*datatype.RaftSessions) {

	var (
		raftSessions    datatype.RaftSessions
		req = "raft_sessions"
	)
	url  = url + "/_/" + req
	gLog.Trace.Println("URL:", url)
	for i := 1; i <= retryNumber; i++ {
		if response, err := http.Get(url); err == nil {
			gLog.Trace.Printf("Response: %v",response)
			if response.StatusCode == 200 {
				defer response.Body.Close()
				if contents, err := ioutil.ReadAll(response.Body); err == nil {
					json.Unmarshal(contents,&raftSessions)
				}
			} else {
				gLog.Error.Printf("Status: %d %s",response.StatusCode,response.Status)
			}
			break
		} else {
			gLog.Error.Printf("Error: %v - number of retries: %d" , err, i )
			time.Sleep(waitTime * time.Millisecond)
		}
	}
	return err,&raftSessions
}


func doGet(url string,result interface{}) (Resp) {
	var (
		err error
		response *http.Response
		res Resp
	)

	if response, err = http.Get(url); err == nil {
		gLog.Trace.Printf("Response: %v", response)

		if response.StatusCode == 200 {
			defer response.Body.Close()
			if contents, err := ioutil.ReadAll(response.Body); err == nil {
				json.Unmarshal(contents, &result)
			}
		}

		gLog.Trace.Printf("doGet url:%s\tStatus Code:%d", url, response.StatusCode)
		res = Resp{
			Result: &result,
			Err:    err,
			Status: response.StatusCode,
		}
	} else {
		res = Resp{
			Err:    err,
		}
	}
	return res

}






