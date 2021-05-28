package bns

import (
	"encoding/json"
	"fmt"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	base64 "github.com/paulmatencio/ring/user/base64j"
	goLog "github.com/paulmatencio/s3c/gLog"
	"net/http"
	"time"
)

func UpdMetadata(client *http.Client, path string, usermd []byte) (error, time.Duration) {
	encoded_usermd := base64.Encode64(usermd)
	updheader := map[string]string{
		"Usermd":       encoded_usermd,
		"Content-Type": "image/tiff",
	}
	err := error(nil)
	var resp *http.Response
	start := time.Now()
	var elapse time.Duration

	if resp, err = sproxyd.UpdMetadata(client, path, updheader); err != nil {
		goLog.Error.Println(err)
	} else {
		elapse := time.Since(start)
		switch resp.StatusCode {
		case 200:
			goLog.Trace.Println(resp.Request.URL.Path, resp.Status, resp.Header["X-Scal-Ring-Key"], elapse)
		case 404:
			goLog.Warning.Println(resp.Request.URL.Path, resp.Status, " not found", elapse)
		case 412:
			goLog.Warning.Println(resp.Request.URL.Path, resp.Status, "key=", resp.Header["X-Scal-Ring-Key"], " does not exist", elapse)
		case 422:
			goLog.Error.Println(resp.Request.URL.Path, resp.Status, resp.Header["X-Scal-Ring-Status"], elapse)
		default:
			goLog.Info.Println(resp.Request.URL.Path, resp.Status, elapse)
		}
		resp.Body.Close() // Sproxyd does  not close the connection
	}
	return err, elapse

}

func AsyncHttpUpdMetadatas(meta string, urls []string, headera []map[string]string) []*sproxyd.HttpResponse {
	// if meta == "Page"
	// Update meta data read from a File
	// TODO Update meta data reda from the Ring
	ch := make(chan *sproxyd.HttpResponse)
	responses := []*sproxyd.HttpResponse{}
	treq := 0
	clientw := &http.Client{}
	for k, url := range urls {

		if len(url) == 0 {
			break
		} else {
			treq += 1
		}
		go func(url string) {
			var (
				pagmeta Pagemeta // OLD METADATA
				// usermd  []byte
				err  error
				resp *http.Response
			)
			// clientw := &http.Client{}
			um, _ := base64.Decode64(headera[k]["Usermd"])
			if err = json.Unmarshal(um, &pagmeta); err == nil {
				// SET NEW METATA HERE
				// pmd := pagmeta.ToPagemeta()
				//	if usermd, err = json.Marshal(&pmd); err == nil {
				//	headera[k]["Usermd"] = base64.Encode64(usermd)
				//	}
			}
			resp, err = sproxyd.UpdMetadata(clientw, url, headera[k])

			if resp != nil {
				resp.Body.Close()
			}
			ch <- &sproxyd.HttpResponse{url, resp, nil, err}
		}(url)
	}
	for {
		select {
		case r := <-ch:
			responses = append(responses, r)
			if len(responses) == treq {
				return responses
			}
		case <-time.After(sproxyd.Timeout * time.Millisecond):
			fmt.Printf(".")
		}
	}
	return responses
}
