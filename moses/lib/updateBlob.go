package bns

//  Asynchronously Update object

import (
	"fmt"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	goLog "github.com/paulmatencio/s3c/gLog"
	"net/http"
	"os"
	"time"

	// hostpool "github.com/bitly/go-hostpool"
)

// UPdate blob

func UpdateBlob(bnsRequest *HttpRequest, url string, buf []byte, header map[string]string) {

	var (
		pid         = os.Getpid()
		action      = "UpdateBlob"
		hostname, _ = os.Hostname()
		result      = AsyncHttpUpdateBlob(bnsRequest, url, buf, header)
	)

	if sproxyd.Test {
		goLog.Trace.Printf("URL => %s \n", result.Url)
		return
	}
	if result.Err != nil {
		goLog.Trace.Printf("%s %d %s status: %s\n", hostname, pid, result.Url, result.Err)
		return
	}
	resp := result.Response
	if resp != nil {
		goLog.Trace.Printf("%s %d %s status: %s\n", hostname, pid, url, result.Response.Status)
	} else {
		goLog.Error.Printf("%s %d %s %s %s", hostname, pid, url, action, "failed")
	}

	switch resp.StatusCode {
	case 200:
		goLog.Trace.Println(hostname, pid, url, resp.Status, resp.Header["X-Scal-Ring-Key"])

	case 412:
		goLog.Warning.Println(hostname, pid, url, resp.Status, "key=", resp.Header["X-Scal-Ring-Key"], "does not exist")

	case 422:
		goLog.Error.Println(hostname, pid, url, resp.Status, resp.Header["X-Scal-Ring-Status"])
	default:
		goLog.Warning.Println(hostname, pid, url, resp.Status)
	}
	resp.Body.Close()
}

func AsyncHttpUpdateBlob(bnsRequest *HttpRequest, url string, buf []byte, header map[string]string) *sproxyd.HttpResponse {

	var (
		ch              = make(chan *sproxyd.HttpResponse)
		sproxydResponse = &sproxyd.HttpResponse{}
		sproxydRequest  = sproxyd.HttpRequest{
			Hspool:    bnsRequest.Hspool,
			Client:    bnsRequest.Client,
			Path:      url,
			ReqHeader: header,
		}
	)

	// asynchronously write the object
	go func(url string) {
		var err error
		var resp *http.Response
		resp, err = sproxyd.Updobject(&sproxydRequest, buf)
		if resp != nil {
			resp.Body.Close()
		}
		// the caller bns must issue resp.Body.Close()
		//
		ch <- &sproxyd.HttpResponse{url, resp, nil, err}
	}(url)

	for {
		select {
		case sproxydResponse = <-ch:
			return sproxydResponse
		case <-time.After(sproxyd.Timeout * time.Millisecond):
			fmt.Printf("w")
		}
	}

	return sproxydResponse
}

// func AsyncHttpUpdateBlobs(hspool hostpool.HostPool, urls []string, bufa [][]byte, headera []map[string]string) []*sproxyd.HttpResponse {

func AsyncHttpUpdateBlobs(bnsResponses []BnsResponse) []*sproxyd.HttpResponse {

	var (
		ch               = make(chan *sproxyd.HttpResponse)
		sproxydResponses = []*sproxyd.HttpResponse{}
		treq             = 0
	)
	for k, v := range bnsResponses {
		treq += 1
		url := sproxyd.TargetEnv + "/" + v.BnsId + "/" + v.PageNumber
		image := bnsResponses[k].Image
		usermd := bnsResponses[k].Usermd
		pagemd := bnsResponses[k].Pagemd
		client := &http.Client{
			Timeout:   sproxyd.WriteTimeout,
			Transport: sproxyd.Transport,
		}
		go func(url string, image []byte, usermd string, pagemd []byte) {
			var err error
			var resp *http.Response
			sproxydRequest := sproxyd.HttpRequest{}
			sproxydRequest.ReqHeader = map[string]string{
				"Usermd": usermd,
			}
			sproxydRequest.Hspool = sproxyd.TargetHP
			sproxydRequest.Client = client
			sproxydRequest.Path = url

			resp, err = sproxyd.Updobject(&sproxydRequest, image)
			if resp != nil {
				resp.Body.Close()
			}
			if !sproxyd.Test {
				defer resp.Body.Close()
			} else {
				time.Sleep(1 * time.Millisecond)
			}
			ch <- &sproxyd.HttpResponse{sproxydRequest.Path, resp, nil, err}
		}(url, image, usermd, pagemd)
	}
	for {
		select {
		case r := <-ch:
			sproxydResponses = append(sproxydResponses, r)
			if len(sproxydResponses) == treq {
				return sproxydResponses
			}
		case <-time.After(sproxyd.Timeout * time.Millisecond):
			fmt.Printf("w")
		}
	}
	return sproxydResponses
}
