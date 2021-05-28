package bns

import (
	"fmt"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	gLog "github.com/paulmatencio/s3c/gLog"
	"net/http"
	"time"
)

func DeleteBlob(bnsRequest *HttpRequest, url string) (error, int) {

	var (
		resp           *http.Response
		start          = time.Now()
		elapse         time.Duration
		err            error
		sproxydRequest = sproxyd.HttpRequest{
			Hspool: bnsRequest.Hspool,
			Client: bnsRequest.Client,
			Path:   url,
		}
	)

	// if resp, err = sproxyd.DeleteObject(hspool, client, path); err != nil {
	if resp, err = sproxyd.Deleteobject(&sproxydRequest); err != nil {
		gLog.Error.Println(err)
		return err, 0
	} else {
		elapse = time.Since(start)
		switch resp.StatusCode {
		case 200:
			gLog.Trace.Println(resp.Request.URL.Path, resp.Status, resp.Header["X-Scal-Ring-Key"], elapse)
		case 404:
			gLog.Warning.Println(resp.Request.URL.Path, resp.Status, " not found", elapse)
		case 412:
			gLog.Warning.Println(resp.Request.URL.Path, resp.Status, "key=", resp.Header["X-Scal-Ring-Key"], " does not exist", elapse)
		case 422:
			gLog.Error.Println(resp.Request.URL.Path, resp.Status, resp.Header["X-Scal-Ring-Status"], elapse)
		default:
			gLog.Info.Println(resp.Request.URL.Path, resp.Status, elapse)
		}
		resp.Body.Close()
	}
	return err, resp.StatusCode
}

func AsyncHttpDeleteBlob(bnsRequest *HttpRequest, url string) *sproxyd.HttpResponse {

	var (
		ch              = make(chan *sproxyd.HttpResponse)
		sproxydResponse = &sproxyd.HttpResponse{}
		sproxydRequest  = sproxyd.HttpRequest{
			Hspool: bnsRequest.Hspool,
			Client: &http.Client{},
			Path:   url,
		}
	)
	if len(url) == 0 {
		return sproxydResponse
	}

	go func(url string) {
		var err error
		var resp *http.Response
		resp, err = sproxyd.Deleteobject(&sproxydRequest)
		if resp != nil {
			resp.Body.Close()
		}
		if !sproxyd.Test {
			defer resp.Body.Close()
		} else {
			time.Sleep(1 * time.Millisecond) // simuate a asynchronous response time
		}

		ch <- &sproxyd.HttpResponse{url, resp, nil, err}
	}(url)

	for {
		select {
		case sproxydResponse = <-ch:
			return sproxydResponse
		case <-time.After(sproxyd.Timeout * time.Millisecond):
			fmt.Printf("d")
		}
	}
	return sproxydResponse
}

func AsyncHttpDeleteBlobs(bnsRequest *HttpRequest) []*sproxyd.HttpResponse {

	ch := make(chan *sproxyd.HttpResponse)
	sproxydResponses := []*sproxyd.HttpResponse{}
	treq := 0
	// fmt.Printf("\n")

	for _, url := range bnsRequest.Urls {

		if len(url) == 0 {
			break
		} else {
			treq += 1
		}
		bnsRequest.Client = &http.Client{}

		go func(url string) {
			sproxydRequest := sproxyd.HttpRequest{
				Hspool: bnsRequest.Hspool,
				Client: bnsRequest.Client,
				Path:   url,
			}
			resp, err := sproxyd.Deleteobject(&sproxydRequest)

			ch <- &sproxyd.HttpResponse{url, resp, nil, err}
		}(url)
	}
	// wait for http response  message
	for {
		select {
		case r := <-ch:
			// fmt.Printf("%s was fetched\n", r.Url)
			sproxydResponses = append(sproxydResponses, r)
			if len(sproxydResponses) == treq {

				return sproxydResponses
			}
		case <-time.After(50 * time.Millisecond):
			fmt.Printf("r")
		}
	}

	return sproxydResponses
}
