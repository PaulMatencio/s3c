package bns

// Asynchronouly PUT Object

import (
	"fmt"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	"net/http"
	"time"
)

func AsyncHttpPutBlob(bnsRequest *HttpRequest, url string, buf []byte, header map[string]string) *sproxyd.HttpResponse {

	ch := make(chan *sproxyd.HttpResponse)
	// create a sproxyd request response
	sproxydResponse := &sproxyd.HttpResponse{}

	// create a sproxyd request structure
	sproxydRequest := sproxyd.HttpRequest{
		Hspool:    bnsRequest.Hspool,
		Client:    bnsRequest.Client,
		Path:      url,
		ReqHeader: header,
	}

	// asynchronously write the object
	go func(url string) {
		var err error
		var resp *http.Response
		// in test test mode , resp and err are nil
		resp, err = sproxyd.Putobject(&sproxydRequest, buf)
		if resp != nil {
			resp.Body.Close()
		}
		// the caller must close resp.Body.Close()
		// bns should close it ( in buildBnsResponse)
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

// func AsyncHttpCopyBlobs(bnsResponses []BnsResponse) []*sproxyd.HttpResponse {
func AsyncHttpPutBlobs(bnsResponses []BnsResponse) []*sproxyd.HttpResponse {
	// Put objects
	ch := make(chan *sproxyd.HttpResponse)
	sproxydResponses := []*sproxyd.HttpResponse{}

	treq := 0
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
			resp, err = sproxyd.Putobject(&sproxydRequest, image)
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
