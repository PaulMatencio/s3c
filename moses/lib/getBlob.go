package bns

//  Get full object
//  Get range of byte ( subpart of an object)

import (
	"fmt"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	"io/ioutil"
	"net/http"
	"time"

	// hostpool "github.com/bitly/go-hostpool"
)

func GetBlob(sproxydRequest *sproxyd.HttpRequest) (*http.Response, error) {
	sproxydRequest.ReqHeader = map[string]string{}
	return sproxyd.Getobject(sproxydRequest)
}

func AsyncHttpGetBlobs(bnsRequest *HttpRequest, getHeader map[string]string) []*sproxyd.HttpResponse {

	var (
		ch               = make(chan *sproxyd.HttpResponse)
		sproxydResponses = []*sproxyd.HttpResponse{}
		sproxydRequest   = sproxyd.HttpRequest{
			Hspool:    bnsRequest.Hspool,
			ReqHeader: getHeader,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		treq = 0
	)
	// sproxydRequest.Client = &http.Client{}
	fmt.Printf("\n")
	for _, url := range bnsRequest.Urls {
		/* just in case, the requested page number is beyond the max number of pages */
		if len(url) == 0 {
			break
		} else {
			treq += 1
		}

		// sproxydRequest.Client = &http.Client{}

		go func(url string) {
			sproxydRequest.Path = url
			resp, err := sproxyd.Getobject(&sproxydRequest)
			defer resp.Body.Close()
			var body []byte
			if err == nil {
				body, _ = ioutil.ReadAll(resp.Body)
			} else {
				resp.Body.Close()
			}
			// WARNING The caller must close the Body after it is consumed
			ch <- &sproxyd.HttpResponse{url, resp, &body, err}
		}(url)
	}
	// wait for  response  message
	for {
		select {
		case r := <-ch:
			sproxydResponses = append(sproxydResponses, r)
			if len(sproxydResponses) == treq /*len(urls)*/ {
				return sproxydResponses
			}
		case <-time.After(50 * time.Millisecond):
			fmt.Printf("r")
		}
	}
	return sproxydResponses
}
