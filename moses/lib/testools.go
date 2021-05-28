package bns

import (
	"fmt"
	"io/ioutil"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	"net/http"
	"time"

	hostpool "github.com/bitly/go-hostpool"
)

// Used to put/update/get a same object multiple times
// Used ONLY by test.go to valide the performance of the  Ring performance
// Use utilities.go for asynchronous operations with different objects

// func AsyncHttpGet(hspool hostpool.HostPool, urls []string) []*sproxyd.HttpResponse {
func AsyncHttpGet(bnsRequest *HttpRequest) []*sproxyd.HttpResponse {

	ch := make(chan *sproxyd.HttpResponse)
	responses := []*sproxyd.HttpResponse{}
	sproxydRequest := sproxyd.HttpRequest{}
	sproxydRequest.Hspool = bnsRequest.Hspool

	treq := 0
	fmt.Printf("\n")
	for _, url := range bnsRequest.Urls {
		/* just in case, the requested page number is beyond the max number of pages */
		if len(url) == 0 {
			break
		} else {
			treq += 1
		}
		// client := &http.Client{}
		sproxydRequest.Client = &http.Client{}
		sproxydRequest.Path = url
		go func(url string) {
			// fmt.Printf("Fetching %s \n", url)

			resp, err := GetBlob(&sproxydRequest)
			var body []byte
			if err == nil {
				body, _ = ioutil.ReadAll(resp.Body)
			} else {

				resp.Body.Close()
			}
			ch <- &sproxyd.HttpResponse{url, resp, &body, err}

		}(url)
	}
	// wait for http response  message
	for {
		select {
		case r := <-ch:
			// fmt.Printf("%s was fetched\n", r.url)
			responses = append(responses, r)
			if len(responses) == treq /*len(urls)*/ {
				return responses
			}
		case <-time.After(100 * time.Millisecond):
			fmt.Printf(".")
		}
	}
	return responses
}

func AsyncHttpUpdate(hspool hostpool.HostPool, urls []string, buf []byte, header map[string]string) []*sproxyd.HttpResponse {

	ch := make(chan *sproxyd.HttpResponse)
	responses := []*sproxyd.HttpResponse{}

	treq := 0

	for _, url := range urls {
		if len(url) == 0 {
			break
		} else {
			treq += 1
		}
		client := &http.Client{}
		go func(url string) {
			var err error
			var resp *http.Response

			resp, err = sproxyd.UpdObject(hspool, client, url, buf, header)
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

func AsyncHttpPut(hspool hostpool.HostPool, urls []string, buf []byte, header map[string]string) []*sproxyd.HttpResponse {

	ch := make(chan *sproxyd.HttpResponse)
	responses := []*sproxyd.HttpResponse{}

	treq := 0

	for _, url := range urls {
		if len(url) == 0 {
			break
		} else {
			treq += 1
		}
		go func(url string) {
			var err error
			var resp *http.Response
			clientw := &http.Client{}
			resp, err = sproxyd.PutObject(hspool, clientw, url, buf, header)
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

/*
func AsyncHttpDeletet(hspool hostpool.HostPool, urls []string, deleteheader map[string]string) []*sproxyd.HttpResponse {

	ch := make(chan *sproxyd.HttpResponse)
	responses := []*sproxyd.HttpResponse{}
	treq := 0

	for _, url := range urls {
		if len(url) == 0 {
			break
		} else {
			treq += 1
		}
		client := &http.Client{}
		go func(url string) {
			var err error
			var resp *http.Response

			resp, err = sproxyd.DeleteObject(hspool, client, url)

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
*/
