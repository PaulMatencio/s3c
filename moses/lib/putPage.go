package bns

// Deprecated

import (
	//"bytes"
	"fmt"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	// goLog "moses/user/goLog"
	"net/http"
	"time"

	hostpool "github.com/bitly/go-hostpool"
)

func AsyncHttpPuts(hspool hostpool.HostPool, urls []string, bufa [][]byte, headera []map[string]string) []*sproxyd.HttpResponse {

	ch := make(chan *sproxyd.HttpResponse)
	responses := []*sproxyd.HttpResponse{}
	treq := 0
	client := &http.Client{} // one client

	for k, url := range urls {

		if len(url) == 0 {
			break
		} else {
			treq += 1
		}
		go func(url string) {
			var err error
			var resp *http.Response
			resp, err = sproxyd.PutObject(hspool, client, url, bufa[k], headera[k])
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
			fmt.Printf("w")
		}
	}
	return responses
}

func AsyncHttpPut2s(hspool hostpool.HostPool, urls []string, bufa [][]byte, bufb [][]byte, headera []map[string]string) []*sproxyd.HttpResponse {

	ch := make(chan *sproxyd.HttpResponse)
	responses := []*sproxyd.HttpResponse{}
	treq := 0

	client := &http.Client{} // one connection for all request

	for k, url := range urls {

		if len(url) == 0 {
			break
		} else {
			treq += 1
		}
		go func(url string) {
			var err error
			var resp *http.Response
			// clientw := &http.Client{}
			resp, err = sproxyd.PutObject(hspool, client, url, bufa[k], headera[k])
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
			fmt.Printf("w")
		}
	}
	return responses
}
