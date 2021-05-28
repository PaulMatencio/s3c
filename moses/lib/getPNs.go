package bns

// a DocId is composed of a TOC and pages

import (
	// "bytes"
	"encoding/json"
	"errors"
	"fmt"
	base64 "github.com/paulmatencio/ring/user/base64j"
	gLog "github.com/paulmatencio/s3c/gLog"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	"net/http"
	"strconv"
	"time"
)

func AsyncGetPns(pns []string, srcEnv string) ([]*OpResponse, int, int) {
	var (
		duration             time.Duration
		media                string = "binary"
		treq, doc404, docErr int    = 0, 0, 0
		ch                          = make(chan *OpResponse)
		responses                   = []*OpResponse{}
		start                       = time.Now()
	)

	SetCPU("100%")

	if len(srcEnv) == 0 {
		srcEnv = sproxyd.Env
	}

	//  launch concurrent get requets
	for _, pn := range pns {

		var (
			srcPath = srcEnv + "/" + pn

			srcUrl = srcPath

			bnsRequest = HttpRequest{
				Hspool: sproxyd.HP, // source sproxyd servers IP address and ports
				Client: &http.Client{
					Timeout:   sproxyd.WriteTimeout,
					Transport: sproxyd.Transport,
				},
				Media: media,
			}
		)
		go func(srcUrl string) {
			treq++
			var (
				docmd         []byte
				encoded_docmd string
				err           error
				statusCode    int
				num, num200   int = 0, 0
			)
			// Get the  Table of Content
			if encoded_docmd, err, statusCode = GetEncodedMetadata(&bnsRequest, srcUrl); err == nil {
				if len(encoded_docmd) > 0 {

					if docmd, err = base64.Decode64(encoded_docmd); err != nil {
						gLog.Error.Println(err) // Invalid meta data
						ch <- &OpResponse{err, pn, num, num200}
						return
					}
				} else {
					if statusCode == 404 {
						err = errors.New("Document " + srcPath + " not found")
						doc404++
					} else {
						err = errors.New("Document metadata is missing for " + srcPath)
						docErr++
					}
					gLog.Warning.Println(err)
					ch <- &OpResponse{err, pn, num, num200}
					return
				}
			} else {
				gLog.Error.Println(err)
				ch <- &OpResponse{err, pn, num, num200}
				return
			}
			// convert the PN  metadata into a go structure
			docmeta := DocumentMetadata{}
			// docmd := bytes.Replace(docmd1, []byte(`"\n  "`), []byte(`{}`), -1)

			if err := json.Unmarshal(docmd, &docmeta); err != nil {
				gLog.Error.Println("Document metadata is invalid ", srcUrl, err)
				gLog.Error.Println(string(docmd), docmeta)
				ch <- &OpResponse{err, pn, num, num200}
				return
			}

			if num, err = docmeta.GetPageNumber(); err != nil {
				ch <- &OpResponse{err, pn, num, num200}
			}

			var (
				urls      = make([]string, num, num)
				getHeader = map[string]string{
					"Content-Type": "application/binary",
				}
			)

			for i := 0; i < num; i++ {
				urls[i] = srcPath + "/p" + strconv.Itoa(i+1)
			}
			bnsRequest.Urls = urls
			bnsRequest.Hspool = sproxyd.HP // Set source sproxyd servers
			bnsRequest.Client = &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			}
			// Get all the pages from the source Ring
			sproxydResponses := AsyncHttpGetBlobs(&bnsRequest, getHeader)
			// Build a response array of BnsResponse array to be used to update the pages  of  destination sproxyd servers
			bnsResponses := make([]BnsResponse, num, num)

			for k, v := range sproxydResponses {
				if err := v.Err; err == nil { //
					resp := v.Response
					if resp.StatusCode == 200 {
						num200++
					} /* http response */ // http response
					body := *v.Body                                                         // http response
					bnsResponse := BuildBnsResponse(resp, getHeader["Content-Type"], &body) // bnsResponse is a Go structure
					bnsResponses[k] = bnsResponse
					resp.Body.Close()
				}
			}
			duration = time.Since(start)
			fmt.Println("Elapsed Get time:", duration)
			gLog.Info.Println("Elapsed Get time:", duration)

			ch <- &OpResponse{err, pn, num, num200}

		}(srcUrl)
	}

	//  Loop wait for results
	for {
		select {
		case r := <-ch:
			responses = append(responses, r)
			if len(responses) == treq {
				return responses, doc404, docErr
			}
		case <-time.After(sproxyd.CopyTimeout * time.Millisecond):
			fmt.Printf("r")
		}
	}
	// return responses, num404
}
