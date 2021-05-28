package bns

// a DocId is composed of a TOC and pages

import (
	"encoding/json"
	"errors"
	"fmt"

	// "io"
	// "io/ioutil"

	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	base64 "github.com/paulmatencio/ring/user/base64j"
	// file "moses/user/files/lib"
	// "bytes"
	goLog "github.com/paulmatencio/s3c/gLog"
	"net/http"
	"os"
	"strconv"
	"time"

	// hostpool "github.com/bitly/go-hostpool"
)

func AsyncUpdatePns(pns []string, srcEnv string, targetEnv string) []*OpResponse {

	var (
		pid         = os.Getpid()
		hostname, _ = os.Hostname()
		start       = time.Now()
		media       = "binary"
		ch          = make(chan *OpResponse)
		responses   = []*OpResponse{}
		treq        = 0
	)

	if len(srcEnv) == 0 {
		srcEnv = sproxyd.Env
	}
	if len(targetEnv) == 0 {
		targetEnv = sproxyd.TargetEnv
	}

	SetCPU("100%")

	for _, pn := range pns {

		var (
			srcPath    = srcEnv + "/" + pn
			dstPath    = targetEnv + "/" + pn
			srcUrl     = srcPath
			dstUrl     = dstPath
			bnsRequest = HttpRequest{
				Hspool: sproxyd.HP, // source sproxyd servers IP address and ports
				Client: &http.Client{
					Timeout:   sproxyd.WriteTimeout,
					Transport: sproxyd.Transport,
				},
				Media: media,
			}
		)

		go func(srcUrl string, dstUrl string) {

			treq++
			var (
				err                                           error
				encoded_docmd                                 string
				docmd                                         []byte
				statusCode                                    int
				num, num200, num404, num412, num422, numOther int = 0, 0, 0, 0, 0, 0
			)

			// Get the PN metadata ( Table of Content)
			if encoded_docmd, err, statusCode = GetEncodedMetadata(&bnsRequest, srcUrl); err == nil {
				if len(encoded_docmd) > 0 {
					if docmd, err = base64.Decode64(encoded_docmd); err != nil {
						goLog.Error.Println(err)
						ch <- &OpResponse{err, pn, num, num200}
						return
					}
				} else {
					if statusCode == 404 {
						err = errors.New("Document " + srcPath + " not found")
					} else {
						err = errors.New("Metadata is missing for " + srcPath)
					}
					goLog.Warning.Println(err)
					ch <- &OpResponse{err, pn, num, num200}
					return
				}
			} else {
				goLog.Error.Println(err)
				ch <- &OpResponse{err, pn, num, num200}
				return
			}
			// convert the PN  metadata (TOC) into a go structure
			docmeta := DocumentMetadata{}

			/*  docmd = bytes.Replace(docmd1, []byte(`\n`), []byte(``), -1) */
			if err := json.Unmarshal(docmd, &docmeta); err != nil {
				goLog.Error.Println("Document metadata is invalid ", srcUrl, err)
				goLog.Error.Println(string(docmd), docmeta)
				ch <- &OpResponse{err, pn, num, num200}
				return
			} else {
				header := map[string]string{
					"Usermd": encoded_docmd,
				}
				buf0 := make([]byte, 0)
				bnsRequest.Hspool = sproxyd.TargetHP // Set Target sproxyd servers
				// Copy the document metadata to the destination with buffer size = 0 byte
				// we could  update the meta data : TODO
				UpdateBlob(&bnsRequest, dstUrl, buf0, header)

			}

			if num, err = docmeta.GetPageNumber(); err != nil {
				ch <- &OpResponse{err, pn, num, num200}
				return
			}
			// COPY EVERY PAGES ASYNCHRONOUSLY
			var (
				duration  time.Duration
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
			/*
				bnsRequest.Client = &http.Client{
					Timeout:   sproxyd.ReadTimeout,
					Transport: sproxyd.Transport,
				}
			*/
			// Get all the pages from the source Ring
			sproxyResponses := AsyncHttpGetBlobs(&bnsRequest, getHeader)

			// Build a response array from the BnsResponse array to update the pages  at the  destination Ring
			bnsResponses := make([]BnsResponse, num, num)

			for k, v := range sproxyResponses {
				if err := v.Err; err == nil { //
					resp := v.Response                                                      /* http response */ // http response
					body := *v.Body                                                         // http response                                                          /* copy of the body */ // http body response
					bnsResponse := BuildBnsResponse(resp, getHeader["Content-Type"], &body) // bnsResponse is a Go structure
					bnsResponses[k] = bnsResponse
					resp.Body.Close()
				}
			}
			duration = time.Since(start)
			fmt.Println("Elapsed Get time:", duration)
			goLog.Info.Println("Elapsed Get time:", duration)

			// var sproxydResponses []*sproxyd.HttpResponse
			//   new &http.Client{}  and hosts pool are set to the target by the AsyncHttpCopyBlobs
			//  			sproxyd.TargetHP
			sproxydResponses := AsyncHttpUpdateBlobs(bnsResponses)

			if !sproxyd.Test {
				for _, v := range sproxydResponses {
					resp := v.Response
					url := v.Url
					switch resp.StatusCode {
					case 200:
						goLog.Trace.Println(hostname, pid, url, resp.Status, resp.Header["X-Scal-Ring-Key"])
						num200++
					case 404:
						goLog.Trace.Println(hostname, pid, url, resp.Status)
						num404++
					case 412:
						goLog.Warning.Println(hostname, pid, url, resp.Status, "key=", resp.Header["X-Scal-Ring-Key"], "does not exist")
						num412++
					case 422:
						goLog.Error.Println(hostname, pid, url, resp.Status, resp.Header["X-Scal-Ring-Status"])
						num422++
					default:
						goLog.Warning.Println(hostname, pid, url, resp.Status)
						numOther++
					}
					// close all the connection
					resp.Body.Close()
				}

				if num200 < num {
					goLog.Warning.Printf("Host name:%s,Pid:%d,Publication:%s,Ins:%d,Outs:%d,Notfound:%d,Existed:%d,Other:%d", hostname, pid, pn, num, num200, num404, num412, numOther)
					err = errors.New("Pages outs < Page ins")
				} else {
					goLog.Info.Printf("Host name:%s,Pid:%d,Publication:%s,Ins:%d,Outs:%d,Notfound:%d,Existed:%d,Other:%d", hostname, pid, pn, num, num200, num404, num412, numOther)
				}
			}
			ch <- &OpResponse{err, pn, num, num200}

		}(srcUrl, dstUrl)
	}

	//  Loop wait for results
	for {
		select {
		case r := <-ch:
			responses = append(responses, r)
			if len(responses) == treq {
				return responses
			}
		case <-time.After(sproxyd.CopyTimeout * time.Millisecond):
			fmt.Printf("u")
		}
	}
	return responses
}
