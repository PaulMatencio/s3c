package bns

// a DocId is composed of a TOC and pages

import (
	// "bytes"
	"encoding/json"
	"errors"
	"fmt"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	base64 "github.com/paulmatencio/ring/user/base64j"
	gLog "github.com/paulmatencio/s3c/gLog"
	"net/http"
	"os"
	"strconv"
	"time"
)

func AsyncCopyPns(pns []string, srcEnv string, targetEnv string) []*OpResponse {
	var (
		duration    time.Duration
		media       string = "binary"
		treq        int    = 0
		ch                 = make(chan *OpResponse)
		responses          = []*OpResponse{}
		pid                = os.Getpid()
		hostname, _        = os.Hostname()
		start              = time.Now()
	)

	SetCPU("100%")

	if len(srcEnv) == 0 {
		srcEnv = sproxyd.Env
	}
	if len(targetEnv) == 0 {
		targetEnv = sproxyd.TargetEnv
	}

	//  launch concurrent requets
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
				docmd                                         []byte
				encoded_docmd                                 string
				err                                           error
				statusCode                                    int
				num, num200, num412, num422, num404, numOther int = 0, 0, 0, 0, 0, 0
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
					} else {
						err = errors.New("Metadata is missing for " + srcPath)
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
			} else {
				header := map[string]string{
					"Usermd": encoded_docmd,
				}
				buf0 := make([]byte, 0)
				bnsRequest.Hspool = sproxyd.TargetHP // Set Target sproxyd servers
				// Copy the document metadata to the destination buffer size = 0 byte
				// we could  update the meta data : TODO
				CopyBlob(&bnsRequest, dstUrl, buf0, header)
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
					resp := v.Response                                                      /* http response */ // http response
					body := *v.Body                                                         // http response
					bnsResponse := BuildBnsResponse(resp, getHeader["Content-Type"], &body) // bnsResponse is a Go structure
					bnsResponses[k] = bnsResponse
					resp.Body.Close() // Close the connection after BuildBnsResponse()
				}
			}
			duration = time.Since(start)
			fmt.Println("Elapsed Get time:", duration)
			gLog.Info.Println("Elapsed Get time:", duration)

			// var sproxydResponses []*sproxyd.HttpResponse
			//   new &http.Client{}  and hosts pool are set to the target by the AsyncHttpCopyBlobs
			//  			sproxyd.TargetHP
			sproxydResponses = AsyncHttpPutBlobs(bnsResponses)

			if !sproxyd.Test {
				for _, v := range sproxydResponses {
					resp := v.Response
					url := v.Url
					switch resp.StatusCode {
					case 200:
						gLog.Trace.Println(hostname, pid, url, resp.Status, resp.Header["X-Scal-Ring-Key"])
						num200++
					case 404:
						gLog.Trace.Println(hostname, pid, url, resp.Status)
						num404++
					case 412:
						gLog.Warning.Println(hostname, pid, url, resp.Status, "key=", resp.Header["X-Scal-Ring-Key"], "already exist")
						num412++
					case 422:
						gLog.Error.Println(hostname, pid, url, resp.Status, resp.Header["X-Scal-Ring-Status"])
						num422++
					default:
						gLog.Warning.Println(hostname, pid, url, resp.Status)
						numOther++
					}
					// close all the connection
					resp.Body.Close()
				}
				if num200 < num {
					gLog.Warning.Printf("\nHost name:%s,Pid:%d,Publication:%s,Ins:%d,Outs:%d,Notfound:%d,Existed:%d,Other:%d", hostname, pid, pn, num, num200, num404, num412, numOther)
					err = errors.New("Pages outs < Page ins")
				} else {
					gLog.Info.Printf("\nHost name:%s,Pid:%d,Publication:%s,Ins:%d,Outs:%d,Notfound:%d,Existed:%d,Other:%d", hostname, pid, pn, num, num200, num404, num412, numOther)
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
			fmt.Printf("c")
		}
	}
	return responses
}
