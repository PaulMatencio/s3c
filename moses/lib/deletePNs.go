package bns

//  DELETE <targetENV>  PNs on the Target RING
//

import (
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

func AsyncDeletePns(pns []string, targetEnv string) []*OpResponse {

	var (
		pid         = os.Getpid()
		hostname, _ = os.Hostname()
		ch          = make(chan *OpResponse)
		responses   = []*OpResponse{}
		media       = "binary"
		statusCode  int
		treq        = 0
	)

	if len(targetEnv) == 0 {
		targetEnv = sproxyd.TargetEnv
	}
	SetCPU("100%")

	//  launch concurrent requets
	for _, pn := range pns {
		targetPath := targetEnv + "/" + pn
		targetUrl := targetPath
		//
		//  Read the PN 's metadata  from the source RING
		//  The SOURCE RING may be the same as the DESTINATION RING
		//  Check the config file sproxyd.HP and sproxyd.TargetHP
		//

		bnsRequest := HttpRequest{
			Hspool: sproxyd.TargetHP, // <<<<<<  sproxyd.TargetHP is the Destination RING
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			Media: media,
		}

		gLog.Info.Println("Deleting", targetUrl)

		go func(targetUrl string) {
			var (
				docmd                                         []byte
				encoded_docmd                                 string
				err                                           error
				num, num200, num404, numOther, num412, num422 = 0, 0, 0, 0, 0, 0
			)
			treq++
			// Get the PN metadata ( Table of Content)
			if encoded_docmd, err, statusCode = GetEncodedMetadata(&bnsRequest, targetUrl); err == nil {
				if len(encoded_docmd) > 0 {
					if docmd, err = base64.Decode64(encoded_docmd); err != nil {
						gLog.Error.Println(err)
						ch <- &OpResponse{err, targetUrl, num, num200}
						return
					}
				} else {
					if statusCode == 404 {
						err = errors.New("Document " + targetUrl + " not found")
					} else {
						err = errors.New("Metadata is missing for " + targetUrl)
					}
					gLog.Warning.Println(err)
					ch <- &OpResponse{err, pn, num, num200}
					return
				}
			} else {
				gLog.Error.Println(err)
				ch <- &OpResponse{err, targetUrl, num, num200}
				return
			}

			// The PN meta data is valid
			// convert the PN  metadata into a go structure
			docmeta := DocumentMetadata{}

			if err := json.Unmarshal(docmd, &docmeta); err != nil {
				gLog.Error.Println("Document metadata is invalid ", targetUrl, err)
				gLog.Error.Println(string(docmd), docmeta)
				ch <- &OpResponse{err, targetUrl, num, num200}
				return
			}

			if num, err = docmeta.GetPageNumber(); err != nil {
				ch <- &OpResponse{err, targetUrl, num, num200}
				return
			}

			//  DELETE THE DOCUMENT ON THE TARGET ENVIRONMENT

			bnsRequest = HttpRequest{}

			fmt.Println("len => ", num)
			bnsRequest.Hspool = sproxyd.TargetHP // set target sproxyd servers ( Destination RING)
			bnsRequest.Urls = make([]string, num, num)
			//  bnsRequest.Client = &http.Client{}
			//  DELETE ALL THE PAGES FIRST

			for i := 0; i < num; i++ {
				bnsRequest.Urls[i] = targetUrl + "/p" + strconv.Itoa(i+1)
			}

			sproxydResponses := AsyncHttpDeleteBlobs(&bnsRequest)
			bnsResponses := make([]BnsResponse, num, num)
			for k, v := range sproxydResponses {
				if err := v.Err; err == nil { //
					resp := v.Response                             /* http response */ // http response
					bnsResponse := BuildBnsResponse(resp, "", nil) // bnsResponse is a Go structure
					bnsResponses[k] = bnsResponse
				}
			}

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
						gLog.Warning.Println(hostname, pid, url, resp.Status, "key=", resp.Header["X-Scal-Ring-Key"], "Precondition Fails")
						num412++
					case 422:
						gLog.Error.Println(hostname, pid, url, resp.Status, resp.Header["X-Scal-Ring-Status"])
						num422++
					default:
						numOther++
						gLog.Warning.Println(hostname, pid, url, resp.Status)
					}
					// close all the connection
					resp.Body.Close()
				}

				if num200 < num {
					gLog.Warning.Printf("Host name:%s,Pid:%d,Publication:%s,Ins:%d,Outs:%d,Notfound:%d,Other:%d", hostname, pid, pn, num, num200, num404, numOther)
					err = errors.New("Pages outs < Page ins")
				} else {
					gLog.Warning.Printf("Host name:%s,Pid:%d,Publication:%s,Ins:%d,Outs:%d,Notfound:%d,Other:%d", hostname, pid, pn, num, num200, num404, numOther)
				}
			}
			// Delete the PN metadata when all pages have been deleted
			if num == num200 {
				bnsRequest := HttpRequest{
					Hspool: sproxyd.TargetHP,
					Client: &http.Client{
						Timeout:   sproxyd.ReadTimeout,
						Transport: sproxyd.Transport,
					},
					Media: media,
				}
				if err, statusCode := DeleteBlob(&bnsRequest, targetUrl); err != nil {
					gLog.Error.Println("Error deleting PN", targetUrl, " Error:", err, "Status Code:", statusCode)
				} else {
					gLog.Info.Println(targetUrl, " is deleted")
				}
			}

			ch <- &OpResponse{err, pn, num, num200}

		}(targetUrl)

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
