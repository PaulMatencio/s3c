// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package lib

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	base64 "github.com/paulmatencio/ring/user/base64j"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	gLog "github.com/paulmatencio/s3c/gLog"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"
)

/*

	Check pn's returned by listObject of the meta bucket

*/
func CheckBlobs(request datatype.ListObjRequest, maxLoop int, maxPage int) {
	var (
		N          int = 0
		nextmarker string
	)
	for {
		var (
			result *s3.ListObjectsOutput
			err    error
		)
		N++ // number of loop
		if result, err = api.ListObject(request); err == nil {
			if l := len(result.Contents); l > 0 {
				var wg1 sync.WaitGroup
				for _, v := range result.Contents {
					pn := *v.Key
					wg1.Add(1)
					go func(pn string) {
						defer wg1.Done()
						if np, err, status := GetPageNumber(pn); err == nil && status == 200 {
							if np > 0 {
								CheckBlob(pn, np, maxPage)
							} else {
								gLog.Error.Printf("The number of pages is %d ", np)
							}
						} else {
							gLog.Error.Printf("Error %v getting  the number of pages  run  with  -l 4  (trace)", err)
						}
					}(pn)
				}
				wg1.Wait()
				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					gLog.Warning.Printf("Truncated %v - Next marker: %s ", *result.IsTruncated, nextmarker)
				}
			}
		} else {
			gLog.Error.Printf("%v", err)
			break
		}

		if *result.IsTruncated && (maxLoop == 0 || N <= maxLoop) {
			request.Marker = nextmarker
		} else {
			break
		}
	}

}

/*

 */

func CheckBlob(pn string, np int, maxPage int) int {

	start := time.Now()
	if np <= maxPage {
		r := checkBlob(pn, np)
		gLog.Info.Printf("Elapsed time %v", time.Since(start))
		return r
	} else {
		r := checkLargeBlob(pn, np, maxPage)
		gLog.Info.Printf("Elapsed time %v", time.Since(start))
		return r
	}
}

/*

	Check document with number of pages <  --maxPage
	called by CheckBlob

*/

func checkBlob(pn string, np int) int {
	var (
		request1 = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		wg2     sync.WaitGroup
		nerrors = 0
		me      = sync.Mutex{}
		err     error
		// usermd string
		start   int
		pdf, p0 bool
	)
	/*
		Check document has a pdf and/or Clipping page
	*/
	if err, pdf, p0 = checkPdfP0(pn); err != nil {
		return 1
	}
	if p0 {
		start = 0
		gLog.Info.Printf("DocId %s contains a page 0", pn)
	} else {
		start = 1
	}

	if pdf {
		gLog.Info.Printf("DocId %s contains a pdf", pn)
		/*
			Compare source vs restored pdf
		*/
		pdfId := pn + "/pdf"
		if err, ok := comparePdf(pdfId); err == nil {
			gLog.Info.Printf("Comparing source and restored PDF: %s - isEqual ? %v", pdfId, ok)
		} else {
			gLog.Error.Printf("Error %v when comparing PDF %s", err, pdfId)
		}
	}

	for k := start; k <= np; k++ {
		request1.Path = sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
		wg2.Add(1)
		go func(request1 sproxyd.HttpRequest, pn string, k int) {
			defer wg2.Done()
			resp, err := sproxyd.Getobject(&request1)
			defer resp.Body.Close()
			var (
				body   []byte
				usermd string
				md     []byte
			)
			if err == nil {
				body, _ = ioutil.ReadAll(resp.Body)
				if len(body) > 0 {
					if _, ok := resp.Header["X-Scal-Usermd"]; ok {
						usermd = resp.Header["X-Scal-Usermd"][0]
						if md, err = base64.Decode64(usermd); err != nil {
							gLog.Warning.Printf("Invalid user metadata %s", usermd)
						} else {
							gLog.Trace.Printf("User metadata %s", string(md))
						}
					}
					if err, ok := compareObj(pn, k, &body, usermd); err == nil {
						gLog.Info.Printf("Comparing source and restored Page: %s/p%d - isEqual ? %v", pn, k, ok)
					} else {
						gLog.Error.Println(err)
					}
				}
			} else {
				gLog.Error.Printf("error %v getting object %s", err, pn)
				me.Lock()
				nerrors += 1
				me.Unlock()
			}
			gLog.Trace.Printf("object %s - length %d %s", request1.Path, len(body), string(md))

		}(request1, pn, k)
	}
	// Write the document to File
	wg2.Wait()
	return nerrors
}

/*

	Check document with number of pages >  --maxPage
	called by CheckBlob

*/

func checkLargeBlob(pn string, np int, maxPage int) int {

	var (
		q, r, start, end int
		nerrors, terrors int = 0, 0
		p0, pdf          bool
		err              error
	)

	/*
		Get the document meta data
	*/

	if err, pdf, p0 = checkPdfP0(pn); err != nil {
		return 1
	}
	if p0 {
		start = 0
		gLog.Info.Printf("Document %s contains a page 0", pn)
	} else {
		start = 1
	}

	if pdf {
		gLog.Info.Printf("Document %s contains a pdf", pn)
		/*   compare source  with  restored pdf   */
		pdfId := pn + "/pdf"
		if err, ok := comparePdf(pdfId); err == nil {
			gLog.Info.Printf("Comparing source and restored PDF:  %s - isEqual ? %v", pdfId, ok)
		} else {
			gLog.Error.Printf("Error %v comparing PDF %s", err, pdfId)
		}
	}
	end = maxPage
	q = np / maxPage
	r = np % maxPage

	gLog.Warning.Printf("Big document %s  - number of pages %d ", pn, np)

	for s := 1; s <= q; s++ {
		nerrors = checkBlobPart(pn, np, start, end)
		start = end + 1
		end += maxPage
		if end > np {
			end = np
		}
		terrors += nerrors
	}
	if r > 0 {
		nerrors = checkBlobPart(pn, np, q*maxPage+1, np)
		if nerrors > 0 {
			terrors += nerrors
		}
	}
	return terrors
	// return WriteDocument(pn, document, outdir)
}

/*
	called by checkBig1
*/
func checkBlobPart(pn string, np int, start int, end int) int {

	var (
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.HP, // IP of source sproxyd
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		nerrors int = 0
		wg2     sync.WaitGroup
	)

	// document := &documentpb.Document{}
	gLog.Info.Printf("Getpart of pn %s - start-page %d - end-page %d ", pn, start, end)
	for k := start; k <= end; k++ {
		wg2.Add(1)
		request.Path = sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
		go func(request1 sproxyd.HttpRequest, pn string, np int, k int) {
			gLog.Trace.Printf("Getpart of pn: %s - url:%s", pn, request1.Path)
			defer wg2.Done()
			if err, usermd, body := GetObject(request1, pn); err == nil {
				if err, ok := compareObj(pn, k, body, usermd); err == nil {
					gLog.Info.Printf("Comparing source and restored Page: %s/p%d - isEqual ? %v", pn, k, ok)
				} else {
					gLog.Error.Println(err)
				}
			} else {
				gLog.Error.Printf("Error %v while getting object %s", err, request1.Path)
			}
		}(request, pn, np, k)
	}
	// Write the document to File
	wg2.Wait()
	return nerrors
}

func compareObj(pn string, pagen int, body *[]byte, usermd string) (error, bool) {
	var (
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			Path: sproxyd.TargetEnv + "/" + pn + "/p" + strconv.Itoa(pagen),
		}
		err     error
		body1   *[]byte
		usermd1 string
	)
	if err, usermd1, body1 = GetObject(request, pn); err == nil {
		if usermd1 == usermd && len(*body1) == len(*body) {
			return err, true
		} else {
			err = errors.New(fmt.Sprintf("usermd1=%s usermd=%% / length body1= %d length body = %d ", len(usermd1), len(usermd), len(*body1), len(*body)))
			return err, false
		}
	}

	return err, false
}

func checkPdfP0(pn string) (error, bool, bool) {

	request1 := sproxyd.HttpRequest{
		Hspool: sproxyd.HP,
		Client: &http.Client{
			Timeout:   sproxyd.ReadTimeout,
			Transport: sproxyd.Transport,
		},
	}
	if err, usermd := GetUserMeta(request1, pn); err != nil {
		gLog.Error.Printf("Error %v  getting usermeta of %s", err, pn)
		return err, false, false
	} else {
		pdf, p0 := CheckPdfAndP0(pn, usermd)
		return err, pdf, p0
	}
}

func comparePdf(pn string) (error, bool) {

	var (
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			Path: sproxyd.Env + "/" + pn,
		}
		err             error
		body, body1     *[]byte
		usermd, usermd1 string
	)

	if err, usermd, body = GetObject(request, pn); err == nil {
		request1 := sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			Path: sproxyd.TargetEnv + "/" + pn,
		}
		if err, usermd1, body1 = GetObject(request1, pn); err == nil {
			/*  vheck */
			if usermd1 == usermd && len(*body1) == len(*body) {
				return err, true
			} else {
				err = errors.New(fmt.Sprintf("usermd1 = %s  usermd = %% / length body1 = %d  length body = %d ", len(usermd1), len(usermd), len(*body1), len(*body)))
				return err, false
			}
		} else {
			gLog.Error.Printf("Error %v Getting the target document %s", err, request.Path)
		}
	} else {
		gLog.Error.Printf("Error %v Getting the source document %s", err, request.Path)
	}
	return err, false
}

/*
	compare source and object metadata
*/
func CheckDocs(request datatype.ListObjRequest, maxLoop int) {
	var (
		N          int = 0
		nextmarker string
		req1       = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			}}
		req2 = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			}}
	)
	for {
		var (
			result *s3.ListObjectsOutput
			err    error
		)
		N++ // number of loop
		if result, err = api.ListObject(request); err == nil {
			if l := len(result.Contents); l > 0 {
				var wg1 sync.WaitGroup
				for _, v := range result.Contents {
					pn := *v.Key
					wg1.Add(1)
					go func(pn string) {
						defer wg1.Done()
						if err1, usermd1 := GetUserMeta(req1, pn); err1 == nil {
							if err2, usermd2 := GetUserMeta(req2, pn); err2 == nil {
								if len(usermd1) != len(usermd2) {
									gLog.Error.Printf("%s  - source doc metadata != target doc metadata", pn)
								}
							} else {
								gLog.Error.Printf("Target %s is missing", pn)
							}
						} else {
							gLog.Error.Printf("%s  - Source %s is missing", pn)
						}
					}(pn)
				}
				wg1.Wait()
				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					gLog.Warning.Printf("Truncated %v - Next marker: %s ", *result.IsTruncated, nextmarker)
				}
			} else {
				gLog.Warning.Printf("No match! is %s bucket empty?",request.Bucket)
			}
		} else {
			gLog.Error.Printf("%v", err)
			break
		}

		if *result.IsTruncated && (maxLoop == 0 || N <= maxLoop) {
			request.Marker = nextmarker
		} else {
			break
		}
	}

}
