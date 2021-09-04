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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	base64 "github.com/paulmatencio/ring/user/base64j"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	gLog "github.com/paulmatencio/s3c/gLog"
	meta "github.com/paulmatencio/s3c/moses-bc/datatype"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Ret struct {
	Ndocs  int
	Npages int
	Nerrs  int
	N404s  int
}

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
	if err, pdf, p0 = checkPdfP0(request1, pn); err != nil {
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
		request1         = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
	)

	/*
		Get the document meta data
	*/

	if err, pdf, p0 = checkPdfP0(request1, pn); err != nil {
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
		request1 = sproxyd.HttpRequest{
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
		request1.Path = sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
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
		}(request1, pn, np, k)
	}
	// Write the document to File
	wg2.Wait()
	return nerrors
}

/*
	compare source and target object
*/
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

/*
	Check if the document contains a pdf and/or  a page 0
*/
func checkPdfP0(request sproxyd.HttpRequest, pn string) (error, bool, bool) {
	/*
		request1 := sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}

	*/

	if err, usermd := GetUserMeta(request, pn); err != nil {
		gLog.Error.Printf("Error %v  getting usermeta of %s", err, pn)
		return err, false, false
	} else {
		pdf, p0 := CheckPdfAndP0(pn, usermd)
		return err, pdf, p0
	}
}

/*
	compare source and target pdf
*/
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
	called by cmd.CheckBlobs  with   --check-target-proxy on
	Check  Target sproxyd for 404

*/
func CheckTargetSproxyd(request datatype.ListObjRequest, maxLoop int, maxPage int) (ret Ret) {

	var (
		N          int = 0
		nextmarker string
		req1       = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			}}
		req2 = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			}}
		le, l4 sync.Mutex
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
					lastModified := *v.LastModified
					wg1.Add(1)

					go func(pn string, req1 *sproxyd.HttpRequest, req2 *sproxyd.HttpRequest, lastModified time.Time) {
						defer wg1.Done()
						if docmeta, err := getDocMeta(pn, req1, req2, lastModified); err == nil  && docmeta.TotalPage >0 {
							ret1 := CheckTargetPages(pn, docmeta, MaxPage, lastModified)
							if ret1.Nerrs > 0 {
								le.Lock()
								ret.Nerrs += ret1.Nerrs
								le.Unlock()
							}
							if ret1.N404s > 0 {
								l4.Lock()
								ret.N404s += ret1.N404s
								l4.Unlock()
							}

						} else {
							gLog.Error.Printf("%v", err)
							le.Lock()
							ret.Nerrs += 1
							le.Unlock()
						}

					}(pn, &req1, &req2, lastModified)
				}
				wg1.Wait()
				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					gLog.Warning.Printf("Loop %d - Ndocs %d - Npages %d - Nerrors %d - N404s %d - Truncated %v - Next marker %s - ", N, ret.Ndocs, ret.Ndocs, ret.Nerrs, ret.N404s, *result.IsTruncated, nextmarker)
				}
			} else {
				gLog.Warning.Printf("No matching! is %s bucket empty?", request.Bucket)
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
	gLog.Info.Printf("Total number of documents %d - Pages  %d - Errors  %d - 404s %d", ret.Ndocs, ret.Npages, ret.Nerrs, ret.N404s)
	return
}

func getDocMeta(pn string, req1 *sproxyd.HttpRequest, req2 *sproxyd.HttpRequest, lastModified time.Time) (docmeta *meta.DocumentMetadata, err error) {

	var (
		// docmeta = meta.DocumentMetadata{}
		resp *http.Response
		// docmd   []byte
	)
	pn1 := sproxyd.TargetEnv + "/" + pn
	req1.Path = pn1
	gLog.Trace.Printf("Get pages number for %s - Host %s ", req1.Path, req1.Hspool.Hosts())
	if resp, err = sproxyd.GetMetadata(req1); err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == 200 {
			userm := resp.Header["X-Scal-Usermd"][0]
			if docmd, err := base64.Decode64(userm); err == nil {
				if err = json.Unmarshal(docmd, &docmeta); err != nil {
					err = errors.New(fmt.Sprintf("Target: %s - Error %v", err, req1.Path))
				}
			} else {
				err = errors.New(fmt.Sprintf("Target: %s - Error %v", err, req1.Path))
			}
		} else {
			gLog.Warning.Printf("Target Docid %s - status code %d - LastModified %v", req1.Path, resp.StatusCode, lastModified)
			/*  Continue with the source */
			pn1 := sproxyd.TargetEnv + "/" + pn
			req2.Path = pn1
			if resp, err = sproxyd.GetMetadata(req2); err == nil {
				defer resp.Body.Close()
				if resp.StatusCode == 200 {
					userm := resp.Header["X-Scal-Usermd"][0]
					if docmd, err := base64.Decode64(userm); err == nil {
						if err = json.Unmarshal(docmd, &docmeta); err != nil {
							err = errors.New(fmt.Sprintf("Source: %s - Error %v", err, req2.Path))
						}
					} else {
						err = errors.New(fmt.Sprintf("Source: %s - Error %v", err, req2.Path))
					}
				} else {
					gLog.Warning.Printf("Source Docid  %s - status code %d - LastModified %v", req2.Path, resp.StatusCode, lastModified)
				}
			} else {
				gLog.Error.Printf("Source: %s - error %v", req2, err)
			}
		}
	} else {
		err = errors.New(fmt.Sprintf("Target: %s - Error %v", err, req1.Path))
	}
	return
}

func CheckTargetPages(pn string, docmeta *meta.DocumentMetadata, maxPage int, lastModified time.Time) (ret Ret) {

	var (
		start = time.Now()
		np = docmeta.TotalPage
	)
	if np <= maxPage {
		ret = checkPages(pn, docmeta, lastModified)
		gLog.Info.Printf("Elapsed time %v", time.Since(start))
		return
	} else {
		ret = checkMaxPages(pn, docmeta, maxPage, lastModified)
		gLog.Info.Printf("Elapsed time %v", time.Since(start))
		return
	}
}

/*
	Check Pages ( number of pages < maxpages)
*/
func checkPages(pn string, docmeta *meta.DocumentMetadata, lastModified time.Time) (ret Ret) {
	var (
		request1 = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		wg2     sync.WaitGroup
		nerrors = 0
		n404s   = 0
		start   int
		le, l4  sync.Mutex
	)

	np := docmeta.TotalPage
	if len(docmeta.FpClipping.CountryCode) > 0 {
		start = 0
		gLog.Info.Printf("DocId %s contains a page 0", pn)
	} else {
		start = 1
	}

	if docmeta.MultiMedia.Pdf {
		gLog.Info.Printf("Document %s contains a pdf", pn)
		request1.Path = sproxyd.TargetEnv + "/" + pn + "/pdf"
		if resp, err := sproxyd.GetMetadata(&request1); err == nil {
			defer resp.Body.Close()
			gLog.Warning.Printf("Target Page %s - status code %d ", request1.Path, resp.StatusCode)
		} else {
			gLog.Error.Printf("Target Page %s - error %v", request1.Path, err)
			ret.Nerrs += 1
			ret.Npages += 1
		}
	}

	for k := start; k <= np; k++ {
		request1.Path = sproxyd.TargetEnv + "/" + pn + "/p" + strconv.Itoa(k)
		wg2.Add(1)

		go func(request1 sproxyd.HttpRequest, pn string, k int) {
			defer wg2.Done()
			if resp, err := sproxyd.GetMetadata(&request1); err == nil {
				defer resp.Body.Close()
				if resp.StatusCode == 404 {
					l4.Lock()
					n404s += 1
					l4.Unlock()
				}
				gLog.Warning.Printf("Target Page %s - status code %d ", request1.Path, resp.StatusCode)
			} else {
				gLog.Error.Printf("Target page %s -  error %v", request1.Path, err)
				le.Lock()
				nerrors += 1
				le.Unlock()
			}
		}(request1, pn, k)
	}

	wg2.Wait()
	ret.Nerrs += nerrors
	ret.N404s += n404s
	ret.Npages += np
	ret.Ndocs += 1
	return ret
}

/*
	Check pages ( numberof pages > maxPages
*/
func checkMaxPages(pn string, docmeta *meta.DocumentMetadata, maxPage int, lastModified time.Time) (ret Ret) {
	var (
		q, r, start, end int
		request1         = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		np = docmeta.TotalPage
	)

	if len(docmeta.FpClipping.CountryCode) > 0 {
		start = 0
		gLog.Info.Printf("DocId %s contains a page 0", pn)
	} else {
		start = 1
	}

	if docmeta.MultiMedia.Pdf {
		gLog.Info.Printf("Document %s contains a pdf", pn)
		request1.Path = sproxyd.TargetEnv + "/" + pn + "/pdf"
		if resp, err := sproxyd.GetMetadata(&request1); err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == 404 {
				gLog.Warning.Printf("Target pdf_page  %s - status code %d ", request1.Path, resp.StatusCode)
				ret.N404s += 1
				ret.Npages += 1
			}
		} else {
			gLog.Error.Printf("Target pdf_page %s -  error %v", request1.Path, err)
			ret.Nerrs += 1
		}

	}
	end = maxPage
	q = np / maxPage
	r = np % maxPage

	gLog.Warning.Printf("Big document %s  - number of pages %d ", pn, np)

	for s := 1; s <= q; s++ {
		ret1 := checkPagePart(&request1, pn, np, start, end)
		start = end + 1
		end += maxPage
		if end > np {
			end = np
		}

		ret.Nerrs += ret1.Nerrs
		ret.N404s += ret1.N404s

	}

	if r > 0 {
		ret1 := checkPagePart(&request1, pn, np, q*maxPage+1, np)
		ret.Nerrs += ret1.Nerrs
		ret.N404s += ret1.N404s
	}

	ret.Npages = np
	ret.Ndocs += 1
	return ret
	// return WriteDocument(pn, document, outdir)

}

func checkPagePart(request1 *sproxyd.HttpRequest, pn string, np int, start int, end int) (ret Ret) {

	var (
		nerrors int = 0
		wg2     sync.WaitGroup
		le, l4  sync.Mutex
		n404s   int
	)

	gLog.Info.Printf("Getpart of pn %s - start-page %d - end-page %d ", pn, start, end)
	for k := start; k <= end; k++ {
		wg2.Add(1)
		request1.Path = sproxyd.TargetEnv + "/" + pn + "/p" + strconv.Itoa(k)
		go func(request1 sproxyd.HttpRequest, pn string, np int, k int) {
			gLog.Trace.Printf("Getpart of pn: %s - url:%s", pn, request1.Path)
			defer wg2.Done()
			request1.Path = pn
			if resp, err := sproxyd.GetMetadata(&request1); err == nil {
				defer resp.Body.Close()
				if resp.StatusCode == 404 {
					l4.Lock()
					n404s = 1
					l4.Unlock()
				}
				gLog.Warning.Printf("Target Page %s - status code %d ", request1.Path, resp.StatusCode)
			} else {
				gLog.Error.Printf("Target page %s -  error %v", request1.Path, err)
				le.Lock()
				nerrors += 1
				le.Unlock()

			}
		}(*request1, pn, np, k)

	}
	// Write the document to File
	wg2.Wait()
	ret.Ndocs = nerrors
	ret.N404s = n404s

	return ret
}
