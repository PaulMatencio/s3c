package lib

import (
	"github.com/aws/aws-sdk-go/service/s3"
	base64 "github.com/paulmatencio/ring/user/base64j"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	gLog "github.com/paulmatencio/s3c/gLog"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	"net/http"
	"strconv"
	"sync"
	"time"
)

/*

	Check pn's returned by listObject of the meta bucket

*/
func ListBlobs(request datatype.ListObjRequest, maxLoop int, maxPage int) {
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
								ListBlob1(pn, np, maxPage)
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

func ListBlob1(pn string, np int, maxPage int) int {

	start := time.Now()
	if np <= maxPage {
		r := _listBlob1(pn, np)
		gLog.Info.Printf("Elapsed time %v", time.Since(start))
		return r
	} else {
		r := _listBig1(pn, np, maxPage)
		gLog.Info.Printf("Elapsed time %v", time.Since(start))
		return r
	}
}

/*
	document with  smaller number if pages number than --maxPage
	called by ListBlob1
*/

func _listBlob1(pn string, np int) int {
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
		start   int
		pdf, p0 bool
	)
	/*
		check if pdf and page 0
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
		pdfId := pn + "/pdf"
		if err, _ := listPdf(pdfId); err != nil {
			gLog.Error.Printf("Error %v when comparing PDF %s", err, pdfId)
		}
	}

	for k := start; k <= np; k++ {
		request1.Path = sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
		wg2.Add(1)
		go func(request1 sproxyd.HttpRequest, pn string, k int) {
			var (
				usermd string
				md     []byte
			)
			defer wg2.Done()
			resp, err := sproxyd.GetMetadata(&request1)
			defer resp.Body.Close()
			if err == nil {
				if _, ok := resp.Header["X-Scal-Usermd"]; ok {
					usermd = resp.Header["X-Scal-Usermd"][0]
					if md, err = base64.Decode64(usermd); err != nil {
						gLog.Warning.Printf("Invalid user metadata %s", usermd)
					} else {
						gLog.Trace.Printf("User metadata %s", string(md))
					}
					gLog.Info.Printf("key %s  - User metadata %s - Content length %d", pn, usermd, resp.ContentLength)
				}

			} else {
				gLog.Error.Printf("error %v getting object %s", err, pn)
				resp.Body.Close()
				me.Lock()
				nerrors += 1
				me.Unlock()
			}

		}(request1, pn, k)
	}
	// Write the document to File
	wg2.Wait()
	return nerrors
}

//  document with bigger  pages number than maxPage

func _listBig1(pn string, np int, maxPage int) int {

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
		/*  list source pdf */
		pdfId := pn + "/pdf"
		if err, _ = listPdf(pdfId); err != nil {
			gLog.Error.Printf("Error %v listing PDF %s", err, pdfId)
		}
	}
	end = maxPage
	q = np / maxPage
	r = np % maxPage

	gLog.Warning.Printf("Big document %s  - number of pages %d ", pn, np)

	for s := 1; s <= q; s++ {
		nerrors = _ListPart1(pn, np, start, end)
		start = end + 1
		end += maxPage
		if end > np {
			end = np
		}
		terrors += nerrors
	}
	if r > 0 {
		nerrors = _ListPart1(pn, np, q*maxPage+1, np)
		if nerrors > 0 {
			terrors += nerrors
		}
	}
	return terrors
	// return WriteDocument(pn, document, outdir)
}

/*
	called by ListBig1
*/
func _ListPart1(pn string, np int, start int, end int) int {

	var (
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.HP, // IP of source sproxyd
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		nerrors int = 0
		size    int64
		err     error
		usermd  string
		wg2     sync.WaitGroup
	)

	// document := &documentpb.Document{}
	gLog.Info.Printf("List part of pn %s - start-page %d - end-page %d ", pn, start, end)
	for k := start; k <= end; k++ {
		wg2.Add(1)
		request.Path = sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
		go func(request1 sproxyd.HttpRequest, pn string, np int, k int) {
			defer wg2.Done()
			if err, usermd, size = GetHeader(request1, pn); err == nil {
				gLog.Info.Printf("key %s  - User metadata %s - Content length %d", pn, usermd, size)
			} else {
				gLog.Error.Printf("Error %v while getting metadata of %s", err, request1.Path)
			}
		}(request, pn, np, k)
	}
	// Write the document to File
	wg2.Wait()
	return nerrors
}

func listPdf(pn string) (error, bool) {

	var (
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			Path: sproxyd.Env + "/" + pn,
		}
		err    error
		usermd string
		size   int64
	)

	if err, usermd, size = GetHeader(request, pn); err == nil {
		gLog.Info.Printf("Key %s - User metadata %s - Content length %d", pn, usermd, size)

	} else {
		gLog.Error.Printf("Error %v Getting  metadata of %s", err, request.Path)
	}
	return err, false
}
