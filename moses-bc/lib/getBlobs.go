package lib

import (
	doc "github.com/paulmatencio/protobuf-doc/lib"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	base64 "github.com/paulmatencio/ring/user/base64j"
	"time"

	"errors"
	"fmt"
	// "github.com/golang/protobuf/proto"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/sproxyd/lib"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
)

type GetBlobResponse struct {
	Size       int
	PageNumber int
	Error      int
	Err        error
	UserMd     string
	Body       *[]byte
}

/*
		Get Blob for cloning
		Not used for the moment
*/
func GetBlobs(pn string, np int, maxPage int) (*documentpb.Document, int) {
	if np <= maxPage {
		return getBlobs(pn, np)
	} else {
		return getBig(pn, np, maxPage)
	}
}
//  document with  smaller pages number than --maxPage
func getBlobs(pn string, np int) (*documentpb.Document, int) {
	var (
		sproxydRequest = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		wg2      sync.WaitGroup
		document = &documentpb.Document{}
		nerrors  = 0
		me       = sync.Mutex{}
	)

	for k := 0; k <= np; k++ {
		wg2.Add(1)
		if k > 0 {
			sproxydRequest.Path = sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
		} else {
			sproxydRequest.Path = sproxyd.Env + "/" + pn
		}

		go func(document *documentpb.Document, sproxydRequest sproxyd.HttpRequest, pn string, np int, k int) {
			defer wg2.Done()
			resp, err := sproxyd.Getobject(&sproxydRequest)
			defer resp.Body.Close()
			var (
				body   []byte
				usermd string
				md     []byte
			)
			if err == nil {
				body, _ = ioutil.ReadAll(resp.Body)
				if body != nil {
					if _, ok := resp.Header["X-Scal-Usermd"]; ok {
						usermd = resp.Header["X-Scal-Usermd"][0]
						if md, err = base64.Decode64(usermd); err != nil {
							gLog.Warning.Printf("Invalid user metadata %s", usermd)
						} else {
							gLog.Trace.Printf("User metadata %s", string(md))
						}
					}
				}
				//  Create  the document
				if k == 0 {
					doc.CreateDocument1(document, pn, usermd, k, np, &body)
					gLog.Trace.Printf("DocId:%s - Number of Pages:%d - Page number:%d", document.DocId, document.NumberOfPages, document.PageNumber)
				} else {
					// Create a page and add it to the document
					pg := doc.CreatePage(pn, usermd, k, &body)
					doc.AddPageToDucument(pg, document)
					gLog.Trace.Printf("DocId:%s - Number of Pages:%d - Page number:%d", document.DocId, document.NumberOfPages, document.PageNumber)
				}
			} else {
				gLog.Error.Printf("error %v getting object %s", err, pn)
				resp.Body.Close()
				me.Lock()
				nerrors += 1
				me.Unlock()
			}
			gLog.Trace.Printf("object %s - length %d %s", sproxydRequest.Path, len(body), string(md))
			/*  add to the protoBuf */
		}(document, sproxydRequest, pn, np, k)
	}
	// Write the document to File
	wg2.Wait()
	return document, nerrors
}

//  document with bigger  pages number than maxPage

//  document with bigger  pages number than maxPage

func getBig(pn string, np int, maxPage int) (*documentpb.Document,int){
	var (
		document = &documentpb.Document {}
		q     int = (np + 1) / maxPage
		r     int = (np + 1) / maxPage
		start int = 0
		end   int = start + maxPage
		nerrors int = 0
		terrors int = 0
	)
	gLog.Warning.Printf("Big document %s  - number of pages %d ",pn,np)
	for s := 1; s <= q; s++ {
		nerrors = GetParts(pn, np,start, end, document)
		start = end + 1
		end += maxPage
		if end > np {
			end = np
		}
		terrors += nerrors
	}
	if r > 0 {
		nerrors = GetParts(pn, np,q*maxPage+1, np, document)
		terrors += nerrors
	}
	return document,terrors
	// return WriteDocument(pn, document, outdir)
}


func GetParts(pn string, np int, start int, end int, document *documentpb.Document) int {

	var (
		sproxydRequest = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		me      sync.Mutex
		nerrors int = 0
		wg2     sync.WaitGroup
	)

	// document := &documentpb.Document{}
	gLog.Info.Printf("Getpart of pn %s - start-page %d - end-page %d ", pn, start, end)
	for k := start; k <= end; k++ {
		wg2.Add(1)
		if k > 0 {
			sproxydRequest.Path = sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
		} else {
			sproxydRequest.Path = sproxyd.Env + "/" + pn
		}
		go func(document *documentpb.Document, sproxydRequest sproxyd.HttpRequest, pn string, np int, k int) {
			gLog.Trace.Printf("Getpart of pn: %s - url:%s", pn, sproxydRequest.Path)
			defer wg2.Done()
			resp, err := sproxyd.Getobject(&sproxydRequest)
			defer resp.Body.Close()
			var (
				body   []byte
				usermd string
				md     []byte
			)
			if err == nil {
				body, _ = ioutil.ReadAll(resp.Body)
				if body != nil {
					if _, ok := resp.Header["X-Scal-Usermd"]; ok {
						usermd = resp.Header["X-Scal-Usermd"][0]
						if md, err = base64.Decode64(usermd); err != nil {
							gLog.Warning.Printf("Invalid user metadata %s", usermd)
						} else {
							gLog.Trace.Printf("User metadata %s", string(md))
						}
					}
				}
				if k == 0 {
					doc.CreateDocument1(document, pn, usermd, k, np, &body)
					gLog.Trace.Printf("DocId:%s - Number of Pages:%d - Page number:%d", document.DocId, document.NumberOfPages, document.PageNumber)
				} else {
					pg := doc.CreatePage(pn, usermd, k, &body)
					doc.AddPageToDucument(pg, document)
					gLog.Trace.Printf("DocId:%s - Number of Pages:%d - Page number:%d", document.DocId, document.NumberOfPages, document.PageNumber)
				}
			} else {
				gLog.Error.Printf("error %v getting object %s", err, pn)
				resp.Body.Close()
				me.Lock()
				nerrors += 1
				me.Unlock()
			}
			gLog.Trace.Printf("object %s - length %d %s", sproxydRequest.Path, len(body), string(md))
			/*  add to the protoBuf */
		}(document, sproxydRequest, pn, np, k)
	}
	// Write the document to File
	wg2.Wait()
	return nerrors
}

/*
		Get blobs  for backup
*/


func GetBlob1(pn string, np int, maxPage int) ([]error,*documentpb.Document) {
	if np <= maxPage {
		return getBlob1(pn,np)
	} else {
		return getBig1(pn, np, maxPage)
	}
}
func getBig1(pn string, np int, maxPage int) ([]error,*documentpb.Document){
	var (
		q     int = (np + 1) / maxPage
		r     int = (np + 1) / maxPage
		start int = 1
		usermd string
		err error
		body     *[]byte
		end   int = start + maxPage-1
		errs []error
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}

	)
	gLog.Warning.Printf("Big document %s  - number of pages %d ",pn,np)
	if err, usermd = GetMetadata(request, pn); err != nil {
		errs = append(errs, err)
		return errs,nil
	}
	//  create the document
	document := doc.CreateDocument(pn, usermd, 0, np, body)
	// gLog.Trace.Printf("Docid: %s - number of pages: %d - document metadata: %s",document.DocId,document.NumberOfPages,document.Metadata)
	for s := 1; s <= q; s++ {
		gLog.Info.Printf("GetPart1 s= %d",s)
		errs,document = GetPart1(document, pn, np,start, end)
		start = end + 1
		end += maxPage
		if end > np {
			end = np
		}
	}
	// if remainder> 0
	if r > 0 {
		gLog.Info.Printf("GetPart1 q=%d r=%d",q,r)
		errs,document = GetPart1(document, pn,np,q*maxPage+1 , np)
	}
	return errs,document
}


//  document with  smaller pages number than maxPage
func getBlob1(pn string, np int) ( []error,*documentpb.Document) {
	var (
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		err      error
		errs     []error
		usermd   string
		body     *[]byte
		ch       = make(chan *GetBlobResponse)
		// document = &documentpb.Document{}
	)

	//  get document
	if err, usermd = GetMetadata(request, pn); err != nil {
		errs = append(errs, err)
		return errs,nil
	}

	//  create the document
	document := doc.CreateDocument(pn, usermd, 0, np, body)
	gLog.Trace.Printf("Docid: %s - number of pages: %d - document metadata: %s",document.DocId,document.NumberOfPages,document.Metadata)
	//  add pages to document
	for k := 1; k <= np; k++ {
		request.Path = sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
		go func(request sproxyd.HttpRequest, k int) {
			err, usermd, body = GetObject(request, pn)
			ch <- &GetBlobResponse{
				Size:       len(*body),
				PageNumber: k,
				Err:        err,
				UserMd:     usermd,
				Body:       body,
			}
			/*  add to the protoBuf */
		}(request, k)
	}
	r1 := 0
	for {
		select {
		case r := <-ch:
			r1++
			if r.Err == nil {
				pg := doc.CreatePage(pn, r.UserMd, r.PageNumber, r.Body)
				gLog.Trace.Printf("Docid: %s - Page Number: %d - Page Id: %s" ,document.DocId,pg.PageNumber,pg.PageId)
				doc.AddPageToDucument(pg, document)
			} else {
				errs = append(errs, r.Err)
			}
			if r1 == np {
				return errs,document
			}
		case <-time.After(100 * time.Millisecond):
			fmt.Printf("r")
		}
	}
}

func GetPart1(document *documentpb.Document, pn string, np int, start int, end int) ([]error, *documentpb.Document) {

	var (
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		err    error
		errs   []error
		usermd string
		body   *[]byte
		ch     = make(chan *GetBlobResponse)
		num   =  end -start +1
	)

	gLog.Info.Printf("Getpart of pn %s - start-page %d - end-page %d ", pn, start, end)
	for k := start; k <= end; k++ {
		request.Path = sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
		go func(request sproxyd.HttpRequest, k int) {
			err, usermd, body = GetObject(request, pn)
			ch <- &GetBlobResponse{
				Size:       len(*body),
				PageNumber: k,
				Err:        err,
				UserMd:     usermd,
				Body:       body,
			}
			/*  add to the protoBuf */
		}(request, k)
	}
	r1 := 0
	for {
		select {
		case r := <-ch:
			r1++
			if r.Err == nil {
				pg := doc.CreatePage(pn, r.UserMd, r.PageNumber, r.Body)
				gLog.Trace.Printf("Docid: %s - Page Number: %d - Page Id: %s" ,document.DocId,pg.PageNumber,pg.PageId)
				doc.AddPageToDucument(pg, document)
			} else {
				errs = append(errs, r.Err)
			}
			if r1 == num {
				return errs, document
			}
		case <-time.After(100 * time.Millisecond):
			fmt.Printf("r")
		}
	}
	return errs, document
}

func GetMetadata(request sproxyd.HttpRequest, pn string) (error, string) {
	var (
		usermd string
		md     []byte
		resp   *http.Response
		err    error
	)
	request.Path = sproxyd.Env + "/" + pn
	if resp, err = sproxyd.GetMetadata(&request); err != nil  {
		return err, usermd
	}
	defer resp.Body.Close()
	if  resp.StatusCode != 200 {
		err = errors.New(fmt.Sprintf("Request document %s  status code %d", request.Path, resp.StatusCode))
		return err, usermd
	}
	if _, ok := resp.Header["X-Scal-Usermd"]; ok {
		usermd = resp.Header["X-Scal-Usermd"][0]
		if md, err = base64.Decode64(usermd); err != nil {
			gLog.Warning.Printf("Invalid user metadata %s", usermd)
		} else {
			gLog.Trace.Printf("User metadata %s", string(md))
		}
	}
	return err, usermd
}

func GetObject(request sproxyd.HttpRequest, pn string) (error, string, *[]byte) {
	var (
		body   []byte
		usermd string
		md     []byte
	)
	resp, err := sproxyd.Getobject(&request)
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == 200 {
			body, _ = ioutil.ReadAll(resp.Body)
			if body != nil {
				if _, ok := resp.Header["X-Scal-Usermd"]; ok {
					usermd = resp.Header["X-Scal-Usermd"][0]
					if md, err = base64.Decode64(usermd); err != nil {
						gLog.Warning.Printf("Invalid user metadata %s", usermd)
					} else {
						gLog.Trace.Printf("User metadata %s", string(md))
					}
				}
			} else {
				err = errors.New(fmt.Sprintf("Request url %s - Body is empty", request.Path))
			}
		} else {
			err = errors.New(fmt.Sprintf("Request url %s -Status Code %d", request.Path, resp.StatusCode))
		}
	}
	return err, usermd, &body
}