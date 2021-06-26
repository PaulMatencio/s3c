package lib

import (
	"fmt"
	doc "github.com/paulmatencio/protobuf-doc/lib"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	base64 "github.com/paulmatencio/ring/user/base64j"
	"time"
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
func CloneBlobs(pn string, np int, maxPage int) (*documentpb.Document, int) {
	if np <= maxPage {
		return cloneBlobs(pn, np)
	} else {
		return cloneBig(pn, np, maxPage)
	}
}
//  document with  smaller pages number than --maxPage
func cloneBlobs(pn string, np int) (*documentpb.Document, int) {
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
				//  todo clone the object

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

func cloneBig(pn string, np int, maxPage int) (*documentpb.Document,int){
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
		nerrors = cloneParts(pn, np,start, end, document)
		start = end + 1
		end += maxPage
		if end > np {
			end = np
		}
		terrors += nerrors
	}
	if r > 0 {
		nerrors = cloneParts(pn, np,q*maxPage+1, np, document)
		terrors += nerrors
	}
	return document,terrors
	// return WriteDocument(pn, document, outdir)
}


func cloneParts(pn string, np int, start int, end int, document *documentpb.Document) int {

	var (
		sproxydRequest = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,  // IP of source sproxyd
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
				//  Todo Clone the object
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


func GetBlob1(pn string, np int, maxPage int) ([]error,*documentpb.Document){
	if np <= maxPage {
		return getBlob1(pn,np)
	} else {
		return getBig1(pn, np, maxPage)
	}
}

//  document with  smaller pages number than maxPage
func getBlob1(pn string, np int) ( []error,*documentpb.Document){
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
		start,end    int
		ch       = make(chan *GetBlobResponse)
		// document = &documentpb.Document{}
	)

	//  get document

	if err, usermd = GetMetadata(request, pn); err != nil {
		errs = append(errs, err)
		return errs,nil
	}

	//  create the document
	start2:= time.Now()
	document := doc.CreateDocument(pn, usermd, -1, np, body)
	gLog.Trace.Printf("Docid: %s - number of pages: %d - document metadata: %s",document.DocId,document.NumberOfPages,document.Metadata)
	/*
		Check if the document has a pdf and/or  page 0 ( Fclip)
	 */
	pdf,p0 := CheckPdfAndP0(pn,usermd)
	if pdf {
		pdfId := pn + "/pdf"
		request.Path = sproxyd.Env + "/" + pdfId
		if err,pmeta,body := GetObject(request, pn); err == nil {
			gLog.Info.Printf("Document %s has a PDF object - size %d",request.Path,len(*body))

			document.Pdf = &documentpb.Pdf {
				Pdf : *body,
				Size: int64(len(*body)),
				Metadata:  pmeta,
				PdfId : pdfId,
			}
		} else {
			gLog.Warning.Printf("Error %v getting object %s ",err,request.Path)
		}
	}
	if p0 {
		start = 0
		end = np +1
		document.Clip = true   /*  fpCliping is stored in page 0  small image  */
		document.NumberOfPages += 1
	} else {
		start = 1
		end = np
	}

	//  add pages to document
	start3 := time.Now()
	for k := start; k <= np; k++ {
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
			/*
			add to the protoBuf */
		}(request, k)
	}
	r1 := 0
	for {
		select {
		case r := <-ch:
			r1++
			if r.Err == nil {
				pg := doc.CreatePage(pn, r.UserMd, r.PageNumber, r.Body)
				document.Size += int64(pg.Size)
				gLog.Trace.Printf("Docid: %s - Page Number: %d - Page Id: %s - Page size:%d" ,document.DocId,pg.PageNumber,pg.PageId,r.Size)
				doc.AddPageToDucument(pg, document)
			} else {
				errs = append(errs, r.Err)
			}
			if r1 == end {
				gLog.Info.Printf("Get pages range %d:%d for %s - Elapsed time %v ",start,np,pn,time.Since(start3))
				gLog.Info.Printf("Backup document %s - number of pages %d - Document size %d - Total elapsed time %v",document.DocId,document.NumberOfPages,document.Size,time.Since(start2))
				return errs,document
			}
		case <-time.After(100 * time.Millisecond):
			fmt.Printf("r")
		}
	}

}

/*
	get document of which  the number of pages > maxPages

 */
func getBig1(pn string, np int, maxPage int) ([]error,*documentpb.Document){
	var (

		start,q,r,end ,npages int
		usermd string
		err error
		body     *[]byte
		errs []error

		request = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
	)
	// gLog.Info.Printf("Backup document if %s  - number of pages %d ",pn,np)
	start2 := time.Now()
	//  Get  document metadata
	if err, usermd = GetMetadata(request, pn); err != nil {
		errs = append(errs, err)
		return errs,nil
	}
	gLog.Info.Printf("Get metadata of %s - Elapsed time %v ",pn,time.Since(start2))

	//  create the document
	document := doc.CreateDocument(pn, usermd, -1, np, body)
	gLog.Trace.Printf("Docid: %s - number of pages: %d - document metadata: %s",document.DocId,document.NumberOfPages,document.Metadata)

	/*
		if document has a pdf
		retrieve the pdf and add it to  the document

	 */

	pdf,p0 := CheckPdfAndP0(pn,usermd)
	if pdf {
		pdfId := pn + "/pdf"
		request.Path = sproxyd.Env + "/" + pdfId
		if err,pmeta,body := GetObject(request, pn); err == nil {
			gLog.Info.Printf("Document %s has a PDF object - size %d",request.Path,len(*body))
			document.Pdf = &documentpb.Pdf {
				Pdf : *body,
				Size: int64(len(*body)),
				Metadata:  pmeta,
				PdfId : pdfId,
			}
		} else {
			gLog.Warning.Printf("Error %v getting object %s ",err,request.Path)
		}
	}
	if p0 {
		start = 0
		document.Clip = true   /*  fpCliping is stored in page 0  small image  */
		document.NumberOfPages += 1
	} else {
		start = 1
	}
	end = maxPage
	npages = end-start+ 1

	q   = np  / maxPage
	r   = np  % maxPage

	for s := 1; s <= q; s++ {
		start3 := time.Now()
		errs,document = getPart1(document, pn, np,start, end)
		gLog.Info.Printf("Get pages range %d:%d for document %s - Elapsed time %v ",start,end,pn,time.Since(start3))
		start = end + 1
		end += maxPage
		if end > np {
			end = np
		}
		npages += end -start +1
	}
	if r > 0 {
		start4 := time.Now()
		start:= q*maxPage+1
		errs,document = getPart1(document, pn,np,start, np)
		gLog.Info.Printf("Get pages range %d:%d for document %s - Elapsed time %v ",start,np,pn,time.Since(start4))
	}
    gLog.Info.Printf("Backup document %s - number of pages %d - Document size %d - Elapsed time %v",document.DocId,npages,document.Size,time.Since(start2))
	return errs,document
}


func getPart1(document *documentpb.Document, pn string, np int, start int, end int) ([]error, *documentpb.Document) {

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
		ch    = make(chan *GetBlobResponse)
		num   =  end -start +1
	)

	// gLog.Info.Printf("Getpart of pn %s - start-page %d - end-page %d ", pn, start, end)
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
				document.Size += int64(pg.Size)
				doc.AddPageToDucument(pg, document)
				gLog.Trace.Printf("Docid: %s - Page Number: %d - Page Id: %s - Page size:%d" ,document.DocId,pg.PageNumber,pg.PageId,r.Size)
			} else {
				errs = append(errs, r.Err)
			}
			if r1 == num {
				//gLog.Info.Printf("Backup document  %s  - number of pages %d  - Document size %d - Elapsed time %v",document.DocId,document.NumberOfPages,document.Size,time.Since(start2))
				return errs, document
			}
		case <-time.After(200 * time.Millisecond):
			fmt.Printf("r")
		}
	}
	return errs, document
}

