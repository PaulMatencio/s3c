package lib

import (
	"fmt"
	doc "github.com/paulmatencio/protobuf-doc/lib"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"time"
	// "github.com/golang/protobuf/proto"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/sproxyd/lib"
	"net/http"
	"strconv"
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
	Get blobs  for backup
*/


func BackupAllBlob(pn string, np int, maxPage int) ([]error,*documentpb.Document){
	if np <= maxPage {
		return backup_regular_blob(pn,np)
	} else {
		return backup_large_blob(pn, np, maxPage)
	}
}

//  document with  smaller pages number than maxPage
func backup_regular_blob(pn string, np int) ( []error,*documentpb.Document){
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
func backup_large_blob(pn string, np int, maxPage int) ([]error,*documentpb.Document){
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
		errs,document = backup_part_large_blob(document, pn, np,start, end)
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
		errs,document = backup_part_large_blob(document, pn,np,start, np)
		gLog.Info.Printf("Get pages range %d:%d for document %s - Elapsed time %v ",start,np,pn,time.Since(start4))
	}
	gLog.Info.Printf("Backup document %s - number of pages %d - Document size %d - Elapsed time %v",document.DocId,npages,document.Size,time.Since(start2))
	return errs,document
}


func backup_part_large_blob(document *documentpb.Document, pn string, np int, start int, end int) ([]error, *documentpb.Document) {

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

