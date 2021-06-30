package lib

import (
	doc "github.com/paulmatencio/protobuf-doc/lib"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"sync"
	"time"
	// "github.com/golang/protobuf/proto"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/sproxyd/lib"
	"net/http"
	"strconv"
)


/*
	Get blobs  for backup
*/


func CloneBlob1(pn string, np int, maxPage int,replace bool) (int,*documentpb.Document){

	if np <= maxPage {
		return _cloneBlob1(pn,np,replace)
	} else {
		return _cloneBig1(pn, np, maxPage,replace)
	}
}

/*
document with  smaller pages number than the maxPage value
 */
func _cloneBlob1(pn string, np int,replace bool) (int,*documentpb.Document){

	var (
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}

		request1 = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP, // IP of target sproxyd
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			//  ReqHeader: map[string]string{},
		}
		err      error
		usermd   string
		body     *[]byte
		start    int
		pu,ps		sync.Mutex
		perrors int
		document = &documentpb.Document{}
	)
	//  get document
	if err, usermd = GetMetadata(request, pn); err != nil {
		gLog.Error.Printf("%v",err)
		return 1,document
	}

	//  clone document meta data
	document.Metadata= usermd
	document.DocId= pn
	document.NumberOfPages= 0
	document.Size= 0

	if nerr,status := WriteDocMetadata(&request1, document,replace); nerr > 0 {
		gLog.Warning.Printf("Document %s is not restored",document.DocId)
		return nerr,document
	} else {
		if status == 412 {
			gLog.Warning.Printf("Document %s is not restored - use --replace=true  ou -r=true to replace the existing document",document.DocId)
			return 1,document
		}
	}
	//  check if page 0 and/or pdf exists
	pdf,p0 := CheckPdfAndP0(pn,usermd)
	// Clone pdf cocument if exists
	if pdf {
		pdfId := pn + "/pdf"
		request.Path = sproxyd.Env + "/" + pdfId
		if err,pmeta,body := GetObject(request, pn); err == nil {
			gLog.Info.Printf("Document %s has a PDF object - size %d",request.Path,len(*body))
			pd:= doc.CreatePdf(pdfId, pmeta,body)
			document.Size += pd.Size  // increment the document size
			WriteDocPdf(pd,replace)
		} else {
			gLog.Warning.Printf("Error %v getting object %s ",err,request.Path)
		}
	}
	//   if p0 exist just clone it
	if p0 {
		start = 0
		// document.NumberOfPages += 1
	} else {
		start = 1
	}
	wg1 := sync.WaitGroup{}
	for k := start; k <= np; k++ {
		wg1.Add(1)
		request.Path = sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
		go func(request sproxyd.HttpRequest, request1 sproxyd.HttpRequest,document *documentpb.Document, k int) {
			defer wg1.Done()
			pn := document.DocId
			err, usermd, body = GetObject(request, pn)
			pg:= doc.CreatePage(pn,usermd,k,body)
			if perr,_ := WriteDocPage(request1, pg,replace); perr > 0 {
				pu.Lock()
				perrors += perr
				pu.Unlock()
			} else {
				ps.Lock()
				document.Size += (int64)(pg.Size)
				document.NumberOfPages +=1
				ps.Unlock()
			}
		}(request, request1, document, k)
	}
	wg1.Wait()
	return perrors,document
}

/*
	get document of which  the number of pages > maxPages

*/
func _cloneBig1(pn string, np int, maxPage int,replace bool) (int,*documentpb.Document){
	var (

		start,q,r,end ,npages int
		usermd string
		err error
		nerr int
		// body     *[]byte
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		request1 = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP, // IP of target sproxyd
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			//  ReqHeader: map[string]string{},
		}
	)
	document:=  &documentpb.Document{}
	start2 := time.Now()

	//  retrieve the  document metadata and clone it
	if err, usermd = GetMetadata(request, pn); err != nil {
		gLog.Error.Printf("%v",err)
		return 1,document
	}

	document.Metadata= usermd
	document.DocId= pn
	document.NumberOfPages= 0
	document.Size= 0

	if nerr,status := WriteDocMetadata(&request1, document,replace); nerr > 0 {
		gLog.Warning.Printf("Document %s is not restored",document.DocId)
		return nerr,document
	} else {
		if status == 412 {
			gLog.Warning.Printf("Document %s is not restored - use --replace=true  ou -r=true to replace the existing document",document.DocId)
			return 1,document
		}
	}
	//  if pdf , retrieve the pdf document and clone it
	pdf,p0 := CheckPdfAndP0(pn,usermd)
	if pdf {
		pdfId := pn + "/pdf"
		request.Path = sproxyd.Env + "/" + pdfId
		if err,pmeta,body := GetObject(request, pn); err == nil {
			gLog.Info.Printf("Document %s has a PDF object - size %d",request.Path,len(*body))
			pd:= doc.CreatePdf(pdfId, pmeta,body)
			document.Size += pd.Size
			WriteDocPdf(pd,replace)
		} else {
			gLog.Warning.Printf("Error %v getting object %s ",err,request.Path)
		}
	}
	//  if page 0 exist , jue
	if p0 {
		start = 0
	} else {
		start = 1
	}
	end = maxPage
	npages = end-start+ 1

	q   = np  / maxPage
	r   = np  % maxPage

	for s := 1; s <= q; s++ {
		start3 := time.Now()
		nerr = _clonePart1(document, pn, np,start, end,replace)
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
		nerr = _clonePart1(document, pn,np,start, np,replace)
		gLog.Info.Printf("Get pages range %d:%d for document %s - Elapsed time %v ",start,np,pn,time.Since(start4))
	}
	gLog.Info.Printf("Backup document %s - number of pages %d - Document size %d - Elapsed time %v",document.DocId,npages,document.Size,time.Since(start2))
	return nerr,document
}


func _clonePart1(document *documentpb.Document, pn string, np int, start int, end int,replace bool) (int) {

	var (
		//  sproxyd request for the source Ring
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		//   sproxyd requestfor the target Ring
		request1 = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		err    error
		nerr   int
		usermd string
		body   *[]byte
		pu,ps  sync.Mutex
		perrors int

	)
	wg1 := sync.WaitGroup{}
	// gLog.Info.Printf("Getpart of pn %s - start-page %d - end-page %d ", pn, start, end)
	for k := start; k <= end; k++ {
		wg1.Add(1)
		request.Path = sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
		go func(request sproxyd.HttpRequest, request1 sproxyd.HttpRequest, document *documentpb.Document,k int,replace bool) {
			defer wg1.Done()
			/*
				get the source object and user metadata
			 */
			err, usermd, body = GetObject(request, pn)
			/*
				create a corresponding page
			*/
			pg:= doc.CreatePage(pn,usermd,k,body)
			if perr,_ := WriteDocPage(request1, pg,replace); perr > 0 {
				pu.Lock()
				perrors += perr
				pu.Unlock()
			} else {
				ps.Lock()
				document.Size += (int64)(pg.Size)
				document.NumberOfPages +=1
				ps.Unlock()
			}
			// gLog.Info.Printf("Time of writing page %s/p%d  Page size %d - %v  ",pg.PageId,pg.PageNumber,pg.Size,time.Since(start))
			wg1.Done()

		}(request, request1,document, k,replace)
	}

	return nerr
}


