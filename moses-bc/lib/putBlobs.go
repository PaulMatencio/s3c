package lib

import (
	// doc "github.com/paulmatencio/protobuf-doc/lib"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/sproxyd/lib"
	"net/http"
	"strconv"
	"sync"
)

//  Put  sproxyd blobs
func PutBlob1(document *documentpb.Document) int {
	var (
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP, // IP of target sproxyd
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			ReqHeader: map[string]string{},
		}
		perrors int
		pu      sync.Mutex
		wg1     sync.WaitGroup
	)

	//   Write document metadata
	perrors += WriteDocMetadata(&request, document)
	pages := document.GetPage()
	for _, pg := range pages {
		wg1.Add(1)
		go func(request sproxyd.HttpRequest,  pg *documentpb.Page) {

			if perr := WriteDocPage(request,  pg); perr > 0 {
				pu.Lock()
				perrors += perr
				pu.Unlock()
			}
			wg1.Done()
		}(request, pg)
	}
	wg1.Wait()
	return perrors
}


func PutBig1(document *documentpb.Document,maxPage int) int {

	var (
		np = int (document.NumberOfPages)
		q     int = np  / maxPage
		r     int = np  % maxPage
		start int = 1
		perrors int
		end   int = start + maxPage-1
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP, // IP of target sproxyd
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			ReqHeader: map[string]string{},
		}
	)
	perrors += WriteDocMetadata(&request, document)
	gLog.Warning.Printf("Big document %s  - number of pages %d ",document.GetDocId(),np)
	// gLog.Trace.Printf("Docid: %s - number of pages: %d - document metadata: %s",document.DocId,document.NumberOfPages,document.Metadata)
	for s := 1; s <= q; s++ {
		perrors = putPart1(document,start, end)
		start = end + 1
		end += maxPage
		if end > np {
			end = np
		}
	}
	if r > 0 {
		perrors = putPart1(document,q*maxPage+1 , np)
	}
	return perrors
}


func putPart1(document *documentpb.Document,start int,end int) (int) {

	var (
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			ReqHeader: map[string]string{},
		}
		perrors int
		// num   =  end -start +1
		pages = document.GetPage()
		pu      sync.Mutex
		wg1 sync.WaitGroup
	)
	/*
		loading
	 */
	gLog.Info.Println(start,end,document.NumberOfPages,len(pages))
	for k := start; k <= end; k++ {
		pg := *pages[k-1]
		wg1.Add(1)
		request.Path = sproxyd.TargetEnv + "/" + pg.PageId + "/p" + strconv.Itoa(int(pg.PageNumber))
		go func(request sproxyd.HttpRequest, pg *documentpb.Page) {
			if perr := WriteDocPage(request,  pg); perr > 0 {
				pu.Lock()
				perrors += perr
				pu.Unlock()
			}
			wg1.Done()
		}(request, &pg)
	}
	wg1.Wait()
	return perrors

}



/*
	Write the document ( publication number) 's meta data
 */

func WriteDocMetadata(request *sproxyd.HttpRequest, document *documentpb.Document) int {

	var (
		pn      = document.GetDocId()
		perrors = 0
	)
	request.Path = sproxyd.TargetEnv + "/" + pn
	request.ReqHeader["Content-Type"] = "application/octet-stream"
	request.ReqHeader["Usermd"] = document.GetMetadata()
	gLog.Trace.Printf("writing pn %s - Path %s ",pn,request.Path)
	if resp, err := sproxyd.PutObj(request, true,[]byte{}); err != nil {
		gLog.Error.Printf("Error %v - Put Document object %s", err, pn)
		perrors++
	} else {
		if resp !=  nil {
			// defer resp.Body.Close()
			if resp.StatusCode != 200 {
				gLog.Error.Printf("Status %s - Put page Object %s", resp.StatusCode, pn)
				perrors++
			}
		}
	}
	return perrors
}

// write a page af a document pn ( publication number)

func WriteDocPage(request sproxyd.HttpRequest, pg *documentpb.Page) int {
	var (
		perrors = 0
		pn = pg.GetPageId()
	)
	request.Path = sproxyd.TargetEnv + "/" + pn + "/p" + strconv.Itoa((int)(pg.PageNumber))
	request.ReqHeader["Usermd"] = pg.GetMetadata()
	request.ReqHeader["Content-Type"] = "application/octet-stream" // Content type
	gLog.Trace.Printf("writing %d bytes to path  %s/%s",pg.Size,sproxyd.TargetDriver,request.Path)
	if resp, err := sproxyd.PutObj(&request,true, pg.GetObject()); err != nil {
		gLog.Error.Printf("Error %v - Put Page object %s", err, pn)
		perrors++
	} else {
		if resp != nil {
			// defer resp.Body.Close()
			if resp.StatusCode != 200 {
				gLog.Error.Printf("Status Code; %d - Put page Object: %s", resp.StatusCode, pn)
				perrors++
			}
		}
	}
	return perrors
}
