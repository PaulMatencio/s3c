package lib

import (
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/sproxyd/lib"
	"net/http"
	"strconv"
	"sync"
)

//  Put  sproxyd blobs
func RestBlob1(document *documentpb.Document,replace bool) int {
	var (
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP, // IP of target sproxyd
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			//  ReqHeader: map[string]string{},
		}
		perrors int
		pu      sync.Mutex
		wg1     sync.WaitGroup
	)

	//   Write document metadata
	if nerr,status := WriteDocMetadata(&request, document,replace); nerr > 0 {
		gLog.Warning.Printf("Document %s is not restored",document.DocId)
		perrors += nerr
		return perrors
	} else {
		if status == 412 {
			gLog.Warning.Printf("Document %s is not restored - use --replace=true  ou -r=true to replace the existing document",document.DocId)
			return 1
		}
	}
	//  get the number of pages
	// start := time.Now()
	pages := document.GetPage()
	for _, pg := range pages {
		wg1.Add(1)
		go func(request sproxyd.HttpRequest,  pg *documentpb.Page) {
			defer wg1.Done()
			if perr,_ := WriteDocPage(request,  pg,replace); perr > 0 {
				pu.Lock()
				perrors += perr
				pu.Unlock()
			}
		}(request, pg)
	}
	wg1.Wait()
	request.Client.CloseIdleConnections()
	return perrors
}


func RestBig1(document *documentpb.Document,maxPage int,replace bool) int {
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
			// ReqHeader: map[string]string{},
		}
	)

	if nerr,status := WriteDocMetadata(&request, document,replace); nerr > 0 {
		gLog.Warning.Printf("Document %s is not restored",document.DocId)
		perrors += nerr
		return perrors
	} else {
		if status == 412 {
			gLog.Warning.Printf("Document %s is not restored - use --replace=true  ou -r=true to replace the existing document",document.DocId)
			return 1
		}
	}

	// gLog.Warning.Printf("Big document %s  - number of pages %d ",document.GetDocId(),np)
	// gLog.Trace.Printf("Docid: %s - number of pages: %d - document metadata: %s",document.DocId,document.NumberOfPages,document.Metadata)

	for s := 1; s <= q; s++ {
		perrors = _restPart1(&request,document,start, end,replace)
		start = end + 1
		end += maxPage
		if end > np {
			end = np
		}
	}
	if r > 0 {
		perrors = _restPart1(&request,document,q*maxPage+1 , np,replace)
	}
	return perrors
}


func _restPart1(request *sproxyd.HttpRequest, document *documentpb.Document,start int,end int,replace bool) (int) {

	var (
		perrors int
		pages = document.GetPage()
		pu      sync.Mutex
		wg1 sync.WaitGroup
	)
	/*
		loading
	 */
	gLog.Trace.Printf("Docid: %s - Starting slot %d - Ending slot  %d - Number of pages %d  - Length of pages array: %d ",document.DocId,start,end,document.NumberOfPages,len(pages))
	for k := start; k <= end; k++ {
		pg := *pages[k-1]
		wg1.Add(1)
		request.Path = sproxyd.TargetEnv + "/" + pg.PageId + "/p" + strconv.Itoa(int(pg.PageNumber))
		go func(request sproxyd.HttpRequest, pg *documentpb.Page,replace bool) {
			defer wg1.Done()
			if perr,_ := WriteDocPage(request,  pg,replace); perr > 0 {
				pu.Lock()
				perrors += perr
				pu.Unlock()
			}

		}(*request, &pg,replace)
	}
	wg1.Wait()
	request.Client.CloseIdleConnections()
	gLog.Trace.Printf("Writedoc document %s  starting slot: %d - endingslot: %d  completed",document.DocId,start,end)
	return perrors

}




