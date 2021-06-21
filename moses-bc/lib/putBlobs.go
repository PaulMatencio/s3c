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
func RestoreBlob1(document *documentpb.Document) int {
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
	pn := document.GetDocId()
	pages := document.GetPage()
	for _, pg := range pages {
		wg1.Add(1)
		go func(request *sproxyd.HttpRequest, pn string, pg *documentpb.Page) {
			if perr := WriteDocPage(*request, pn, pg); perr > 0 {
				pu.Lock()
				perrors += perr
				pu.Unlock()
			}
			wg1.Done()
		}(&request, pn, pg)
	}
	wg1.Wait()
	return perrors
}

//   write the document ( publication number) 's meta data
func WriteDocMetadata(request *sproxyd.HttpRequest, document *documentpb.Document) int {

	var (
		pn      = document.GetDocId()
		perrors = 0
	)
	request.Path = sproxyd.TargetEnv + "/" + pn
	request.ReqHeader["Content-Type"] = "application/octet-stream"
	request.ReqHeader["Usermd"] = document.GetMetadata()
	gLog.Info.Printf("writing pn %s - Path %s ",pn,request.Path)
	/*
	if resp, err := sproxyd.Putobject(request, []byte{}); err != nil {
		gLog.Error.Printf("Error %v - Put Document object %s", err, pn)
		perrors++
	} else {
		if resp.StatusCode != 200 {
			gLog.Error.Printf("Status %s - Put page Object %s", resp.StatusCode, pn)
			perrors++
		}
	}strconv.Itoa(k)

	*/


	return perrors
}

// write a page af a document pn ( publication number)

func WriteDocPage(request sproxyd.HttpRequest, pn string, pg *documentpb.Page) int {
	var perrors = 0
	request.Path = sproxyd.TargetEnv + "/" + pn + "/p" + strconv.Itoa((int)(pg.PageNumber))
	request.ReqHeader["Usermd"] = pg.GetMetadata()
	request.ReqHeader["Content-Type"] = "application/octet-stream" // Content type
	gLog.Info.Printf("writing pn %s - Path  %s",pn,request.Path)
	/*
	if resp, err := sproxyd.Putobject(&request, pg.GetObject()); err != nil {
		gLog.Error.Printf("Error %v - Put Page object %s", err, pn)
		perrors++
	} else {
		if resp.StatusCode != 200 {
			gLog.Error.Printf("Status %s - Put page Object %s", resp.StatusCode, pn)
			perrors++
		}
	}
	 */

	return perrors
}
