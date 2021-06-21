package lib

import (
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/sproxyd/lib"
	"net/http"
	"sync"
)

//  Put  sproxyd blobs
func RestoreBlob1(document *documentpb.Document) ( int) {
	var (
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP,  // IP of target sproxyd
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		perrors int
		pu sync.Mutex
		wg1 sync.WaitGroup
	)

	//   Write document metadata
	perrors += WriteDocMetadata(&request,document)
	pn := document.GetDocId()
	pages := document.GetPage()
	for k,pg := range  pages {
		wg1.Add(1)
		go func(request *sproxyd.HttpRequest,k int ,pn string, pg *documentpb.Page) {
			p:= (int)(pg.GetPageNumber())
			if (k != p){
				gLog.Error.Printf("Document %s - Invalid page number: %d/%d ",pn,p,k)
				pu.Lock()
				perrors += 1
				pu.Unlock()
			}
			if perr:= WriteDocPage(*request,pn,pg);perr > 0{
				pu.Lock()
				perrors += perr
				pu.Unlock()
			}
			wg1.Done()
		}(&request,k, pn, pg )
	}
	wg1.Wait()
	return perrors
}

