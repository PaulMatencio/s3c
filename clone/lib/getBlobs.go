package lib

import (
	doc "github.com/paulmatencio/protobuf-doc/lib"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	base64 "github.com/paulmatencio/ring/user/base64j"
	// "github.com/golang/protobuf/proto"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/sproxyd/lib"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
)

func GetBlobs(pn string, np int, maxPage int) (*documentpb.Document,int) {

	if np <=  maxPage {
		return getBlobs(pn, np)
	} else {
		return getBig(pn, np, maxPage)
	}
}

func getBlobs(pn string, np int) (*documentpb.Document,int){
	var (
		sproxydRequest = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		wg2      sync.WaitGroup
		document = &documentpb.Document{
			NumberOfPages: int32(np),
		}
		nerrors = 0
		me = sync.Mutex{}
	)

	for k := 0; k <= np; k++ {
		wg2.Add(1)
		url := sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
		// l := np+1
		go func(document *documentpb.Document, url string, pn string, k int) {
			sproxydRequest.Path = url
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
					document = doc.CreateDocument(pn, usermd, k, &body)
				} else {
					// Create a page and add it to the document
					pg := doc.CreatePage(pn, usermd, k, &body)
					doc.AddPageToDucument(pg, document)
				}
			} else {
				gLog.Error.Printf("error %v getting object %s",err,pn)
				resp.Body.Close()
				me.Lock()
				nerrors += 1
				me.Unlock()
			}
			gLog.Trace.Printf("object %s - length %d %s", url, len(body), string(md))
			/*  add to the protoBuf */
		}(document, url, pn, k)
	}
	// Write the document to File
	wg2.Wait()
	return document,nerrors
}

func getBig(pn string, np int, maxPage int) (*documentpb.Document,int){

	var (

		document = &documentpb.Document{
			NumberOfPages: int32(np+1),    //inclusive p0
		}
		q     int = (np + 1) / maxPage
		r     int = (np + 1) / maxPage
		start int = 0
		end   int = start + maxPage
		nerrors int = 0
		terrors int = 0

	)
	gLog.Warning.Printf("Big document %s  - number of pages %d ",pn,np)
	for s := 1; s <= q; s++ {
		nerrors = GetParts(pn, start, end, document)
		start = end + 1
		end += maxPage
		if end > np {
			end = np
		}
		terrors += nerrors
	}
	if r > 0 {
		nerrors = GetParts(pn, q*maxPage+1, np, document)
		terrors += nerrors
	}
	return document,terrors
	// return WriteDocument(pn, document, outdir)
}

func GetParts(pn string, start int, end int, document *documentpb.Document) int {

	var (
		sproxydRequest = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		me sync.Mutex
		nerrors int = 0
		wg2 sync.WaitGroup
	)

	// document := &documentpb.Document{}
	gLog.Info.Printf("Getpart of pn %s - start-page %d - end-page %d ",pn,start,end)
	for k := start; k <= end; k++ {
		wg2.Add(1)
		url := sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
		// l := np+1
		go func(document *documentpb.Document, url string, pn string, k int) {
			gLog.Trace.Printf("Getpart of pn: %s - url:%s",pn,url)
			sproxydRequest.Path = url
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
					document = doc.CreateDocument(pn, usermd, k, &body)
				} else {
					pg := doc.CreatePage(pn, usermd, k, &body)
					doc.AddPageToDucument(pg, document)
				}
			} else {
				gLog.Error.Printf("error %v getting object %s",err,pn)
				resp.Body.Close()
				me.Lock()
				nerrors += 1
				me.Unlock()

			}
			gLog.Trace.Printf("object %s - length %d %s", url, len(body), string(md))
			/*  add to the protoBuf */
		}(document, url, pn, k)
	}
	// Write the document to File
	wg2.Wait()
	return nerrors

}


