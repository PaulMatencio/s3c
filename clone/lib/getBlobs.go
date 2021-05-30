package lib

import (
	doc "github.com/paulmatencio/protobuf-doc/lib"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	base64 "github.com/paulmatencio/ring/user/base64j"
	"os"
	"path/filepath"
	// "github.com/golang/protobuf/proto"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/sproxyd/lib"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
)

func GetBlobs(pn string, np int, maxPage int) {
	if np <=  maxPage {
		getBlobs(pn, np)
	} else {
		getBig(pn, np, maxPage)
	}

}

func getBlobs(pn string, np int) {
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
				resp.Body.Close()
			}
			gLog.Trace.Printf("object %s - length %d %s", url, len(body), string(md))
			/*  add to the protoBuf */
		}(document, url, pn, k)
	}
	// Write the document to File
	wg2.Wait()
	home, _ := os.UserHomeDir()
	dest := "testbackup"
	outdir := filepath.Join(home, dest)
	WriteDocument(pn, document, outdir)
}

func getBig(pn string, np int, maxPage int) {
	var (
		document = &documentpb.Document{}
		q     int = (np + 1) / maxPage
		r     int = (np + 1) / maxPage
		start int = 0
		end   int = start + maxPage
	)
	gLog.Warning.Printf("Big document %s  - number of pages %d ",pn,np)
	for s := 1; s <= q; s++ {
		GetParts(pn, start, end, document)
		start = end + 1
		end += maxPage
		if end > np {
			end = np
		}
	}
	if r > 0 {
		GetParts(pn, q*maxPage+1, np, document)
	}
	home, _ := os.UserHomeDir()
	dest := "testbackup"
	outdir := filepath.Join(home, dest)
	WriteDocument(pn, document, outdir)
}

func GetParts(pn string, start int, end int, document *documentpb.Document) {

	var (
		sproxydRequest = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		wg2 sync.WaitGroup
	)

	// document := &documentpb.Document{}

	for k := start; k <= end; k++ {
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
				if k == 0 {
					document = doc.CreateDocument(pn, usermd, k, &body)
				} else {
					pg := doc.CreatePage(pn, usermd, k, &body)
					doc.AddPageToDucument(pg, document)
				}
			} else {
				resp.Body.Close()
			}
			gLog.Trace.Printf("object %s - length %d %s", url, len(body), string(md))
			/*  add to the protoBuf */
		}(document, url, pn, k)
	}
	// Write the document to File
	wg2.Wait()

}


