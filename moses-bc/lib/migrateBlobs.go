package lib

import (
	doc "github.com/paulmatencio/protobuf-doc/lib"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"github.com/paulmatencio/s3c/datatype"
	"sync"
	"time"
	// "github.com/golang/protobuf/proto"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/sproxyd/lib"
	"net/http"
	"strconv"
)

/*
	Migrate Scality Blobs to  S3 objects
*/

func Migrate_blob(reqm datatype.Reqm, s3meta string, pn string, np int, maxPage int, replace bool) (int, *documentpb.Document) {

	if np <= maxPage {
		return migrate_regular_blob(reqm, s3meta, pn, np, replace)
	} else {
		return migrate_large_blob(reqm, s3meta, pn, np, maxPage, replace)
	}
}

/*
document with  smaller pages number than the maxPage value
*/
func migrate_regular_blob(reqm datatype.Reqm, s3meta string, pn string, np int, replace bool) (int, *documentpb.Document) {

	var (
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}

		err      error
		usermd   string
		body     *[]byte
		start    int
		pu, ps   sync.Mutex
		perrors  int
		document = &documentpb.Document{}
		buck     string
		incr     = reqm.Incremental
	)
	//  get the document  metadata
	if err, usermd = GetMetadata(request, pn); err != nil {
		gLog.Error.Printf("%v", err)
		return 1, document
	}

	//
	document.Metadata = usermd
	document.S3Meta = s3meta
	document.DocId = pn
	document.NumberOfPages = 0
	document.Size = 0
	/*
		Write the document metadata to target S3
	*/
	if incr {
		buck = SetBucketName(pn, reqm.TgtBucket)
	} else {
		buck = reqm.TgtBucket
	}
	if result, err := WriteS3Metadata(reqm.TgtS3, buck, document, replace); err != nil {
		gLog.Warning.Printf("Document %s is not migrated", document.DocId)
		return 1, document
	} else {
		gLog.Trace.Printf("Document metadata %s is migrated to S3 bucket %s - Etag %s - Version Id %s", pn, reqm.TgtBucket, result.ETag, result.VersionId)
	}
	//  check if page 0 and/or pdf exists
	pdf, p0 := CheckPdfAndP0(pn, usermd)
	// migrate pdf document if exists
	if pdf {
		pdfId := pn + "/pdf"
		request.Path = sproxyd.Env + "/" + pdfId
		if err, pmeta, body := GetObject(request, pn); err == nil {
			gLog.Info.Printf("Document %s has a PDF object - size %d", request.Path, len(*body))
			pd := doc.CreatePdf(pdfId, pmeta, body)
			document.Size += pd.Size // increment the document size
			if _, err = WriteS3Pdf(reqm.TgtS3, buck, pd, replace); err != nil {
				gLog.Warning.Printf("Error %v writing pdf document  %s ", err, pdfId)
				return 1, document
			}
		} else {
			gLog.Warning.Printf("Error %v getting object %s ", err, request.Path)
			return 1, document
		}
	}

	//   if p0 exist just migrate it
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
		go func(request sproxyd.HttpRequest, document *documentpb.Document, buck string, k int) {
			defer wg1.Done()
			pn := document.DocId
			if err, usermd, body = GetObject(request, pn); err == nil {
				pg := doc.CreatePage(pn, usermd, k, body)
				if result, err := WriteS3Page(reqm.TgtS3, buck, pg, replace); err != nil {
					gLog.Error.Printf("%v", err)
					pu.Lock()
					perrors += 1
					pu.Unlock()
				} else {
					ps.Lock()
					document.Size += (int64)(pg.Size)
					document.NumberOfPages += 1
					ps.Unlock()
					gLog.Trace.Printf("Document metadata %s is migrated to S3 bucket %s - Etag %s - Version Id %s", pn, reqm.TgtBucket, result.ETag, result.VersionId)
				}
			} else {
				gLog.Error.Printf("%v", err)
				pu.Lock()
				perrors += 1
				pu.Unlock()
			}
		}(request, document, buck, k)
	}
	wg1.Wait()
	return perrors, document
}

/*
	get document of which  the number of pages > maxPages

*/
func migrate_large_blob(reqm datatype.Reqm, s3meta string, pn string, np int, maxPage int, replace bool) (int, *documentpb.Document) {

	var (
		start, q, r, end, npages int
		usermd                   string
		err                      error
		nerr                     int
		// body     *[]byte
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		incr = reqm.Incremental
		buck string
	)
	document := &documentpb.Document{}
	start2 := time.Now()

	//  retrieve the  document metadata and migrate it
	if err, usermd = GetMetadata(request, pn); err != nil {
		gLog.Error.Printf("%v", err)
		return 1, document
	}

	document.Metadata = usermd
	document.S3Meta = s3meta
	document.DocId = pn
	document.NumberOfPages = 0
	document.Size = 0
	if incr {
		buck = SetBucketName(pn, reqm.TgtBucket)
	} else {
		buck = reqm.TgtBucket
	}

	if result, err := WriteS3Metadata(reqm.TgtS3, buck, document, replace); err != nil {
		gLog.Warning.Printf("Document %s is not restored", document.DocId)
		return 1, document
	} else {
		gLog.Trace.Printf("Document metadata %s is migrated to S3 bucket %s - Etag %s - Version Id %s", pn, reqm.TgtBucket, result.ETag, result.VersionId)
	}
	//  if pdf , retrieve the pdf document and migrate  it
	pdf, p0 := CheckPdfAndP0(pn, usermd)
	if pdf {
		pdfId := pn + "/pdf"
		request.Path = sproxyd.Env + "/" + pdfId
		if err, pmeta, body := GetObject(request, pn); err == nil {
			gLog.Info.Printf("Document %s has a PDF object - size %d", request.Path, len(*body))
			pd := doc.CreatePdf(pdfId, pmeta, body)
			document.Size += pd.Size
			WriteS3Pdf(reqm.TgtS3, buck, pd, replace)
		} else {
			gLog.Warning.Printf("Error %v getting object %s ", err, request.Path)
		}
	}
	//  if page 0 exist , jue
	if p0 {
		start = 0
	} else {
		start = 1
	}
	end = maxPage
	npages = end - start + 1

	q = np / maxPage
	r = np % maxPage

	for s := 1; s <= q; s++ {
		start3 := time.Now()
		nerr = migrate_part_large_blob(reqm, document, pn, np, start, end, replace)
		gLog.Info.Printf("Get pages range %d:%d for document %s - Elapsed time %v ", start, end, pn, time.Since(start3))
		start = end + 1
		end += maxPage
		if end > np {
			end = np
		}
		npages += end - start + 1
	}
	if r > 0 {
		start4 := time.Now()
		start := q*maxPage + 1
		nerr = migrate_part_large_blob(reqm, document, pn, np, start, np, replace)
		gLog.Info.Printf("Get pages range %d:%d for document %s - Elapsed time %v ", start, np, pn, time.Since(start4))
	}
	gLog.Info.Printf("Migrate document %s - number of pages %d - Document size %d - Elapsed time %v", document.DocId, npages, document.Size, time.Since(start2))
	return nerr, document
}

func migrate_part_large_blob(reqm datatype.Reqm, document *documentpb.Document, pn string, np int, start int, end int, replace bool) int {

	var (
		//  sproxyd request for the source Ring
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}

		err     error
		nerr    int
		usermd  string
		body    *[]byte
		pu, ps  sync.Mutex
		perrors int
		incr    = reqm.Incremental
		buck    string
	)

	wg1 := sync.WaitGroup{}
	for k := start; k <= end; k++ {
		wg1.Add(1)
		request.Path = sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
		if incr {
			buck = SetBucketName(pn, reqm.TgtBucket)
		} else {
			buck = reqm.TgtBucket
		}
		go func(request sproxyd.HttpRequest, document *documentpb.Document, buck string, k int, replace bool) {
			defer wg1.Done()
			/*
				get the source object and user metadata
			*/
			if err, usermd, body = GetObject(request, pn); err == nil {
				/*
					create a corresponding page
				*/
				pg := doc.CreatePage(pn, usermd, k, body)
				if _, err := WriteS3Page(reqm.TgtS3, buck, pg, replace); err != nil {
					gLog.Error.Printf("%v", err)
					pu.Lock()
					perrors += 1
					pu.Unlock()
				} else {
					ps.Lock()
					document.Size += (int64)(pg.Size)
					document.NumberOfPages += 1
					ps.Unlock()
				}
			} else {
				gLog.Error.Printf("%v", err)
				pu.Lock()
				perrors += 1
				pu.Unlock()
			}
		}(request, document, buck, k, replace)
	}
	wg1.Wait()

	return nerr
}
