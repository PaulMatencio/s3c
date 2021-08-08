package lib

import (
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	// "github.com/paulmatencio/s3c/datatype"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/sproxyd/lib"
	"net/http"
	"strconv"
	"sync"
)

func RestoreBlobs(document *documentpb.Document) int {

	gLog.Info.Printf("Restore blobs - Number of pages %d - MaxPage %d - Replace %v",document.NumberOfPages, MaxPage,Replace)
	if document.NumberOfPages <= int32(MaxPage) {
		return restoreBlob(document)
	} else {
		return restoreLargeBlob(document)
	}
}

func Restores3Objects(service *s3.S3, bucket string, document *documentpb.Document) int {

	gLog.Info.Printf("Restore blobs - Number of pages %d - MaxPage %d - Replace %v",document.NumberOfPages, MaxPage,Replace)
	if document.NumberOfPages <= int32(MaxPage) {
		return restoreS3Object(service,bucket,document)
	} else {
		return restoreLargeS3Object(service,bucket,document)
	}
}
//  Put  sproxyd blobs
func restoreBlob(document *documentpb.Document) int {
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
	gLog.Trace.Printf("Restore regular blobs - Number of pages %d - MaxPage %d - Replace %v",document.NumberOfPages, MaxPage,Replace)
	//   Write document metadata
	if nerr, status := WriteDocMetadata(&request, document,Replace); nerr > 0 {
		gLog.Warning.Printf("Document %s is not restored", document.DocId)
		perrors += nerr
		return perrors
	} else {
		if status == 412 {
			gLog.Warning.Printf("Document %s is not restored - use --replace=true  ou -r=true to replace the existing document", document.DocId)
			return 1
		}
	}
	//  get the number of pages
	// start := time.Now()
	pages := document.GetPage()
	for _, pg := range pages {
		wg1.Add(1)
		go func(request sproxyd.HttpRequest, pg *documentpb.Page) {
			defer wg1.Done()
			if perr, _ := WriteDocPage(request, pg,Replace); perr > 0 {
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

func restoreLargeBlob(document *documentpb.Document) int {
	var (
		np          = int(document.NumberOfPages)
		q       int = np / MaxPage
		r       int = np % MaxPage
		start   int = 1
		perrors int
		end     int = start + MaxPage - 1
		request     = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP, // IP of target sproxyd
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			// ReqHeader: map[string]string{},
		}
	)
	gLog.Info.Printf("Restore large blobs to sproxyd - Number of pages %d - MaxPage %d - Replace %v",document.NumberOfPages, MaxPage,Replace)
	if nerr, status := WriteDocMetadata(&request, document, Replace); nerr > 0 {
		gLog.Warning.Printf("Error writing document metadata - Document %s is not restored", document.DocId)
		perrors += nerr
		return perrors
	} else {
		if status == 412 {
			gLog.Warning.Printf("Document %s is not restored - use --replace=true  ou -r=true to replace the existing document", document.DocId)
			return 1
		}
	}

	for s := 1; s <= q; s++ {
		perrors = restoreLargeBlobPart(&request, document, start, end)
		start = end + 1
		end += MaxPage
		if end > np {
			end = np
		}
	}
	if r > 0 {
		perrors = restoreLargeBlobPart(&request, document, q*MaxPage+1, np)
	}
	return perrors
}

func restoreLargeBlobPart(request *sproxyd.HttpRequest, document *documentpb.Document, start int, end int) int {

	var (
		perrors int
		pages   = document.GetPage()
		pu      sync.Mutex
		wg1     sync.WaitGroup
	)
	/*
		loading
	*/
	gLog.Info.Printf("Docid: %s - Starting slot %d - Ending slot  %d - Number of pages %d  - Length of pages array: %d ", document.DocId, start, end, document.NumberOfPages, len(pages))
	for k := start; k <= end; k++ {
		pg := *pages[k-1]
		wg1.Add(1)
		request.Path = sproxyd.TargetEnv + "/" + pg.PageId + "/p" + strconv.Itoa(int(pg.PageNumber))
		go func(request sproxyd.HttpRequest, pg *documentpb.Page, replace bool) {
			defer wg1.Done()
			if perr, _ := WriteDocPage(request, pg, replace); perr > 0 {
				pu.Lock()
				perrors += perr
				pu.Unlock()
			}

		}(*request, &pg, Replace)
	}
	wg1.Wait()
	request.Client.CloseIdleConnections()
	gLog.Trace.Printf("Writedoc document %s  starting slot: %d - ending slot: %d  completed", document.DocId, start, end)
	return perrors

}


func restoreS3Object( service *s3.S3,bucket string, document *documentpb.Document) int {

	var (
		perrors int
		result  *s3.PutObjectOutput
		err error
		wg1 sync.WaitGroup
		pu sync.Mutex
	)
	//   Write document metadata

	if result,err  = WriteS3Metadata(service,bucket , document); err!= nil  {
		gLog.Warning.Printf("Error %v writing document metadata - Document %s is not restored", err,document.DocId)
		perrors += 1
		return perrors
	} else {
		gLog.Info.Printf("Document %s - metadata is restored to S3 bucket %s - Etag %s ",document.DocId, bucket , result.ETag)
	}
	//  get the number of pages
	// start := time.Now()
	pages := document.GetPage()
	for _, pg := range pages {
		wg1.Add(1)
		go func(service *s3.S3, bucket string, pg *documentpb.Page) {
			defer wg1.Done()
			if _,err = WriteS3Page(service,bucket,pg); err != nil {
				pu.Lock()
				perrors += 1
				pu.Unlock()
				gLog.Error.Printf("Error %v writing page %d",err,pg.PageNumber)
			}
		}(service,bucket, pg)
	}
	wg1.Wait()
	return perrors
}

func restoreLargeS3Object(service *s3.S3, bucket string, document *documentpb.Document) int {

	var (
		np          = int(document.NumberOfPages)
		q       int = np / MaxPage
		r       int = np % MaxPage
		start   int = 1
		perrors int
		end     int = start + MaxPage - 1

	)
	gLog.Info.Printf("Restoring large blobs to S3 bucket %s- Number of pages %d - MaxPage %d - Replace %v",bucket, document.NumberOfPages, MaxPage,Replace)
	if result, err := WriteS3Metadata(service,bucket, document); err != nil  {
		gLog.Warning.Printf("Error %v writing document  %s metadata", err,document.DocId)
		perrors += 1
		return perrors
	} else {
		gLog.Info.Printf("Document %s - metadata is restored to S3 bucket %s - Etag %s ",document.DocId, bucket , *result.ETag)
	}

	for s := 1; s <= q; s++ {
		perrors = restoreLargeS3ObjectPart(service,bucket, document, start, end)
		start = end + 1
		end += MaxPage
		if end > np {
			end = np
		}
	}
	if r > 0 {
		perrors = restoreLargeS3ObjectPart(service,bucket, document, q*MaxPage+1, np)
	}
	return perrors
}


func restoreLargeS3ObjectPart(service *s3.S3,bucket string, document *documentpb.Document, start int, end int) int {

	var (
		perrors int
		pages   = document.GetPage()
		pu      sync.Mutex
		wg1     sync.WaitGroup
		err error
	)

	/*
		loading
	*/

	gLog.Info.Printf("Docid: %s - Starting slot %d - Ending slot  %d - Number of pages %d  - Length of pages array: %d ", document.DocId, start, end, document.NumberOfPages, len(pages))
	for k := start; k <= end; k++ {
		pg := *pages[k-1]
		wg1.Add(1)

		go func(service *s3.S3,bucket string, pg *documentpb.Page) {

			defer wg1.Done()
			if _,err = WriteS3Page(service,bucket,pg); err != nil {
				pu.Lock()
				perrors += 1
				pu.Unlock()
				gLog.Error.Printf("Error %v writing page %d",err,pg.PageNumber)
			}

		}(service,bucket, &pg)
	}
	wg1.Wait()

	gLog.Trace.Printf("WriteS3  document %s  starting slot: %d - ending slot: %d  completed", document.DocId, start, end)
	return perrors

}