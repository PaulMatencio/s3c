package lib

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/protobuf/proto"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"strconv"

	// sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"

	// mosesbc "github.com/paulmatencio/s3c/moses-bc/cmd"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/viper"
	"net/http"
	"sort"
	"time"
)

type Resp struct {
	Cp  *s3.CompletedPart
	Cl  int64
	Err error
}

/*
	Write document to S3
*/

func WriteS3(service *s3.S3, bucket string, document *documentpb.Document) (*s3.PutObjectOutput, error) {
	if data, err := proto.Marshal(document); err == nil {
		var (
			usermd    = document.GetS3Meta()
			versionId = document.VersionId
			metadata  = make(map[string]*string)
		)
		metadata["Usermd"] = &usermd
		metadata["VersionId"] = &versionId

		req := datatype.PutObjRequest3{
			Service:     service,
			Bucket:      bucket,
			Key:         document.DocId,
			Buffer:      bytes.NewBuffer(data), // convert []byte into *bytes.Buffer
			Metadata:    metadata,              // user metadata
			ContentType: http.DetectContentType(data),
		}
		return api.PutObject3(req)
	} else {
		return nil, err
	}
}

func WriteS3Multipart(service *s3.S3, bucket string, maxPartSize int64, document *documentpb.Document) ([]error, error) {

	var (
		n, t                      = 0, 0
		curr, remaining, partSize int64
		partNumber                int
		documentSize              int64
		completedParts            []*s3.CompletedPart
		// buffer                                 = document.GetObject()
		key      = document.GetDocId()
		buffer   []byte
		err      error
		resp     *s3.CreateMultipartUploadOutput
		errs     []error
		metadata = make(map[string]*string)
	)
	/*
		if retryNumber := utils.GetRetryNumber(*viper.GetViper()); retryNumber == 0 {
		 	retryNumber = mosesbc.RETRY
		}

		if waitTime :=  utils.GetWaitTime(*viper.GetViper()); waitTime == 0 {
		 	waitTime = mosesbc.WaitTime
		}
	*/
	if buffer, err = proto.Marshal(document); err != nil {
		return errs, err
	}
	documentSize = int64(len(buffer))
	metadata["Usermd"] = &document.Metadata
	metadata["VersionId"] = &document.VersionId

	create := datatype.CreateMultipartUploadRequest{
		Service:  service,
		Bucket:   bucket,
		Key:      key,
		Metadata: metadata,
		// VersionId:   document.VersionId,
		ContentType: http.DetectContentType(buffer),
	}

	if resp, err = api.CreateMultipartUpload(create); err == nil {

		partNumber = 1 //  start with 1
		// maxCon = 0  /* maximum //  upload */
		upload := datatype.UploadPartRequest{
			Service: service,
			Resp:    resp,
		}
		partSize = maxPartSize
		remaining = documentSize
		ch := make(chan *Resp)
		start := time.Now()
		for curr = 0; remaining != 0; curr += partSize {
			if remaining < maxPartSize {
				partSize = remaining
			} else {
				partSize = maxPartSize
			}
			content := buffer[curr : curr+partSize]
			n++
			go func(upload datatype.UploadPartRequest, partNumber int, content []byte) {
				// gLog.Trace.Printf("Uploading part :%d- Size %d", partNumber, len(content))
				upload.PartNumber = partNumber
				upload.Content = content
				if completedPart, err := UploadPart(upload); err == nil {
					ch <- &Resp{
						Cp:  completedPart,
						Cl:  int64(len(content)),
						Err: nil,
					}
				} else {
					ch <- &Resp{
						Cp:  nil,
						Err: err,
					}
				}
			}(upload, partNumber, content)
			remaining -= partSize
			partNumber++
		}
		done := false
		for ok := true; ok; ok = !done {
			select {
			case cp := <-ch:
				t++
				if cp.Err == nil {
					gLog.Trace.Printf("Appending completed part %d - Etag %s - size %d", *cp.Cp.PartNumber, *cp.Cp.ETag, cp.Cl)
					completedParts = append(completedParts, cp.Cp)
				} else {
					errs = append(errs, cp.Err)
					gLog.Error.Printf("Error %v uploading part %d size %d", cp.Err, cp.Cp.PartNumber, cp.Cl)
				}
				if t == n {
					gLog.Info.Printf("%d parts are uploaded to bucket %s", n, bucket)
					done = true
				}
			case <-time.After(200 * time.Millisecond):
				fmt.Printf("w")
			}
		}

		//   completed the multipart uploaded
		CompletedUpload(service, resp, completedParts)
		elapsed := time.Since(start)
		MBsec := float64(len(buffer)) / float64(elapsed)
		gLog.Info.Printf("Elapsed time %v - MB/sec %3.1f", elapsed, MBsec)
	}
	return errs, err
}

func CompletedUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, part []*s3.CompletedPart) {

	sort.Sort(datatype.ByPart(part))
	comp := datatype.CompleteMultipartUploadRequest{
		Service:        svc,
		Resp:           resp,
		CompletedParts: part,
	}
	if cResp, err := api.CompleteMultipartUpload(comp); err == nil {
		gLog.Info.Printf("Key: %s - Etag: %s - Bucket: %s", *cResp.Key, *cResp.ETag, *cResp.Bucket)
	} else {
		gLog.Error.Printf("%v", err)
	}
}

func UploadPart(upload datatype.UploadPartRequest) (*s3.CompletedPart, error) {

	gLog.Trace.Printf("Uploading part :%d - Size %d", upload.PartNumber, len(upload.Content))

	if retryNumber := utils.GetRetryNumber(*viper.GetViper()); retryNumber == 0 {
		upload.RetryNumber = 5
	} else {
		upload.RetryNumber = retryNumber
	}

	if waitTime := utils.GetWaitTime(*viper.GetViper()); waitTime == 0 {
		upload.WaitTime = 200
	} else {
		upload.WaitTime = waitTime
	}

	if completedPart, err := api.UploadPart(upload); err != nil {
		gLog.Error.Printf("Error: %v uploading part:%d", err, upload.PartNumber)
		gLog.Warning.Printf("Aborting multipart upload")
		abort := datatype.AbortMultipartUploadRequest{
			Service: upload.Service,
			Resp:    upload.Resp,
		}
		if err = api.AbortMultipartUpload(abort); err != nil {
			gLog.Error.Printf("Error %v", err)
		}
		return nil, err
	} else {
		return completedPart, err
	}
}

func WriteS3Metadata(service *s3.S3,bucket string, document *documentpb.Document) (*s3.PutObjectOutput, error) {
	var metadata = map[string]*string{
		"Usermd": &document.Metadata,
		"s3Meta": &document.S3Meta,
	}
	req := datatype.PutObjRequest3{
		Service:     service,
		Bucket:      bucket,
		Key:         document.GetDocId(),
		Buffer:      bytes.NewBuffer(make([]byte, 0, 0)), // convert []byte into *bytes.Buffer
		Metadata:    metadata,                      // user metadata

	}
	return api.PutObject3(req)
}


func WriteS3Pdf(service *s3.S3, bucket string, pdf *documentpb.Pdf) (*s3.PutObjectOutput, error) {

	var metadata = map[string]*string{
		"Usermd": &pdf.Metadata,
	}

	req := datatype.PutObjRequest3{
		Service:     service,
		Bucket:      bucket,
		Key:         pdf.GetPdfId(),
		Buffer:      bytes.NewBuffer(pdf.Pdf), // convert []byte into *bytes.Buffer
		Metadata:    metadata,                 // user metadata
		ContentType: http.DetectContentType(pdf.Pdf),
	}
	return api.PutObject3(req)

}

func WriteS3Page(service *s3.S3, bucket string, page *documentpb.Page) (*s3.PutObjectOutput, error) {

	var metadata = map[string]*string{
		"Usermd": &page.Metadata,
	}
	req := datatype.PutObjRequest3{
		Service:     service,
		Bucket:      bucket,
		Key:         page.PageId + "/p" + strconv.Itoa(int(page.PageNumber)),
		Buffer:      bytes.NewBuffer(page.Object), // convert []byte into *bytes.Buffer
		Metadata:    metadata,                      // user metadata
		ContentType: page.GetContentType(),
	}
	return api.PutObject3(req)

}
