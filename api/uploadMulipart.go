package api

import (
	"bytes"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	// "github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
)

func CreateMultipartUpload(req datatype.CreateMultipartUploadRequest) (*s3.CreateMultipartUploadOutput, error) {
	gLog.Trace.Printf("Creating multipart upload for key: %s to bucket: %s", req.Key,req.Bucket)
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(req.Bucket),
		Key:    aws.String(req.Key),
	}
	return req.Service.CreateMultipartUpload(input)
}

func CompleteMultipartUpload(req datatype.CompleteMultipartUploadRequest) (*s3.CompleteMultipartUploadOutput, error) {

	gLog.Trace.Printf("Completing multipart upload for UploadId# %s", *req.Resp.UploadId)
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   req.Resp.Bucket,
		Key:      req.Resp.Key,
		UploadId: req.Resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: req.CompletedParts,
		},
	}
	return req.Service.CompleteMultipartUpload(completeInput)
}

func AbortMultipartUpload(req datatype.AbortMultipartUploadRequest) error {
	gLog.Trace.Printf("Aborting multipart upload for UploadId# %s", *req.Resp.UploadId)
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   req.Resp.Bucket,
		Key:      req.Resp.Key,
		UploadId: req.Resp.UploadId,
	}
	_, err := req.Service.AbortMultipartUpload(abortInput)
	return err
}

func UploadPart(req datatype.UploadPartRequest) (*s3.CompletedPart, error) {
	var (
		err          error
		uploadOutput *s3.UploadPartOutput
		waitTime     = req.WaitTime
		retryNumber  = req.RetryNumber
	)
	gLog.Trace.Printf("Uploading part: %v - part length: %d\n", req.PartNumber,len(req.Content))
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(req.Content),
		Bucket:        req.Resp.Bucket,
		Key:           req.Resp.Key,
		PartNumber:    aws.Int64(int64(req.PartNumber)),
		UploadId:      req.Resp.UploadId,
		ContentLength: aws.Int64(int64(len(req.Content))),
	}
	for r := 1; r <= retryNumber; r++ {
		if uploadOutput, err = req.Service.UploadPart(partInput); err == nil {
			gLog.Trace.Printf("Uploaded part: %v - part length: %d\n", req.PartNumber,len(req.Content))
			return &s3.CompletedPart{
				ETag:       uploadOutput.ETag,
				PartNumber: aws.Int64(int64(req.PartNumber)),
			}, err
		} else {
			gLog.Error.Printf("Retrying to upload part: #%v - Retry#: %d \n", req.PartNumber, r)
			time.Sleep(waitTime * time.Millisecond)
		}
	}
	return nil, err
}
