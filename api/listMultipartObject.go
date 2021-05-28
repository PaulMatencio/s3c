package api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/datatype"
)

func ListMultipartObject(req datatype.ListMultipartObjRequest) (*s3.ListMultipartUploadsOutput, error) {

	input := &s3.ListMultipartUploadsInput{

		Bucket:    aws.String(req.Bucket),
		Prefix:    aws.String(req.Prefix),
		KeyMarker: aws.String(req.KeyMarker),
		MaxUploads: aws.Int64(req.MaxUploads),
		UploadIdMarker: aws.String(req.UploadIdmarker),
		Delimiter: aws.String(req.Delimiter),
	}

	// svc.ListObjectsRequest(input)

	return req.Service.ListMultipartUploads(input)

}
