
package api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/datatype"
	"time"
)

func ListObjectWithContext(timeout time.Duration, req datatype.ListObjRequest)  ( *s3.ListObjectsOutput, error) {
	
	input := &s3.ListObjectsInput{

		Bucket: aws.String(req.Bucket),
		Prefix: aws.String(req.Prefix),
		MaxKeys: aws.Int64(req.MaxKey),
		Marker: aws.String(req.Marker),
		Delimiter: aws.String(req.Delimiter),
	}

	ctx,cancel := SetContext(timeout)
	defer cancel()
	return  req.Service.ListObjectsWithContext(ctx,input);

}

func ListObjectWithContextV2(timeout time.Duration,req datatype.ListObjV2Request)  ( *s3.ListObjectsV2Output, error) {


	input := &s3.ListObjectsV2Input{

		Bucket: aws.String(req.Bucket),
		Prefix: aws.String(req.Prefix),
		MaxKeys: aws.Int64(req.MaxKey),
		StartAfter: aws.String(req.Marker),
		Delimiter: aws.String(req.Delimiter),
	}
	if len(req.Continuationtoken) > 0 {
		input.ContinuationToken = &req.Continuationtoken
	}
	ctx,cancel := SetContext(timeout)
	defer cancel()
	return  req.Service.ListObjectsV2WithContext(ctx,input)

}

func ListObjectVersionsWithContext( req datatype.ListObjVersionsRequest)  ( *s3.ListObjectVersionsOutput, error) {

	input := &s3.ListObjectVersionsInput{
		Bucket: aws.String(req.Bucket),
		Prefix: aws.String(req.Prefix),
		MaxKeys: aws.Int64(req.MaxKey),
		Delimiter: aws.String(req.Delimiter),
		KeyMarker: aws.String(req.KeyMarker),
		VersionIdMarker: aws.String(req.VersionIdMarker),
	}
	return  req.Service.ListObjectVersions(input);
}