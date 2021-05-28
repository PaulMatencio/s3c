package api

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/datatype"
)

func ListBucket(req datatype.ListBucketRequest) (*s3.ListBucketsOutput, error){

	input := &s3.ListBucketsInput{}
	return  req.Service.ListBuckets(input)

}

/*
func ListBucket() (*s3.ListBucketsOutput, error){

	svc := s3.New(CreateSession())
	input := &s3.ListBucketsInput{}
	return  svc.ListBuckets(input)

}
*/

