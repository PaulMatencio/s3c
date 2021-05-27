package api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/datatype"
)

func GetBucketPolicy(req datatype.GetBucketPolicyRequest) (*s3.GetBucketPolicyOutput,error){

	input := &s3.GetBucketPolicyInput{
		Bucket: aws.String(req.Bucket),
	}
	return req.Service.GetBucketPolicy(input)
}

