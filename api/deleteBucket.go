
package api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/datatype"
)

func DeleteBucket(req datatype.DeleteBucketRequest ) (*s3.DeleteBucketOutput,error){

	input := &s3.DeleteBucketInput {
		Bucket: aws.String(req.Bucket),
	}

	return req.Service.DeleteBucket(input)

}
