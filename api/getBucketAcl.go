package api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/datatype"
)

func GetBucketAcl(req datatype.GetBucketAclRequest) (*s3.GetBucketAclOutput,error){
	//  fmt.Println(req.Bucket)
	input := &s3.GetBucketAclInput{
		Bucket: aws.String(req.Bucket),

	}
	return req.Service.GetBucketAcl(input)
}
