
package api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/datatype"
)
func GetObject(req datatype.GetObjRequest) (*s3.GetObjectOutput,error){

	input := &s3.GetObjectInput{
		Bucket: aws.String(req.Bucket),
		Key:    aws.String(req.Key),
	}
	if len(req.VersionId) >0 {
		input.VersionId = aws.String(req.VersionId)
	}
	return req.Service.GetObject(input)
}




