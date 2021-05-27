package api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
)

func CopyObject(req datatype.CopyObjRequest) (*s3.CopyObjectOutput,error){

	input := &s3.CopyObjectInput{
		Bucket: aws.String(req.Tbucket),
		Key:    aws.String(req.Tkey),
	}
	input.CopySource= aws.String("/"+req.Sbucket + "/"+ req.Skey)
	gLog.Trace.Printf("Copy object %s",*input.CopySource)

	return req.Service.CopyObject(input)
}




