package utils

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/gLog"
)

func ProcS3Error(err error) {

	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		case s3.ErrCodeNoSuchKey:
			gLog.Warning.Printf("Error: [%v]  Error: [%v]",s3.ErrCodeNoSuchKey, aerr.Error())
		default:
			gLog.Error.Printf("error [%v]",aerr.Error())
		}
	} else {
		gLog.Error.Printf("[%v]",err.Error())
	}
}

