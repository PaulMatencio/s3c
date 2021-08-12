package api

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/utils"
	"os"
	"time"
)

func FputObject(req datatype.FputObjRequest) (*s3.PutObjectOutput,error){
	f, err := os.Open(req.Inputfile)
	if err != nil {
		fmt.Println(err)
		return nil,err
	}
	defer f.Close()
	fileInfo, _ := f.Stat()
	var size int64 = fileInfo.Size()
	buffer := make([]byte, size)
	// read file content to buffer
	f.Read(buffer)
	input := &s3.PutObjectInput{
		Bucket: aws.String(req.Bucket),
		Key:    aws.String(req.Key),
		Body:   bytes.NewReader(buffer),
		Metadata: utils.BuildUserMeta(req.Meta),
	}
	return req.Service.PutObject(input)
}

func FputObject2(req datatype.FputObjRequest) (*s3.PutObjectOutput,error){
	f, err := os.Open(req.Inputfile)
	if err != nil {
		fmt.Println(err)
		return nil,err
	}
	defer f.Close()
	fileInfo, _ := f.Stat()
	var size int64 = fileInfo.Size()
	buffer := make([]byte, size)
	// read file content to buffer
	f.Read(buffer)

	input := &s3.PutObjectInput{
		Bucket: aws.String(req.Bucket),
		Key:    aws.String(req.Key),
		Body:   bytes.NewReader(buffer),
		Metadata: utils.BuildUsermd(req.Usermd),
	}
	return req.Service.PutObject(input)
}

func PutObject(req datatype.PutObjRequest) (*s3.PutObjectOutput,error){
	input := &s3.PutObjectInput{
		Bucket: aws.String(req.Bucket),
		Key:    aws.String(req.Key),
		Body: bytes.NewReader(req.Buffer.Bytes()),
		Metadata: utils.BuildUserMeta(req.Meta),
	}
	return req.Service.PutObject(input)
}

func PutObject2(req datatype.PutObjRequest) (*s3.PutObjectOutput,error){
	input := &s3.PutObjectInput{
		Bucket: aws.String(req.Bucket),
		Key:    aws.String(req.Key),
		Body: bytes.NewReader(req.Buffer.Bytes()),
		Metadata: utils.BuildUsermd(req.Usermd),
	}
	return req.Service.PutObject(input)
}

func PutObjectWithContext(timeout time.Duration,req datatype.PutObjRequest3) (*s3.PutObjectOutput,error){

	input := &s3.PutObjectInput{
		Bucket: aws.String(req.Bucket),
		Key:    aws.String(req.Key),
		Body: bytes.NewReader(req.Buffer.Bytes()),
		Metadata: req.Metadata,
		ContentType: aws.String(req.ContentType),
	}
	ctx,cancel := SetContext(timeout)
	defer cancel()
	return req.Service.PutObjectWithContext(ctx,input)
}


