package api

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/datatype"
)

func DeleteObjects(req datatype.DeleteObjRequest) (*s3.DeleteObjectOutput, error) {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(req.Bucket),
		Key:    aws.String(req.Key),
	}
	if len(req.VersionId) > 0 {
		input.VersionId = aws.String(req.VersionId)
	}
	return req.Service.DeleteObject(input)
}

func DeleteObjectVersions(req []*datatype.DeleteObjRequest) (*s3.DeleteObjectsOutput, error) {
	var (
		delete   = &s3.Delete{}
		objectId = &s3.ObjectIdentifier{}
		objects   []*s3.ObjectIdentifier
	)

	for _, req1 := range req {
		objectId.Key = aws.String(req1.Key)
		objectId.VersionId = aws.String(req1.VersionId)
	}
	objects = append(objects, objectId)
	delete.Objects = objects
	delete.Quiet = aws.Bool(false)
	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(req[0].Bucket),
		Delete: delete,
	}

	return req[0].Service.DeleteObjects(input)
}
