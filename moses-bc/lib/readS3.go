package lib

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	// "github.com/golang/protobuf/proto"
	// "github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"

)

func ReadS3(service *s3.S3, bucket string, key string ) (*s3.GetObjectOutput,error) {

		req := datatype.GetObjRequest{
			Service:     service,
			Bucket:      bucket,
			Key:         key,
		}
		return api.GetObject(req)

}

func ReadMultipartS3(service *s3.S3, bucket string , key string) (int64, *aws.WriteAtBuffer,error){

		req := datatype.GetMultipartObjRequest{
		Service:        service,
		Bucket:         bucket,
		Key:            key,
		PartNumber:     int64(PartNumber),
		PartSize:       MaxPartSize,
		Concurrency:    MaxCon,
	}
	return api.GetMultipartToBuffer(req)
}