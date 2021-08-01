package lib

import (
	"bytes"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
)

func IndexDocument(document *documentpb.Document, bucket string, service *s3.S3) (*s3.PutObjectOutput, error) {
	var (
		data     = make([]byte, 0, 0) // empty byte array
	)
	// gLog.Info.Println(document.S3Meta)
	metadata := make(map[string]*string)
	metadata["Usermd"] = &document.S3Meta
	putReq := datatype.PutObjRequest3{
		Service: service,
		Bucket:  bucket,
		Key:     document.GetDocId(),
		Buffer:  bytes.NewBuffer(data), // convert []byte into *bytes.Buffer
		Metadata:    metadata,
	}
	return api.PutObject3(putReq)
}
