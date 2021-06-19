package lib

import (
	"bytes"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/protobuf/proto"
	doc "github.com/paulmatencio/protobuf-doc/lib"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	"os"
	"path/filepath"
	"strings"
)

func WriteDirectory(pn string, document *documentpb.Document, outdir string) (int){
	var (
		err error
		bytes []byte
	)
	if bytes, err = proto.Marshal(document); err == nil {
		//gLog.Info.Printf("Document %s  - length %d ",pn, len(bytes))
		pn = strings.Replace(pn, "/", "_", -1)
		ofn := filepath.Join(outdir, pn)
		if f, err := os.OpenFile(ofn, os.O_WRONLY|os.O_CREATE, 0600); err == nil {
			if err := doc.Write(f, bytes); err == nil {
				gLog.Info.Printf("%d bytes have be written to %s\n", len(bytes), ofn)
			}
		} else {
			gLog.Info.Println(err)
		}
	} else {
		gLog.Error.Println(err)
	}
	return  len(bytes)
}

func WriteS3 (service *s3.S3,bucket string,  document *documentpb.Document)(*s3.PutObjectOutput,error){

	if data, err := proto.Marshal(document); err == nil{
		meta := document.GetS3Meta()
		req:= datatype.PutObjRequest{
			//Service : s3.New(api.CreateSession()),
			Service: service,
			Bucket: bucket,
			Key: document.DocId,
			Buffer: bytes.NewBuffer(data), // convert []byte into *bytes.Buffer
			Meta : []byte(meta),

		}
		return api.PutObject(req)
	} else {
		return nil,err
	}
}