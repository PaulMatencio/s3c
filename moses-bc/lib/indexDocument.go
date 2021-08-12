// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package lib

import (
	"bytes"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"time"
)

func IndexDocument(document *documentpb.Document, bucket string, service *s3.S3,ctimeout time.Duration) (*s3.PutObjectOutput, error) {
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
	return api.PutObjectWithContext(ctimeout,putReq)
}
