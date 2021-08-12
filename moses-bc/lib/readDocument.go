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
	"github.com/golang/protobuf/proto"
	doc "github.com/paulmatencio/protobuf-doc/lib"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"github.com/paulmatencio/s3c/gLog"
	"os"
	"path/filepath"
	"strings"
)


/*
	Read Document from a file  into a document structure
 */
func ReadDocument(pn string, inDir string) (*documentpb.Document,error) {
	var (
		bytes    []byte
		document= documentpb.Document{}
		err error
		f *os.File
	)
	//gLog.Info.Printf("Document %s  - length %d ",pn, len(bytes))
	pn = strings.Replace(pn, "/", "_", -1)
	ifn := filepath.Join(inDir, pn)
	if f, err = os.OpenFile(ifn, os.O_RDONLY, 0600); err == nil {
		if bytes, err = doc.Read(f); err == nil {
			gLog.Info.Printf("%d bytes are read\n", len(bytes))
			if err = proto.Unmarshal(bytes, &document); err != nil {
				gLog.Error.Println("Error %v unma  file %s", ifn, err)
			}
		} else {
			gLog.Error.Println("Error %v reading  file %s", ifn, err)
		}
	} else {
		gLog.Error.Printf("Error %v  opening file %s", ifn, err)
	}
	return &document,err
}

/*
	Document -> document structure
 */
func GetDocument(bytes  []byte) (*documentpb.Document,error){
	var (
		document= documentpb.Document{}
		err error
	)
	err = proto.Unmarshal(bytes, &document)
	gLog.Info.Printf("Actual buffer size %d - Document size %d ",len(bytes),document.Size)
	return &document,err
}

