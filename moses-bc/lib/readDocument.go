
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
	Read Document from file
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
	Extract the document for the  backup
 */
func GetDocument(bytes  []byte) (*documentpb.Document,error){
	var (
		document= documentpb.Document{}
		err error
	)
	err = proto.Unmarshal(bytes, &document)
	return &document,err
}
