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

func WriteDocument(pn string, document *documentpb.Document, outdir string) (int){
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