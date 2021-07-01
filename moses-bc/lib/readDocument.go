
package lib

import (
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	doc "github.com/paulmatencio/protobuf-doc/lib"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	base64 "github.com/paulmatencio/ring/user/base64j"
	"github.com/paulmatencio/s3c/gLog"
	moses "github.com/paulmatencio/s3c/moses/lib"
	"github.com/paulmatencio/s3c/sproxyd/lib"
	"io/ioutil"
	"net/http"
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
	gLog.Info.Printf("Actual buffer size %d - Document size %d ",len(bytes),document.Size)
	return &document,err
}

func GetMetadata(request sproxyd.HttpRequest, pn string) (error, string) {
	var (
		usermd string
		// md     []byte
		resp   *http.Response
		err    error
	)
	request.Path = sproxyd.Env + "/" + pn
	if resp, err = sproxyd.GetMetadata(&request); err != nil  {
		return err, usermd
	}
	defer resp.Body.Close()
	if  resp.StatusCode != 200 {
		err = errors.New(fmt.Sprintf("Request document %s  status code %d", request.Path, resp.StatusCode))
		return err, usermd
	}
	if _, ok := resp.Header["X-Scal-Usermd"]; ok {
		usermd = resp.Header["X-Scal-Usermd"][0]

		/*
			if md, err = base64.Decode64(usermd); err != nil {
				gLog.Warning.Printf("Invalid user metadata %s", usermd)
			} else {
				gLog.Trace.Printf("User metadata %s", string(md))
			}
		*/
	} else {
		err = errors.New(fmt.Sprintf("Docid %d does not have user metadata ",pn))
	}
	return err, usermd
}

func GetObject(request sproxyd.HttpRequest, pn string) (error, string, *[]byte) {
	var (
		body   []byte
		usermd string
		md     []byte
	)
	resp, err := sproxyd.Getobject(&request)
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == 200 {
			body, _ = ioutil.ReadAll(resp.Body)
			if body != nil {
				if _, ok := resp.Header["X-Scal-Usermd"]; ok {
					usermd = resp.Header["X-Scal-Usermd"][0]
					if md, err = base64.Decode64(usermd); err != nil {
						gLog.Warning.Printf("Invalid user metadata %s", usermd)
					} else {
						gLog.Trace.Printf("User metadata %s", string(md))
					}
				}
			} else {
				err = errors.New(fmt.Sprintf("Request url %s - Body is empty", request.Path))
			}
		} else {
			err = errors.New(fmt.Sprintf("Request url %s -Status Code %d", request.Path, resp.StatusCode))
		}
	}
	return err, usermd, &body
}

func CheckPdfAndP0(pn string, usermd string ) (bool,bool){

	var (
		docmeta = moses.DocumentMetadata{}
		pdf bool = false
		p0  bool =  false

	)
	if err:= docmeta.UsermdToStruct(usermd); err != nil  {
		gLog.Warning.Printf("Error %v - Document %s has invalid user metadata",pn,err)
	} else {
		if docmeta.MultiMedia.Pdf {
			pdf = true
		}
		if len(docmeta.FpClipping.CountryCode) > 0 {
			p0 = true
		}
	}
	return pdf,p0
}