
package lib

import (
	"errors"
	"fmt"
	base64 "github.com/paulmatencio/ring/user/base64j"
	"github.com/paulmatencio/s3c/gLog"
	moses "github.com/paulmatencio/s3c/moses/lib"
	"github.com/paulmatencio/s3c/sproxyd/lib"
	"io/ioutil"
	"net/http"
)

type  RingId struct {
	Err   error
	UserMeta string
	Key string
	Object *[]byte

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
	// request.ReqHeader["X-Scal-Replica-Policy"] = "immutable"
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

func GetObjId(request sproxyd.HttpRequest, pn string) (RingId) {
	var (
		body   []byte
		usermd,ringKey string

	)
	//request.ReqHeader["X-Scal-Replica-Policy"] = "immutable"
	resp, err := sproxyd.Getobject(&request)
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == 200 {
			body, _ = ioutil.ReadAll(resp.Body)
			if body != nil {
				if _, ok := resp.Header["X-Scal-Usermd"]; ok {
					usermd = resp.Header["X-Scal-Usermd"][0]
					if md, err := base64.Decode64(usermd); err != nil {
						gLog.Warning.Printf("Invalid user metadata %s", usermd)
					} else {
						gLog.Trace.Printf("User metadata %s", string(md))
					}
				}
				if _, ok := resp.Header["X-Scal-Ring-Key"]; ok {
					ringKey = resp.Header["X-Scal-Ring-Key"][0]
				}

			} else {
				err = errors.New(fmt.Sprintf("Request url %s - Body is empty", request.Path))
			}
		} else {
			err = errors.New(fmt.Sprintf("Request url %s -Status Code %d", request.Path, resp.StatusCode))
		}
	}
	return RingId{
		Err : err,
		UserMeta: usermd,
		Key: ringKey,
		Object: &body,
	}
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