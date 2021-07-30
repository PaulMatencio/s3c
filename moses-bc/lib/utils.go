
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
	"strings"
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

	request.ReqHeader =  map[string]string{
		"X-Scal-Replica-Policy": "immutable",
	}
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



func GetObjAndId(request sproxyd.HttpRequest, pn string) (RingId) {
	var (
		body   []byte
		usermd,ringKey string
	)
	request.ReqHeader =  map[string]string{
		"X-Scal-Replica-Policy": "immutable",
	}
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


func SetBucketName(prefix string,bucket string) string{
	var s  string
	if len(prefix)> 0 {
		pref := strings.Split(prefix, "/")[0]
		if pref != "XP" {
			s = fmt.Sprintf("%02d", HashKey(pref, 5))
		} else {
			s = "05"
		}
	}
	gLog.Trace.Printf("Set bucket name %s ",bucket+"-"+s)
	return bucket+"-" +s
}

func HasSuffix(bucket string) bool {
	if  bucket[len(bucket)-2:len(bucket)] >= "00"  && bucket[len(bucket)-2:len(bucket)] <="05" {
		return true
	}
	return false
}

func CheckBucketName(srcBucket string, tgtBucket string) (error) {
	var err error
	if srcBucket == tgtBucket {
		return errors.New(fmt.Sprintf("Target bucket %s and source bucket %s are the same", tgtBucket, srcBucket))

	} else {
		if len(srcBucket) > 2 && len(tgtBucket)>  2 {
			sl2 := srcBucket[len(srcBucket)-2:len(srcBucket)]
			tl2 := tgtBucket[len(tgtBucket)-2:len(tgtBucket)]
			if sl2 != tl2 {
				return errors.New(fmt.Sprintf("The suffix <%s> of source bucket %s  and the suffix <%s> target bucket %s are different", sl2, srcBucket, tl2, tgtBucket))

			}
		}
	}
	return err
}

func GetBucketSuffix(bucket string, prefix string) (error,string) {
	var (
		err error
		s  string
	)
	pref := strings.Split(prefix,"/")[0]
	if pref != "XP" {
		s = fmt.Sprintf("%02d", HashKey(pref, 5))
	}  else {
		s = "05"
	}
	suf := bucket[len(bucket)-2:len(bucket)]
	if (suf >="00" && suf <="05") {
		if suf != s {
			return errors.New(fmt.Sprintf("For the given document id prefix %s, the suffix of the bucket %s should be %s\n\t\t\tYou can use the suffix commmand to detemine the suffix for a given prefix \n\t\t\tor just omit it,the correct suffix will be appended to the bucket name", prefix, bucket, s)), s
		} else {
			return err,""
		}
	}

	return err,s
}

func HashKey(key string, modulo int) (int) {
	v := 0;
	for k :=0; k < len(key); k++ {
		v += int(key[k])
	}
	return v % modulo
}

func ParseLog(line string ) (error, string, string ){
	gLog.Trace.Println(line)
	if len(line) > 0 {
		L := strings.Split(line, " ")
		gLog.Trace.Println(L)
		if len(L) ==  2 {
			gLog.Trace.Printf("Method %s Key %s",L[0],L[1])
			return nil, L[0], L[1]
		}
	}
	return errors.New(fmt.Sprintf("Invalid input parameter  <method> <key> in %s",line)),"",""
}