package lib

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/protobuf/proto"
	doc "github.com/paulmatencio/protobuf-doc/lib"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	"strconv"

	// mosesbc "github.com/paulmatencio/s3c/moses-bc/cmd"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/viper"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)
/*
const (
	MinPartSize                = 5 * 1024 * 1024     // 5 MB
	MaxFileSize                = MinPartSize * 409.6 // 2 GB
	DefaultDownloadConcurrency = 5
	Dummy                      = "/dev/null"
	RetryNumber                = 5
	WaitTime	= 200
	//MaxPartSize					= MinPartSize * 4
)

 */

type Resp struct {
	Cp  *s3.CompletedPart
	Cl  int64
	Err error
}

/*
	Wrote document to file
*/
func WriteDirectory(pn string, document *documentpb.Document, outdir string) (error, int) {
	var (
		err   error
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
		}
	}
	return err, len(bytes)
}

/*
	Wrote document to S3
*/
func WriteS3(service *s3.S3, bucket string, document *documentpb.Document) (*s3.PutObjectOutput, error) {
	if data, err := proto.Marshal(document); err == nil {
		meta := document.GetS3Meta()
		req := datatype.PutObjRequest{
			Service: service,
			Bucket:  bucket,
			Key:     document.DocId,
			Buffer:  bytes.NewBuffer(data), // convert []byte into *bytes.Buffer
			Meta:    []byte(meta),
		}
		return api.PutObject(req)
	} else {
		return nil, err
	}
}

func WriteS3Multipart(service *s3.S3, bucket string, maxPartSize int64,document *documentpb.Document) ([]error,error){

	var (
		n, t                                   = 0, 0
		curr, remaining, partSize int64
		partNumber                             int
		documentSize                           int64
		completedParts                         []*s3.CompletedPart
		// buffer                                 = document.GetObject()
		key                                    = document.GetDocId()
		buffer				[]byte
		err										error
		resp									*s3.CreateMultipartUploadOutput
		errs								[]error
	)
	/*
	if retryNumber :=utils.GetRetryNumber(*viper.GetViper()); retryNumber == 0 {
	 	retryNumber = mosesbc.RETRY
	}

	if waitTime :=utils.GetWaitTime(*viper.GetViper()); waitTime == 0 {
	 	waitTime = mosesbc.WaitTime
	}
	*/
	if buffer, err = proto.Marshal(document); err != nil {
		return errs, err
	}
	documentSize = int64(len(buffer))
	fType  := http.DetectContentType(buffer)
	create := datatype.CreateMultipartUploadRequest{
		Service:     service,
		Bucket:      bucket,
		Key:         key,
		ContentType: fType,
	}

	if resp, err = api.CreateMultipartUpload(create); err == nil {

		partNumber = 1 //  start with 1
		// maxCon = 0  /* maximum //  upload */
		upload := datatype.UploadPartRequest{
			Service: service,
			Resp:    resp,
		}
		partSize = maxPartSize
		remaining = documentSize
		ch := make(chan *Resp)
		start := time.Now()
		for curr = 0; remaining != 0; curr += partSize {
			if remaining < maxPartSize {
				partSize = remaining
			} else {
				partSize = maxPartSize
			}
			content := buffer[curr : curr+partSize]
			n++
			go func(upload datatype.UploadPartRequest, partNumber int, content []byte) {
				// gLog.Trace.Printf("Uploading part :%d- Size %d", partNumber, len(content))
				upload.PartNumber = partNumber
				upload.Content = content
				if completedPart, err := UploadPart(upload); err == nil {
					ch <- &Resp{
						Cp:  completedPart,
						Cl:  int64(len(content)),
						Err: nil,
					}
				} else {
					ch <- &Resp{
						Cp:  nil,
						Err: err,
					}
				}
			}(upload, partNumber, content)
			remaining -= partSize
			partNumber++
		}
		done := false
		for ok := true; ok; ok = !done {
			select {
			case cp := <-ch:
				t++
				if cp.Err == nil {
					gLog.Trace.Printf("Appending completed part %d - Etag %s - size %d", *cp.Cp.PartNumber, *cp.Cp.ETag, cp.Cl)
					completedParts = append(completedParts, cp.Cp)
				} else {
					errs = append(errs,cp.Err)
					gLog.Error.Printf("Error %v uploading part %d size %d", cp.Err, cp.Cp.PartNumber, cp.Cl)
				}
				if t == n {
					gLog.Info.Printf("%d parts are uploaded to bucket %s", n, bucket)
					done = true
				}
			case <-time.After(200 * time.Millisecond):
				fmt.Printf("w")
			}
		}

		//   completed the multipart uploaded
		CompletedUpload(service, resp, completedParts)
		elapsed := time.Since(start)
		MBsec := len(buffer) / int(elapsed)
		gLog.Info.Printf("Elapsed time %v MB/sec %f3.1", elapsed, MBsec)
	}
	return errs,err
}

func CompletedUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, part []*s3.CompletedPart) {

	sort.Sort(datatype.ByPart(part))
	comp := datatype.CompleteMultipartUploadRequest{
		Service:        svc,
		Resp:           resp,
		CompletedParts: part,
	}
	if cResp, err := api.CompleteMultipartUpload(comp); err == nil {
		gLog.Info.Printf("Key: %s - Etag: %s - Bucket: %s", *cResp.Key, *cResp.ETag, *cResp.Bucket)
	} else {
		gLog.Error.Printf("%v", err)
	}
}

func UploadPart(upload datatype.UploadPartRequest) (*s3.CompletedPart, error) {

	gLog.Trace.Printf("Uploading part :%d - Size %d", upload.PartNumber, len(upload.Content))

	if retryNumber := utils.GetRetryNumber(*viper.GetViper()); retryNumber == 0 {
	 	upload.RetryNumber = 5
	} else {
		upload.RetryNumber = retryNumber
	}

	if waitTime := utils.GetWaitTime(*viper.GetViper()); waitTime == 0 {
	 	upload.WaitTime = 200
	} else {
		upload.WaitTime= waitTime
	}

	if completedPart, err := api.UploadPart(upload); err != nil {
		gLog.Error.Printf("Error: %v uploading part:%d", err, upload.PartNumber)
		gLog.Warning.Printf("Aborting multipart upload")
		abort := datatype.AbortMultipartUploadRequest{
			Service: upload.Service,
			Resp:    upload.Resp,
		}
		if err = api.AbortMultipartUpload(abort); err != nil {
			gLog.Error.Printf("Error %v", err)
		}
		return nil, err
	} else {
		return completedPart, err
	}
}


/*
	Write the document ( publication number) 's meta data
*/

func WriteDocMetadata(request *sproxyd.HttpRequest, document *documentpb.Document,replace bool) (int,int) {

	var (
		pn      = document.GetDocId()
		perrors = 0
		err	error
		resp  *http.Response
	)
	request.Path = sproxyd.TargetEnv + "/" + pn
	request.ReqHeader =  map[string]string{}
	request.ReqHeader["Content-Type"] = "application/octet-stream"
	request.ReqHeader["Usermd"] = document.GetMetadata()
	gLog.Trace.Printf("writing pn %s - Path %s ",pn,request.Path)
	if resp, err = sproxyd.PutObj(request, replace,[]byte{}); err != nil {
		gLog.Error.Printf("Error %v - Put Document object %s", err, pn)
		perrors++
	} else {

		if resp != nil  {
			defer resp.Body.Close()
			switch resp.StatusCode {
			case 200:
				gLog.Trace.Printf("Path/Key %s/%s has been written", request.Path, resp.Header["X-Scal-Ring-Key"])
			case 412:
				gLog.Warning.Printf("Path/Key %s/%s already existed", request.Path,resp.Header["X-Scal-Ring-Key"])
			default:
				gLog.Error.Printf("putObj Path/key %s/%s - resp.Status %d",request.Path, resp.Header["X-Scal-Ring-Key"],resp.StatusCode)
				perrors++
			}
			return perrors,resp.StatusCode
		}
	}
	return perrors,-1
}

/*
	write a page af a document pn ( publication number)
 */

func WriteDocPage(request sproxyd.HttpRequest, pg *documentpb.Page, replace bool) (int,int) {

	var (
		perrors = 0
		pn = pg.GetPageId()
		resp *http.Response
		err error
	)
	request.Path = sproxyd.TargetEnv + "/" + pn + "/p" + strconv.Itoa((int)(pg.PageNumber))
	request.ReqHeader =  map[string]string{}
	request.ReqHeader["Usermd"] = pg.GetMetadata()
	request.ReqHeader["Content-Type"] = "application/octet-stream" // Content type
	gLog.Trace.Printf("writing %d bytes to path  %s/%s",pg.Size,sproxyd.TargetDriver,request.Path)
	if resp, err = sproxyd.PutObj(&request,replace, pg.GetObject()); err != nil {
		gLog.Error.Printf("Error %v - Put Page object %s", err, pn)
		perrors++
	} else {
		if resp != nil   {
			defer resp.Body.Close()
			switch resp.StatusCode {
			case 200:
				gLog.Trace.Printf("Path/Key %s/%s has been written", request.Path, resp.Header["X-Scal-Ring-Key"])
			case 412:
				gLog.Warning.Printf("Path/Key %s/%s already existed", request.Path,resp.Header["X-Scal-Ring-Key"])
			default:
				gLog.Error.Printf("putObj Path/key %s/%s - resp.Status %d",request.Path, resp.Header["X-Scal-Ring-Key"],resp.Status)
				perrors++
			}
			return perrors,resp.StatusCode
		}
	}
	return perrors,-1
}

func WriteDocPdf( /*request *sproxyd.HttpRequest */  pd *documentpb.Pdf, replace bool) (int,int) {

	var (
		pn = pd.GetPdfId()
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			Path : sproxyd.TargetEnv + "/" + pn,
			ReqHeader :  map[string]string{
				"Usermd" : pd.GetMetadata(),
				"Content-Type" : "application/octet-stream",
			},
		}
		perrors = 0
		resp *http.Response
		err error
	)

	/*
	request.Path = sproxyd.TargetEnv + "/" + pn
	request.ReqHeader = map[string]string{
				"Usermd" : pd.GetMetadata(),
				"Content-Type" : "application/octet-stream",
			}
	*/


	gLog.Trace.Printf("writing %d bytes to path  %s/%s",pd.Size,sproxyd.TargetDriver,request.Path)

	if resp, err = sproxyd.PutObj(&request,replace, pd.GetPdf()); err != nil {
		gLog.Error.Printf("Error %v - Put Pdf object %s", err, pn)
		perrors++
	} else {
		if resp != nil   {
			defer resp.Body.Close()
			switch resp.StatusCode {
			case 200:
				gLog.Trace.Printf("Path/Key %s/%s has been written", request.Path, resp.Header["X-Scal-Ring-Key"])
			case 412:
				gLog.Warning.Printf("Path/Key %s/%s already existed", request.Path,resp.Header["X-Scal-Ring-Key"])
			default:
				gLog.Error.Printf("putObj Path/key %s/%s - resp.Status %d",request.Path, resp.Header["X-Scal-Ring-Key"],resp.Status)
				perrors++
			}
			return perrors,resp.StatusCode
		}
	}
	return perrors,-1
}
