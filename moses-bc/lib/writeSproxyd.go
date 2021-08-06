
package lib

import (
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"github.com/paulmatencio/s3c/gLog"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	"strconv"

	"net/http"
)




/*
	Write the document ( publication number) 's meta data
*/

func WriteDocMetadata(request *sproxyd.HttpRequest, document *documentpb.Document, replace bool) (int, int) {

	var (
		pn      = document.GetDocId()
		perrors = 0
		err     error
		resp    *http.Response
	)

	if sproxyd.TargetDriver[0:2] == "bp" {
		request.Path = sproxyd.TargetEnv + "/" + pn
	} else {
		request.Path = pn /*   pn is object key   */
	}

	request.ReqHeader = map[string]string{}
	request.ReqHeader["Content-Type"] = "application/octet-stream"
	request.ReqHeader["Usermd"] = document.GetMetadata()
	gLog.Trace.Printf("writing pn %s - Path %s ", pn, request.Path)
	if resp, err = sproxyd.PutObj(request, replace, []byte{}); err != nil {
		gLog.Error.Printf("Error %v - Put Document object %s", err, pn)
		perrors++
	} else {
		if resp != nil {
			defer resp.Body.Close()
			switch resp.StatusCode {
			case 200:
				gLog.Trace.Printf("Path/Key %s/%s has been written - http status code %d", request.Path, resp.Header["X-Scal-Ring-Key"], resp.StatusCode)
			case 412:
				gLog.Warning.Printf("Path/Key %s/%s already existed - http status code %d", request.Path, resp.Header["X-Scal-Ring-Key"], resp.StatusCode)
			default:
				gLog.Error.Printf("putObj Path/key %s/%s - http Status code %d", request.Path, resp.Header["X-Scal-Ring-Key"], resp.StatusCode)
				perrors++
			}
			return perrors, resp.StatusCode
		}
	}
	return perrors, -1
}

func DeleteDocMetadata(request *sproxyd.HttpRequest, document *documentpb.Document) (int, int) {

	var (
		pn      = document.GetDocId()
		perrors = 0
	)

	if sproxyd.TargetDriver[0:2] == "bp" {
		request.Path = sproxyd.TargetEnv + "/" + pn
	} else {
		request.Path = pn /*   pn is the Ring key   */
	}

	gLog.Trace.Printf("deleting pn %s - Path %s ", pn, request.Path)
	if resp, err := sproxyd.Deleteobject(request); err != nil {
		gLog.Error.Printf("Error %v - deleting  %s", err)
		perrors++
	} else {
		if resp != nil {
			defer resp.Body.Close()
			switch resp.StatusCode {
			case 200:
				gLog.Info.Printf("Host: %s - Ring key/path %s has been deleted - Response status %d", request.Hspool.Hosts()[0], request.Path, resp.StatusCode)
			case 404:
				gLog.Warning.Printf("Host: %s - Ring Key/path %s does not exist - Response status %d ", request.Hspool.Hosts()[0], request.Path, resp.StatusCode)
			default:
				gLog.Error.Printf("Host: %s Delete Ring key/path %s - Response status %d", request.Hspool.Hosts()[0], request.Path, resp.StatusCode)
				perrors++
			}
			return perrors, resp.StatusCode
		}
	}
	return perrors, -1
}

/*
	write a page af a document pn ( publication number)
*/

func WriteDocPage(request sproxyd.HttpRequest, pg *documentpb.Page, replace bool) (int, int) {

	var (
		perrors = 0
		pn      = pg.GetPageId()
		resp    *http.Response
		err     error
	)
	request.Path = sproxyd.TargetEnv + "/" + pn + "/p" + strconv.Itoa((int)(pg.PageNumber))
	request.ReqHeader = map[string]string{}
	request.ReqHeader["Usermd"] = pg.GetMetadata()
	request.ReqHeader["Content-Type"] = "application/octet-stream" // Content type
	gLog.Trace.Printf("writing %d bytes to path  %s/%s", pg.Size, sproxyd.TargetDriver, request.Path)
	if resp, err = sproxyd.PutObj(&request, replace, pg.GetObject()); err != nil {
		gLog.Error.Printf("Error %v - Put Page object %s", err, pn)
		perrors++
	} else {
		if resp != nil {
			defer resp.Body.Close()
			switch resp.StatusCode {
			case 200:
				gLog.Trace.Printf("Path/Key %s/%s has been written - http status code %d", request.Path, resp.Header["X-Scal-Ring-Key"], resp.StatusCode)
			case 412:
				gLog.Warning.Printf("Path/Key %s/%s already existed - http status code %d", request.Path, resp.Header["X-Scal-Ring-Key"], resp.StatusCode)
			default:
				gLog.Error.Printf("putObj Path/key %s/%s - http Status code %d", request.Path, resp.Header["X-Scal-Ring-Key"], resp.StatusCode)
				perrors++
			}
			return perrors, resp.StatusCode
		}
	}
	return perrors, -1
}

func WriteDocPdf( /*request *sproxyd.HttpRequest */ pd *documentpb.Pdf, replace bool) (int, int) {

	var (
		pn      = pd.GetPdfId()
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			Path: sproxyd.TargetEnv + "/" + pn,
			ReqHeader: map[string]string{
				"Usermd":       pd.GetMetadata(),
				"Content-Type": "application/octet-stream",
			},
		}
		perrors = 0
		resp    *http.Response
		err     error
	)

	/*
		request.Path = sproxyd.TargetEnv + "/" + pn
		request.ReqHeader = map[string]string{
					"Usermd" : pd.GetMetadata(),
					"Content-Type" : "application/octet-stream",
				}
	*/

	gLog.Trace.Printf("writing %d bytes to path  %s/%s", pd.Size, sproxyd.TargetDriver, request.Path)

	if resp, err = sproxyd.PutObj(&request, replace, pd.GetPdf()); err != nil {
		gLog.Error.Printf("Error %v - Put Pdf object %s", err, pn)
		perrors++
	} else {
		if resp != nil {
			defer resp.Body.Close()
			switch resp.StatusCode {
			case 200:
				gLog.Trace.Printf("Path/Key %s/%s has been written - http status code %d", request.Path, resp.Header["X-Scal-Ring-Key"], resp.StatusCode)
			case 412:
				gLog.Warning.Printf("Path/Key %s/%s already existed - http status code %d", request.Path, resp.Header["X-Scal-Ring-Key"], resp.StatusCode)
			default:
				gLog.Error.Printf("putObj Path/key %s/%s - http Status code %d", request.Path, resp.Header["X-Scal-Ring-Key"], resp.StatusCode)
				perrors++
			}
			return perrors, resp.StatusCode
		}
	}
	return perrors, -1
}

