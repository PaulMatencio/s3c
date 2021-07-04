package lib

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	gLog "github.com/paulmatencio/s3c/gLog"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	"net/http"
	"strconv"
	"sync"
)

/*

	Check pn's returned by listObject of the meta bucket

*/
func OpByIds(method string, request datatype.ListObjRequest, maxLoop int, replace bool, check bool) {
	var (
		N          int = 0
		nextmarker string
	)
	for {
		var (
			result *s3.ListObjectsOutput
			err    error
		)
		N++ // number of loop
		if result, err = api.ListObject(request); err == nil {
			if l := len(result.Contents); l > 0 {
				var wg1 sync.WaitGroup
				for _, v := range result.Contents {
					pn := *v.Key
					wg1.Add(1)
					go func(pn string) {
						defer wg1.Done()
						if np, err, status := GetPageNumber(pn); err == nil && status == 200 {
							if np > 0 {
								_opById1(method, pn, np, replace, check)
							} else {
								gLog.Error.Printf("The number of pages is %d ", np)
							}
						} else {
							gLog.Error.Printf("Error %v getting  the number of pages  run  with  -l 4  (trace)", err)
						}
					}(pn)
				}
				wg1.Wait()
				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					gLog.Warning.Printf("Truncated %v - Next marker: %s ", *result.IsTruncated, nextmarker)
				}
			}
		} else {
			gLog.Error.Printf("%v", err)
			break
		}

		if *result.IsTruncated && (maxLoop == 0 || N <= maxLoop) {
			request.Marker = nextmarker
		} else {
			break
		}
	}

}

func _opById1(method string, pn string, np int, replace bool, check bool) int {
	var (
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		request1 = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		wg2     sync.WaitGroup
		perrors = 0
		pe      sync.Mutex
		err     error
		start   int
		p0      bool
	)
	/*
		Check document has a Clipping page
	*/
	if err, _, p0 = checkPdfP0(pn); err != nil {
		return 1
	}
	if p0 {
		start = 0
		gLog.Info.Printf("DocId %s contains a page 0", pn)
	} else {
		start = 1
	}
	/*
		if pdf {
			gLog.Info.Printf("DocId %s contains a pdf", pn)
			pdfId := pn + "/pdf"
			if err, ok := comparePdf(pdfId); err == nil {
				gLog.Info.Printf("Comparing source and restored PDF: %s - isEqual ? %v", pdfId, ok)
			} else {
				gLog.Error.Printf("Error %v when comparing PDF %s", err, pdfId)
			}
		}
	*/

	for k := start; k <= np; k++ {
		request.Path = sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
		wg2.Add(1)
		go func(request sproxyd.HttpRequest, request1 sproxyd.HttpRequest, pn string, k int) {
			defer wg2.Done()
			ringId := GetObjAndId(request, pn)
			if ringId.Err == nil {
				request1.Hspool = sproxyd.TargetHP
				request1.Path = ringId.Key
				request1.ReqHeader = map[string]string{}
				// gLog.Info.Printf("Source %s/%s - Target %s/%s - usermd %s ", request.Hspool.Hosts()[0], request.Path, request1.Hspool.Hosts()[0], request1.Path, ringId.UserMeta)
				/*  Write it    */
				if !check {
					switch method {
					case "put":
						request1.ReqHeader["Usermd"] = ringId.UserMeta
						request1.ReqHeader["Content-Type"] = "application/octet-stream" // Content type
						if resp, err := sproxyd.PutObj(&request1, replace, *ringId.Object); err != nil {
							gLog.Error.Printf("Error %v - Put id %s", err, ringId.Key)
							pe.Lock()
							perrors++
							pe.Unlock()
						} else {
							if resp != nil {
								defer resp.Body.Close()
								switch resp.StatusCode {
								case 200:
									gLog.Info.Printf("Host: %s - Ring Key %s has been written - Response status Code %d", request1.Hspool.Hosts()[0],request1.Path, resp.Header["X-Scal-Ring-Key"])
								case 412:
									gLog.Warning.Printf("Host: %s - Ring key %s already existed - Response status Code %d", request1.Hspool.Hosts()[0], request1.Path,resp.StatusCode )
								default:
									gLog.Error.Printf("Host: %s - Put Ring key %s - response status %d", request1.Hspool.Hosts()[0], request1.Path,  resp.StatusCode)
									pe.Lock()
									perrors++
									pe.Unlock()
								}
								return
							}
						}

					case "get":
						gLog.Info.Printf("Method get isnot yet implemented")

					case "delete":
						if resp, err := sproxyd.Deleteobject(&request1); err != nil {
							gLog.Error.Printf("Error %v - delete  %s", err, )
							pe.Lock()
							perrors++
							pe.Unlock()
						} else {
							if resp != nil {
								defer resp.Body.Close()
								switch resp.StatusCode {
								case 200:
									gLog.Info.Printf("Host: %s - Ring key %s has been deleted - Response status %d", request1.Hspool.Hosts()[0],request1.Path,resp.StatusCode)
								case 404:
									gLog.Warning.Printf("Host: %s - Ring Key %s does not exist - Response status %d ", request1.Hspool.Hosts()[0], request1.Path,resp.StatusCode)
								default:
									gLog.Error.Printf("Host: %s Delete Ring key %s - Response status %d", request1.Hspool.Hosts()[0], request1.Path, resp.StatusCode)
									pe.Lock()
									perrors++
									pe.Unlock()
								}
								return
							}
						}

					default:
					}
				} else {
					gLog.Info.Printf("Method %s - Source %s/%s - Target %s/%s", method, request.Hspool.Hosts()[0], request.Path, request1.Hspool.Hosts()[0], request1.Path)
				}
			} else {
				gLog.Error.Printf("%v", ringId.Err)
			}
		}(request, request1, pn, k)
	}
	// Write the document to File
	wg2.Wait()
	return perrors
}
