package bns

//  Get full object
//  Get range of byte ( subpart of an object)

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	// goLog "moses/user/goLog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func GetPageType(bnsRequest *HttpRequest, url string) (*http.Response, error) {
	/*
		bnsRequest structure
			Hspool hostpool.HostPool
			Urls   []string
			Client *http.Client
			Media  string
	*/
	var (
		usermd []byte
		err    error
		resp   *http.Response
	)
	// fmt.Printf("\nMeta fetching %s", url)
	usermd, err, _ = GetMetadata(bnsRequest, url)
	if err != nil {
		return nil, errors.New("Page metadata is missing or invalid")
	} else {
		// c, _ := base64.Decode64(string(usermd))
		//goLog.Trace.Println("Usermd=", string(usermd))
		if len(usermd) == 0 {
			return nil, errors.New("Page metadata is missing. Please check the warning log for the reason")
		}
		var pagemeta Pagemeta
		if err := json.Unmarshal(usermd, &pagemeta); err != nil {
			return nil, err
		}
		// create a sproxyd request structure
		sproxydRequest := &sproxyd.HttpRequest{
			Path:      url,
			Hspool:    bnsRequest.Hspool,
			Client:    bnsRequest.Client,
			ReqHeader: map[string]string{},
		}

		sproxydRequest.ReqHeader["Content-Type"] = "image/" + strings.ToLower(bnsRequest.Media)

		if contentType, ok := sproxydRequest.ReqHeader["Content-Type"]; ok {

			switch contentType {
			case "image/tiff", "image/tif":
				start := strconv.Itoa(pagemeta.TiffOffset.Start)
				end := strconv.Itoa(pagemeta.TiffOffset.End)
				sproxydRequest.ReqHeader["Range"] = "bytes=" + start + "-" + end
				//goLog.Trace.Println(sproxydRequest.Path,sproxydRequest.ReqHeader)
				resp, err = sproxyd.Getobject(sproxydRequest)
				// goLog.Trace.Println("Object fetched ", resp.Request.URL.Path, resp.StatusCode, err)
			case "image/png":
				start := strconv.Itoa(pagemeta.PngOffset.Start)
				end := strconv.Itoa(pagemeta.PngOffset.End)
				sproxydRequest.ReqHeader["Range"] = "bytes=" + start + "-" + end
				//goLog.Trace.Println(sproxydRequest.Path, sproxydRequest.ReqHeader)
				resp, err = sproxyd.Getobject(sproxydRequest)
				//goLog.Trace.Println(resp.Request.URL.Path, resp.StatusCode, err)
			case "image/pdf":
				if pagemeta.PdfOffset.Start > 0 {
					start := strconv.Itoa(pagemeta.PdfOffset.Start)
					end := strconv.Itoa(pagemeta.PdfOffset.End)
					sproxydRequest.ReqHeader["Range"] = "bytes=" + start + "-" + end
					// goLog.Trace.Println(sproxydRequest.ReqHeader)
					resp, err = sproxyd.Getobject(sproxydRequest)
				} else {
					resp = nil
					err = errors.New("Content-type " + contentType + " does not exist")
				}
			default:
				err = errors.New("Content-type is missing or invalid")
			}
		} else {
			err = errors.New("Content-type is missing or invalid")
		}
	}
	return resp, err
}

func AsyncHttpGetpageType(bnsRequest *HttpRequest) []*sproxyd.HttpResponse {

	ch := make(chan *sproxyd.HttpResponse)
	sproxydResponses := []*sproxyd.HttpResponse{}
	treq := 0
	bnsRequest.Client = &http.Client{
		Timeout:   sproxyd.ReadTimeout,
		Transport: sproxyd.Transport,
	}

	for _, url := range bnsRequest.Urls {
		/* just in case, the requested page number is beyond the max number of pages */
		if len(url) == 0 {
			break
		} else {
			treq += 1
		}

		go func(url string) {

			resp, err := GetPageType(bnsRequest, url)

			var body []byte
			if err == nil {
				body, _ = ioutil.ReadAll(resp.Body)
				defer resp.Body.Close()
			}
			ch <- &sproxyd.HttpResponse{url, resp, &body, err}
		}(url)
	}
	// wait for http response  message
	for {
		select {
		case r := <-ch:
			// fmt.Printf("\n%s was fetched %d\n", r.Url, len(sproxydResponses))
			sproxydResponses = append(sproxydResponses, r)
			if len(sproxydResponses) == treq {
				return sproxydResponses
			}
		case <-time.After(100 * time.Millisecond):
			fmt.Printf("r")
		}
	}

	return sproxydResponses
}
