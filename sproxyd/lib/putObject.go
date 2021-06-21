// sproxyd project sproxyd.go
package sproxyd

import (
	"bytes"
	hostpool "github.com/bitly/go-hostpool"
	"net/http"
	"strconv"
)

func PutObject(hspool hostpool.HostPool, client *http.Client, path string, object []byte, putHeader map[string]string) (*http.Response, error) {

	url := DummyHost + path
	req, _ := http.NewRequest("PUT", url, bytes.NewBuffer(object))
	if usermd, ok := putHeader["Usermd"]; ok {
		req.Header.Add("X-Scal-Usermd", usermd)
	}
	if contentType, ok := putHeader["Content-Type"]; ok {
		req.Header.Add("Content-Type", contentType)
	}
	if contentLength, ok := putHeader["Content-Length"]; ok {
		req.Header.Add("Content-Length", contentLength)
	} else {
		req.Header.Add("Content-Length", strconv.Itoa(len(object)))
	}
	if policy, ok := putHeader["X-Scal-Replica-Policy"]; ok {
		req.Header.Add("X-Scal-Replica-Policy", policy)
	}
	req.Header.Add("If-None-Match", "*")
	return DoRequest(hspool, client, req, object)

}

func Putobject(sproxydRequest *HttpRequest, object []byte) (*http.Response, error) {

	url := DummyHost + sproxydRequest.Path
	req, _ := http.NewRequest("PUT", url, bytes.NewBuffer(object))
	if usermd, ok := sproxydRequest.ReqHeader["Usermd"]; ok {
		/*
			if pagemd, err := base64.Decode64(usermd); err == nil {
				fmt.Println("Update Object : url=>", url, "page meata=>", string(pagemd), " Image length ", len(object))
			}
		*/
		req.Header.Add("X-Scal-Usermd", usermd)
	}
	if contentType, ok := sproxydRequest.ReqHeader["Content-Type"]; ok {
		req.Header.Add("Content-Type", contentType)
	}
	if contentLength, ok := sproxydRequest.ReqHeader["Content-Length"]; ok {
		req.Header.Add("Content-Length", contentLength)
	} else {
		req.Header.Add("Content-Length", strconv.Itoa(len(object)))
	}
	if policy, ok := sproxydRequest.ReqHeader["X-Scal-Replica-Policy"]; ok {
		req.Header.Add("X-Scal-Replica-Policy", policy)
	}
	req.Header.Add("If-None-Match", "*")

	// Test is a global sproxyd variable
	if !Test {
		return DoRequest(sproxydRequest.Hspool, sproxydRequest.Client, req, object)
	} else {
		return DoRequestTest(sproxydRequest.Hspool, sproxydRequest.Client, req, object)
	}

}
