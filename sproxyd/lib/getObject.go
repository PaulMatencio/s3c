package sproxyd

import (
	"net/http"
)

func Getobject(sproxydRequest *HttpRequest) (*http.Response, error) {

	// net.Dialer.Timeout = 100 * time.Millisecond
	req, _ := http.NewRequest("GET", DummyHost+sproxydRequest.Path, nil)
	if Range, ok := sproxydRequest.ReqHeader["Range"]; ok {
		req.Header.Add("Range", Range)
	}
	if ifmod, ok := sproxydRequest.ReqHeader["If-Modified-Since"]; ok {
		req.Header.Add("If-Modified-Since", ifmod)
	}
	if ifunmod, ok := sproxydRequest.ReqHeader["If-Unmodified-Since"]; ok {
		req.Header.Add("If-Unmodified-Since", ifunmod)
	}
	if replica,ok := sproxydRequest.ReqHeader["X-Scal-Replica-Policy"];ok {
		req.Header.Add("X-Scal-Replica-Policy",replica)
	}
	return DoRequest(sproxydRequest.Hspool, sproxydRequest.Client, req, nil)
}
