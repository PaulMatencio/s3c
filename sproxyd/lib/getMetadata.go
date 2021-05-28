package sproxyd

import (
	"net/http"

	// hostpool "github.com/bitly/go-hostpool"
)

func GetMetadata(sproxydRequest *HttpRequest) (*http.Response, error) {

	// url := DummyHost + sproxydRequest.Path
	req, _ := http.NewRequest("HEAD", DummyHost+sproxydRequest.Path, nil)

	if ifmod, ok := sproxydRequest.ReqHeader["If-Modified-Since"]; ok {
		req.Header.Add("If-Modified-Since", ifmod)
	}
	if ifunmod, ok := sproxydRequest.ReqHeader["If-Unmodified-Since"]; ok {
		req.Header.Add("If-Unmodified-Since", ifunmod)
	}

	return DoRequest(sproxydRequest.Hspool, sproxydRequest.Client, req, nil)
}
