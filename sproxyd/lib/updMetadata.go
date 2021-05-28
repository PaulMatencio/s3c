package sproxyd

import (
	"net/http"
)

func UpdMetadata(client *http.Client, path string, updHeader map[string]string) (*http.Response, error) {

	url := DummyHost + path
	req, _ := http.NewRequest("PUT", url, nil)
	if usermd, ok := updHeader["Usermd"]; ok {
		req.Header.Add("X-Scal-Usermd", usermd)

		req.Header.Add("x-scal-cmd", "update-usermd") // tell Scality Ring to Update only the metadata
		req.Header.Add("If-Match", "*")
		//resp, err := client.Do(req)
		if !Test {
			return DoRequest(HP, client, req, nil)
		} else {
			return DoRequestTest(HP, client, req, nil)
		}
	} else {
		// custom http response
		resp := new(http.Response)
		resp.StatusCode = 504
		resp.Status = "504 Metadata is missing"
		err := error(nil)
		return resp, err
	}

}

func Updmetadata(sproxydRequest *HttpRequest) (*http.Response, error) {

	url := DummyHost + sproxydRequest.Path
	req, _ := http.NewRequest("PUT", url, nil)
	if usermd, ok := sproxydRequest.ReqHeader["Usermd"]; ok {
		req.Header.Add("X-Scal-Usermd", usermd)
		/* update the metadata if the object exist */
		req.Header.Add("x-scal-cmd", "update-usermd") // tell Scality Ring to Update only the metadata
		req.Header.Add("If-Match", "*")
		//resp, err := client.Do(req)
		if !Test {
			return DoRequest(sproxydRequest.Hspool, sproxydRequest.Client, req, nil)
		} else {
			return DoRequestTest(sproxydRequest.Hspool, sproxydRequest.Client, req, nil)
		}
	} else {
		// custom http response
		resp := new(http.Response)
		resp.StatusCode = 504
		resp.Status = "504 Metadata is missing"
		err := error(nil)
		return resp, err
	}

}
