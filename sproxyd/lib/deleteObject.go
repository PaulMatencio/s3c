package sproxyd

import (
	"net/http"

	// hostpool "github.com/bitly/go-hostpool"
)

/*
func DeleteObject(hspool hostpool.HostPool, client *http.Client, path string) (*http.Response, error) {

	url := DummyHost + path
	req, _ := http.NewRequest("DELETE", url, nil)
	// resp, err := client.Do(req)
	return DoRequest(hspool, client, req, nil)
}


func DeleteObjectTest(hspool hostpool.HostPool, client *http.Client, path string) (*http.Response, error) {

	url := DummyHost + path
	req, _ := http.NewRequest("DELETE", url, nil)
	// resp, err := client.Do(req)
	return DoRequestTest(hspool, client, req, nil)
}
*/

func Deleteobject(sproxyRequest *HttpRequest) (*http.Response, error) {

	url := DummyHost + sproxyRequest.Path
	req, _ := http.NewRequest("DELETE", url, nil)
	/* Test is a sproxyd global variable */
	if !Test {
		return DoRequest(sproxyRequest.Hspool, sproxyRequest.Client, req, nil)
	} else {
		return DoRequestTest(sproxyRequest.Hspool, sproxyRequest.Client, req, nil)
	}
}
