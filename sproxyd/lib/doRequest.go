package sproxyd

import (
	"errors"

	"bytes"
	goLog "github.com/paulmatencio/s3c/gLog"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	hostpool "github.com/bitly/go-hostpool"
)

func DoRequest(hspool hostpool.HostPool, client *http.Client, req *http.Request, object []byte) (*http.Response, error) {
	var (
		resp     *http.Response
		err      error
		r        int
		hpool    hostpool.HostPoolResponse
		waittime time.Duration
	)
	u := req.URL
	uPath := u.Path
	waittime = 50 * time.Millisecond
	// goLog.Trace.Println(req.Method, req.Header, u.Host, u.Path, len(object))
	for r = 1; r <= DoRetry; r++ {
		// hpool = HP.Get()
		hpool = hspool.Get()
		host := hpool.Host()
		u1, _ := url.Parse(host)
		req.URL.Host = u1.Host
		req.Host = u1.Host
		// req.URL.Path = u1.Path + u.Path
		req.URL.Path = u1.Path + uPath
		// goLog.Trace.Println(r, host, req.URL.Path)
		if r > 1 && req.ContentLength > 0 && len(object) > 0 {
			var body io.Reader
			body = bytes.NewBuffer(object)
			rc, ok := body.(io.ReadCloser)
			if !ok && body != nil {
				rc = ioutil.NopCloser(body)
				req.Body = rc
			}
		}
		goLog.Trace.Println(req.URL)
		start := time.Now()
		if resp, err = client.Do(req); err != nil {
			goLog.Error.Printf("Retry=%d, Url=%s Error=%s", r, req.URL, err.Error())
			if strings.Index(err.Error(), "dial tcp") >= 0 {
				hpool.Mark(err)
			} else {
				hpool.Mark(nil)
			}
		} else {
			goLog.Trace.Println(host, uPath, resp.Status, time.Since(start))
			switch resp.StatusCode {
			case 500, 503:
				hpool.Mark(errors.New(resp.Status))
				goLog.Error.Printf("Retry=%d, Url=%s Status Code=%s", r, req.URL, resp.Status)
			case 423:
				hpool.Mark(err)
				goLog.Warning.Printf("Retry=%d after %s , Url=%s Status Code=%s", r, waittime, req.URL, resp.Status)
				time.Sleep(waittime)
			case 422:
				hpool.Mark(err)
				goLog.Warning.Printf("Retry=%d, Url=%s Status Code=%s Ring Status=%s", r, req.URL, resp.Status, resp.Header.Get("X-Scal-Ring-Status"))
			default:
				hpool.Mark(err)
				goto exit
			}
		}
	}
exit:
	return resp, err
}

func DoRequestTest(hspool hostpool.HostPool, client *http.Client, req *http.Request, object []byte) (*http.Response, error) {
	var hpool hostpool.HostPoolResponse
	u := req.URL
	goLog.Trace.Println(req.Method, req.Header, u.Host, u.Path, len(object))
	// hpool = HP.Get()
	hpool = hspool.Get()
	host := hpool.Host()
	u1, _ := url.Parse(host)
	req.URL.Host = u1.Host
	req.Host = u1.Host
	req.URL.Path = u1.Path + u.Path
	goLog.Trace.Println(host, u1.Host, req.URL)
	hpool.Mark(nil)
	return nil, nil
}
