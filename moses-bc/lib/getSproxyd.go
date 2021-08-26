// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package lib

import (
	"errors"
	"fmt"
	base64 "github.com/paulmatencio/ring/user/base64j"
	"github.com/paulmatencio/s3c/gLog"
	moses "github.com/paulmatencio/s3c/moses/lib"
	"github.com/paulmatencio/s3c/sproxyd/lib"
	"io/ioutil"
	"net/http"
)

type RingId struct {
	Err      error
	UserMeta string
	Key      string
	Object   *[]byte
}

func GetPageNumber(key string) (int, error, int) {
	var (
		pgn            int
		err            error
		sproxydRequest = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			Path: sproxyd.Env + "/" + key,
		}
		resp *http.Response
	)

	metadata := moses.DocumentMetadata{}
	if resp, err = sproxyd.GetMetadata(&sproxydRequest); err == nil {
		defer resp.Body.Close()
		switch resp.StatusCode {
		case 200:
			encoded_usermd := resp.Header["X-Scal-Usermd"]
			if err = metadata.UsermdToStruct(string(encoded_usermd[0])); err == nil {
				if pgn, err = metadata.GetPageNumber(); err == nil {
					gLog.Info.Printf("Key %s - Number of pages %d", key, pgn)
					return pgn, err, resp.StatusCode
				} else {
					gLog.Error.Printf("key %s - Error getting the number of pages %d", key, pgn)
				}
			} else {
				gLog.Error.Println(err)
			}
		default:
			err = errors.New(fmt.Sprintf("Key %s - Status code %d", key, resp.StatusCode))
		}
		return pgn, err, resp.StatusCode
	}
	return pgn, err, -1
}

func GetDocumentMeta(key string) (*moses.DocumentMetadata, error, int) {
	var (
		err            error
		sproxydRequest = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			Path: sproxyd.Env + "/" + key,
		}
		resp *http.Response
	)

	metadata := moses.DocumentMetadata{}
	if resp, err = sproxyd.GetMetadata(&sproxydRequest); err == nil {
		defer resp.Body.Close()
		switch resp.StatusCode {
		case 200:
			encoded_usermd := resp.Header["X-Scal-Usermd"]
			if err = metadata.UsermdToStruct(string(encoded_usermd[0])); err == nil {
				return &metadata, err, resp.StatusCode
			} else {
				gLog.Error.Println(err)
			}
		default:
			err = errors.New("Check the status Code for the root cause")
		}
		return &metadata, err, resp.StatusCode
	}
	return &metadata, err, -1
}

func GetPathName(pathId string) (err error, pathName string, status int) {

	var (
		sproxydRequest = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			Path: pathId,
			ReqHeader: map[string]string{
				"X-Scal-Replica-Policy": "immutable",
			},
		}
		resp *http.Response
		usermd string
	)

	metadata := moses.Pagemeta{}
	status = -1
	if resp, err = sproxyd.GetMetadata(&sproxydRequest); err == nil {
		defer resp.Body.Close()
		status = resp.StatusCode
		switch status {
		case 200:
			if _, ok := resp.Header["X-Scal-Usermd"]; ok {
				usermd = resp.Header["X-Scal-Usermd"][0]
				if err = metadata.UsermdToStruct(string(usermd)); err == nil {
					if pathName = metadata.GetPathName(); err == nil {
						return
					} else {
						err = errors.New(fmt.Sprintf("Error getting path name for path id %s", pathId))
						return
					}
				}
			} else {
				err = errors.New(fmt.Sprintf("Path Id %s does not have user metadata ", pathId))
				return
			}
		default:
			err = errors.New(fmt.Sprintf("Error getting path name for path id %s - Status code: %s", pathId,status))
		}
		return
	}
	return
}

func GetUserMetaWithPathId(pathId string) (err error, usermd string, status int) {

	var (
		sproxydRequest = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			Path: pathId,
			ReqHeader: map[string]string{
				"X-Scal-Replica-Policy": "immutable",
			},
		}
		resp *http.Response
		md []byte
	)
	status = -1
	if resp, err = sproxyd.GetMetadata(&sproxydRequest); err == nil {
		defer resp.Body.Close()
		status = resp.StatusCode
		switch status {
		case 200:
			if _, ok := resp.Header["X-Scal-Usermd"]; ok {
				if md, err = base64.Decode64(resp.Header["X-Scal-Usermd"][0]); err != nil {
						gLog.Warning.Printf("Invalid user metadata %s", usermd)
					} else {
						usermd = string(md)
						gLog.Trace.Printf("User metadata %s", string(md))
						return
					}
			} else {
				err = errors.New(fmt.Sprintf("Path Id %s does not have user metadata ", pathId))
				return
			}
		default:
			err = errors.New(fmt.Sprintf("Error getting path name for path id %s - Status code: %s", pathId, resp.StatusCode))
		}
		return
	}
	return
}


func GetUserMeta(request sproxyd.HttpRequest, pn string) (err error, usermd string) {

	var (
		//usermd string
		resp *http.Response
		// err  error
	)
	request.Path = sproxyd.Env + "/" + pn
	if resp, err = sproxyd.GetMetadata(&request); err != nil {
		return err, usermd
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err = errors.New(fmt.Sprintf("Request document %s  status code %d", request.Path, resp.StatusCode))
		return
	}
	if _, ok := resp.Header["X-Scal-Usermd"]; ok {
		usermd = resp.Header["X-Scal-Usermd"][0]
	} else {
		err = errors.New(fmt.Sprintf("Docid %d does not have user metadata ", pn))
	}
	return
}

func GetHeader(request sproxyd.HttpRequest) (err error, usermd string, contentLength int64) {

	var (
		resp *http.Response
	)
	// request.Path = sproxyd.Env + "/" + pn
	if resp, err = sproxyd.GetMetadata(&request); err != nil {
		return err, usermd, 0
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err = errors.New(fmt.Sprintf("Request document %s  status code %d", request.Path, resp.StatusCode))
		return err, usermd, 0
	}
	if _, ok := resp.Header["X-Scal-Usermd"]; ok {
		usermd = resp.Header["X-Scal-Usermd"][0]
	} else {
		err = errors.New(fmt.Sprintf("Page %s does not have user metadata ", request.Path))
	}
	contentLength = resp.ContentLength
	return
}

func GetObject(request sproxyd.HttpRequest, pn string) (error, string, *[]byte) {

	var (
		body   []byte
		usermd string
		md     []byte
	)

	request.ReqHeader = map[string]string{
		"X-Scal-Replica-Policy": "immutable",
	}
	resp, err := sproxyd.Getobject(&request)
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == 200 {
			body, _ = ioutil.ReadAll(resp.Body)
			if body != nil {
				if _, ok := resp.Header["X-Scal-Usermd"]; ok {
					usermd = resp.Header["X-Scal-Usermd"][0]
					if md, err = base64.Decode64(usermd); err != nil {
						gLog.Warning.Printf("Invalid user metadata %s", usermd)
					} else {
						gLog.Trace.Printf("User metadata %s", string(md))
					}
				}
			} else {
				err = errors.New(fmt.Sprintf("Request url %s - Body is empty", request.Path))
			}
		} else {
			err = errors.New(fmt.Sprintf("Request url %s -Status Code %d", request.Path, resp.StatusCode))
		}
	}
	return err, usermd, &body
}

func GetObjAndId(request sproxyd.HttpRequest, pn string) RingId {
	var (
		body            []byte
		usermd, ringKey string
	)
	request.ReqHeader = map[string]string{
		"X-Scal-Replica-Policy": "immutable",
	}
	resp, err := sproxyd.Getobject(&request)
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == 200 {
			body, _ = ioutil.ReadAll(resp.Body)
			if body != nil {
				if _, ok := resp.Header["X-Scal-Usermd"]; ok {
					usermd = resp.Header["X-Scal-Usermd"][0]
					if md, err := base64.Decode64(usermd); err != nil {
						gLog.Warning.Printf("Invalid user metadata %s", usermd)
					} else {
						gLog.Trace.Printf("User metadata %s", string(md))
					}
				}
				if _, ok := resp.Header["X-Scal-Ring-Key"]; ok {
					ringKey = resp.Header["X-Scal-Ring-Key"][0]
				}

			} else {
				err = errors.New(fmt.Sprintf("Request url %s - Body is empty", request.Path))
			}
		} else {
			err = errors.New(fmt.Sprintf("Request url %s -Status Code %d", request.Path, resp.StatusCode))
		}
	}
	return RingId{
		Err:      err,
		UserMeta: usermd,
		Key:      ringKey,
		Object:   &body,
	}
}

func CheckPdfAndP0(pn string, usermd string) (pdf bool, p0 bool) {

	var docmeta = moses.DocumentMetadata{}
	pdf = false
	p0 = false
	if err := docmeta.UsermdToStruct(usermd); err != nil {
		gLog.Warning.Printf("Error %v - Document %s has invalid user metadata", pn, err)
	} else {
		if docmeta.MultiMedia.Pdf {
			pdf = true
		}
		if len(docmeta.FpClipping.CountryCode) > 0 {
			p0 = true
		}
	}
	return
}
