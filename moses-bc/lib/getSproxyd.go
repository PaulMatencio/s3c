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
					gLog.Info.Printf("Key %s  - Number of pages %d", key, pgn)
					return pgn, err, resp.StatusCode
				} else {
					gLog.Error.Printf("Error getting page number %d", pgn)
				}
			} else {
				gLog.Error.Println(err)
			}
		default:
			err = errors.New("Check the status Code for the root cause")
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

func GetPathName(pathId string) (error, string, int) {
	var (
		pathName       string
		err            error
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
	)

	metadata := moses.Pagemeta{}
	if resp, err = sproxyd.GetMetadata(&sproxydRequest); err == nil {
		defer resp.Body.Close()
		switch resp.StatusCode {
		case 200:
			encoded_usermd := resp.Header["X-Scal-Usermd"]
			if err = metadata.UsermdToStruct(string(encoded_usermd[0])); err == nil {
				if pathName = metadata.GetPathName(); err == nil {
					// gLog.Info.Printf("PathId %s - Pathname %s",pathId,pathName)
					return err, pathName, resp.StatusCode
				} else {
					err = errors.New(fmt.Sprintf("Error getting path name for path id %s", pathId))
				}
			}
		default:
			err = errors.New(fmt.Sprintf("Error getting path name for path id %s - Status code: %s", pathId, resp.StatusCode))
		}
		return err, pathName, resp.StatusCode
	}
	return err, pathName, -1
}

func GetUserMeta(request sproxyd.HttpRequest, pn string) (error, string) {

	var (
		usermd string
		resp *http.Response
		err  error
	)
	request.Path = sproxyd.Env + "/" + pn
	if resp, err = sproxyd.GetMetadata(&request); err != nil {
		return err, usermd
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err = errors.New(fmt.Sprintf("Request document %s  status code %d", request.Path, resp.StatusCode))
		return err, usermd
	}
	if _, ok := resp.Header["X-Scal-Usermd"]; ok {
		usermd = resp.Header["X-Scal-Usermd"][0]
	} else {
		err = errors.New(fmt.Sprintf("Docid %d does not have user metadata ", pn))
	}
	return err, usermd
}

func GetHeader(request sproxyd.HttpRequest) (error, string, int64) {

	var (
		usermd string
		// md     []byte
		resp *http.Response
		err  error
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
	return err, usermd, resp.ContentLength
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

func CheckPdfAndP0(pn string, usermd string) (bool, bool) {

	var (
		docmeta      = moses.DocumentMetadata{}
		pdf     bool = false
		p0      bool = false
	)
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
	return pdf, p0
}


