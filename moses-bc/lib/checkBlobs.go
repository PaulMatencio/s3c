package lib

import (
	"errors"
	"fmt"
	base64 "github.com/paulmatencio/ring/user/base64j"
	// "github.com/golang/protobuf/proto"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/sproxyd/lib"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
)

/*
	Get Blob for cloning
	Not used for the moment
*/
func CheckBlob1(pn string, np int, maxPage int) (int) {
	if np <= maxPage {
		return checkBlob1(pn, np)
	} else {
		return checkBig1(pn, np, maxPage)
	}
}

//  document with  smaller pages number than --maxPage
func checkBlob1(pn string, np int) int {
	var (
		request1 = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		wg2     sync.WaitGroup
		nerrors = 0
		me      = sync.Mutex{}
	)

	for k := 1; k <= np; k++ {
		request1.Path = sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
		wg2.Add(1)
		go func(request1 sproxyd.HttpRequest, pn string, k int) {
			defer wg2.Done()
			resp, err := sproxyd.Getobject(&request1)
			defer resp.Body.Close()
			var (
				body   []byte
				usermd string
				md     []byte
			)
			if err == nil {
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
					compareObj(pn, k, &body, usermd)
				}
				//  check the object with the restored one

			} else {
				gLog.Error.Printf("error %v getting object %s", err, pn)
				resp.Body.Close()
				me.Lock()
				nerrors += 1
				me.Unlock()
			}
			gLog.Trace.Printf("object %s - length %d %s", request1.Path, len(body), string(md))

		}(request1, pn, k)
	}
	// Write the document to File
	wg2.Wait()
	return nerrors
}

//  document with bigger  pages number than maxPage

func checkBig1(pn string, np int, maxPage int) (int) {
	var (
		q        int = (np + 1) / maxPage
		r        int = (np + 1) / maxPage
		start    int = 0
		end      int = start + maxPage
		nerrors  int = 0
		terrors  int = 0
	)
	gLog.Warning.Printf("Big document %s  - number of pages %d ", pn, np)
	for s := 1; s <= q; s++ {
		nerrors = checkPart1(pn, np, start, end)
		start = end + 1
		end += maxPage
		if end > np {
			end = np
		}
		terrors += nerrors
	}
	if r > 0 {
		nerrors = checkPart1(pn, np, q*maxPage+1, np)
		terrors += nerrors
	}
	return terrors
	// return WriteDocument(pn, document, outdir)
}

func checkPart1(pn string, np int, start int, end int) int {

	var (
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.HP, // IP of source sproxyd
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}

		nerrors int = 0
		wg2     sync.WaitGroup
	)

	// document := &documentpb.Document{}
	gLog.Info.Printf("Getpart of pn %s - start-page %d - end-page %d ", pn, start, end)

	for k := start; k <= end; k++ {
		wg2.Add(1)
		if k > 0 {
			request.Path = sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
		} else {
			request.Path = sproxyd.Env + "/" + pn
		}
		go func(request1 sproxyd.HttpRequest, pn string, np int, k int) {
			gLog.Trace.Printf("Getpart of pn: %s - url:%s", pn, request1.Path)
			defer wg2.Done()
			if err, usermd, body := GetObject(request1, pn); err != nil {
				compareObj(pn, k, body, usermd)
			}
		}(request, pn, np, k)
	}
	// Write the document to File
	wg2.Wait()
	return nerrors
}

func compareObj(pn string, pagen int, body *[]byte, usermd string) (error, bool) {
	var (
		request = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		err error
		body1 *[]byte
		usermd1 string
	)
	request.Path = sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(pagen)
	if err, usermd1, body1 = GetObject(request, pn); err == nil {
		/*  vheck */
		if usermd1 == usermd  && len(*body1) == len(*body) {
			return err, true
		} else {
			err= errors.New(fmt.Sprintf("usermd1=%s usermd=%% / length body1= %d length body = %d ",len(usermd1),len(usermd),len(*body1),len(*body)))
			return err, false
		}
	} else {
		gLog.Error.Printf("Error %v Getting  %s",err,request.Path)
	}
	return err, false
}