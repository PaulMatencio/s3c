package lib

import (
	"errors"
	"fmt"
	gLog "github.com/paulmatencio/s3c/gLog"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"
)

func CatchUpBlobs(pn string, np int, maxPage int, repair bool) (ret Ret) {

	var (
		start = time.Now()
	)
	gLog.Trace.Printf("Document %s - Number of pages %d  - max page %d", pn, np, maxPage)
	if np <= maxPage {
		ret = catchUpPages(pn, np, repair)
		gLog.Trace.Printf("Catchup docid %s - number of pages %d - number of 404's %d  - Elapsed time %v", pn, np, ret.N404s, time.Since(start))
	} else {
		ret = catchUpMaxPages(pn, np, maxPage, repair)
		gLog.Trace.Printf("Catchup docid %s - number of pages %d - number of 404's %d - Elapsed time %v", pn, np, ret.N404s, time.Since(start))
	}
	return
}

/*
	CatchUp Pages ( number of pages < maxpages)
*/
func catchUpPages(pn string, np int, repair bool) (ret Ret) {
	var (
		request1 = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			ReqHeader: map[string]string{
				"X-Scal-Replica-Policy": "immutable",
			},
		}
		request2 = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			ReqHeader: map[string]string{
				"X-Scal-Replica-Policy": "immutable",
			},
		}
		request3 = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
		wg2            sync.WaitGroup
		nerrors, n404s = 0, 0
		start          int
		err            error
		pdf, p0        bool = false, false
		le, l4         sync.Mutex
	)

	err, status, docmd := GetDocMetaStatus(request1, sproxyd.TargetEnv, pn)
	if err == nil {
		if status == 200 {
			pdf, p0 = CheckPdfAndP0(pn, docmd)
		} else {
			gLog.Warning.Printf("Target docid %s - status code %d ", sproxyd.TargetEnv+"/"+pn, status)
			request2.Path = sproxyd.Env + "/" + pn
			resp2, err := sproxyd.Getobject(&request2)
			if err == nil {
				defer resp2.Body.Close()
				if resp2.StatusCode == 200 {
					if _, ok := resp2.Header["X-Scal-Usermd"]; ok {
						docmd = resp2.Header["X-Scal-Usermd"][0]
						pdf, p0 = CheckPdfAndP0(pn, docmd)
						if repair {
							request3.Path = sproxyd.TargetEnv + "/" + pn
							repairIt(resp2, &request3, false)
						} else {
							// gLog.Info.Printf("Target docid %s is repairable", sproxyd.TargetEnv+"/"+pn)
							isSync := "0"
							if _, ok := resp2.Header["X-Scal-Attr-Is-Sync"]; ok {
								isSync = resp2.Header["X-Scal-Attr-Is-Sync"][0]
							}
							// gLog.Info.Printf("Target docid %s is missing, is synced? %s but is recoverable", sproxyd.TargetEnv+"/"+pn, isSync)
							ct := resp2.Header["Content-Type"][0]
							sz := resp2.ContentLength
							gLog.Info.Printf("Target docid %s is missing, is synced? %s but is recoverable - content-type %s - Content-length %d ", sproxyd.TargetEnv+"/"+pn , isSync, ct, sz)
						}
					}
				} else {
					gLog.Warning.Printf("Source docid %s - status code %d ", sproxyd.Env+"/"+pn, status)
				}
			} else {
				gLog.Error.Printf("Source docid %s -  error %v", sproxyd.Env+"/"+pn, err)
			}
		}
	}

	if p0 {
		start = 0
		gLog.Trace.Printf("DocId %s contains a page 0", pn)
	} else {
		start = 1
	}

	if pdf {
		gLog.Trace.Printf("DocId %s contains a pdf", pn)
		request1.Path = sproxyd.TargetEnv + "/" + pn + "/pdf"
		if resp, err := sproxyd.GetMetadata(&request1); err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == 404 {
				ret.N404s += 1
				gLog.Warning.Printf("Target Pdf %s - status code %d ", request1.Path, resp.StatusCode)
				/*
					Check if 404 at source
				*/
				request2.Path = sproxyd.Env + "/" + pn + "/pdf"
				if resp2, err := sproxyd.Getobject(&request2); err == nil {
					defer resp2.Body.Close()
					if resp2.StatusCode == 404 {
						ret.N404s -= 1
						gLog.Warning.Printf("Source Pdf %s - status code %d ", request2.Path, resp2.StatusCode)
					} else {
						if repair {
							request3.Path = request1.Path
							repairIt(resp2, &request3, false)
						} else {
							// gLog.Info.Printf("Target Pdf %s is repairable", request1.Path)
							isSync := "0"
							if _, ok := resp2.Header["X-Scal-Attr-Is-Sync"]; ok {
								isSync = resp2.Header["X-Scal-Attr-Is-Sync"][0]
							}
							// gLog.Info.Printf("Target Pdf %s is missing, is synced? %s but is recoverable", request1.Path, isSync)
							ct := resp2.Header["Content-Type"][0]
							sz := resp2.ContentLength
							gLog.Info.Printf("Target Pdf %s is missing, is synced? %s but is recoverable - content-type %s - Content-length %d ", request1.Path, isSync, ct, sz)
						}
					}
				} else {
					gLog.Error.Printf("Target Pdf %s -  error %v", request1.Path, err)
					ret.Nerrs += 1
				}
			} else {
				gLog.Trace.Printf("Target Pdf %s - status code %d ", request1.Path, resp.StatusCode)
			}
		} else {
			gLog.Error.Printf("Target Pdf %s -  error %v", request1.Path, err)
			ret.Nerrs += 1
		}
	}

	gLog.Trace.Printf("Document %s has %n pages", pn, np)
	for k := start; k <= np; k++ {
		request1.Path = sproxyd.TargetEnv + "/" + pn + "/p" + strconv.Itoa(k)
		wg2.Add(1)
		go func(request1 sproxyd.HttpRequest, pn string, k int) {
			defer wg2.Done()
			if resp, err := sproxyd.GetMetadata(&request1); err == nil {
				defer resp.Body.Close()
				/*
					check if the  target Page is missing
					if not continue
				*/

				if resp.StatusCode == 404 {
					l4.Lock()
					ret.N404s += 1
					l4.Unlock()
					gLog.Warning.Printf("Target Page %s - status code %d ", request1.Path, resp.StatusCode)
					/*
							Prepare to retrieve the source page
						    if it is missing (404) then continue
						    if it exists (200)   then repair it if requested
					*/
					request2.Path = sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
					if resp2, err := sproxyd.Getobject(&request2); err == nil {
						defer resp2.Body.Close()
						// check if the source page is also missing ?
						if resp2.StatusCode == 404 {
							l4.Lock()
							ret.N404s -= 1
							l4.Unlock()
							gLog.Warning.Printf("Source Page %s - status code %d ", request2.Path, resp2.StatusCode)

						} else if resp2.StatusCode == 200 {
							if repair {
								request3.Path = request1.Path
								repairIt(resp2, &request3, false)
							} else {
								// gLog.Info.Printf("Target Page %s is repairable", request1.Path)
								isSync := "0"
								if _, ok := resp2.Header["X-Scal-Attr-Is-Sync"]; ok {
									isSync = resp2.Header["X-Scal-Attr-Is-Sync"][0]
								}
								// gLog.Info.Printf("Target Page %s is missing, is synced? %s but is recoverable", request1.Path, isSync)
								ct := resp2.Header["Content-Type"][0]
								sz := resp2.ContentLength
								gLog.Info.Printf("Target Page %s is missing, is synced? %s but is recoverable - content-type %s - Content-length %d ", request1.Path, isSync, ct, sz)
							}
						}
					}
				} else {
					gLog.Trace.Printf("Target Page %s - status code %d ", request1.Path, resp.StatusCode)
				}

			} else {
				gLog.Error.Printf("Target Page %s - error %v", request1.Path, err)
				le.Lock()
				ret.Nerrs += 1
				le.Unlock()
			}
		}(request1, pn, k)
	}

	wg2.Wait()
	ret.Nerrs += nerrors
	ret.N404s += n404s
	ret.Npages += np
	ret.Ndocs += 1
	return ret
}

/*
	Check pages ( numberof pages > maxPages
*/
func catchUpMaxPages(pn string, np int, maxPage int, repair bool) (ret Ret) {

	var (
		q, r, start, end int
		err              error
		pdf, p0          bool
		request1         = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			ReqHeader: map[string]string{
				"X-Scal-Replica-Policy": "immutable",
			},
		}
		request2 = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			ReqHeader: map[string]string{
				"X-Scal-Replica-Policy": "immutable",
			},
		}
		request3 = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
	)

	err, status, docmd := GetDocMetaStatus(request1, sproxyd.TargetEnv, pn)
	if err == nil {
		if status == 200 {
			pdf, p0 = CheckPdfAndP0(pn, docmd)
		} else {
			gLog.Warning.Printf("Target docid %s - status code %d ", sproxyd.TargetEnv+"/"+pn, status)
			request2.Path = sproxyd.Env + "/" + pn
			resp2, err := sproxyd.Getobject(&request2)
			if err == nil {
				defer resp2.Body.Close()
				if resp2.StatusCode == 200 {
					/*  */
					if _, ok := resp2.Header["X-Scal-Usermd"]; ok {
						docmd = resp2.Header["X-Scal-Usermd"][0]
						pdf, p0 = CheckPdfAndP0(pn, docmd)
						if repair {
							request3.Path = sproxyd.TargetEnv + "/" + pn
							repairIt(resp2, &request3, false)
						} else {
							isSync := "0"
							if _, ok := resp2.Header["X-Scal-Attr-Is-Sync"]; ok {
								isSync = resp2.Header["X-Scal-Attr-Is-Sync"][0]
							}
							// gLog.Info.Printf("Target docid %s is missing, is synced? %s but is recoverable", sproxyd.TargetEnv+"/"+pn, isSync)
							ct := resp2.Header["Content-Type"][0]
							sz := resp2.ContentLength
							gLog.Info.Printf("Target docid %s is missing, is synced? %s but is recoverable - content-type %s - Content-length %d ",  sproxyd.TargetEnv+"/"+pn, isSync, ct, sz)
						}
					}
				} else {
					gLog.Warning.Printf("Source docid %s - status code %d ", sproxyd.Env+"/"+pn, status)
				}
			} else {
				gLog.Error.Printf("Source docid %s -  error %v", sproxyd.Env+"/"+pn, err)
			}
		}
	}

	if p0 {
		start = 0
		gLog.Info.Printf("DocId %s contains a page 0", pn)
	} else {
		start = 1
	}

	if pdf {
		gLog.Trace.Printf("DocId %s contains a pdf", pn)
		request1.Path = sproxyd.TargetEnv + "/" + pn + "/pdf"
		if resp, err := sproxyd.GetMetadata(&request1); err == nil {
			defer resp.Body.Close()
			/*
				check if target page is missing (404)
				if not then just continue
			*/
			if resp.StatusCode == 404 {
				ret.N404s += 1
				gLog.Warning.Printf("Target Pdf %s - status code %d ", request1.Path, resp.StatusCode)

				/*
						Prepare to retrieve the source pdf
					    if it is missing (404) then continue
					    if it exists (200)   then repair it if requested
				*/

				request2.Path = sproxyd.TargetEnv + "/" + pn + "/pdf"
				if resp2, err := sproxyd.GetMetadata(&request2); err == nil {
					defer resp2.Body.Close()
					if resp2.StatusCode == 404 {
						ret.N404s -= 1
						gLog.Warning.Printf("Source Pdf %s - status code %d ", request2.Path, resp2.StatusCode)
					} else {
						if repair {
							request3.Path = request1.Path
							repairIt(resp2, &request3, false)
						} else {
							// gLog.Info.Printf("Target Pdf %s  is repairable", request1.Path)
							isSync := "0"
							if _, ok := resp2.Header["X-Scal-Attr-Is-Sync"]; ok {
								isSync = resp2.Header["X-Scal-Attr-Is-Sync"][0]
							}
							// gLog.Info.Printf("Target Pdf %s is missing, is synced? %s but is recoverable", request1.Path, isSync)
							ct := resp2.Header["Content-Type"][0]
							sz := resp2.ContentLength
							gLog.Info.Printf("Target Pdf %s is missing, is synced? %s but is recoverable - content-type %s - Content-length %d ", request1.Path, isSync, ct, sz)
						}
					}
				} else {
					gLog.Error.Printf("Source Pdf %s -  error %v", request1.Path, err)
				}
			} else {
				gLog.Trace.Printf("Target Pdf %s - status code %d ", request1.Path, resp.StatusCode)
			}
		} else {
			gLog.Error.Printf("Target Pdf %s -  error %v", request1.Path, err)
			ret.Nerrs += 1
		}
	}

	end = maxPage
	q = np / maxPage
	r = np % maxPage

	gLog.Trace.Printf("Big document %s  - number of pages %d ", pn, np)

	for s := 1; s <= q; s++ {
		ret1 := catchUpPagePart(&request1, pn, np, start, end, repair)
		start = end + 1
		end += maxPage
		if end > np {
			end = np
		}

		ret.Nerrs += ret1.Nerrs
		ret.N404s += ret1.N404s

	}

	if r > 0 {
		ret1 := catchUpPagePart(&request1, pn, np, q*maxPage+1, np, repair)
		ret.Nerrs += ret1.Nerrs
		ret.N404s += ret1.N404s
	}

	ret.Npages = np
	ret.Ndocs += 1
	return ret
	// return WriteDocument(pn, document, outdir)

}

func catchUpPagePart(request1 *sproxyd.HttpRequest, pn string, np int, start int, end int, repair bool) (ret Ret) {

	var (
		nerrors  int = 0
		wg2      sync.WaitGroup
		le, l4   sync.Mutex
		n404s    int
		request2 = sproxyd.HttpRequest{
			Hspool: sproxyd.HP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
			ReqHeader: map[string]string{
				"X-Scal-Replica-Policy": "immutable",
			},
		}
		request3 = sproxyd.HttpRequest{
			Hspool: sproxyd.TargetHP,
			Client: &http.Client{
				Timeout:   sproxyd.ReadTimeout,
				Transport: sproxyd.Transport,
			},
		}
	)

	gLog.Trace.Printf("Getpart of pn %s - start-page %d - end-page %d ", pn, start, end)

	for k := start; k <= end; k++ {
		wg2.Add(1)
		request1.Path = sproxyd.TargetEnv + "/" + pn + "/p" + strconv.Itoa(k)
		go func(request1 sproxyd.HttpRequest, pn string, np int, k int) {
			gLog.Trace.Printf("Getpart of pn: %s - url:%s", pn, request1.Path)
			defer wg2.Done()
			if resp, err := sproxyd.GetMetadata(&request1); err == nil {
				defer resp.Body.Close()
				/*
						check if the target page is missing
					if not then just continue
				*/
				if resp.StatusCode == 404 {
					l4.Lock()
					n404s = 1
					l4.Unlock()
					gLog.Warning.Printf("Target Page %s - status code %d ", request1.Path, resp.StatusCode)
					/*
							Prepare to retrieve the source page
						    if it is missing (404) then continue
						    if it exists (200)   then repair it if requested
					*/
					request2.Path = sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
					if resp2, err := sproxyd.GetMetadata(&request2); err == nil {
						defer resp2.Body.Close()
						if resp2.StatusCode == 404 {
							ret.N404s -= 1
							gLog.Warning.Printf("Source Page %s - status code %d ", request2.Path, resp2.StatusCode)
						} else {
							if repair {
								request3.Path = request1.Path
								repairIt(resp2, &request3, false)
							} else {
								// gLog.Info.Printf("Target Page %s  is repairable", request1.Path)
								isSync := "0"
								if _, ok := resp2.Header["X-Scal-Attr-Is-Sync"]; ok {
									isSync = resp2.Header["X-Scal-Attr-Is-Sync"][0]
								}
								ct := resp2.Header["Content-Type"][0]
								sz := resp2.ContentLength
								gLog.Info.Printf("Target Page %s is missing, is synced? %s but is recoverable - content-type %s - Content-length %d ", request1.Path, isSync, ct, sz)
							}
						}
					}
				} else {
					gLog.Trace.Printf("Target Page %s - status code %d ", request1.Path, resp.StatusCode)
				}
			} else {
				gLog.Error.Printf("Target page %s -  error %v", request1.Path, err)
				le.Lock()
				nerrors += 1
				le.Unlock()

			}
		}(*request1, pn, np, k)

	}
	// Write the document to File
	wg2.Wait()
	ret.Ndocs = nerrors
	ret.N404s = n404s

	return ret
}

func putObj(request *sproxyd.HttpRequest, pn string, usermd string, object *[]byte) (err error, resp *http.Response) {
	request.ReqHeader = map[string]string{}
	request.ReqHeader["Content-Type"] = "application/octet-stream"
	request.ReqHeader["Usermd"] = usermd
	request.Path = pn
	gLog.Trace.Printf("writing pn %s ", pn)
	resp, err = sproxyd.PutObj(request, false, *object)
	return
}

func repairIt(resp *http.Response, request *sproxyd.HttpRequest, replace bool) {

	err, status := catch(resp, request, replace)
	if err != nil {
		if status == 200 {
			gLog.Info.Printf("Target docid %s is copied", request.Path)
		} else {
			gLog.Warning.Printf("Target docid %s is not copied - status code %d ", request.Path, status)
		}
	} else {
		gLog.Error.Printf("Target docid %s - Error catching up %v", request.Path, err)
	}
}

func catch(resp *http.Response, request *sproxyd.HttpRequest, replace bool) (err error, status int) {

	var body = []byte{}

	if resp.StatusCode == 200 {
		if resp.ContentLength > 0 {
			body, _ = ioutil.ReadAll(resp.Body)
		}
		if body != nil {
			if _, ok := resp.Header["X-Scal-Usermd"]; ok {

				/* Write Object */
				request.ReqHeader = map[string]string{}
				request.ReqHeader["Usermd"] = resp.Header["X-Scal-Usermd"][0]

				request.ReqHeader["Content-Type"] = "application/octet-stream"
				request.ReqHeader["Content-Type"] = resp.Header["Content-Type"][0]

				err = errors.New(fmt.Sprintf("Lock mode for %v", request.ReqHeader))
				status = 423

				/*
					    resp1, err1 := sproxyd.PutObj(request, replace, body)
						if err1 != nil {
							defer resp1.Body.Close()
							gLog.Info.Printf("")
							status = resp1.StatusCode
						} else {
							err = err1
						}
				*/
			}
		} else {
			err = errors.New(fmt.Sprintf("Request url %s - Body is null", request.Path))
		}
	} else {
		err = errors.New(fmt.Sprintf("Request url %s -Status Code %d", request.Path, resp.StatusCode))
	}

	return
}
