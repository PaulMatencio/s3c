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

	var start = time.Now()

	gLog.Trace.Printf("Document %s - Number of pages %d - max page %d", pn, np, maxPage)
	if np <= maxPage {
		ret = catchUpPages(pn, np, repair)
		gLog.Trace.Printf("Catchup docid %s - number of pages %d - number of 404's %d - Elapsed time %v", pn, np, ret.N404s, time.Since(start))
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
		wg2        sync.WaitGroup
		start      int
		err        error
		pdf, p0    bool = false, false
		le, l4, lr sync.Mutex
	)

	err, status, docmd := GetDocMetaStatus(request1, sproxyd.TargetEnv, pn)
	if err == nil {
		if status == 200 {
			pdf, p0 = CheckPdfAndP0(pn, docmd)
		} else {
			if status == 404 {
				ret.N404s +=1
			}
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
							if err1 := repairIt(resp2, &request3, false); err1 == nil {
								ret.Nreps += 1
							}
						} else {
							isSync := "0"
							if _, ok := resp2.Header["X-Scal-Attr-Is-Sync"]; ok {
								isSync = resp2.Header["X-Scal-Attr-Is-Sync"][0]
							}
							ct := resp2.Header["Content-Type"][0]
							sz := resp2.ContentLength
							gLog.Info.Printf("Target docid %s is missing, is synced? %s but is recoverable - content-type %s - Content-length %d ", sproxyd.TargetEnv+"/"+pn, isSync, ct, sz)
						}
					}
				} else {
					if status == 404 {
						ret.N404s -= 1
					}
					gLog.Warning.Printf("Source docid %s - status code %d ", sproxyd.Env+"/"+pn, status)
				}
			} else {
				gLog.Error.Printf("Source docid %s -  error %v", sproxyd.Env+"/"+pn, err)
				ret.Nerrs += 1
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
							if err1 := repairIt(resp2, &request3, false); err1 == nil {
								ret.Nreps += 1
							}
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

	gLog.Trace.Printf("Document %s has %d pages", pn, np)

	for k := start; k <= np; k++ {
		wg2.Add(1)
		go func(request1 sproxyd.HttpRequest, request2 sproxyd.HttpRequest, request3 sproxyd.HttpRequest, pn string, k int) {
			defer wg2.Done()
			request1.Path = sproxyd.TargetEnv + "/" + pn + "/p" + strconv.Itoa(k)
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
								if err1 := repairIt(resp2, &request3, false); err1 == nil {
									lr.Lock()
									ret.Nreps += 1
									lr.Unlock()
								}
							} else {
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
				gLog.Error.Printf("Target Page %s - error %v", request1.Path, err)
				le.Lock()
				ret.Nerrs += 1
				le.Unlock()
			}
		}(request1, request2, request3, pn, k)
	}

	wg2.Wait()
	ret.Npages += np - start + 1
	ret.Ndocs = 1
	return ret
}

/*
	Check pages ( number of pages > maxPages
*/
func catchUpMaxPages(pn string, np int, maxPage int, repair bool) (ret Ret) {

	var (
		q, r, start, end int
		err              error
		pdf, p0          bool
		// nerrs,nreps,n404s int
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
	)

	err, status, docmd := GetDocMetaStatus(request1, sproxyd.TargetEnv, pn)
	if err == nil {
		if status == 200 {
			pdf, p0 = CheckPdfAndP0(pn, docmd)
		} else {
			if status == 404 {
				ret.N404s += 1
			}
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
							if err1 := repairIt(resp2, &request3, false); err1 == nil {
								ret.Nreps += 1
							}
						} else {
							isSync := "no"
							if _, ok := resp2.Header["X-Scal-Attr-Is-Sync"]; ok {
								isSync = resp2.Header["X-Scal-Attr-Is-Sync"][0]
							}
							ct := resp2.Header["Content-Type"][0]
							sz := resp2.ContentLength
							gLog.Info.Printf("Target docid %s is missing, is synced? %s but is recoverable - content-type %s - Content-length %d ", sproxyd.TargetEnv+"/"+pn, isSync, ct, sz)
						}
					}
				} else {
					if status == 404 {
						ret.N404s -= 1
					}
					gLog.Warning.Printf("Source docid %s - status code %d ", sproxyd.Env+"/"+pn, status)
				}
			} else {
				gLog.Error.Printf("Source docid %s - error %v", sproxyd.Env+"/"+pn, err)
				ret.Nerrs += 1
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
				if resp2, err := sproxyd.Getobject(&request2); err == nil {
					defer resp2.Body.Close()
					if resp2.StatusCode == 404 {
						ret.N404s -= 1
						gLog.Warning.Printf("Source Pdf %s - status code %d ", request2.Path, resp2.StatusCode)
					} else {
						if repair {
							request3.Path = request1.Path
							if err1 := repairIt(resp2, &request3, false); err1 == nil {
								ret.Nreps += 1
							}
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
		ret.Nreps += ret1.Nreps

	}

	if r > 0 {
		ret1 := catchUpPagePart(&request1, pn, np, q*maxPage+1, np, repair)
		ret.Nerrs += ret1.Nerrs
		ret.N404s += ret1.N404s
		ret.Nreps += ret1.Nreps
	}

	ret.Npages = np - start + 1
	ret.Ndocs = 1
	return ret

}

func catchUpPagePart(request1 *sproxyd.HttpRequest, pn string, np int, start int, end int, repair bool) (ret Ret) {

	var (
		wg2        sync.WaitGroup
		le, l4, lr sync.Mutex
		request2   = sproxyd.HttpRequest{
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
		go func(request1 sproxyd.HttpRequest, request2 sproxyd.HttpRequest, request3 sproxyd.HttpRequest, pn string, np int, k int) {
			gLog.Trace.Printf("Getpart of pn: %s - url:%s", pn, request1.Path)
			defer wg2.Done()
			request1.Path = sproxyd.TargetEnv + "/" + pn + "/p" + strconv.Itoa(k)
			if resp, err := sproxyd.GetMetadata(&request1); err == nil {
				defer resp.Body.Close()
				/*
					check if the target page is missing
					if not then just continue
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
						if resp2.StatusCode == 404 {
							l4.Lock()
							ret.N404s -= 1
							l4.Unlock()
							gLog.Warning.Printf("Source Page %s - status code %d ", request2.Path, resp2.StatusCode)
						} else {
							if repair {

								request3.Path = request1.Path
								if err1 := repairIt(resp2, &request3, false); err1 == nil {
									lr.Lock()
									ret.Nreps += 1
									lr.Unlock()
								}
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
				ret.Nerrs += 1
				le.Unlock()

			}
		}(*request1, request2, request3, pn, np, k)

	}
	// Write the document to File
	wg2.Wait()
	return ret
}

func repairIt(resp *http.Response, request *sproxyd.HttpRequest, replace bool) error {
	err, status := catch(resp, request, replace)
	if err == nil {
		if status == 200 {
			gLog.Info.Printf("Target  object %s is recovered - status code %d", request.Path, status)
		} else {
			gLog.Warning.Printf("Target object %s is not recovered - status code %d", request.Path, status)
		}
	} else {
		gLog.Error.Printf("Target object %s is not recovered - Error %v", request.Path, err)
	}
	return err
}

func catch(resp *http.Response, request *sproxyd.HttpRequest, replace bool) (err error, status int) {

	var body = []byte{}
	if resp.StatusCode == 200 {
		/*
			check if  object is already synched
		 */
		if _, ok := resp.Header["X-Scal-Attr-Is-Sync"]; ok {

			if resp.ContentLength > 0 {
				body, _ = ioutil.ReadAll(resp.Body)
			}
			if body != nil {
				if _, ok := resp.Header["X-Scal-Usermd"]; ok {
					/*
						Write Object
					*/
					request.ReqHeader = map[string]string{}
					request.ReqHeader["Usermd"] = resp.Header["X-Scal-Usermd"][0]
					request.ReqHeader["Content-Type"] = resp.Header["Content-Type"][0]
					// gLog.Info.Printf("Body length %d  content-type %s", len(body), request.ReqHeader["Content-Type"])
					resp1, err1 := sproxyd.PutObj(request, replace, body)
					if err1 == nil {
						defer resp1.Body.Close()
						status = resp1.StatusCode
					} else {
						err = err1
					}
					//err = nil
					// status = 413
				}
			} else {
				err = errors.New(fmt.Sprintf("Request url %s  with a nil body", request.Path))
			}
		} else {
			 status= 412
			 err = errors.New(fmt.Sprintf("Object is not yet synched %s - status %d ", request.Path,status))
		}
	} else {
		err = errors.New(fmt.Sprintf("Request url %s -Status Code %d", request.Path, resp.StatusCode))
	}

	return
}
