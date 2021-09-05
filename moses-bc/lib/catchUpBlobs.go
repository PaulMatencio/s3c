package lib

import (
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	gLog "github.com/paulmatencio/s3c/gLog"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
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
		gLog.Trace.Printf("Catchup docid %s - number of pages %d - number of 404's %d  - Elapsed time %v", pn,np, ret.N404s, time.Since(start))
	} else {
		ret = catchUpMaxPages(pn, np, maxPage, repair)
		gLog.Trace.Printf("Catchup docid %s - number of pages %d - number of 404's %d - Elapsed time %v", pn,np,ret.N404s,time.Since(start))
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
		wg2      sync.WaitGroup
		nerrors  = 0
		n404s    = 0
		start    int
		err      error
		pdf, p0  bool = false, false
		le, l4   sync.Mutex
	)

	err, status, docmd := GetDocMetaStatus(request1, sproxyd.TargetEnv, pn)
	if err == nil {
		if status == 200 {
			pdf, p0 = CheckPdfAndP0(pn, docmd)
		} else {
			gLog.Warning.Printf("Target document %s - status code %d ", request1.Path, status)
			err, status, docmd := GetDocMetaStatus(request2, sproxyd.Env, pn)
			if err == nil && status == 200 {
				pdf, p0 = CheckPdfAndP0(pn, docmd)
				if repair {
					request3.Path = sproxyd.TargetEnv + "/" + pn

				} else {
					gLog.Info.Printf("Target object %s  is repairable", request1.Path)
				}
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
		gLog.Info.Printf("DocId %s contains a pdf", pn)
		request1.Path = sproxyd.TargetEnv + "/" + pn + "/pdf"
		if resp, err := sproxyd.GetMetadata(&request1); err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == 404 {
				ret.N404s += 1
				gLog.Warning.Printf("Target Page %s - status code %d ", request1.Path, resp.StatusCode)
				/*
					Check if 404 at source
				*/
				request2.Path = sproxyd.Env + "/" + pn + "/pdf"
				if resp2, err := sproxyd.Getobject(&request2); err == nil {
					defer resp2.Body.Close()
					if resp2.StatusCode == 404 {
						ret.N404s -= 1
						gLog.Warning.Printf("Source Page %s - status code %d ", request2.Path, resp2.StatusCode)
					} else {
						if repair {
							request3.Path = request1.Path

						} else {
							gLog.Info.Printf("Target object %s  is repairable", request1.Path)
						}
					}
				}
			}
			gLog.Trace.Printf("Target Page %s - status code %d ", request1.Path, resp.StatusCode)
		} else {
			gLog.Error.Printf("Target page %s -  error %v", request1.Path, err)
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
				if resp.StatusCode == 404 {
					l4.Lock()
					ret.N404s += 1
					l4.Unlock()
					gLog.Warning.Printf("Target Page %s - status code %d ", request1.Path, resp.StatusCode)
					request2.Path = sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
					if resp2, err := sproxyd.Getobject(&request2); err == nil {
						defer resp2.Body.Close()
						if resp2.StatusCode == 404 {
							ret.N404s -= 1
							gLog.Warning.Printf("Source Page %s - status code %d ", request2.Path, resp2.StatusCode)

						} else if resp2.StatusCode == 200 {
							if repair {
								request3.Path = request1.Path

							} else {
								gLog.Info.Printf("Target object %s  is repairable", request1.Path)
							}
						}
					}
				}
				gLog.Trace.Printf("Target Page %s - status code %d ", request1.Path, resp.StatusCode)

			} else {
				gLog.Error.Printf("Target page %s -  error %v", request1.Path, err)
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
			gLog.Warning.Printf("Target document %s - status code %d ", request1.Path, status)
			/* get it from the source */
			err, status, docmd = GetDocMetaStatus(request2, sproxyd.Env, pn)
			if err == nil {
				if status == 200 {
					pdf, p0 = CheckPdfAndP0(pn, docmd) /* getSproxyd.go */
					if repair {
						request3.Path = sproxyd.TargetEnv + "/" + pn
					}
				} else {
					gLog.Warning.Printf("Target document %s - status code %d ", request1.Path, status)
				}

			} else {
				gLog.Error.Printf("Source document %s -  error %v", request2.Path, err)
			}
		}
	} else {
		gLog.Error.Printf("Target document %s -  error %v", request1.Path, err)
		// gLog.Error.Printf("Target document %s - status code %d ", request1.Path, status)
	}

	if p0 {
		start = 0
		gLog.Info.Printf("DocId %s contains a page 0", pn)
	} else {
		start = 1
	}

	if pdf {
		gLog.Info.Printf("DocId %s contains a pdf", pn)
		request1.Path = sproxyd.TargetEnv + "/" + pn + "/pdf"
		if resp, err := sproxyd.GetMetadata(&request1); err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == 404 {
				ret.N404s += 1
				gLog.Warning.Printf("Target Page %s - status code %d ", request1.Path, resp.StatusCode)
				/*
					Check if 404 at source
				*/
				request2.Path = sproxyd.TargetEnv + "/" + pn + "/pdf"
				if resp2, err := sproxyd.GetMetadata(&request2); err == nil {
					defer resp2.Body.Close()
					if resp2.StatusCode == 404 {
						ret.N404s -= 1
						gLog.Warning.Printf("Source Page %s - status code %d ", request2.Path, resp2.StatusCode)
					} else {
						if repair {
							request3.Path = request1.Path

						} else {
							gLog.Info.Printf("Target object %s  is repairable", request1.Path)
						}
					}
				}
			}
			gLog.Trace.Printf("Target Page %s - status code %d ", request1.Path, resp.StatusCode)
		} else {
			gLog.Error.Printf("Target page %s -  error %v", request1.Path, err)
			ret.Nerrs += 1
		}
	}

	end = maxPage
	q = np / maxPage
	r = np % maxPage

	gLog.Trace.Printf("Big document %s  - number of pages %d ", pn, np)

	for s := 1; s <= q; s++ {
		ret1 := catchUpPagePart(&request1, pn, np, start, end,repair)
		start = end + 1
		end += maxPage
		if end > np {
			end = np
		}

		ret.Nerrs += ret1.Nerrs
		ret.N404s += ret1.N404s

	}

	if r > 0 {
		ret1 := catchUpPagePart(&request1, pn, np, q*maxPage+1, np,repair)
		ret.Nerrs += ret1.Nerrs
		ret.N404s += ret1.N404s
	}

	ret.Npages = np
	ret.Ndocs += 1
	return ret
	// return WriteDocument(pn, document, outdir)

}

func catchUpPagePart(request1 *sproxyd.HttpRequest, pn string, np int, start int, end int,repair bool) (ret Ret) {

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
				if resp.StatusCode == 404 {
					l4.Lock()
					n404s = 1
					l4.Unlock()
					gLog.Warning.Printf("Target Page %s - status code %d ", request1.Path, resp.StatusCode)
					request2.Path = sproxyd.Env + "/" + pn + "/p" + strconv.Itoa(k)
					if resp2, err := sproxyd.GetMetadata(&request2); err == nil {
						defer resp2.Body.Close()
						if resp2.StatusCode == 404 {
							ret.N404s -= 1
							gLog.Warning.Printf("Source Page %s - status code %d ", request2.Path, resp2.StatusCode)
						}  else {
							if repair {
								request3.Path = request1.Path


							} else {
								gLog.Info.Printf("Target object %s  is repairable", request1.Path)
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

func putDocMeta(request *sproxyd.HttpRequest, document *documentpb.Document, replace bool) (err error, status int) {

	var (
		pn   = document.GetDocId()
		resp *http.Response
	)
	request.ReqHeader = map[string]string{}
	request.ReqHeader["Content-Type"] = "application/octet-stream"
	request.ReqHeader["Usermd"] = document.GetMetadata()
	gLog.Trace.Printf("writing pn %s - Path %s ", pn, request.Path)
	resp, err = sproxyd.PutObj(request, replace, []byte{})
	if err != nil {
		defer resp.Body.Close()
		status = resp.StatusCode
	}
	return
}
