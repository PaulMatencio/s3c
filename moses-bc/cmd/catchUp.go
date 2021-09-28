// Copyright © 2021 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"encoding/base64"
	"encoding/json"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	// "fmt"
	"github.com/paulmatencio/s3c/datatype"
	gLog "github.com/paulmatencio/s3c/gLog"
	meta "github.com/paulmatencio/s3c/moses-bc/datatype"
	mosesbc "github.com/paulmatencio/s3c/moses-bc/lib"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

/*
	Check "clone" and "restore"
		Compare the cloned or restored objects with the source objects
*/

var (
	catchUpCmd = &cobra.Command{
		Use:   "catch-up",
		Short: "Command to copy missing sproxyd  objects",
		Long:  `Command to copy missing sproxyd  objects`,
		Run:   CatchUp,
	}
	catchUpEdrexCmd = &cobra.Command{
		Use:   "catch-up-edrex",
		Short: "Command to copy missing sproxyd edrex  objects",
		Long:  `Command to copy missing sproxyd  edrex objects`,
		Run:   CatchUpEdrex,
	}
	levelDBUrl string
	repair     bool
)

func initCatFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&srcBucket, "source-bucket", "", "", "name of source S3 bucket without its suffix 00..05")
	cmd.Flags().StringVarP(&inFile, "input-file", "i", "", uInputFile)
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", uPrefix)
	cmd.Flags().Int64VarP(&maxKey, "max-key", "m", 20, "maximum number of documents (keys) to be checked up concurrently -Check --max-page for maximum number of concurrent pages")
	cmd.Flags().StringVarP(&marker, "marker", "M", "", "start processing from this key - Useful for rerun")
	cmd.Flags().IntVarP(&maxPage, "max-page", "", 50, "maximum number of concurrent pages per document. check  --max-key for maximum number of concurrent documents")
	cmd.Flags().IntVarP(&maxLoop, "max-loop", "", 1, "maximum number of loop, 0 means no upper limit")
	cmd.Flags().StringVarP(&srcUrl, "source-sproxyd-url", "s", "", "source sproxyd endpoints  http://xx.xx.xx.xx:81/proxy,http://xx.xx.xx.xx:81/proxy")
	cmd.Flags().StringVarP(&targetUrl, "target-sproxyd-url", "t", "", "target sproxyd endpoint URL http://xx.xx.xx.xx:81/proxy,http:// ...")
	cmd.Flags().StringVarP(&driver, "source-sproxyd-driver", "", "", "source sproxyd driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&targetDriver, "target-sproxyd-driver", "", "", "target sproxyd driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&env, "source-sproxyd-env", "", "", "source sproxyd environment [prod|osa]")
	cmd.Flags().StringVarP(&targetEnv, "target-sproxyd-env", "", "", "target sproxyd environment [prod|osa]")
	cmd.Flags().StringVarP(&levelDBUrl, "levelDB-url", "", "http://10.147.68.133:9000", "levelDB Url - moses s3 index (directory)")
	cmd.Flags().BoolVarP(&repair, "repair", "", false, "trigger the copy of the missing object if it exists")
}
func initCatEdrexFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&inFile, "input-file", "i", "", uInputFile)
	cmd.Flags().Int64VarP(&maxKey, "max-key", "m", 20, "maximum number of documents (keys) to be checked up concurrently")
	cmd.Flags().StringVarP(&marker, "marker", "M", "", "start processing from this key - Useful for rerun")
	cmd.Flags().IntVarP(&maxLoop, "max-loop", "", 1, "maximum number of loop, 0 means no upper limit")
	cmd.Flags().StringVarP(&srcUrl, "source-sproxyd-url", "s", "", "source sproxyd endpoints  http://xx.xx.xx.xx:81/proxy,http://xx.xx.xx.xx:81/proxy")
	cmd.Flags().StringVarP(&targetUrl, "target-sproxyd-url", "t", "", "target sproxyd endpoint URL http://xx.xx.xx.xx:81/proxy,http:// ...")
	cmd.Flags().StringVarP(&driver, "source-sproxyd-driver", "", "", "source sproxyd driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&targetDriver, "target-sproxyd-driver", "", "", "target sproxyd driver [bpchord|bparc]")
	cmd.Flags().BoolVarP(&repair, "repair", "", false, "trigger the copy of the missing object if it exists")
}

func init() {
	rootCmd.AddCommand(catchUpCmd)
	rootCmd.AddCommand(catchUpEdrexCmd)
	initCatFlags(catchUpCmd)
	initCatEdrexFlags(catchUpEdrexCmd)
}

func CatchUp(cmd *cobra.Command, args []string) {

	var (
		err error
		skipInput = 0
	)
	if err = mosesbc.SetSourceSproxyd("check", srcUrl, driver, env); err != nil {
		gLog.Error.Printf("%v", err)
		return
	}

	if err = mosesbc.SetTargetSproxyd("check", targetUrl, targetDriver, targetEnv); err != nil {
		gLog.Error.Printf("%v", err)
		return
	}

	gLog.Info.Printf("Source Env: %s - Source Driver: %s - Source Url: %s", sproxyd.Env, sproxyd.Driver, sproxyd.Url)
	gLog.Info.Printf("Target Env: %s - Target Driver: %s - Target Url: %s", sproxyd.TargetEnv, sproxyd.TargetDriver, sproxyd.TargetUrl)
	gLog.Info.Printf("Level DB URL: %s", levelDBUrl)

	// Pns from --input-file
	if len(inFile) > 0 {
		if listpn, err = utils.Scanner(inFile); err != nil {
			gLog.Error.Printf("Error %v  scanning --input-file  %s ", err, inFile)
			return
		} else {
			if len(srcBucket) == 0 {
				gLog.Error.Printf("Source bucket is missing")
				return
			} else {
				if mosesbc.HasSuffix(srcBucket) {
					gLog.Error.Printf("Remove the bucket suffix of --source-bucket  %s", srcBucket)
					return
				} else {
					if srcS3 = mosesbc.CreateS3Session("backup", "source"); srcS3 == nil {
						gLog.Error.Printf("Failed to create a session with the source S3 endpoint")
						return
					}
				}
			}
			if skipInput > 0 {
				gLog.Info.Printf("Skipping the first %d entries of the input file %s", skipInput, inFile)
				for sk := 1; sk <= skipInput; sk++ {
					listpn.Scan()
				}
			}
			catchUpPns(srcS3)
		}
	}

	//  Pn from Moses --source-bucket

	if len(srcBucket) == 0 {
		gLog.Error.Printf("Source bucket is missing")
		return
	}

	//  check only documents metadata
	if len(prefix) == 0 {
		if len(srcBucket) == 0 {
			gLog.Error.Printf("Source bucket is missing")
			return
		}
	} else {
		if err, suf := mosesbc.GetBucketSuffix(srcBucket, prefix); err != nil {
			gLog.Error.Printf("%v", err)
			return
		} else {
			if len(suf) > 0 {
				srcBucket += "-" + suf
				gLog.Warning.Printf("A suffix %s is appended to the source Bucket %s", suf, srcBucket)
			}
		}
	}

	var (
		client    = &http.Client{}
		transport = &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   time.Duration(CONTIMEOUT) * time.Millisecond, // connection timeout
				KeepAlive: time.Duration(KEEPALIVE) * time.Millisecond,
			}).DialContext,
			TLSHandshakeTimeout: 10 * time.Second,
			ForceAttemptHTTP2:   true,
			MaxIdleConns:        100,
			MaxConnsPerHost:     100,
			// MaxIdleConnsPerHost: 100,
			IdleConnTimeout:       90 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
	)
	client.Transport = transport
	CatchUpSproxyd(client, srcBucket)

}


func CatchUpEdrex(cmd *cobra.Command, args []string) {
	var (
		err error
		skipInput = 0
	)
	if err = mosesbc.SetSourceSproxyd("check", srcUrl, driver, env); err != nil {
		gLog.Error.Printf("%v", err)
		return
	}

	if err = mosesbc.SetTargetSproxyd("check", targetUrl, targetDriver, targetEnv); err != nil {
		gLog.Error.Printf("%v", err)
		return
	}

	gLog.Info.Printf("Source Env: %s - Source Driver: %s - Source Url: %s", sproxyd.Env, sproxyd.Driver, sproxyd.Url)
	gLog.Info.Printf("Target Env: %s - Target Driver: %s - Target Url: %s", sproxyd.TargetEnv, sproxyd.TargetDriver, sproxyd.TargetUrl)
	if len(inFile) > 0 {
		if listpn, err = utils.Scanner(inFile); err != nil {
			gLog.Error.Printf("Error %v  scanning --input-file  %s ", err, inFile)
			return
		} else {
			if skipInput > 0 {
				gLog.Info.Printf("Skipping the first %d entries of the input file %s", skipInput, inFile)
				for sk := 1; sk <= skipInput; sk++ {
					listpn.Scan()
				}
			}
			catchUpEdrexs()
		}
	} else {
		gLog.Error.Printf("--input-file is missing")
		return
	}
}


func CatchUpSproxyd(client *http.Client, bucket string) {

	var (
		s3Meta                             = datatype.S3Metadata{}
		nextMarker                         string
		N                                  = 0
		start                              = time.Now()
		l4, lp, le                         sync.Mutex
		n404s, ndocs, npages, nerrs, nreps int
	)

	for {
		start1 := time.Now()
		err, result := listS3bPref(client, bucket, prefix, marker)
		if err != nil || len(result) == 0 {
			if err != nil {
				gLog.Error.Println(err)
			} else {
				gLog.Info.Println("Result set is empty")
			}
		} else {

			if err = json.Unmarshal([]byte(result), &s3Meta); err == nil {
				l := len(s3Meta.Contents)
				wg1 := sync.WaitGroup{}
				for _, c := range s3Meta.Contents {
					ndocs += 1
					m := &c.Value.XAmzMetaUsermd
					usermd, _ := base64.StdEncoding.DecodeString(*m)
					s3meta := meta.UserMd{} //  moses s3 index
					if err = json.Unmarshal(usermd, &s3meta); err == nil {
						wg1.Add(1)
						go func(c datatype.Contents, s3meta *meta.UserMd) {
							np, _ := strconv.Atoi(s3meta.TotalPages)
							lp.Lock()
							npages += np
							lp.Unlock()
							ret := mosesbc.CatchUpBlobs(c.Key, np, maxPage, repair)
							if ret.N404s > 0 {
								l4.Lock()
								n404s += ret.N404s
								nreps += ret.Nreps
								l4.Unlock()
							}
							/*
								if ret.Nreps > 0 {
									lr.Lock()
									nreps += ret.Nreps
									lr.Unlock()
								}
							*/
							if ret.Nerrs > 0 {
								le.Lock()
								nerrs += ret.Nerrs
							}
							wg1.Done()
						}(c, &s3meta)

					} else {
						gLog.Error.Printf("%v", err)
					}

				}
				wg1.Wait()
				if l > 0 {
					nextMarker = s3Meta.Contents[l-1].Key
					gLog.Info.Printf("Loop %d - Next marker %s - Istruncated %v - Elapsed time %v - Cumulative elapsed Time %v", N, nextMarker, s3Meta.IsTruncated, time.Since(start1), time.Since(start))
					gLog.Info.Printf("Number of docs %d  - number of pages %d - number of 404's %d - number of repairs %d - number of errors %d ", ndocs, npages, n404s, nreps, nerrs)
				}
				N++
			} else {
				gLog.Info.Println(err)
			}
		}
		if !s3Meta.IsTruncated {
			return
		} else {
			marker = nextMarker
		}
		if maxLoop != 0 && N >= maxLoop {
			return
		}
	}

}

func listS3bPref(client *http.Client, bucket string, prefix string, marker string) (error, string) {

	var (
		//err      error
		result   string
		contents []byte
		delim    string
	)

	request := "/default/bucket/" + bucket + "?listingType=DelimiterMaster&prefix="
	limit := "&maxKeys=" + strconv.Itoa(int(maxKey))

	if len(delimiter) > 0 {
		delim = "&delimiter=" + delimiter
	}

	keyMarker := "&marker=" + marker
	// url := Host +":"+Port+request+prefix+limit+keyMarker+delim
	url := levelDBUrl + request + prefix + limit + keyMarker + delim
	//gLog.Trace.Println("URL:", url)
	for i := 1; i <= RetryNumber; i++ {
		if response, err := client.Get(url); err == nil {
			//gLog.Trace.Printf("Status code %d",response.StatusCode)
			if response.StatusCode == 200 {
				defer response.Body.Close()
				if contents, err = ioutil.ReadAll(response.Body); err == nil {
					return err, ContentToJson(contents)
				}
			} else {
				gLog.Warning.Printf("Get url %s - Http status: %d", url, response.Status)
			}
		} else {
			gLog.Error.Printf("Error: %v - number of retries: %d", err, i)
			time.Sleep(WaitTime * time.Millisecond)
		}
	}
	return err, result
}

// transform content returned by the bucketd API into JSON string
func ContentToJson(contents []byte) string {
	result := strings.Replace(string(contents), "\\", "", -1)
	result = strings.Replace(result, "\"{", "{", -1)
	// result = strings.Replace(result,"\"}]","}]",-1)
	result = strings.Replace(result, "\"}\"}", "\"}}", -1)
	result = strings.Replace(result, "}\"", "}", -1)
	gLog.Trace.Println(result)
	return result
}

func catchUpPns(service *s3.S3) (ret mosesbc.Ret) {

	var (
		nextmarker, token string
		N                 int
		re, si sync.Mutex
		req    datatype.ListObjV2Request
	)

	// start0 := time.Now()
	for {
		var (
			result *s3.ListObjectsV2Output
			err    error
			wg1    sync.WaitGroup
		)
		N++ // number of loop

		gLog.Info.Printf("Listing documents from file %s", inFile)
		result, err = mosesbc.ListPn(listpn, int(maxKey))

		// result contains the list of docid ( PUT CC/PN/KC) to be checked
		if err == nil {
			if l := len(result.Contents); l > 0 {
				//	start := time.Now()
				var (
					buck1 string
					key string
				)
				gLog.Info.Printf("Total number of documents %d", l)
				for _, v := range result.Contents {
					if *v.Key != nextmarker {
						if err, method, key = mosesbc.ParseLog(*v.Key); err != nil {
							gLog.Error.Printf("%v", err)
							// ec.WriteBdb([]byte(bNSpace), []byte(errSuffix+"/"+*v.Key), []byte(err.Error()), myBdb)
							continue // skip it
						}
						buck1 = mosesbc.SetBucketName(key, srcBucket)
						ret.Ndocs += 1
						//  prepare the request to retrieve S3 meta data
						request := datatype.StatObjRequest{
							Service: service,
							Bucket:  buck1,
							Key:     key,
						}
						wg1.Add(1)
						go func(request datatype.StatObjRequest, repair bool) {
							defer wg1.Done()
							ret1 := catchUpPn(request, repair)
							if ret1.Nerrs > 0 {
								re.Lock()
								ret.Nerrs += ret1.Nerrs
								re.Unlock()
							}
							if ret1.N404s >0 {
								ret.N404s += ret1.N404s
								ret.Nreps += ret1.Nreps
							}
							si.Lock()
							ret.Npages += ret1.Npages
							ret.Ndocs += ret1.Ndocs
							si.Unlock()
						}(request, repair)
					}
				}
				wg1.Wait()
				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					gLog.Warning.Printf("Truncated %v - Next marker: %s  - Next continuation token: %s", *result.IsTruncated, nextmarker, token)
					gLog.Info.Printf("Number of docs %d  - number of pages %d - number of 404's %d - number of repairs %d - number of errors %d ", ret.Ndocs, ret.Npages, ret.N404s, ret.Nreps, ret.Nerrs)
				}
				//gLog.Info.Printf("Number of cloned documents: %d of %d - Number of pages: %d  - Documents size: %d - Number of errors: %d -  Elapsed time: %v", ndocs, ndocr, npages, docsizes, nerrors, time.Since(start))
			}
		} else {
			gLog.Error.Printf("%v", err)
			break
		}

		if *result.IsTruncated && (maxLoop == 0 || N < maxLoop) {
			// req1.Marker = nextmarker
			req.Continuationtoken = token
		} else {
			//	gLog.Info.Printf("Total number of cloned documents: %d of %d - total number of pages: %d  - Total document size: %d - Total number of errors: %d - Total elapsed time: %v", tdocs, tdocr, tpages, tsizes, terrors, time.Since(start0))
			break
		}
	}
	return ret
}

func catchUpPn(request datatype.StatObjRequest, repair bool) (ret mosesbc.Ret) {

	var (
		rh = datatype.Rh{
			Key: request.Key,
		}
		err        error
		usermd, pn string
		np         int
	)

	if rh.Result, rh.Err = api.StatObject(request); rh.Err == nil {
		// get S3 user metadata
		if usermd, err = utils.GetUserMeta(rh.Result.Metadata); err == nil {
			userm := meta.UserMd{}
			json.Unmarshal([]byte(usermd), &userm)
			pn = rh.Key
			if np, err = strconv.Atoi(userm.TotalPages); err == nil {
				ret = mosesbc.CatchUpBlobs(pn, np, maxPage, repair)
			}
		} else {
			gLog.Error.Printf("Error %v -  metadata %s ", err,rh.Result.Metadata)
		}
	} else {
		gLog.Error.Printf("Error %v - Bucket %s - Key %s", rh.Err,request.Bucket,request.Key)
	}
	return
}

func catchUpEdrexs() (ret mosesbc.Ret)  {
	var (
		nextmarker, token string
		N                 int
		re, si sync.Mutex
		req    datatype.ListObjV2Request
	)

	// start0 := time.Now()
	for {
		var (
			result *s3.ListObjectsV2Output
			err    error
			wg1    sync.WaitGroup
		)
		N++ // number of loop

		gLog.Info.Printf("Listing documents from file %s", inFile)
		result, err = mosesbc.ListPn(listpn, int(maxKey))

		// result contains the list of docid ( PUT CC/PN/KC) to be checked
		if err == nil {
			if l := len(result.Contents); l > 0 {
				//	start := time.Now()

				gLog.Info.Printf("Total number of documents %d", l)
				for _, v := range result.Contents {
					if *v.Key != nextmarker {
						if err, method, key = mosesbc.ParseLog(*v.Key); err != nil {
							gLog.Error.Printf("%v", err)
							continue // skip it
						}
						ret.Ndocs += 1
						//  prepare the request to retrieve S3 meta data

						wg1.Add(1)
						go func(key string, repair bool) {
							defer wg1.Done()
							ret1 := catchUpEdrex(key, repair)
							if ret1.Nerrs > 0 {
								re.Lock()
								ret.Nerrs += ret1.Nerrs
								re.Unlock()
							}
							if ret1.N404s >0 {
								ret.N404s += ret1.N404s
								ret.Nreps += ret1.Nreps
							}
							si.Lock()
							ret.Npages += ret1.Npages
							si.Unlock()
						}(key,repair)
					}
				}
				wg1.Wait()
				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					//gLog.Warning.Printf("Truncated %v - Next marker: %s  - Next continuation token: %s", *result.IsTruncated, nextmarker, token)
					//gLog.Info.Printf("Number of docs %d  - number of pages %d - number of 404's %d - number of repairs %d - number of errors %d ", ret.Ndocs, ret.Npages, ret.N404s, ret.Nreps, ret.Nerrs)
				}
				gLog.Warning.Printf("Truncated %v - Next marker: %s  - Next continuation token: %s", *result.IsTruncated, nextmarker, token)
				gLog.Info.Printf("Number of PUT docs %d  - number of docs %d - number of 404's %d - number of repairs %d - number of errors %d ", ret.Ndocs, ret.Npages, ret.N404s, ret.Nreps, ret.Nerrs)
			}
		} else {
			gLog.Error.Printf("%v", err)
			break
		}

		if *result.IsTruncated && (maxLoop == 0 || N < maxLoop) {
			// req1.Marker = nextmarker
			req.Continuationtoken = token
		} else {
			break
		}
	}
	return ret

}

func catchUpEdrex(key string, repair bool) (ret mosesbc.Ret) {
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
			Path: key,
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
			Path: key,
		}
		resp,resp1 *http.Response
	)
	if resp, err = sproxyd.GetMetadata(&request1); err == nil {
		defer resp.Body.Close()
		switch resp.StatusCode {
		case 200:
			ret.Npages =1
		case 404:
			ret.N404s = 1
			if resp1, err = sproxyd.GetMetadata(&request2); err == nil {
				defer resp1.Body.Close()
				switch resp1.StatusCode {
				case 404:
					ret.N404s--
					gLog.Warning.Printf("Source - Head %s - status Code %d",key,resp.StatusCode)
				default:
					gLog.Trace.Printf("Source - Head %s - status Code %d",key,resp.StatusCode)
				}
			} else {
				ret.Nerrs +=1
			}
			if ret.N404s > 0 {
				gLog.Warning.Printf("Target key %s is missing",key)
			}
		default:
			gLog.Trace.Printf("Target - Head %s - status Code %d",key,resp.StatusCode)
		}

	} else {
		ret.Nerrs = 1
	}
	return ret
}
