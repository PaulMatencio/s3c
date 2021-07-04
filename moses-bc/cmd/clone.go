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
	"bufio"
	"encoding/json"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	mosesbc "github.com/paulmatencio/s3c/moses-bc/lib"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"strconv"
	"sync"
	"time"
)

var (
	cloneCmd = &cobra.Command{
		Use:   "clone",
		Short: "Command to clone Moses objects",
		Long:  ``,
		Run:   Clone,
	}
	// s3Src, s3Tgt  datatype.CreateSession
	reIndex bool
)

func initCloFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&srcBucket, "source-bucket", "", "", "name of source s3 bucket")
	cmd.Flags().StringVarP(&tgtBucket, "target-bucket", "", "", "name of the target s3 bucket if moeses reIndexing is required")
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", "key's prefix; key= moses-document in the form of cc/pn/kc")
	cmd.Flags().Int64VarP(&maxKey, "maxKey", "m", 20, "maximum number of moses documents  to be cloned concurrently")
	cmd.Flags().StringVarP(&marker, "marker", "M", "", "start processing from this key; key= moses-document in the form of cc/pn/kc")
	cmd.Flags().IntVarP(&maxPage, "maxPage", "", 50, "maximum number of concurrent moses pages to be concurrently procsessed")
	cmd.Flags().IntVarP(&maxLoop, "maxLoop", "", 1, "maximum number of loop, 0 means no upper limit")
	cmd.Flags().BoolVarP(&replace, "replace", "r", false, "replace the existing target moses pages")
	cmd.Flags().BoolVarP(&reIndex, "reIndex", "", false, "re-index the target moses documents")
	cmd.Flags().StringVarP(&inFile, "input-file", "i", "", "input file containing the list of documents to clone")
	cmd.Flags().StringVarP(&srcUrl, "source-sproxyd-url", "s", "", "source sproxyd endpoints  http://xx.xx.xx.xx:81/proxy,http://xx.xx.xx.xx:81/proxy")
	cmd.Flags().StringVarP(&driver, "source-sproxyd-driver", "", "", "source sproxyd driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&targetDriver, "target-sproxyd-driver", "", "", "target sproxyd driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&targetUrl, "target-sproxyd-url", "t", "", "target sproxyd endpoint URL http://xx.xx.xx.xx:81/proxy,http:// ...")

}

func init() {
	rootCmd.AddCommand(cloneCmd)
	initCloFlags(cloneCmd)
}

func Clone(cmd *cobra.Command, args []string) {

	var listpn *bufio.Scanner
	mosesbc.SetSourceSproxyd("clone", srcUrl, driver)
	mosesbc.SetTargetSproxyd("check", targetUrl, targetDriver)

	if len(srcBucket) == 0 {
		if len(inFile) == 0 {
			gLog.Warning.Printf("%s", missingsrcBucket)
			gLog.Warning.Printf("%s", missingiFile)
			return
		} else {
			if listpn, err = utils.Scanner(inFile); err != nil {
				gLog.Error.Printf("Error %v  scanning %d ", err, inFile)
				return
			}
		}
	}

	if len(tgtBucket) == 0 {
		gLog.Warning.Printf("%s", missingtgtBucket)
		return
	}

	if err := mosesbc.CheckBucketName(srcBucket, tgtBucket); err != nil {
		gLog.Warning.Printf("%v", err)
		return
	}

	if srcS3 = mosesbc.CreateS3Session("clone", "source"); srcS3 == nil {
		gLog.Error.Printf("Failed to create a S3 source session")
		return
	}

	if reIndex {
		if tgtS3 = mosesbc.CreateS3Session("clone", "target"); tgtS3 == nil {
			gLog.Error.Printf("Failed to create a S3 target session")
			return
		}
	}
	start := time.Now()
	if nextMarker, err := _cloneBlobs(srcS3, tgtS3, listpn); err != nil {
		gLog.Error.Printf("error %v - Next marker %s", err, nextMarker)
	} else {
		gLog.Info.Printf("Next Marker %s", nextMarker)
	}
	gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))
}

func _cloneBlobs(srcS3 *s3.S3, tgtS3 *s3.S3, listpn *bufio.Scanner) (string, error) {

	var (
		nextmarker, token            string
		N                            int
		tdocs, tpages, tsizes, tdocr int64
		terrors                      int
		re, si                       sync.Mutex
	)
	req1 := datatype.ListObjV2Request{
		Service:          srcS3,
		Bucket:           srcBucket,
		Prefix:           prefix,
		MaxKey:           int64(maxKey),
		Marker:           marker,
		Continuationtoken: token,
	}
	gLog.Info.Println(req1)
	start0 := time.Now()
	for {
		var (
			result       *s3.ListObjectsV2Output
			err          error
			ndocs, ndocr int   = 0, 0
			npages       int   = 0
			docsizes     int64 = 0
			gerrors      int   = 0
			wg1 sync.WaitGroup
		)
		N++ // number of loop
		if len(srcBucket) > 0 {
			result, err = api.ListObjectV2(req1)
			gLog.Info.Printf("target bucket %s - source metadata bucket %s - number of documents: %d", tgtBucket, srcBucket, len(result.Contents))
		} else {
			result, err = ListPn(listpn, int(maxKey))
			gLog.Info.Println(inFile, len(result.Contents))
		}
		if err == nil {
			if l := len(result.Contents); l > 0 {
				start := time.Now()
				for _, v := range result.Contents {
					if *v.Key != nextmarker {
						ndocr += 1
						svc1 := req1.Service
						request := datatype.StatObjRequest{
							Service: svc1,
							Bucket:  req1.Bucket,
							Key:     *v.Key,
						}
						wg1.Add(1)
						go func(request datatype.StatObjRequest, replace bool) {
							var (
								rh = datatype.Rh{
									Key: request.Key,
								}
								err    error
								usermd string
								np     int
								pn     string
							)
							defer wg1.Done()
							rh.Result, rh.Err = api.StatObject(request)
							if usermd, err = utils.GetUserMeta(rh.Result.Metadata); err == nil {
								userm := UserMd{}
								json.Unmarshal([]byte(usermd), &userm)
								pn = rh.Key
								if np, err = strconv.Atoi(userm.TotalPages); err == nil {
									start3 := time.Now()
									nerr, document := mosesbc.CloneBlob1(pn, np, maxPage, replace)
									if nerr == 0 {
										si.Lock()
										npages += int(document.NumberOfPages)
										docsizes += document.Size
										ndocs += 1
										si.Unlock()
										gLog.Info.Printf("Document id %s is cloned - Number of pages %d - Document size %d - Number of errors %d - Elapsed time %v ", document.DocId, document.NumberOfPages, document.Size, nerr, time.Since(start3))
									} else {
										re.Lock()
										gerrors += nerr
										re.Unlock()
										gLog.Info.Printf("Document id %s is not fully cloned - Number of pages %d - Document size %d - Number of errors %d - Elapsed time %v ", document.DocId, document.NumberOfPages, document.Size, nerr, time.Since(start3))
									}
								}
							}
						}(request, replace)
					}
				}
				wg1.Wait()
				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					token = *result.NextContinuationToken
					gLog.Warning.Printf("Truncated %v - Next marker: %s  - Nextcontinuation token: %s", *result.IsTruncated, nextmarker,token)
				}
				// ndocs = ndocs - gerrors
				gLog.Info.Printf("Number of cloned documents: %d of %d - Number of pages: %d  - Documents size: %d - Number of errors: %d -  Elapsed time: %v", ndocs, ndocr, npages, docsizes, gerrors, time.Since(start))
				tdocs += int64(ndocs)
				tdocr += int64(ndocr)
				tpages += int64(npages)
				tsizes += int64(docsizes)
				terrors += gerrors
			}
		} else {
			gLog.Error.Printf("%v", err)
			break
		}

		if *result.IsTruncated && (maxLoop == 0 || N <= maxLoop) {
			// req1.Marker = nextmarker
			req1.Continuationtoken = token
		} else {
			gLog.Info.Printf("Total number of cloned documents: %d of %d - total number of pages: %d  - Total document size: %d - Total number of errors: %d - Total elapsed time: %v", tdocs, tdocr, tpages, tsizes, terrors, time.Since(start0))
			break
		}
	}
	return nextmarker, nil
}
