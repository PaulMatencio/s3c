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
	"encoding/json"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	mosesbc "github.com/paulmatencio/s3c/moses-bc/lib"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"strconv"
	"sync"
	"time"
)

var (
	cloneCmd = &cobra.Command{
		Use:   "clone",
		Short: "Command to clone Moses",
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
	cmd.Flags().Int64VarP(&maxKey, "maxKey", "m", 40, "maximum number of moses documents  to be cloned concurrently")
	cmd.Flags().StringVarP(&marker, "marker", "M", "", "start processing from this key; key= moses-document in the form of cc/pn/kc")
	cmd.Flags().IntVarP(&maxPage, "maxPage", "", 50, "maximum number of concurrent moses pages to be concurrently procsessed")
	cmd.Flags().IntVarP(&maxLoop, "maxLoop", "", 1, "maximum number of loop, 0 means no upper limit")
	cmd.Flags().BoolVarP(&replace, "replace", "r", false, "replace the existing target moses pages")
	cmd.Flags().BoolVarP(&reIndex, "reIndex", "I", false, "re-index the target moses documents")
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

	mosesbc.SetSourceSproxyd("clone", srcUrl, driver)
	mosesbc.SetTargetSproxyd("check", targetUrl, targetDriver)

	if len(srcBucket) == 0 {
		if len(viper.GetString("clone.s3.source.bucket")) == 0 {
			gLog.Warning.Printf("%s", "missing clone.s3.source.bucket in the config file")
			return
		} else {
			srcBucket = viper.GetString("clone.s3.source.bucket")
		}
	}

	if len(tgtBucket) == 0 && reIndex {
		if len(viper.GetString("clone.s3.target.bucket")) == 0 {
			gLog.Warning.Printf("%s", "missing clone.s3.target.bucket in the config file")
			return
		} else {
			tgtBucket = viper.GetString("clone.s3.target.bucket")
		}
	}
	// setS3Session("clone","source")
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

	nm, nerr := cloneBlobs(srcS3, tgtS3)
	gLog.Info.Printf("Next marker %s -  Total number of errors %d", nm, nerr)

}

func cloneBlobs(srcS3 *s3.S3, tgtS3 *s3.S3) (string, error) {

	var (
		nextmarker            string
		N                     int
		tdocs, tpages, tsizes,tdocr  int64
		terrors               int
		re,si                 sync.Mutex
	)
	req1 := datatype.ListObjRequest{
		Service: srcS3,
		Bucket:  srcBucket,
		Prefix:  prefix,
		MaxKey:  maxKey,
		Marker:  marker,
	}
	gLog.Info.Println(req1)
	start0 := time.Now()
	for {
		var (
			result   *s3.ListObjectsOutput
			err      error
			ndocs,ndocr    int = 0,0
			npages   int = 0
			docsizes int64 = 0
			gerrors  int = 0
		)
		N++ // number of loop
		if result, err = api.ListObject(req1); err == nil {
			gLog.Info.Printf("target backup bucket %s - source  metadata bucket %s - number of documents: %d", tgtBucket, srcBucket, len(result.Contents))
			if l := len(result.Contents); l > 0 {
				ndocr += int(l)
				var wg1 sync.WaitGroup
				start := time.Now()
				for _, v := range result.Contents {
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
									ndocs +=1
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
				wg1.Wait()
				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					gLog.Warning.Printf("Truncated %v - Next marker: %s ", *result.IsTruncated, nextmarker)
				}
				// ndocs = ndocs - gerrors
				gLog.Info.Printf("Number of documents cloned: %d of %d - Number of pages: %d  - Documents size: %d - Number of errors: %d -  Elapsed time: %v", ndocs, ndocr,npages, docsizes, gerrors, time.Since(start))
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
			req1.Marker = nextmarker
		} else {
			gLog.Info.Printf("Total number of documents restored: %d of %d - total number of pages: %d  - Total document size: %d - Total number of errors: %d - Total elapsed time: %v", tdocs, tdocr,tpages, tsizes, terrors, time.Since(start0))
			break
		}
	}
	return nextmarker, nil
}
