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
	clone "github.com/paulmatencio/s3c/clone/lib"
	"github.com/paulmatencio/s3c/datatype"
	// "github.com/golang/protobuf/proto"
	"github.com/paulmatencio/s3c/gLog"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"strconv"
	"sync"
	"time"
)

// listObjectCmd represents the listObject command
var (
	loshort = "Command to list objects of a given bucket"
	lS3Cmd  = &cobra.Command{
		Use:    "lsS3",
		Short:  loshort,
		Long:   ``,
		Hidden: true,
		Run:    listS3,
	}

	backupCmd = &cobra.Command{
		Use:   "backupMoses",
		Short: "Command to backup MOSES",
		Long:  ``,
		Run:   backup,
	}
	prefix,outDir,delimiter   string
	maxKey           int64
	marker           string
	maxLoop, maxPage int
	missingoDir ="Missing output directory"

)

type UserMd struct {
	DocID      string `json:"docId"`
	PubDate    string `json:"pubDate"`
	SubPartFP  string `json:"subPartFP"`
	TotalPages string `json:"totalPages"`
}

func initBkFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&bucket, "bucket", "b", "", "the name of the bucket")
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", "key prefix")
	cmd.Flags().Int64VarP(&maxKey, "maxKey", "m", 40, "maximum number of keys to be processed concurrently")
	cmd.Flags().StringVarP(&marker, "marker", "M", "", "start processing from this key")
	cmd.Flags().StringVarP(&delimiter, "delimiter", "d", "", "key delimiter")
	cmd.Flags().StringVarP(&outDir, "outDir", "O", "", "output directory")
	// cmd.Flags().BoolVarP(&loop,"loop","L",false,"loop until all keys are processed")
	cmd.Flags().IntVarP(&maxPage, "maxPage", "", 50, "maximum number of concurrent pages ")
	cmd.Flags().IntVarP(&maxLoop, "maxLoop", "", 1, "maximum number of loop, 0 means no upper limit")

}

func init() {

	rootCmd.AddCommand(lS3Cmd)
	rootCmd.AddCommand(backupCmd)
	rootCmd.MarkFlagRequired("bucket")
	initBkFlags(lS3Cmd)
	initBkFlags(backupCmd)
}

func listS3(cmd *cobra.Command, args []string) {
	var (
		start       = utils.LumberPrefix(cmd)
		total int64 = 0
	)

	if len(bucket) == 0 {
		gLog.Warning.Printf("%s", missingBucket)
		// utils.Return(start)
		return
	}
	if len(outDir) ==0{
		gLog.Warning.Printf("%s", missingoDir)
		return
	}

	req := datatype.ListObjRequest{
		Service:   s3.New(api.CreateSession()),
		Bucket:    bucket,
		Prefix:    prefix,
		MaxKey:    maxKey,
		Marker:    marker,
		Delimiter: delimiter,
	}
	L := 1
	for {
		var (
			nextmarker string
			result     *s3.ListObjectsOutput
			err        error
		)
		if result, err = api.ListObject(req); err == nil {
			if l := len(result.Contents); l > 0 {
				total += int64(l)
				for _, v := range result.Contents {
					gLog.Info.Printf("Key: %s - Size: %d  - LastModified: %v", *v.Key, *v.Size, v.LastModified)
				}
				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					// nextmarker = *result.NextMarker
					gLog.Warning.Printf("Truncated %v  - Next marker : %s ", *result.IsTruncated, nextmarker)
				}
			}
		} else {
			gLog.Error.Printf("%v", err)
			break
		}
		L++
		if *result.IsTruncated && (maxLoop == 0 || L <= maxLoop) {
			req.Marker = nextmarker
		} else {
			gLog.Info.Printf("Total number of objects returned: %d", total)
			break
		}
	}
	utils.Return(start)
}

func backup(cmd *cobra.Command, args []string) {
	var (
		nextmarker string
		err        error
	)
	start := time.Now()
	if nextmarker, err = BackupBlobs(marker, bucket); err != nil {
		gLog.Error.Printf("error %v - Next marker %s", err, nextmarker)
	} else {
		gLog.Info.Printf("Next Marker %s", nextmarker)
	}
	gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))

}

/* S3 API list user metadata  function */
func BackupBlobs(marker string, bucket string) (string,  error) {

	var (
		nextmarker string
		N  int
		tdocs,tpages,tsizes int64
		terrors int
		mu     sync.Mutex
		mt     sync.Mutex
	)

	req := datatype.ListObjRequest{
		Service:   s3.New(api.CreateSession()),
		Bucket:    bucket,
		Prefix:    prefix,
		MaxKey:    maxKey,
		Marker:    marker,
		Delimiter: delimiter,
	}
	for {
		var (
			// nextmarker string
			result *s3.ListObjectsOutput
			err    error
			ndocs  int = 0
			npages int  = 0
			docsizes int = 0
			gerrors int = 0
		)
		N++ // number of loop
		if result, err = api.ListObject(req); err == nil {
			gLog.Info.Println(bucket, len(result.Contents))

			if l := len(result.Contents); l > 0 {
				ndocs += int(l)
				var wg1 sync.WaitGroup
				wg1.Add(len(result.Contents))
				for _, v := range result.Contents {
					gLog.Trace.Printf("Key: %s - Size: %d  - LastModified: %v", *v.Key, *v.Size, v.LastModified)
					svc := req.Service
					head := datatype.StatObjRequest{
						Service: svc,
						Bucket:  req.Bucket,
						Key:     *v.Key,
					}
					go func(request datatype.StatObjRequest) {
						var (
							rh = datatype.Rh{
								Key: head.Key,
							}
							np, status,docsize int
							err    error
							usermd string
						)
						defer wg1.Done()
						rh.Result, rh.Err = api.StatObject(head)
						if usermd, err = utils.GetUserMeta(rh.Result.Metadata); err == nil {
							userm := UserMd{}
							json.Unmarshal([]byte(usermd), &userm)
							pn := rh.Key
							if np, err = strconv.Atoi(userm.TotalPages); err == nil {
								if document,nerrors := clone.GetBlobs(pn, np, maxPage); nerrors == 0 {
									clone.WriteDocument(pn, document, outDir)
								} else {
									mt.Lock()
									gerrors += nerrors
									mt.Unlock()
								}
							} else {
								gLog.Error.Printf("Document %s - Invalid number of pages in %s ", pn, usermd)
								if np, err, status = clone.GetPageNumber(pn); err == nil {
									if document,nerrors := clone.GetBlobs(pn, np, maxPage); nerrors == 0 {
										clone.WriteDocument(pn, document, outDir)
									} else {
										mt.Lock()
										gerrors += nerrors
										mt.Unlock()
									}
								} else {
									gLog.Error.Printf(" Error %v - Status Code: %v  - Getting number of pagess for %s ", err, status, pn)
									mt.Lock()
									gerrors += 1
									mt.Unlock()
								}
							}
						}
						mu.Lock()
						npages += np
						docsizes += docsize
						mu.Unlock()
						// utils.PrintUsermd(rh.Key, rh.Result.Metadata)
					}(head)
				}
				wg1.Wait()
				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					gLog.Warning.Printf("Truncated %v - Next marker: %s ", *result.IsTruncated, nextmarker)

				}
				gLog.Info.Printf("Total number of documents returned: %d  - total number of pages: %d  - Total document size: %d - Total number of errors: %d", ndocs,npages,docsizes,gerrors)
				tdocs += int64(ndocs)
				tpages += int64(npages)
				tsizes += int64(docsizes)
				terrors += gerrors
			}
		} else {
			gLog.Error.Printf("%v", err)
			break
		}
		if N < maxLoop && *result.IsTruncated {
			req.Marker = nextmarker
		} else {
			gLog.Info.Printf("Total number of objects returned: %d  - total number of pages: %d  - Total document size: %d - Total number of errors: %d", tdocs,tpages,tsizes,terrors)
			break
		}
	}
	return nextmarker, nil
}
