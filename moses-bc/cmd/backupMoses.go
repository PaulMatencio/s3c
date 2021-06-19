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
	clone "github.com/paulmatencio/s3c/moses-bc/lib"
	"github.com/spf13/viper"

	"errors"
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
		Use:   "backup",
		Short: "Command to backup MOSES",
		Long:  ``,
		Run:   backup,
	}
	prefix, outDir, delimiter string
	maxKey                    int64
	marker, mbucket, bbucket  string
	maxLoop, maxPage          int
	missingoDir               = "Missing backup output directory"
	missingbBucket            = "Missing backup bucket"
	back,meta                datatype.CreateSession
	svcb, svcm                *s3.S3
)

type UserMd struct {
	DocID      string `json:"docId"`
	PubDate    string `json:"pubDate"`
	SubPartFP  string `json:"subPartFP"`
	TotalPages string `json:"totalPages"`
}

func initBkFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&mbucket, "mbucket", "b", "", "the name of the metadata bucket")
	cmd.Flags().StringVarP(&bbucket, "bbucket", "t", "", "the name of the backup bucket")
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", "key prefix")
	cmd.Flags().Int64VarP(&maxKey, "maxKey", "m", 40, "maximum number of keys to be processed concurrently")
	cmd.Flags().StringVarP(&marker, "marker", "M", "", "start processing from this key")
	cmd.Flags().StringVarP(&delimiter, "delimiter", "d", "", "key delimiter")
	cmd.Flags().StringVarP(&outDir, "outDir", "O", "", "output directory for --backupMedia = File")
	cmd.Flags().IntVarP(&maxPage, "maxPage", "", 50, "maximum number of concurrent pages")
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
	/*
		if len(outDir) ==0 {
			gLog.Warning.Printf("%s", missingoDir)
			return
		}
	*/

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

	if metaUrl = viper.GetString("meta.s3.url"); len(metaUrl) == 0 {
		gLog.Error.Println(errors.New(missingMetaurl))
		return
	}

	if metaAccessKey = viper.GetString("meta.s3.access_key_id"); len(bS3AccessKey) == 0 {
		gLog.Error.Println(errors.New(missingMetaak))
		return
	}

	if metaSecretKey = viper.GetString("meta.s3.secret_access_key"); len(bS3SecretKey) == 0 {
		gLog.Error.Println(errors.New(missingMetask))
		return
	}

	meta = datatype.CreateSession{
		Region:    viper.GetString("meta.s3.region"),
		EndPoint:  metaUrl,
		AccessKey: metaAccessKey,
		SecretKey: metaSecretKey,
	}
	svcm= s3.New(api.CreateSession2(meta))

	if bMedia == "S3" {

		if len(bbucket) == 0 {
			gLog.Warning.Printf("%s", missingoDir)
			return
		}

		if bS3Url = viper.GetString("backup.s3.url"); len(bS3Url) == 0 {
			gLog.Error.Println(errors.New(missingBS3url))
			return
		}

		if bS3AccessKey = viper.GetString("backup.s3.access_key_id"); len(bS3AccessKey) == 0 {
			gLog.Error.Println(errors.New(missingBS3ak))
			return
		}

		if bS3SecretKey = viper.GetString("backup.s3.secret_access_key"); len(bS3SecretKey) == 0 {
			gLog.Error.Println(errors.New(missingBS3sk))
			return
		}
		//  create the
		back = datatype.CreateSession{
			Region:    viper.GetString("backup.s3.region"),
			EndPoint:  bS3Url,
			AccessKey: bS3AccessKey,
			SecretKey: bS3SecretKey,
		}
		svcb = s3.New(api.CreateSession2(back))

	} else {
		if len(outDir) == 0 {
			gLog.Warning.Printf("%s", missingoDir)
			return
		}
		//  create the output directory if it does not exist
		utils.MakeDir(outDir)
	}
	// start backing up
	if nextmarker, err = BackupBlobs(marker, mbucket); err != nil {
		gLog.Error.Printf("error %v - Next marker %s", err, nextmarker)
	} else {
		gLog.Info.Printf("Next Marker %s", nextmarker)
	}
	gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))

}

/* S3 API list user metadata  function */

func BackupBlobs(marker string, bucket string) (string, error) {

	var (
		nextmarker            string
		N                     int
		tdocs, tpages, tsizes int64
		terrors               int
		mu                    sync.Mutex
		mt                    sync.Mutex
	)

	req := datatype.ListObjRequest{
		// Service:   s3.New(api.CreateSession()),
		Service:  svcm,
		Bucket:    bucket,
		Prefix:    prefix,
		MaxKey:    maxKey,
		Marker:    marker,
		Delimiter: delimiter,
	}
	for {
		var (
			result   *s3.ListObjectsOutput
			err      error
			ndocs    int = 0
			npages   int = 0
			docsizes int = 0
			gerrors  int = 0
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
							np, status, docsize int
							err                 error
							usermd              string
						)
						defer wg1.Done()
						rh.Result, rh.Err = api.StatObject(head)
						if usermd, err = utils.GetUserMeta(rh.Result.Metadata); err == nil {
							userm := UserMd{}
							json.Unmarshal([]byte(usermd), &userm)
							pn := rh.Key
							if np, err = strconv.Atoi(userm.TotalPages); err == nil {
								if errs, document := clone.GetBlob1(pn, np, maxPage); len(errs) == 0 {
									document.S3Meta = usermd
									if bMedia != "S3" {
										docsize = clone.WriteDirectory(pn, document, outDir)
									} else {
										clone.WriteS3(svcb, bucket, document)
									}
									gLog.Trace.Printf("Docid: %s - number of pages: %d - Document metadata: %s", document.DocId, document.NumberOfPages, document.Metadata)
								} else {
									printErr(errs)
									mt.Lock()
									gerrors += len(errs)
									mt.Unlock()
								}
							} else {
								gLog.Error.Printf("Document %s - Invalid number of pages in %s ", pn, usermd)
								if np, err, status = clone.GetPageNumber(pn); err == nil {
									if errs, document := clone.GetBlob1(pn, np, maxPage); len(errs) == 0 {
										//  Add  s3 moses meta to the document even if it may be  invalid
										document.S3Meta = usermd
										if bMedia != "S3" {
											docsize = clone.WriteDirectory(pn,  document, outDir)
										} else {
											clone.WriteS3(svcb, bucket, document)
										}

									} else {
										printErr(errs)
										mt.Lock()
										gerrors += len(errs)
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
				gLog.Info.Printf("Total number of documents returned: %d  - total number of pages: %d  - Total document size: %d - Total number of errors: %d", ndocs, npages, docsizes, gerrors)
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
			gLog.Info.Printf("Total number of documents returned: %d  - total number of pages: %d  - Total document size: %d - Total number of errors: %d", tdocs, tpages, tsizes, terrors)
			break
		}
	}
	return nextmarker, nil
}

func printErr(errs []error) {
	for _,e := range errs {
		gLog.Error.Println(e)
	}
}