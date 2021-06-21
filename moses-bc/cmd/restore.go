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
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	base64 "github.com/paulmatencio/ring/user/base64j"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/gLog"
	clone "github.com/paulmatencio/s3c/moses-bc/lib"
	"os"
	"path/filepath"
	"strings"
	// "strconv"
	"sync"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	// "os"
	//"path/filepath"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/utils"
	"time"
)

// restoreMosesCmd represents the restoreMoses command
var (
	pn, iDir   string
	restoreCmd = &cobra.Command{
		Use:   "restore",
		Short: "Command to restore Moses",
		Long:  ``,
		Run:   restore,
	}
)

func initResFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&bbucket, "bbucket", "b", "", "the name of the backup bucket")
	cmd.Flags().StringVarP(&mbucket, "mbucket", "t", "", "the name of the metadata bucket to be restored")
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", "key prefix")
	cmd.Flags().Int64VarP(&maxKey, "maxKey", "m", 100, "maximum number of keys to be restored concurrently")
	cmd.Flags().StringVarP(&marker, "marker", "M", "", "start processing from this key")
	cmd.Flags().StringVarP(&delimiter, "delimiter", "d", "", "key delimiter")
	cmd.Flags().StringVarP(&iDir, "inDir", "I", "", "input directory")
	cmd.Flags().StringVarP(&outDir, "outDir", "O", "", "output directory")
	cmd.Flags().StringVarP(&pn, "pn", "k", "", "publication number to be restored")
	cmd.Flags().IntVarP(&maxPage, "maxPage", "", 50, "maximum number of concurrent pages ")
	cmd.Flags().IntVarP(&maxLoop, "maxLoop", "", 1, "maximum number of loop, 0 means no upper limit")
}

func init() {
	rootCmd.AddCommand(restoreCmd)
	initResFlags(restoreCmd)
}

func restore(cmd *cobra.Command, args []string) {
	var (
		nextMarker string
		err        error
	)
	start := time.Now()
	// OUTPUT
	if metaUrl = viper.GetString("meta.s3.url"); len(metaUrl) == 0 {
		gLog.Error.Println(errors.New(missingMetaurl))
		return
	}
	if metaAccessKey = viper.GetString("meta.credential.access_key_id"); len(metaAccessKey) == 0 {
		gLog.Error.Println(errors.New(missingMetaak))
		return
	}

	if metaSecretKey = viper.GetString("meta.credential.secret_access_key"); len(metaSecretKey) == 0 {
		gLog.Error.Println(errors.New(missingMetask))
		return
	}

	meta = datatype.CreateSession{
		Region:    viper.GetString("meta.s3.region"),
		EndPoint:  metaUrl,
		AccessKey: metaAccessKey,
		SecretKey: metaSecretKey,
	}
	svcm = s3.New(api.CreateSession2(meta))

	// INPUT
	if bMedia == "S3" {

		if len(bbucket) == 0 {
			gLog.Warning.Printf("%s", missingbBucket)
			return
		}

		if bS3Url = viper.GetString("backup.s3.url"); len(bS3Url) == 0 {
			gLog.Error.Println(errors.New(missingBS3url))
			return
		}

		if bS3AccessKey = viper.GetString("backup.credential.access_key_id"); len(bS3AccessKey) == 0 {
			gLog.Error.Println(errors.New(missingBS3ak))
			return
		}

		if bS3SecretKey = viper.GetString("backup.credential.secret_access_key"); len(bS3SecretKey) == 0 {
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
		if len(iDir) == 0 {
			gLog.Warning.Printf("%s", "missing input directory")
			return
		}
	}
	//  create the output directory if it does not exist
	utils.MakeDir(outDir)

	if nextMarker, err = RestoreBlobs(marker, bbucket); err != nil {
		gLog.Error.Printf("error %v - Next marker %s", err, nextMarker)
	} else {
		gLog.Info.Printf("Next Marker %s", nextMarker)
	}
	gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))

}

func RestoreBlobs(marker string, bucket string) (string, error) {

	var (
		nextmarker            string
		N                     int
		tdocs, tpages, tsizes int64
		terrors               int
		re                    sync.Mutex
	)

	req := datatype.ListObjRequest{
		// Service:   s3.New(api.CreateSession()),
		Service:   svcb,
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
					request := datatype.GetObjRequest{
						Service: svc,
						Bucket:  req.Bucket,
						Key:     *v.Key,
					}
					go func(request datatype.GetObjRequest) {
						var (
							err      error
							usermd   string
							result   *s3.GetObjectOutput
							document *documentpb.Document
						)
						defer wg1.Done()
						if result, err = api.GetObject(request); err != nil {
							if aerr, ok := err.(awserr.Error); ok {
								switch aerr.Code() {
								case s3.ErrCodeNoSuchKey:
									gLog.Warning.Printf("Error: [%v]  Error: [%v]", s3.ErrCodeNoSuchKey, aerr.Error())
								default:
									gLog.Error.Printf("Error: %v", aerr.Error())
									re.Lock()
									gerrors += 1
									re.Unlock()

								}
							} else {
								gLog.Error.Printf("Error:%v", err.Error())
								re.Lock()
								gerrors += 1
								re.Unlock()
							}
						} else {
							if usermd, err = utils.GetUserMeta(result.Metadata); err == nil {
								userm := UserMd{}
								json.Unmarshal([]byte(usermd), &userm)
							} else {
								gLog.Error.Printf("Error %v - invalid user metadata %s", err, result.Metadata)
							}
							if body, err := utils.ReadObject(result.Body); err == nil {
								document, err = clone.GetDocument(body.Bytes())
								// WriteDocumentToFile(document,request.Key,outDir)
								if document.NumberOfPages <= int32(maxPage) {
									if nerr := clone.PutBlob1(document); nerr > 0 {
										re.Lock()
										gerrors += nerr
										re.Unlock()
									}
								} else {
									if nerr := clone.PutBig1(document,maxPage); nerr > 0 {
										re.Lock()
										gerrors += nerr
										re.Unlock()
									}

								}
							} else {
								gLog.Error.Printf("Error %v reading body", err)
								re.Lock()
								gerrors += 1
								re.Unlock()
							}
						}

					}(request)
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

		if *result.IsTruncated && (maxLoop == 0 || N <= maxLoop) {
			req.Marker = nextmarker
		} else {
			gLog.Info.Printf("Total number of documents returned: %d  - total number of pages: %d  - Total document size: %d - Total number of errors: %d", tdocs, tpages, tsizes, terrors)
			break
		}
	}
	return nextmarker, nil
}

func WriteDocumentToFile(document *documentpb.Document, pn string, outDir string) {

	var (
		err    error
		usermd []byte
	)

	gLog.Info.Printf("Document id %s - Page Numnber %d ", document.DocId, document.PageNumber)
	if usermd, err = base64.Decode64(document.GetMetadata()); err == nil {
		gLog.Info.Printf("Document metadata %s", string(usermd))
	}
	// write document metadata
	pnm := pn + ".md"
	if fi, err := os.OpenFile(filepath.Join(outDir, pnm), os.O_WRONLY|os.O_CREATE, 0600); err == nil {
		defer fi.Close()
		if _, err := fi.Write(usermd); err != nil {
			fmt.Printf("Error %v writing Document metadat %s", err, pnm)
		}
	}

	// write s3 moses meta
	pnm = pn + ".meta"
	if fi, err := os.OpenFile(filepath.Join(outDir, pnm), os.O_WRONLY|os.O_CREATE, 0600); err == nil {
		defer fi.Close()
		if _, err := fi.Write([]byte(document.GetS3Meta())); err != nil {
			fmt.Printf("Error %v writing s3 moses metada %s", err, pnm)
		} else {
			gLog.Error.Println(err)
		}
	}

	pages := document.GetPage()
	gLog.Info.Printf("Number of pages %d", len(pages))
	if len(pages) != int(document.NumberOfPages) {
		gLog.Error.Printf("Backup of document is inconsistent %s  %d - %d ", pn, len(pages), document.NumberOfPages)
		return
	}
	for _, page := range pages {

		//object := page.GetObject()
		// pn = strings.Replace(pn,"/","_",-1)
		pfd := strings.Replace(pn, "/", "_", -1) + "_" + fmt.Sprintf("%04d", page.GetPageNumber())
		if fi, err := os.OpenFile(filepath.Join(outDir, pfd), os.O_WRONLY|os.O_CREATE, 0600); err == nil {
			defer fi.Close()
			bytes := page.GetObject()
			if _, err := fi.Write(bytes); err != nil {
				fmt.Printf("Error %v writing file %s to output directory %s", err, pfd, outDir)
			}
		} else {
			gLog.Error.Println("Error opening file %s/%s", outDir, pfd)
		}

		pfm := pfd + ".md"
		if fm, err := os.OpenFile(filepath.Join(outDir, pfm), os.O_WRONLY|os.O_CREATE, 0600); err == nil {
			defer fm.Close()
			// meta:= page.GetMetadata()
			if usermd, err := base64.Decode64(page.GetMetadata()); err == nil {
				if _, err := fm.Write(usermd); err != nil {
					fmt.Printf("Error %v writing page %s", err, pfm)
				}
			} else {
				gLog.Error.Println(err)
			}
		} else {
			gLog.Error.Println(err)
		}
	}

}

