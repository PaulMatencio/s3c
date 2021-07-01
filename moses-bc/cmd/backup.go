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
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	mosesbc "github.com/paulmatencio/s3c/moses-bc/lib"
	"google.golang.org/protobuf/types/known/timestamppb"

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
	backupCmd = &cobra.Command{
		Use:   "backup",
		Short: "Command to backup MOSES objects",
		Long: `Command to backup MOSES objects
Moses metadata buckets are split into 6 buckets ( -xx ) 
	<bucket>-xx  xx=00..05 
	The source and target bucket 
Backup all the documents prefixed with FR. The find out  the source bucket suffix, use the hash command
	moses-bc backup --source-bucket <source bucket> --target-bucket <target bucket> --prefix FR/ --maxLoop 0 
Backup all the documents listed in a given bucket 
	moses-bc backup --source-bucket <source bucket> --target-bucket <target bucket>  --maxLoop 0 
`,
		Run: Backup,
	}
	prefix, inFile, outDir, delimiter string
	maxPartSize, maxKey               int64
	marker                            string
	srcBucket, tgtBucket              string
	maxLoop, maxPage                  int
	missingoDir                       = "Missing output directory --output-directory"
	missingiFile                      = "Missing input file --input-file"
	missingsrcBucket                  = "Missing source S3 bucket --source-bucket"
	missingtgtBucket                  = "Missing target S3 bucket --target-bucket"
	// back, meta                datatype.CreateSession
	tgtS3, srcS3 *s3.S3
)

type UserMd struct {
	FpClipping string `json:"fpClipping,omitempty"`
	DocID      string `json:"docId"`
	PubDate    string `json:"pubDate"`
	SubPartFP  string `json:"subPartFP"`
	TotalPages string `json:"totalPages"`
}

func initBkFlags(cmd *cobra.Command) {

	cmd.Flags().StringVarP(&srcBucket, "source-bucket", "", "", "name of source s3 bucket")
	cmd.Flags().StringVarP(&tgtBucket, "target-bucket", "", "", "name of the target s3 bucket")
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", "key's prefix; key= moses-document in the form of cc/pn/kc")
	cmd.Flags().Int64VarP(&maxKey, "maxKey", "m", 20, "maximum number of moses documents  to be cloned concurrently")
	cmd.Flags().StringVarP(&marker, "marker", "M", "", "start processing from this key; key= moses-document in the form of cc/pn/kc")
	cmd.Flags().IntVarP(&maxPage, "maxPage", "", 50, "maximum number of concurrent moses pages to be concurrently procsessed")
	cmd.Flags().IntVarP(&maxLoop, "maxLoop", "", 1, "maximum number of loop, 0 means no upper limit")
	cmd.Flags().StringVarP(&inFile, "input-file", "i", "", "input file containing the list of documents to backup")
	cmd.Flags().StringVarP(&outDir, "output-directory", "o", "", "output directory for --backupMedia = File")
	cmd.Flags().Int64VarP(&maxPartSize, "maxPartSize", "", 40, "Maximum partsize (MB)  for multipart upload")
	cmd.Flags().StringVarP(&srcUrl, "source-sproxyd-url", "s", "", "source sproxyd endpoints  http://xx.xx.xx.xx:81/proxy,http://xx.xx.xx.xx:81/proxy")
	cmd.Flags().StringVarP(&driver, "source-sproxyd-driver", "", "", "source sproxyd driver [bpchord|bparc]")

}

func init() {
	rootCmd.AddCommand(backupCmd)
	initBkFlags(backupCmd)
	// viper.BindPFlag("maxPartSize",rootCmd.PersistentFlags().Lookup("maxParSize"))
}

func Backup(cmd *cobra.Command, args []string) {

	var (
		nextmarker string
		err        error
		listpn     *bufio.Scanner
	)
	start := time.Now()

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

	srcS3 = mosesbc.CreateS3Session("backup", "source")
	maxPartSize = maxPartSize * 1024 * 1024

	mosesbc.SetSourceSproxyd("backup", srcUrl, driver)

	if bMedia == "S3" {

		if len(tgtBucket) == 0 {
			gLog.Warning.Printf("%s", missingtgtBucket)
			return
		}

		if err := mosesbc.CheckBucketName(srcBucket, tgtBucket); err != nil {
			gLog.Warning.Printf("%v", err)
			return
		}
		tgtS3 = mosesbc.CreateS3Session("backup", "target")
	} else {
		if len(outDir) == 0 {
			gLog.Warning.Printf("%s", missingoDir)
			return
		}
		//  create the output directory if it does not exist
		utils.MakeDir(outDir)
	}
	utils.MakeDir(outDir)

	// start backing up
	if nextmarker, err = _backupBlobs(marker, srcS3, srcBucket, listpn, tgtS3, tgtBucket); err != nil {
		gLog.Error.Printf("error %v - Next marker %s", err, nextmarker)
	} else {
		gLog.Info.Printf("Next Marker %s", nextmarker)
	}
	gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))

}

/* S3 API list user metadata  function */

func _backupBlobs(marker string, srcS3 *s3.S3, srcBucket string, listpn *bufio.Scanner, tgtS3 *s3.S3, tgtBucket string) (string, error) {

	var (
		nextmarker            string
		N                     int
		tdocs, tpages, tsizes int64
		terrors               int
		mu                    sync.Mutex
		mt                    sync.Mutex
	)

	req1 := datatype.ListObjRequest{
		Service: srcS3,
		Bucket:  srcBucket,
		Prefix:  prefix,
		MaxKey:  int64(maxKey),
		Marker:  marker,
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
		if len(srcBucket) > 0 {
			result, err = api.ListObject(req1)
			gLog.Info.Printf("target bucket %s - source metadata bucket %s - number of documents: %d", tgtBucket, srcBucket, len(result.Contents))
		} else {
			result, err = ListPn(listpn, int(maxKey))
			gLog.Info.Printf("target bucket %s - input file %s - number of documents: %d", tgtBucket, inFile, len(result.Contents))
		}
		if err == nil {
			if l := len(result.Contents); l > 0 {
				ndocs += int(l)
				var wg1 sync.WaitGroup
				wg1.Add(len(result.Contents))
				for _, v := range result.Contents {
					gLog.Trace.Printf("Key: %s - Size: %d  - LastModified: %v", *v.Key, *v.Size, v.LastModified)
					svc := req1.Service
					request := datatype.StatObjRequest{
						Service: svc,
						Bucket:  req1.Bucket,
						Key:     *v.Key,
					}
					go func(request datatype.StatObjRequest) {

						var (
							rh = datatype.Rh{
								Key: request.Key,
							}
							np, status, docsize, npage int
							err                        error
							usermd                     string
						)

						defer wg1.Done()

						rh.Result, rh.Err = api.StatObject(request)
						if usermd, err = utils.GetUserMeta(rh.Result.Metadata); err == nil {
							userm := UserMd{}
							json.Unmarshal([]byte(usermd), &userm)
							pn := rh.Key
							if np, err = strconv.Atoi(userm.TotalPages); err == nil {
								if errs, document := mosesbc.BackBlob1(pn, np, maxPage); len(errs) == 0 {
									document.S3Meta = usermd
									switch bMedia {
									case "s3":
										/*
											if err, docsize = mosesbc.WriteDirectory(pn, document, outDir); err != nil {
												gLog.Error.Printf("Error:%v writing document: %s to directory %s", err, document.DocId, outDir)
												mt.Lock()
												gerrors += 1
												mt.Unlock()
											} else {
												docsize = (int)(document.Size)
												npage = (int)(document.NumberOfPages)
											}

										*/
										if _, err := writeS3(tgtS3, tgtBucket, maxPartSize, document); err != nil {
											gLog.Error.Printf("Error:%v writing document: %s to bucket %s", err, document.DocId, bucket)
											mt.Lock()
											gerrors += 1
											mt.Unlock()
										} else {
											docsize = (int)(document.Size)
											npage = (int)(document.NumberOfPages)
											// gLog.Trace.Printf("Docid: %s - Etag %v", document.DocId, so.ETag)
										}

									case "File":
										if err, docsize = mosesbc.WriteDirectory(pn, document, outDir); err != nil {
											gLog.Error.Printf("Error:%v writing document: %s to directory %s", err, document.DocId, outDir)
											mt.Lock()
											gerrors += 1
											mt.Unlock()
										} else {
											docsize = (int)(document.Size)
											npage = (int)(document.NumberOfPages)
										}
									default:
										//gLog.Info.Printf("bMedia option should be [S3|File]")
										if _, err := writeS3(tgtS3, tgtBucket, maxPartSize, document); err != nil {
											gLog.Error.Printf("Error:%v writing document: %s to bucket %s", err, document.DocId, bucket)
											mt.Lock()
											gerrors += 1
											mt.Unlock()
										} else {
											docsize = (int)(document.Size)
											npage = (int)(document.NumberOfPages)
											// gLog.Trace.Printf("Docid: %s - Etag %v", document.DocId, so.ETag)
										}
									}
									gLog.Trace.Printf("Docid: %s - number of pages: %d - Document metadata: %s", document.DocId, document.NumberOfPages, document.Metadata)
								} else {
									printErr(errs)
									mt.Lock()
									gerrors += len(errs)
									mt.Unlock()
								}
							} else {
								gLog.Error.Printf("Document %s - S3 Metadata has invalid number of pages in %s - Try to get it from the document user metadata ", pn, usermd)
								if np, err, status = mosesbc.GetPageNumber(pn); err == nil {
									if errs, document := mosesbc.BackBlob1(pn, np, maxPage); len(errs) == 0 {
										/*
											Add  s3 moses metadata to the document even if it may be  invalid from the source
											the purpose of the backup is not to fix  data
										*/
										document.S3Meta = usermd
										document.LastUpdated = timestamppb.Now()
										switch bMedia {
										case "S3":
											if _, err := writeS3(tgtS3, tgtBucket, maxPartSize, document); err != nil {
												gLog.Error.Printf("Error:%v writing document: %s to bucket %s", err, document.DocId, bucket)
												mt.Lock()
												gerrors += 1
												mt.Unlock()
											} else {
												docsize = (int)(document.Size)
												npage = (int)(document.NumberOfPages)
												// gLog.Trace.Printf("Docid: %s - Etag %v", document.DocId, so.ETag)
											}

										case "File":
											if err, docsize = mosesbc.WriteDirectory(pn, document, outDir); err != nil {
												gLog.Error.Printf("Error:%v writing document: %s to  directory %s", err, document.DocId, outDir)
												mt.Lock()
												gerrors += 1
												mt.Unlock()
											} else {
												docsize = (int)(document.Size)
												npage = (int)(document.NumberOfPages)
											}

										default:
											//gLog.Info.Printf("bMedia option should  is [S3|File]")
											if _, err := writeS3(tgtS3, tgtBucket, maxPartSize, document); err != nil {
												gLog.Error.Printf("Error:%v writing document: %s to bucket %s", err, document.DocId, bucket)
												mt.Lock()
												gerrors += 1
												mt.Unlock()
											} else {
												docsize = (int)(document.Size)
												npage = (int)(document.NumberOfPages)
												// gLog.Trace.Printf("Docid: %s - Etag %v", document.DocId, so.ETag)
											}
										}
									} else {
										/*
											some errors have been found by getBlob1
										*/
										printErr(errs)
										mt.Lock()
										gerrors += len(errs)
										mt.Unlock()
									}
								} else {
									gLog.Error.Printf(" Error %v - Status Code: %v  - Getting number of pages for %s ", err, status, pn)
									mt.Lock()
									gerrors += 1
									mt.Unlock()
								}
							}
						}
						mu.Lock()
						npages += npage
						docsizes += docsize
						mu.Unlock()
						// utils.PrintUsermd(rh.Key, rh.Result.Metadata)
					}(request)
				}
				wg1.Wait()

				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					gLog.Warning.Printf("Truncated %v - Next marker: %s ", *result.IsTruncated, nextmarker)

				}
				gLog.Info.Printf("Total number of backed up documents: %d  - total number of pages: %d  - Total document size: %d - Total number of errors: %d", ndocs, npages, docsizes, gerrors)
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
			req1.Marker = nextmarker
		} else {
			gLog.Info.Printf("Total number of backed up documents: %d - total number of pages: %d  - Total document size: %d - Total number of errors: %d", tdocs, tpages, tsizes, terrors)
			break
		}
	}
	return nextmarker, nil
}

func printErr(errs []error) {
	for _, e := range errs {
		gLog.Error.Println(e)
	}
}

func writeS3(service *s3.S3, bucket string, maxPartSize int64, document *documentpb.Document) (interface{}, error) {

	if maxPartSize > 0 && document.Size > maxPartSize {
		gLog.Warning.Printf("Multipart upload %s - size %d - max part size %d", document.DocId, document.Size, maxPartSize)
		return mosesbc.WriteS3Multipart(service, bucket, maxPartSize, document)
	} else {
		return mosesbc.WriteS3(service, bucket, document)
	}

}

func ListPn(buf *bufio.Scanner, num int) (*s3.ListObjectsOutput, error) {

	var (
		T            = true
		Z      int64 = 0
		D            = time.Now()
		result       = &s3.ListObjectsOutput{
			IsTruncated: &T,
		}
		err     error
		objects []*s3.Object
	)
	for k := 1; k <= num; k++ {
		var object s3.Object
		if buf.Scan() {
			if text := buf.Text(); len(text) > 0 {
				object.Key = &text
				object.Size = &Z
				object.LastModified = &D
				objects = append(objects, &object)
				result.Marker = &text
			} else {
				T = false
				result.IsTruncated = &T

			}
		} else {
			T = false
			result.IsTruncated = &T
		}
	}
	result.Contents = objects
	return result, err
}
