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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	base64 "github.com/paulmatencio/ring/user/base64j"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/gLog"
	mosesbc "github.com/paulmatencio/s3c/moses-bc/lib"
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

const CHUNKSIZE =262144

var (
	pn, iDir   string
	restoreCmd = &cobra.Command{
		Use:   "restore",
		Short: "Command to restore Moses",
		Long:  ``,
		Run:   restore,
	}
	replace bool
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
	cmd.Flags().BoolVarP(&replace, "replace", "r", false, "replace existing pages")
	cmd.Flags().Int64VarP(&maxPartSize, "maxPartSize", "", 40, "Maximum partsize (MB) for multipart download")
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

	if nextMarker, err = restoreBlobs(marker, bbucket,replace); err != nil {
		gLog.Error.Printf("error %v - Next marker %s", err, nextMarker)
	} else {
		gLog.Info.Printf("Next Marker %s", nextMarker)
	}
	gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))
}

func restoreBlobs(marker string, bucket string, replace bool) (string, error) {

	var (
		nextmarker            string
		N                     int
		tdocs, tpages, tsizes int64
		terrors               int
		re,si                    sync.Mutex
	)

	req := datatype.ListObjRequest{
		Service:   svcb,
		Bucket:    bucket,
		Prefix:    prefix,
		MaxKey:    maxKey,
		Marker:    marker,
		Delimiter: delimiter,
	}
	start0 := time.Now()
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
			gLog.Info.Printf("Backup bucket %s - target metadata bucket %s - number of documents: %d", bbucket, mbucket, len(result.Contents))
			if l := len(result.Contents); l > 0 {
				ndocs += int(l)
				var wg1 sync.WaitGroup
				//wg1.Add(len(result.Contents))
				start := time.Now()
				for _, v := range result.Contents {
					svc := req.Service
					request := datatype.GetObjRequest{
						Service: svc,
						Bucket:  req.Bucket,
						Key:     *v.Key,
					}
					wg1.Add(1)
					go func(request datatype.GetObjRequest,replace bool) {
						var (
							err      error
							usermd   string
							result   *s3.GetObjectOutput
							document *documentpb.Document
						)
						gLog.Info.Printf("Restoring document: %s from backup bucket %s ", request.Key,request.Bucket)
						defer wg1.Done()
						start2 := time.Now()
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
							gLog.Info.Printf("Get Object key  %s - Elapsed time %v ",*v.Key,time.Since(start2))
							/*
								retrieve the backup document
							 */
							start3:= time.Now()
							if body, err := utils.ReadObjectv(result.Body,CHUNKSIZE); err == nil  {
								document, err = mosesbc.GetDocument(body.Bytes())
								pd := document.Pdf
								if len(pd.Pdf) > 0 {
									/*   restore the pdf first document first     */
									if nerr,status := mosesbc.WriteDocPdf(pd,replace); nerr == 0 {
										if status == 200 {
											gLog.Info.Printf("Document pdf %s  has been restored - Size %d",pd.PdfId,pd.Size)
										} else {
											gLog.Info.Printf("Document pdf %s  is not restored - Status %d",pd.PdfId,status)
										}
									} else {
										gLog.Info.Printf("Document pdf %s is not restored - Check the error within WriteDocPdf ",pd.PdfId)
									}
								}
								gLog.Info.Printf("Document id %s is retrieved - Number of pages %d - Document size %d - Elapsed time %v ",document.DocId,document.NumberOfPages,document.Size,time.Since(start3))
								/*
									restoring the document
								 */
								start4:= time.Now()
								nerr := 0
								if document.NumberOfPages <= int32(maxPage) {
									nerr = mosesbc.PutBlob1(document,replace)
								} else {
									nerr = mosesbc.PutBig1(document, maxPage,replace)
								}
								si.Lock()
									npages += (int)(document.NumberOfPages)
									docsizes += int (document.Size)
								si.Unlock()

								/*
									if loading error, increment the general error counter
								 */

								if nerr > 0 {
									gLog.Info.Printf("Document id %s is not fully restored  because of %d errors - Number of pages %d - Document size %d - Elapsed time %v ",document.DocId,nerr,document.NumberOfPages,document.Size,time.Since(start4))
									re.Lock()
									gerrors += nerr
									re.Unlock()
								} else {
									gLog.Info.Printf("Document id %s is fully restored - Number of pages %d - Document size %d - Elapsed time %v ",document.DocId,document.NumberOfPages,document.Size,time.Since(start4))
									/* start  indexing */
									start5:= time.Now()
									if _,err = indexDocument(document, mbucket, svcm); err != nil {
										gLog.Error.Printf("Error %v adding document id %s to bucket %s",err,document.DocId,mbucket)
										re.Lock()
										gerrors += 1
										re.Unlock()
									} else {
										gLog.Info.Printf("Document id %s is indexed - bucket %s - Elapsed time %v",document.DocId,mbucket,time.Since(start5))
									}
								}
								/*
								indexing the document
								 */

							} else {
								gLog.Error.Printf("Error %v reading document body", err)
								re.Lock()
								gerrors += 1
								re.Unlock()
							}
						}
					}(request,replace)
				}
				wg1.Wait()
				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					gLog.Warning.Printf("Truncated %v - Next marker: %s ", *result.IsTruncated, nextmarker)
				}
				ndocs = ndocs - gerrors
				gLog.Info.Printf("Number of documents restored: %d  - Number of pages: %d  - Documents size: %d - Number of errors: %d -  Elapsed time: %v", ndocs, npages, docsizes, gerrors,time.Since(start))
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
			gLog.Info.Printf("Total number of documents restored: %d  - total number of pages: %d  - Total document size: %d - Total number of errors: %d - Total elapsed time: %v", tdocs, tpages, tsizes, terrors,time.Since(start0))
			break
		}
	}
	return nextmarker, nil
}

func indexDocument(document *documentpb.Document, bucket string, svc *s3.S3) (*s3.PutObjectOutput,error) {
	var (
		data = make([]byte, 0, 0) // empty byte array
		putReq     = datatype.PutObjRequest{
			Service: svc,
			Bucket:  bucket,
			Key:     document.GetDocId(),
			Buffer: bytes.NewBuffer(data), // convert []byte into *bytes.Buffer
			Meta : []byte(document.GetS3Meta()),
		}
	)
	return  api.PutObject(putReq);
}
/*
	Write the document to a file
 */
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
		pfd := strings.Replace(pn, "/", "_", -1) + "_" + fmt.Sprintf("%04d", page.GetPageNumber())
		if fi, err := os.OpenFile(filepath.Join(outDir, pfd), os.O_WRONLY|os.O_CREATE, 0600); err == nil {
			defer fi.Close()
			bytes := page.GetObject()
			if _, err := fi.Write(bytes); err != nil {
				fmt.Printf("Error %v writing file %s to output directory %s", err, pfd, outDir)
			}
		} else {
			gLog.Error.Printf("Error opening file %s/%s", outDir, pfd)
		}

		pfm := pfd + ".md"
		if fm, err := os.OpenFile(filepath.Join(outDir, pfm), os.O_WRONLY|os.O_CREATE, 0600); err == nil {
			defer fm.Close()
			if usermd, err := base64.Decode64(page.GetMetadata()); err == nil {
				if _, err := fm.Write(usermd); err != nil {
					fmt.Printf("Error %v writing page %s", err, pfm)
				}
			} else {
				gLog.Error.Printf("Error %v decoding user metadata",err)
			}
		} else {
			gLog.Error.Printf("Error opening file %s/%s", outDir, pfm)
		}
	}
}
