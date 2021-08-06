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
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmatencio/protobuf-doc/src/document/documentpb"
	"github.com/paulmatencio/s3c/api"
	"github.com/paulmatencio/s3c/datatype"
	"github.com/paulmatencio/s3c/gLog"
	mosesbc "github.com/paulmatencio/s3c/moses-bc/lib"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"sync"
	"time"
)

// restoreMosesCmd represents the restoreMoses command

const CHUNKSIZE = 262144

var (
	pn, iDir, versionId string
	partNumber int64
	maxCon  int
	restoreCmd          = &cobra.Command{
		Use:   "_restore_",
		Short: "Command to restore Moses objects to another Scality Ring or S3 objects",
		Long: `Command to restore Moses documents which are previously backed up with the backup command
	Examples
	restore --prefix ES/123 --source-bucket meta-moses-bkp-pn-02 --target-bucket meta-moses-prod-pn-02 
	will restore documents whose document id prefixed with  ES/123. 
	
	Use the suffix command to obtain the suffix of a  bucket for a given prefix. Ex suffix ES/123 will return 02

	restore --source-bucket meta-moses-bkp-pn-02 --target-bucket meta-moses-prod-pn-02 will restore  
	all the documents which are backed up and stored in the meta-moses-bkp-pn-02 bucket
                 `,
		Run:    Restore_bucket,
		Hidden: true,
	}
	replace bool
)

func initResFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&srcBucket, "source-bucket", "", "", "name of the S3 backup bucket")
	cmd.Flags().StringVarP(&tgtBucket, "target-bucket", "", "", "name of the target metadata bucket")
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", "key prefix")
	cmd.Flags().Int64VarP(&maxKey, "max-key", "m", 20, "maximum number of keys to be restored concurrently")
	cmd.Flags().StringVarP(&marker, "marker", "M", "", "start processing from this key")
	cmd.Flags().StringVarP(&delimiter, "delimiter", "d", "", "key delimiter")
	cmd.Flags().StringVarP(&pn, "key", "k", "", "publication number to be restored")
	cmd.Flags().StringVarP(&versionId, "versionId", "", "", "Version id of the publication number to be restored - default the last version will be restored ")
	cmd.Flags().StringVarP(&inFile, "input-file", "i", "", "input file containing the list of documents to restore")
	cmd.Flags().IntVarP(&maxPage, "max-page", "", 50, "maximum number of concurrent pages ")
	cmd.Flags().IntVarP(&maxLoop, "max-loop", "", 1, "maximum number of loop, 0 means no upper limit")
	cmd.Flags().BoolVarP(&replace, "replace", "r", false, "replace existing pages if exist")
	cmd.Flags().Int64VarP(&maxPartSize, "max-part-size", "", 40, "Maximum partsize (MB) for multipart download")
	cmd.Flags().Int64VarP(&partNumber, "part-number", "", 0, "Part number")
	cmd.Flags().IntVarP(&maxCon, "max-con", "", 5, "Maximum concurrent parts download , 0 => all parts")
	cmd.Flags().StringVarP(&targetDriver, "target-sproxyd-driver", "", "", "target sproxyd driver [bpchord|bparc]")
	cmd.Flags().StringVarP(&targetUrl, "target-sproxyd-url", "t", "", "target sproxyd endpoint URL http://xx.xx.xx.xx:81/proxy,http:// ...")
	cmd.Flags().StringVarP(&targetEnv, "target-sproxyd-env", "", "", "target sproxyd environment [prod|osa]")

}

func init() {
	rootCmd.AddCommand(restoreCmd)
	initResFlags(restoreCmd)

}

func Restore_bucket(cmd *cobra.Command, args []string) {

	var (
		nextMarker string
		err        error
	)
	start := time.Now()
	mosesbc.MaxPage = maxPage
	mosesbc.MaxCon = maxCon
	mosesbc.PartNumber = partNumber
	mosesbc.Replace = replace
	gLog.Info.Printf("Restore bucket - MaxPage %d - Replace %v", mosesbc.MaxPage, mosesbc.Replace)

	if err = mosesbc.SetTargetSproxyd("restore", targetUrl, targetDriver, targetEnv); err != nil {
		gLog.Error.Printf("%v", err)
		return
	}

	if len(srcBucket) == 0 {
		gLog.Warning.Printf("%s", missingSrcBucket)
		return
	}
	if len(tgtBucket) == 0 {
		gLog.Warning.Printf("%s", missingTgtBucket)
		return
	}
	if len(prefix) > 0 {
		if len(inFile) > 0 {
			gLog.Warning.Println("--prefix  and --input-file are incompatible ; --input-file is ignored")
		}
		if len(srcBucket) > 0 {
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
		if err, suf := mosesbc.GetBucketSuffix(tgtBucket, prefix); err != nil {
			gLog.Error.Printf("%v", err)
			return
		} else {
			if len(suf) > 0 {
				tgtBucket += "-" + suf
				gLog.Warning.Printf("A suffix %s is appended to the target Bucket %s", suf, tgtBucket)
			}
		}
	}
	// source and target buckets must have the same suffix
	if err := mosesbc.CheckBucketName(srcBucket, tgtBucket); err != nil {
		gLog.Warning.Printf("%v", err)
		return
	}
	//
	if len(inFile) > 0 {
		if listpn, err = utils.Scanner(inFile); err != nil {
			gLog.Error.Printf("Error %v  scanning %s ", err, inFile)
			return
		}
	}

	maxPartSize = maxPartSize*1024*1024  // convert into bytes
	if maxPartSize < MinPartSize {
		gLog.Warning.Printf("max part size %d < min part size %d", maxPartSize,MinPartSize)
		maxPartSize = MinPartSize
		gLog.Warning.Printf("min part size %d will be used for max part size",maxPartSize)
	}

	mosesbc.MaxPartSize = maxPartSize
	srcS3 = mosesbc.CreateS3Session("restore", "source")
	//  create the output directory if it does not exist
	utils.MakeDir(outDir)
	//   bucket for indexing
	tgtS3 = mosesbc.CreateS3Session("restore", "target")

	if nextMarker, err = restore_bucket(); err != nil {
		gLog.Error.Printf("error %v - Next marker %s", err, nextMarker)
	} else {
		gLog.Info.Printf("Next Marker %s", nextMarker)
	}
	gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))
}

/*
	List  documents to be restored from the backup bucket or from an input file
    Restore every document of the returned list if it was backed up

*/
func restore_bucket() (string, error) {
	var (
		nextmarker, token     string
		N                     int
		tdocs, tpages, tsizes int64
		terrors               int
		re, si                sync.Mutex
	)
	// mosesbc.SetSourceSproxyd("restore",srcUrl,driver)

	req := datatype.ListObjV2Request{
		Service:           srcS3,
		Bucket:            srcBucket,
		Prefix:            prefix,
		MaxKey:            int64(maxKey),
		Marker:            marker,
		Delimiter:         delimiter,
		Continuationtoken: token,
	}
	start0 := time.Now()
	for {
		var (
			result   *s3.ListObjectsV2Output
			err      error
			ndocs    int = 0
			npages   int = 0
			docsizes int = 0
			nerrors  int = 0
		)
		N++ // number of loop

		if len(inFile) > 0 {
			result, err = ListPn(listpn, int(maxKey)) //  restore documents listed in --input-file
		} else {
			result, err = api.ListObjectV2(req) // restore  document listed in backup bucket
		}
		if err == nil {
			gLog.Info.Printf("Backup bucket %s - target metadata bucket %s - number of documents: %d", srcBucket, tgtBucket, len(result.Contents))
			if l := len(result.Contents); l > 0 {
				var wg1 sync.WaitGroup
				start := time.Now()
				for _, v := range result.Contents {
					if *v.Key != nextmarker {
						ndocs += 1
						svc := req.Service
						request := datatype.GetObjRequest{
							Service: svc,
							Bucket:  req.Bucket,
							Key:     *v.Key,
						}
						wg1.Add(1)
						go func(request datatype.GetObjRequest, size int64, replace bool) {
							var (
								pages, sizes, errs int
							)
							gLog.Info.Printf("Restoring document: %s from backup bucket %s - Size %d - maxPartSize %d", request.Key, request.Bucket,size,MaxPartSize)
							defer wg1.Done()
							if size <= maxPartSize {
								pages, sizes, errs = restore_pn(request, replace)
							} else {
								pages, sizes, errs = restore_multipart_pn(request, replace)
							}
							if errs > 0 {
								re.Lock()
								nerrors += errs
								re.Unlock()
							}
							si.Lock()
							npages += pages
							docsizes += sizes
							si.Unlock()

						}(request, *v.Size, replace)
					}
				}
				wg1.Wait()
				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					gLog.Warning.Printf("Truncated %v - Next marker: %s ", *result.IsTruncated, nextmarker)
				}
				ndocs = ndocs - nerrors
				gLog.Info.Printf("Number of restored documents: %d  - Number of pages: %d  - Documents size: %d - Number of errors: %d -  Elapsed time: %v", ndocs, npages, docsizes, nerrors, time.Since(start))
				tdocs += int64(ndocs)
				tpages += int64(npages)
				tsizes += int64(docsizes)
				terrors += nerrors
			}
		} else {
			if len(inFile) == 0 {
				gLog.Error.Printf("%v - Listing bucket %s", err, req.Bucket)
			} else {
				gLog.Error.Printf("%v - Reading file %s", err, inFile)
			}
			break
		}

		if *result.IsTruncated && (maxLoop == 0 || N < maxLoop) {
			req.Marker = nextmarker
		} else {
			gLog.Info.Printf("Total number of restored documents: %d  - total number of pages: %d  - Total document size: %d - Total number of errors: %d - Total elapsed time: %v", tdocs, tpages, tsizes, terrors, time.Since(start0))
			break
		}
	}
	return nextmarker, nil
}

/*

		Get  the backup object
		verify that  user metadata stored in the backup object is valid .Exit if not
        if metadata valid
			Read the backup object
        	Extract the pdf document if it exist and  restore it
        	check if page 0 exist the document
			Extract  pages ( page 0 inclusive if exists)
			restore pages
			reindexing the document if requested
*/

func restore_pn(request datatype.GetObjRequest, replace bool) (int, int, int) {

	var (
		result                    *s3.GetObjectOutput
		npages, docsizes, nerrors int = 0, 0, 0
		usermd                    string
		document                  *documentpb.Document
		nerr, status              int
		start2                    = time.Now()
	)

	if result, err = api.GetObject(request); err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				gLog.Warning.Printf("Error: [%v]  Error: [%v]", s3.ErrCodeNoSuchKey, aerr.Error())
			default:
				gLog.Error.Printf("Error: %v", aerr.Error())
				nerrors += 1
			}
		} else {
			gLog.Error.Printf("Error:%v", err.Error())
			nerrors += 1
		}
	} else {
		defer result.Body.Close()
		if usermd, err = utils.GetUserMeta(result.Metadata); err == nil {
			userm := UserMd{}
			json.Unmarshal([]byte(usermd), &userm)
		} else {
			gLog.Error.Printf("Error %v - The user metadata %s is invalid", err, result.Metadata)
			return 0, 0, 1
		}
		gLog.Info.Printf("Get Object key %s - Elapsed time %v ", request.Key, time.Since(start2))

		/*
			retrieve the backup document
		*/

		start3 := time.Now()
		if body, err := utils.ReadObjectv(result.Body, CHUNKSIZE); err == nil {
			defer result.Body.Close()
			document, err = mosesbc.GetDocument(body.Bytes())
			pd := document.Pdf
			if len(pd.Pdf) > 0 {

				/*   restore the pdf document first   - Check the number of errors returned by WriteDocPdf  */

				if nerr, status = mosesbc.WriteDocPdf(pd, replace); nerr == 0 {
					if status == 200 {
						gLog.Info.Printf("Document pdf %s  has been restored - Size %d", pd.PdfId, pd.Size)
					} else {
						gLog.Info.Printf("Document pdf %s  is not restored - Status %d", pd.PdfId, status)
					}
				} else {
					gLog.Info.Printf("Document pdf %s is not restored - Check the error returned by  WriteDocPdf ", pd.PdfId)
				}
			}
			gLog.Info.Printf("Document id %s is retrieved - Number of pages %d - Document size %d - Elapsed time %v ", document.DocId, document.NumberOfPages, document.Size, time.Since(start3))
			/*
					restore every pages of a document
				    if the number of pages >  maxPage -> PutBlob1
				    else -> PutBig1
			*/
			start4 := time.Now()
			nerr += mosesbc.RestoreBlobs(document)
			npages = (int)(document.NumberOfPages)
			docsizes = int(document.Size)

			/*
				Check the number of returned errors
				if the number = 0  ->  index the document
			*/

			if nerr > 0 {
				gLog.Info.Printf("Document id %s is not fully restored  because of %d errors - Number of pages %d - Document size %d - Elapsed time %v ", document.DocId, nerr, document.NumberOfPages, document.Size, time.Since(start4))
				nerrors = nerr
			} else {
				gLog.Info.Printf("Document id %s is fully restored - Number of pages %d - Document size %d - Elapsed time %v ", document.DocId, document.NumberOfPages, document.Size, time.Since(start4))
				/* start  indexing */
				start5 := time.Now()
				if _, err = mosesbc.IndexDocument(document, tgtBucket, tgtS3); err != nil {
					gLog.Error.Printf("Error %v while indexing the  document id %s into  bucket %s", err, document.DocId, tgtBucket)
					nerrors = 1
				} else {
					gLog.Info.Printf("Document id %s is now indexed in the bucket %s - Elapsed time %v", document.DocId, tgtBucket, time.Since(start5))
				}
			}
		} else {
			gLog.Error.Printf("Error %v when retrieving the document %s", err, request.Key)
			nerrors = 1
		}
	}
	return npages, docsizes, nerrors
}

func restore_multipart_pn(request datatype.GetObjRequest, replace bool) (int, int, int) {

	var (
		// result                    *s3.GetObjectOutput
		npages, docsizes, nerrors int = 0, 0, 0
		//usermd                    string
		document     *documentpb.Document
		nerr, status int
		start2       = time.Now()
	)

	req := datatype.GetMultipartObjRequest{
		Service:        request.Service,
		Bucket:         request.Bucket,
		Key:            request.Key,
		PartNumber:     partNumber,
		PartSize:       maxPartSize,
		Concurrency:    maxCon,
	}
	gLog.Info.Printf("Get Object key %s - Elapsed time %v ", request.Key, time.Since(start2))

	start3 := time.Now()
	_,buff, err := api.GetMultipartToBuffer(req)


	if err == nil {
		// defer result.Body.Close()
		document, err = mosesbc.GetDocument(buff.Bytes())
		// write PDF ifit exists
		pd := document.Pdf
		if len(pd.Pdf) > 0 {
			/*   restore the pdf document first   - Check the number of errors returned by WriteDocPdf  */
			if nerr, status = mosesbc.WriteDocPdf(pd, replace); nerr == 0 {
				if status == 200 {
					gLog.Info.Printf("Document pdf %s  has been restored - Size %d", pd.PdfId, pd.Size)
				} else {
					gLog.Info.Printf("Document pdf %s  is not restored - Status %d", pd.PdfId, status)
				}
			} else {
				gLog.Info.Printf("Document pdf %s is not restored - Check the error returned by  WriteDocPdf ", pd.PdfId)
			}
		}
		gLog.Info.Printf("Document id %s is retrieved - Number of pages %d - Document size %d - Elapsed time %v ", document.DocId, document.NumberOfPages, document.Size, time.Since(start3))
		/*
				restore every pages of a document
			    if the number of pages >  maxPage -> PutBlob1
			    else -> PutBig1
		*/
		start4 := time.Now()
		nerr += mosesbc.RestoreBlobs(document)
		npages = (int)(document.NumberOfPages)
		docsizes = int(document.Size)

		/*
			Check the number of returned errors
			if the number = 0  ->  index the document
		*/

		if nerr > 0 {
			gLog.Info.Printf("Document id %s is not fully restored  because of %d errors - Number of pages %d - Document size %d - Elapsed time %v ", document.DocId, nerr, document.NumberOfPages, document.Size, time.Since(start4))
			nerrors = nerr
		} else {
			gLog.Info.Printf("Document id %s is fully restored - Number of pages %d - Document size %d - Elapsed time %v ", document.DocId, document.NumberOfPages, document.Size, time.Since(start4))
			/* start  indexing */
			start5 := time.Now()
			if _, err = mosesbc.IndexDocument(document, tgtBucket, tgtS3); err != nil {
				gLog.Error.Printf("Error %v while indexing the  document id %s into  bucket %s", err, document.DocId, tgtBucket)
				nerrors = 1
			} else {
				gLog.Info.Printf("Document id %s is now indexed in the bucket %s - Elapsed time %v", document.DocId, tgtBucket, time.Since(start5))
			}
		}
	} else {
		gLog.Error.Printf("Error %v when retrieving the document %s", err, request.Key)
		nerrors = 1
	}
	return npages, docsizes, nerrors

}
/*
func readS3(service *s3.S3, bucket string, size int64, key string) (interface{}, error) {

	if size > MaxPartSize {
		return mosesbc.ReadMultipartS3(service, bucket, key)
	} else {
		return mosesbc.ReadS3(service, bucket, key)
	}

}
 */
