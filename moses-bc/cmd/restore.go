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
	partNumber          int64
	maxCon              int
	restoreCmd          = &cobra.Command{
		Use:   "_restore_",
		Short: "Command to restore Moses's assets to sproxyd or S3 storage",
		Long: `Command to restore Moses's assets to sproxyd or S3 storage
	
     Usage:

          moses-bc --help or -h  to list all the commands
          moses-bc  <command> -h or --help to list all the arguments related to a specific <command>
  
        Config file:

      	The Default config file is located $HOME/.clone/config.yaml. Use the --config or -c to change the default 
           moses-bc -c  <full path of another config.file>  _backup_  .....

        Example of a Bucket name:

           - Full name :  meta-moses-prod-pn-xx   ( xx = 00..05)
                Full name is  used for full restore or migrate ( --toS3=true) 
           
           - Partial name:  meta-moses-prod-pn    ( without suffix)
                Partial name is used when a --prefix , --input-file  or --input-bucket  is specified 
                Partial is used for incremental restore

        The argument --index-bucket is required only when you want to re-index the restored documents. 
        
        By default the restored document will be re-indexed, you must specify  --re-index=false to skip re-indexation
	    
        By default exiting  moses documents in sproxyd storage will not be replaced unless you specify  --replace=true 
    
    Examples
    
    Restore documents whose document ids are prefixed with  ES/123 and which were backed up to the bucket name identified by the --source-bucket argument.
    The bucket name's suffix is not required. The argument --max-loop  0 is to restore all documents prefixed by --prefix argument.

	  moses-bc _restore_ --prefix ES/123 --source-bucket meta-moses-bkp-pn [ --index-bucket meta-moses-prod-pn ] --max-loop 0 [ --re-index=false ]

    Restore all the documents which were backed up to  meta-moses-bkp-pn-02. Replace the restored documents if they already existed

	  moses-bc _restore_ --source-bucket meta-moses-bkp-pn-02 --index-bucket meta-moses-prod-pn-02 --max-loop 0  --replace=true
	
	Restore all the documents which were backed up to  meta-moses-bkp-pn-xx in 2021. Replace the restored documents if they already existed

	  moses-bc _restore_ --source-bucket meta-moses-bkp-pn --input-bucket <history backup bucket> --prefix 2021\
      --index-bucket meta-moses-prod-pn --max-loop 0  --replace=true
	
    Restore all the documents which were backed up to  meta-moses-bkp-pn-xx in January 2021. Replace the restored documents if they already existed
    
	  moses-bc _restore_ --source-bucket meta-moses-bkp-pn --input-bucket <history backup bucket> --prefix 2021/01 \ 
      --index-bucket meta-moses-prod-pn --max-loop 0  --replace=true
	
    Migrate to S3  all moses documents and directories  which were backed up to  meta-moses-bkp-pn-02. 
    If you want to preserve and reuse the current Moses directory buckets ( as for instance meta-moses-prod-pn-02), you must specify --re-index=false
    if you want to re-index the S3 moses with other buckets, you must specify  --index-bucket <full bucket name>  and re-index=true  

	  moses-bc _restore_ --toS3=true --source-bucket meta-moses-bkp-pn-02 --target-bucket meta-moses-prod-s3-pn-02 [--index-bucket meta-moses-prod-pn-02] --max-loop 0  [--re-index=false]

                 `,
		Run:    RestorePns,
		Hidden: true,
	}
	replace        bool
	toS3, byBucket bool
	indBucket      string
	tgtSproxyd string
)

func initResFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&srcBucket, "source-bucket", "", "", uSrcBucket)
	cmd.Flags().StringVarP(&tgtBucket, "target-bucket", "", "", uTgtBucket + ". It is required when --toS3 is specified")
	cmd.Flags().StringVarP(&indBucket, "index-bucket", "", "", uIndBucket + ". It is requited when --re-index=true")
	cmd.Flags().StringVarP(&iBucket, "input-bucket", "", "", uInputBucket)
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", uPrefix)
	cmd.Flags().Int64VarP(&maxKey, "max-key", "m", 21, uMaxkey)
	cmd.Flags().IntVarP(&maxPage, "max-page", "", 50, uMaxPage)
	cmd.Flags().IntVarP(&maxLoop, "max-loop", "", 1, uMaxLoop)
	cmd.Flags().StringVarP(&marker, "marker", "M", "", uMaker)
	// cmd.Flags().StringVarP(&delimiter, "delimiter", "d", "", "key delimiter (not used) ")
	cmd.Flags().StringVarP(&pn, "key", "k", "", "process only this given key")
	cmd.Flags().StringVarP(&versionId, "versionId", "", "", "the version id of the S3 object to be processed - default the last version will be processed")
	cmd.Flags().StringVarP(&inFile, "input-file", "i", "", uInputFile)
	cmd.Flags().BoolVarP(&replace, "replace", "r", false, uReplace)
	cmd.Flags().Int64VarP(&maxPartSize, "max-part-size", "", 64, uMaxPartSize)
	cmd.Flags().Int64VarP(&partNumber, "part-number", "", 0, "Part number")
	cmd.Flags().IntVarP(&maxCon, "max-con", "", 5, "maximum concurrent parts download , 0 => all parts for a given document will be concurrently processed")
	cmd.Flags().StringVarP(&targetDriver, "target-sproxyd-driver", "", "", uTgtDriver)
	cmd.Flags().StringVarP(&targetUrl, "target-sproxyd-url", "t", "", uTgtUrl)
	cmd.Flags().StringVarP(&targetEnv, "target-sproxyd-env", "", "", uTgtEnv)
	cmd.Flags().BoolVarP(&toS3, "toS3", "", false, "migrate moses to S3 straight from the backup copy")
	cmd.Flags().BoolVarP(&reIndex, "re-index", "", true, uReIndex)
	cmd.Flags().DurationVarP(&ctimeout, "ctimeout", "", 10, uCtimeout)
	cmd.Flags().BoolVarP(&check, "check", "", true, uDryRun)
}

func init() {
	rootCmd.AddCommand(restoreCmd)
	initResFlags(restoreCmd)
}

func RestorePns(cmd *cobra.Command, args []string) {

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

	// The source bucket  is compulsory
	if len(srcBucket) == 0 {
		gLog.Warning.Printf("%s", missingSrcBucket)
		return
	}

	if len(iBucket) == 0 && len(inFile) == 0 {
		byBucket = true
	}
	// always set target sproxyd  if not  a migration to S3
	if !toS3 {
		if err = mosesbc.SetTargetSproxyd("restore", targetUrl, targetDriver, targetEnv); err != nil {
			gLog.Error.Printf("%v", err)
			return
		}
	}

	/*
		if by bucket, set bucket name according to the prefix
	*/
	if byBucket {
		if len(prefix) > 0 {
			if err, suf := mosesbc.GetBucketSuffix(srcBucket, prefix); err != nil {
				gLog.Error.Printf("%v", err)
				return
			} else {
				if len(suf) > 0 {
					srcBucket += "-" + suf
					gLog.Warning.Printf("A suffix %s is appended to the source Bucket %s", suf, srcBucket)
				}
			}
		} else {
			/*
				source bucket must contain a suffix
			*/
			if !mosesbc.HasSuffix(srcBucket) {
				gLog.Error.Printf("%s does not have a suffix 00..05", srcBucket)
			}
		}

		if reIndex {
			if len(indBucket) == 0 {
				gLog.Warning.Printf("%s", missingIndBucket)
				return
			} else
			if len(prefix) > 0 {
				if err, suf := mosesbc.GetBucketSuffix(indBucket, prefix); err != nil {
					gLog.Error.Printf("%v", err)
					return
				} else {
					if len(suf) > 0 {
						indBucket += "-" + suf
						gLog.Warning.Printf("A suffix %s is appended to the source Bucket %s", suf, indBucket)
					}
				}
			} else {
				if !mosesbc.HasSuffix(indBucket) {
					gLog.Error.Printf("%s does not have a suffix 00..05 ", indBucket)
				}
			}
		}

		if toS3 {
			if len(tgtBucket) == 0 {
				gLog.Warning.Printf("%s", missingTgtBucket)
				return
			} else {
				if len(prefix) > 0 {
					if err, suf := mosesbc.GetBucketSuffix(tgtBucket, prefix); err != nil {
						gLog.Error.Printf("%v", err)
						return
					} else {
						if len(suf) > 0 {
							tgtBucket += "-" + suf
							gLog.Warning.Printf("A suffix %s is appended to the source Bucket %s", suf, tgtBucket)
						}
					}
				}
			}
		}

	} else {
		/*
			--input-file or --input-bucket
		*/
		if len(iBucket) > 0 && len(inFile) > 0 {
			gLog.Error.Printf("arguments --input-file and --input-bucket are mutually exclusive")
			return
		}
		if len(iBucket) > 0 {
			if logS3 = mosesbc.CreateS3Session("restore", "logger"); tgtS3 == nil {
				gLog.Error.Printf("Missing s3.restore.logger ... in the config.yaml file")
				return
			}
		}
		// if --input-file then --prefix is ignored
		if len(inFile) > 0 {
			gLog.Warning.Printf("--prefix  is ignored")
			prefix = ""
			if listpn, err = utils.Scanner(inFile); err != nil {
				gLog.Error.Printf("Error %v  scanning %s ", err, inFile)
				return
			}
		}
	}

	// source and target buckets must have the same suffix
	if reIndex {
		if err := mosesbc.CheckBucketName(srcBucket, indBucket); err != nil {
			gLog.Warning.Printf("%v", err)
			return
		}
	}

	//  --index-bucket and --target-bucket must be different
	if tgtBucket == indBucket {
		gLog.Error.Printf("target bucket %s  and index bucket %s are the same", tgtBucket, indBucket)
		return
	}

	maxPartSize = maxPartSize * 1024 * 1024 // convert into bytes
	if maxPartSize < MinPartSize {
		gLog.Warning.Printf("max part size %d < min part size %d", maxPartSize, MinPartSize)
		maxPartSize = MinPartSize
		gLog.Warning.Printf("min part size %d will be used for max part size", maxPartSize)
	}

	mosesbc.MaxPartSize = maxPartSize
	if srcS3 = mosesbc.CreateS3Session("restore", "source"); srcS3 == nil {
		gLog.Error.Printf("Missing s3.restore.source... in the config fle")
		return
	}

	//  create the output directory if it does not exist
	// utils.MakeDir(outDir)

	if tgtS3 = mosesbc.CreateS3Session("restore", "target"); tgtS3 == nil {
		gLog.Error.Printf("Missing s3.restore.target... in the config fle")
		return
	}

	mosesbc.Profiling(profiling)

	if nextMarker, err = restorePns(); err != nil {
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
func restorePns() (string, error) {
	var (
		nextmarker, token     string
		N                     int
		tdocs, tpages, tsizes int64
		terrors               int
		re, si                sync.Mutex
		req                   datatype.ListObjV2Request
	)
	// mosesbc.SetSourceSproxyd("restore",srcUrl,driver)

	req = datatype.ListObjV2Request{
		Service: srcS3,
		Bucket:  srcBucket,
		Prefix:  prefix,
		MaxKey:  int64(maxKey),
		Marker:  marker,
		// Delimiter:         delimiter,
		Continuationtoken: token,
	}
	if len(iBucket) > 0 {
		req = datatype.ListObjV2Request{
			Service:           logS3,
			Bucket:            iBucket,
			Prefix:            prefix,
			MaxKey:            int64(maxKey),
			Marker:            marker,
			Continuationtoken: token,
		}
	}

	start0 := time.Now()
	for {
		var (
			result     *s3.ListObjectsV2Output
			err        error
			ndocs      int = 0
			npages     int = 0
			docsizes   int = 0
			nerrors    int = 0
			// tgtSproxyd string
		)
		N++ // number of loop
		if len(inFile) > 0 {
			result, err = ListPn(listpn, int(maxKey)) //  listpn returns the list of documents to be restored
		} else {
			result, err = api.ListObjectWithContextV2(ctimeout, req)
		}
		if err == nil {
			if !toS3 {
				tgtSproxyd = targetUrl + "/" + targetDriver + "/" + targetEnv
				gLog.Info.Printf("Restoring from backup bucket %s to target sproxyd %s - indexing bucket %s - number of documents: %d", srcBucket, tgtSproxyd, indBucket, len(result.Contents))
			} else {
				gLog.Info.Printf("Restoring from backup bucket %s to S3 bucket %s - number of documents: %d", srcBucket, tgtBucket, len(result.Contents))
			}

			if l := len(result.Contents); l > 0 {
				var wg1 sync.WaitGroup
				start := time.Now()
				for _, v := range result.Contents {
					if *v.Key != nextmarker {
						key := *v.Key
						ndocs += 1
						service := srcS3 // alwyas S3  backup
						buck := req.Bucket
						if len(iBucket) > 0 {

							/*
									check the content of the input bucket
								    key  should be in the form
									yyyymmdd/cc/pn/kc
							*/

							if err, key = mosesbc.ParseLoggerKey(*v.Key); err != nil {
								gLog.Error.Printf("Error %v in bucket %s", err, iBucket)
								continue
							}
							buck = mosesbc.SetBucketName(key, req.Bucket)
						} else {
							if len(inFile) > 0 {
								key = *v.Key
								buck = mosesbc.SetBucketName(key, req.Bucket)
							}
						}
						request := datatype.GetObjRequest{
							Service: service,
							Bucket:  buck,
							Key:     key,
						}
						wg1.Add(1)
						go func(request datatype.GetObjRequest, size int64, replace bool) {
							var (
								pages, sizes, errs int
							)
							gLog.Info.Printf("Restoring document: %s from backup bucket %s - Size %d - maxPartSize %d", request.Key, request.Bucket, size, maxPartSize)
							defer wg1.Done()

							if size <= maxPartSize {
								pages, sizes, errs = restorePn(request, replace)
							} else {
								pages, sizes, errs = restoreMultipartPn(request, replace)
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
					if len(inFile) == 0 {
						token = *result.NextContinuationToken
					}
					gLog.Warning.Printf("Truncated %v - Next marker: %s ", *result.IsTruncated, nextmarker)
				}
				ndocs = ndocs - nerrors
				gLog.Info.Printf("Number of restored documents: %d - Number of pages: %d - Documents size: %d - Number of errors: %d - Elapsed time: %v", ndocs, npages, docsizes, nerrors, time.Since(start))
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
			gLog.Info.Printf("Total number of restored documents: %d - total number of pages: %d - Total document size: %d - Total number of errors: %d - Total elapsed time: %v", tdocs, tpages, tsizes, terrors, time.Since(start0))
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

func restorePn(request datatype.GetObjRequest, replace bool) (int, int, int) {

	var (
		result                    *s3.GetObjectOutput
		npages, docsizes, nerrors int = 0, 0, 0
		usermd                    string
		document                  *documentpb.Document
		nerr, status              int
		start2                    = time.Now()
	)
	//  get the backup document
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
		//  extract  the backup user meta data
		if usermd, err = utils.GetUserMeta(result.Metadata); err == nil {
			userm := UserMd{}
			json.Unmarshal([]byte(usermd), &userm)
		} else {
			gLog.Error.Printf("Error %v - The user metadata %s is invalid", err, result.Metadata)
			return 0, 0, 1
		}
		gLog.Info.Printf("Get Object key %s - Elapsed time %v ", request.Key, time.Since(start2))

		/*
			download the backup object
		*/

		start3 := time.Now()
		if body, err := utils.ReadObjectv(result.Body, CHUNKSIZE); err == nil {
			defer result.Body.Close()
			// extract the document
			document, err = mosesbc.GetDocument(body.Bytes())
			pd := document.Pdf
			//  check if the document has a pdf
			if len(pd.Pdf) > 0 {
				if !toS3 {
					/*   restore the pdf document to sproxyd first  */
					if !check {
						nerr, status = mosesbc.WriteDocPdf(pd, replace)
						if nerr == 0 {
							if status == 200 {
								gLog.Info.Printf("Document pdf %s has been restored - Size %d", pd.PdfId, pd.Size)
							} else {
								gLog.Info.Printf("Document pdf %s is not restored - Status %d", pd.PdfId, status)
							}
						} else {
							gLog.Info.Printf("Document pdf %s is not restored - Check the error returned by  WriteDocPdf ", pd.PdfId)
						}
					} else {
						gLog.Info.Printf("Dry Run: Writing pdf document %s to sproxyd %s ", pd.PdfId, tgtSproxyd)
					}
				} else {
					buck1:= mosesbc.SetBucketName1(!byBucket,pd.PdfId,tgtBucket)
					if !check {
						if _, err = mosesbc.WriteS3Pdf(tgtS3, buck1, pd, ctimeout); err != nil {
							gLog.Info.Printf("Document pdf %s is not restored - error %v ", pd.PdfId, err)
							nerr += 1
						} else {
							gLog.Info.Printf("Document pdf %s has been restored - Size %d", pd.PdfId, pd.Size)
						}
					} else {
						gLog.Info.Printf("Dry Run: Writing pdf document %s to bucket %s - S3 endpoint %s", pd.PdfId, buck1, tgtS3.Endpoint)
					}
				}
			}
			gLog.Info.Printf("Document id %s is retrieved - Number of pages %d - Document size %d - Elapsed time %v ", document.DocId, document.NumberOfPages, document.Size, time.Since(start3))
			/*
					restore every pages of a document
				    if the number of pages >  maxPage -> PutBlob1
				    else -> PutBig1
			*/
			start4 := time.Now()
			if !toS3 {
				//  restore back to sproxyd storage
				if !check {
					nerr += mosesbc.RestoreBlobs(document, ctimeout)
				}  else {
					gLog.Info.Printf("Dry Run: Restore document %s to sproxyd %s",document.DocId,tgtSproxyd)
				}
			} else {
				// restore to migrate to S3 storage
				/*
				var buck1 string
				if !byBucket {
					buck1 = mosesbc.SetBucketName(document.DocId, tgtBucket)
				} else {
					buck1 = tgtBucket
				}
				 */
				buck1:= mosesbc.SetBucketName1(!byBucket,document.DocId,tgtBucket)
				if !check {
					nerr += mosesbc.Restores3Objects(tgtS3, buck1, document, ctimeout)
				}  else {
					gLog.Info.Printf("Dry Run: Restore document %s to bucket %s - S3 endpoint %s",document.DocId,buck1,tgtS3.Endpoint)
				}
			}
			npages = (int)(document.NumberOfPages)
			docsizes = int(document.Size)

			/*
				Check the number of returned errors
				if the number = 0  ->  index the document
			*/

			if nerr > 0 {
				gLog.Info.Printf("Document id %s is not fully restored due to %d errors - Number of pages %d - Document size %d - Elapsed time %v ", document.DocId, nerr, document.NumberOfPages, document.Size, time.Since(start4))
				nerrors = nerr
			} else {
				gLog.Info.Printf("Document id %s is fully restored - Number of pages %d - Document size %d - Elapsed time %v ", document.DocId, document.NumberOfPages, document.Size, time.Since(start4))
				/* start  indexing */
				start5 := time.Now()
				if reIndex {
					/*
					var buck1 string
					if !byBucket {
						buck1 = mosesbc.SetBucketName(document.DocId, tgtBucket)
					} else {
						buck1 = tgtBucket
					}

					 */
					buck1:= mosesbc.SetBucketName1(!byBucket,document.DocId,tgtBucket)
					if !check {
						if _, err = mosesbc.IndexDocument(document, buck1, tgtS3, ctimeout); err != nil {
							gLog.Error.Printf("Error %v while indexing the document id %s iwith the bucket %s", err, document.DocId, indBucket)
							nerrors += 1
						} else {
							gLog.Info.Printf("Document id %s is now indexed in the bucket %s - Elapsed time %v", document.DocId, indBucket, time.Since(start5))
						}
					} else {
						gLog.Info.Printf("Dry Run: Indexing document %s in the bucket %s - S3 endpoint %s", document.DocId, buck1, tgtS3.Endpoint)
					}
				}
			}
		} else {
			gLog.Error.Printf("Error %v when retrieving the document %s", err, request.Key)
			nerrors = 1
		}
	}
	return npages, docsizes, nerrors
}

func restoreMultipartPn(request datatype.GetObjRequest, replace bool) (int, int, int) {

	var (
		// result                    *s3.GetObjectOutput
		npages, docsizes, nerrors int = 0, 0, 0
		//usermd                    string
		document     *documentpb.Document
		nerr, status int
		start2       = time.Now()
	)
	gLog.Info.Printf("partNumber %d - PartSize %d - Max concurrency %d", partNumber, maxPartSize, maxCon)
	req := datatype.GetMultipartObjRequest{
		Service:     request.Service,
		Bucket:      request.Bucket,
		Key:         request.Key,
		PartNumber:  partNumber,
		PartSize:    maxPartSize,
		Concurrency: maxCon,
	}

	_, buff, err := api.GetMultipartToBuffer(req)
	gLog.Info.Printf("Get Object key %s - Elapsed time %v ", request.Key, time.Since(start2))

	if err == nil {
		start3 := time.Now()
		// defer result.Body.Close()
		document, err = mosesbc.GetDocument(buff.Bytes())
		// write PDF if  it exists
		pd := document.Pdf
		if !toS3 {
			if len(pd.Pdf) > 0 {
				/*   restore the pdf document first    */
				if !check {
					if nerr, status = mosesbc.WriteDocPdf(pd, replace); nerr == 0 {
						if status == 200 {
							gLog.Info.Printf("Document pdf %s has been restored - Size %d", pd.PdfId, pd.Size)
						} else {
							gLog.Info.Printf("Document pdf %s is not restored - Status %d", pd.PdfId, status)
						}
					} else {
						gLog.Info.Printf("Document pdf %s is not restored - Check the error returned by  WriteDocPdf ", pd.PdfId)
					}
				} else {
					gLog.Info.Printf("Dry Run: Writing pdf document %s to sproxyd %s", pd.PdfId, tgtSproxyd)
				}
			}
		} else {
			/*
			var buck1 string
			if !byBucket {
				buck1 = mosesbc.SetBucketName(pd.PdfId, tgtBucket)
			} else {
				buck1 = tgtBucket
			}
			 */

			buck1:= mosesbc.SetBucketName1(!byBucket,pd.PdfId,tgtBucket)
			if !check {
				if _, err = mosesbc.WriteS3Pdf(tgtS3, buck1, pd, ctimeout); err != nil {
					gLog.Info.Printf("Document pdf %s is not restored - error %v ", pd.PdfId, err)
					nerr += 1
				} else {
					gLog.Info.Printf("Document pdf %s has been restored - Size %d", pd.PdfId, pd.Size)
				}
			} else {
				gLog.Info.Printf("Dry Run: Writing pdf document %s to bucket %s - S3 endpoint %s", pd.PdfId, buck1, tgtS3.Endpoint)
			}
		}
		gLog.Info.Printf("Document id %s is retrieved - Number of pages %d - Document size %d - Elapsed time %v ", document.DocId, document.NumberOfPages, document.Size, time.Since(start3))

		/*
				restore every pages of a document
			    if the number of pages >  maxPage -> PutBlob1
			    else -> PutBig1
		*/

		start4 := time.Now()
		if !toS3 {
			if !check {
				nerr += mosesbc.RestoreBlobs(document, ctimeout)
			}  else {
				gLog.Info.Printf("Dry Run: Restore document %s to sproxyd %s",document.DocId,tgtSproxyd)
			}
		} else {
			/*
			var buck1 string
			if !byBucket {
				buck1 = mosesbc.SetBucketName(document.DocId, tgtBucket)
			} else {
				buck1 = tgtBucket
			}
			 */
			buck1:= mosesbc.SetBucketName1(!byBucket,document.DocId,tgtBucket)
			if !check {
				nerr += mosesbc.Restores3Objects(tgtS3, buck1, document, ctimeout)
			} else {
				gLog.Info.Printf("Dry Run: Restore document %s to bucket %s - S3 endpoint %s",document.DocId,buck1,tgtS3.Endpoint)
			}
		}
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
			/*
			var buck1 string
			if !byBucket {
				buck1 = mosesbc.SetBucketName(document.DocId, tgtBucket)
			} else {
				buck1 = tgtBucket
			}
			 */

			buck1:= mosesbc.SetBucketName1(!byBucket,document.DocId,tgtBucket)
			if !check {
				if _, err = mosesbc.IndexDocument(document, buck1, tgtS3, ctimeout); err != nil {
					gLog.Error.Printf("Error %v while indexing the document id %s with the bucket %s", err, document.DocId, tgtBucket)
					nerrors = 1
				} else {
					gLog.Info.Printf("Document id %s is now indexed in the bucket %s - Elapsed time %v", document.DocId, tgtBucket, time.Since(start5))
				}
			} else {
				gLog.Info.Printf("Dry Run: Indexing  document %s in the bucket %s - S3 endpoint %s", document.DocId, buck1, tgtS3.Endpoint)
			}
		}
	} else {
		gLog.Error.Printf("Error %v when retrieving the document %s", err, request.Key)
		nerrors = 1
	}
	return npages, docsizes, nerrors

}
