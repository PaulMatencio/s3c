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
	meta "github.com/paulmatencio/s3c/moses-bc/datatype"
	mosesbc "github.com/paulmatencio/s3c/moses-bc/lib"
	sproxyd "github.com/paulmatencio/s3c/sproxyd/lib"
	"github.com/paulmatencio/s3c/utils"
	"github.com/spf13/cobra"
	"strconv"
	"sync"
	"time"
)

var (
	cloneCmd = &cobra.Command{
		Use:   "_clone_",
		Short: "Command to clone MOSES objects to another Scality Ring",
		Long: ` 
        Command to clone MOSES objects to another Scality Ring
        Moses data are stored in Scality Ring native object storage accessed via sproxyd driver 
        Moses directories are stored in S3 buckets. Each bucket name has a suffix ( <bucket-name>-xx ; xx=00..05)
        
        Usage: 
        moses-bc --help or -h  to list all the commands
        moses-bc  <command> -h or --help to list all the arguments related to a specific <command>
  
        Config file: 
      	The Default config file is located $HOME/.clone/config.file 

        Example of full cloning of documents listed in a moses directory 

        Clone all the objects listed in the S3 --source-bucket meta-moses-prod-pn-01  to the S3 bucket meta-moses-osa-pn-01 
     	
        moses-bc -c $HOME/.clone/config.yaml _clone_ --source-bucket meta-moses-prod-pn-01 --target-bucket meta-moses-prod-bkup-pn-01
        **  suffix is requird and both source and target bucket must have the same suffix, for instance -01 ** 

        Example of cloning  of a specific --prefix 

        moses-bc -c $HOME/.clone/config.yaml _clone_ --source-bucket meta-moses-prod-pn --prefix  FR/ 
        --target-bucket meta-moses-osa-pn     ** bucket suffix is not required **
		
        Example of incremental cloning from YYY-MM-DDT10:00:00Z to YYY-MM-DDT012:00:00Z
        
        moses-bc -c $HOME/.clone/config.yaml _clone_ --input-bucket last-loaded-prod --prefix dd/mm/yy \
        --from-date YYY-MM-DDT00:00:00Z --to-date YYY-MM-DDT00:00:00Z \
        --target-bucket meta-moses-osa-pn    ** bucket suffix is not required  **

        Example of incremental cloning  from an input file containing the new publication numbers

        moses-bc -c $HOME/.clone/config.yaml _clone_ --input-file <file containing new publication number> --prefix dd/mm/yy \
        --target-bucket meta-moses-prod-bkup-pn    ** bucket suffix is not required **
         
		`,
		Hidden: true,
		Run:    ClonePns,
	}
	// s3Src, s3Tgt  datatype.CreateSession
	reIndex bool
)

func initCloFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&srcBucket, "source-bucket", "", "", uSrcBucket)
	cmd.Flags().StringVarP(&tgtBucket, "target-bucket", "", "", uTgtBucket + ". Required when --re-index=true")
	cmd.Flags().StringVarP(&prefix, "prefix", "p", "", uPrefix)
	cmd.Flags().Int64VarP(&maxKey, "max-key", "m", 21, uMaxkey)
	cmd.Flags().StringVarP(&marker, "marker", "M", "", uMaker)
	cmd.Flags().IntVarP(&maxPage, "max-page", "", 50, uMaxPage)
	cmd.Flags().IntVarP(&maxLoop, "max-loop", "", 1, uMaxLoop)
	cmd.Flags().BoolVarP(&replace, "replace", "r", false, uReplace)
	cmd.Flags().BoolVarP(&reIndex, "re-index", "", false, uReIndex)
	cmd.Flags().StringVarP(&inFile, "input-file", "i", "", uInputFile)
	cmd.Flags().StringVarP(&iBucket, "input-bucket", "", "", uInputBucket)
	cmd.Flags().StringVarP(&srcUrl, "source-sproxyd-url", "s", "", uSrcUrl)
	cmd.Flags().StringVarP(&driver, "source-sproxyd-driver", "", "", uSrcDriver)
	cmd.Flags().StringVarP(&targetDriver, "target-sproxyd-driver", "", "", uTgtDriver)
	cmd.Flags().StringVarP(&targetUrl, "target-sproxyd-url", "t", "", uTgtUrl)
	cmd.Flags().StringVarP(&env, "source-sproxyd-env", "", "", uSrcEnv)
	cmd.Flags().StringVarP(&targetEnv, "target-sproxyd-env", "", "", uTgtEnv)
	cmd.Flags().DurationVarP(&ctimeout, "ctimeout", "", 10, uCtimeout)
}

func init() {
	rootCmd.AddCommand(cloneCmd)
	initCloFlags(cloneCmd)
}

func ClonePns(cmd *cobra.Command, args []string) {

	var err error
	if err = mosesbc.SetSourceSproxyd("clone", srcUrl, driver, env); err != nil {
		gLog.Error.Printf("%v", err)
		return
	}
	if err = mosesbc.SetTargetSproxyd("clone", targetUrl, targetDriver, targetEnv); err != nil {
		gLog.Error.Printf("%v", err)
		return
	}
	gLog.Info.Printf("Source Env: %s - Source Driver: %s - Source Url: %s", sproxyd.Env, sproxyd.Driver, sproxyd.Url)
	gLog.Info.Printf("Target Env: %s - Target Driver: %s - Target Url: %s", sproxyd.TargetEnv, sproxyd.TargetDriver, sproxyd.TargetUrl)

	if len(srcBucket) == 0 {
		gLog.Error.Printf(missingSrcBucket)
		return
	}

	if len(inFile) > 0 && len(iBucket) > 0 {
		gLog.Error.Printf("--input-file  and --input-bucket are mutually exclusive", inFile, iBucket)
		return
	}

	if reIndex {
		if len(tgtBucket) == 0 {
			gLog.Warning.Printf("%s", missingTgtBucket)
			return
		}
	}

	if len(inFile) > 0 || len(iBucket) > 0 {

		if len(inFile) > 0 && len(iBucket) > 0 {
			gLog.Error.Printf("--input-file  and --input-bucket are mutually exclusive", inFile, iBucket)
			return
		}
		if len(prefix) > 0 {
			gLog.Warning.Printf("Prefix is ignored with --input-file or --input-bucket ")
			prefix = ""
		}
		/*
			Prepare to Scan the input file
		*/
		if len(inFile) > 0 {
			if listpn, err = utils.Scanner(inFile); err != nil {
				gLog.Error.Printf("Error %v  scanning --input-file  %s ", err, inFile)
				return
			}
		}
		incr = true
		// bucket must not  have a suffix
		if mosesbc.HasSuffix(srcBucket) {
			gLog.Error.Printf("Source bucket %s must not have a suffix", srcBucket)
			return
		}
		if mosesbc.HasSuffix(tgtBucket) {
			gLog.Error.Printf("target bucket %s must not have a suffix", tgtBucket)
			return
		}

	}
	//   Full backup
	if len(prefix) > 0 {
		/*
			get the suffix of the bucket and append it to the source bucket
		*/

		if err, suf := mosesbc.GetBucketSuffix(srcBucket, prefix); err != nil {
			gLog.Error.Printf("%v", err)
			return
		} else {
			if len(suf) > 0 {
				srcBucket += "-" + suf
				gLog.Warning.Printf("A suffix %s is appended to the source Bucket %s", suf, srcBucket)
			}
		}

		/*
			get the suffix of the bucket and append it to the target  bucket
		*/
		if reIndex {
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
	} else {
		//  For Full backup  buckets must have a suffix
		if !incr && !mosesbc.HasSuffix(srcBucket) {
			gLog.Error.Printf("Source bucket %s does not have a suffix. It should be  00..05 ", srcBucket)
			return
		}

		if reIndex && !incr && !mosesbc.HasSuffix(tgtBucket) {
			gLog.Error.Printf("Target bucket %s does not have a suffix. It should be  00..05 ", tgtBucket)
			return
		}

	}
	// Check the validity of source and target bucket names
	if reIndex {
		if err := mosesbc.CheckBucketName(srcBucket, tgtBucket); err != nil {
			gLog.Warning.Printf("%v", err)
			return
		}
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
	mosesbc.Profiling(profiling)

	reqm := datatype.Reqm{
		SrcS3:       srcS3,
		SrcBucket:   srcBucket,
		TgtS3:       tgtS3,
		TgtBucket:   tgtBucket,
		Incremental: incr,
	}
	start := time.Now()
	if nextMarker, err := clonePns(reqm); err != nil {
		gLog.Error.Printf("error %v - Next marker %s", err, nextMarker)
	} else {
		gLog.Info.Printf("Next Marker %s", nextMarker)
	}
	gLog.Info.Printf("Total Elapsed time: %v", time.Since(start))
}

func clonePns(reqm datatype.Reqm) (string, error) {

	var (
		nextmarker, token            string
		N                            int
		tdocs, tpages, tsizes, tdocr int64
		terrors                      int
		re, si                       sync.Mutex
		req, reql                    datatype.ListObjV2Request
		incr                         = reqm.Incremental
	)

	req = datatype.ListObjV2Request{
		Service:           srcS3,
		Bucket:            srcBucket,
		Prefix:            prefix,
		MaxKey:            int64(maxKey),
		Marker:            marker,
		Continuationtoken: token,
	}

	if len(iBucket) > 0 {
		reql = datatype.ListObjV2Request{
			Service:           srcS3,
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
			result       *s3.ListObjectsV2Output
			err          error
			ndocs, ndocr int   = 0, 0
			npages       int   = 0
			docsizes     int64 = 0
			nerrors      int   = 0
			wg1          sync.WaitGroup
		)
		N++ // number of loop
		if !incr {
			gLog.Info.Printf("Listing documents from  %s", reqm.SrcBucket)
			result, err = api.ListObjectV2(req)
		} else {
			if len(inFile) > 0 {
				gLog.Info.Printf("Listing documents from file %s", inFile)
				result, err = mosesbc.ListPn(listpn, int(maxKey))
			} else {
				gLog.Info.Printf("Listing documents from bucket  %s", iBucket)
				result, err = api.ListObjectV2(reql)
			}
		}

		// result contains the list of documents to clone
		if err == nil {
			if l := len(result.Contents); l > 0 {
				start := time.Now()
				var buck1 string
				gLog.Info.Printf("Total number of documents %d", l)
				for _, v := range result.Contents {
					if *v.Key != nextmarker {
						ndocr += 1
						svc1 := req.Service
						if incr {
							buck1 = mosesbc.SetBucketName(*v.Key, req.Bucket)
						} else {
							buck1 = req.Bucket
						}
						//  prepare the request to retrieve S3 meta data
						request := datatype.StatObjRequest{
							Service: svc1,
							Bucket:  buck1,
							Key:     *v.Key,
						}
						wg1.Add(1)
						go func(request datatype.StatObjRequest, replace bool) {
							defer wg1.Done()
							r := clonePn(request, replace)
							if r.Nerrors > 0 {
								re.Lock()
								nerrors += r.Nerrors
								re.Unlock()
							}
							si.Lock()
							npages += r.Npages
							docsizes += int64(r.Docsizes)
							ndocs += r.Ndocs
							si.Unlock()
						}(request, replace)
					}
				}
				wg1.Wait()
				if *result.IsTruncated {
					nextmarker = *result.Contents[l-1].Key
					token = *result.NextContinuationToken
					gLog.Warning.Printf("Truncated %v - Next marker: %s  - Nextcontinuation token: %s", *result.IsTruncated, nextmarker, token)
				}
				gLog.Info.Printf("Number of cloned documents: %d of %d - Number of pages: %d  - Documents size: %d - Number of errors: %d -  Elapsed time: %v", ndocs, ndocr, npages, docsizes, nerrors, time.Since(start))
				tdocs += int64(ndocs)
				tdocr += int64(ndocr)
				tpages += int64(npages)
				tsizes += docsizes
				terrors += nerrors
			}
		} else {
			gLog.Error.Printf("%v", err)
			break
		}

		if *result.IsTruncated && (maxLoop == 0 || N < maxLoop) {
			// req1.Marker = nextmarker
			req.Continuationtoken = token
		} else {
			gLog.Info.Printf("Total number of cloned documents: %d of %d - total number of pages: %d  - Total document size: %d - Total number of errors: %d - Total elapsed time: %v", tdocs, tdocr, tpages, tsizes, terrors, time.Since(start0))
			break
		}
	}
	return nextmarker, nil
}

/*
		Get the document metadata
	    retrieve its  total number  pages
	    if number of pages > 0  Clone the document
		if re-indexing then re-index the document in the target bucket

*/

func clonePn(request datatype.StatObjRequest, replace bool) datatype.Rm {
	var (
		rh = datatype.Rh{
			Key: request.Key,
		}
		err                        error
		usermd, pn                 string
		npages, ndocs, nerrors, np int
		docsizes                   int64
	)

	if rh.Result, rh.Err = api.StatObject(request); rh.Err == nil {
		// get S3 user metadata
		if usermd, err = utils.GetUserMeta(rh.Result.Metadata); err == nil {
			userm := meta.UserMd{}
			json.Unmarshal([]byte(usermd), &userm)
			pn = rh.Key
			if np, err = strconv.Atoi(userm.TotalPages); err == nil {
				start3 := time.Now()
				nerr, document := mosesbc.CloneBlob(pn, np, maxPage, replace)
				if nerr == 0 {
					npages = int(document.NumberOfPages)
					docsizes = document.Size
					ndocs = 1
					gLog.Info.Printf("Document id %s is cloned - Number of pages %d - Document size %d - Number of errors %d - Elapsed time %v ", document.DocId, document.NumberOfPages, document.Size, nerr, time.Since(start3))

					/*
						indexing the document if no cloning error
					*/

					if reIndex {
						start5 := time.Now()
						if _, err = mosesbc.IndexDocument(document, tgtBucket, tgtS3,ctimeout); err != nil {
							gLog.Error.Printf("Error %v while indexing the  document id %s into  bucket %s", err, document.DocId, tgtBucket)
							nerrors = 1
						} else {
							gLog.Info.Printf("Document id %s is now indexed in the bucket %s - Elapsed time %v", document.DocId, tgtBucket, time.Since(start5))
						}
					}
				} else {
					nerrors = nerr
					ndocs = 0
					gLog.Info.Printf("Document id %s is not fully cloned - Number of pages %d - Document size %d - Number of errors %d - Elapsed time %v ", document.DocId, document.NumberOfPages, document.Size, nerr, time.Since(start3))
				}
			}
		} else {
			gLog.Error.Printf("%v", err)
		}
	} else {
		gLog.Error.Printf("%v", rh.Err)
	}
	r := datatype.Rm{
		nerrors,
		ndocs,
		npages,
		int(docsizes),
	}
	return r
}
